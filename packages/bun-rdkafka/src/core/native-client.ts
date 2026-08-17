/**
 * core/native-client.ts — handle native + state machine (design §5.1).
 *
 * ```
 * INIT → CONNECTING → READY → DISCONNECTING → CLOSED
 * ```
 *
 * Responsibilities:
 *  - Wraps the `void* handle`: every method checks state before calling FFI
 *    (calling into a destroyed handle = process crash, so this guard is
 *    mandatory — NFR-3).
 *  - Manages reusable ArrayBuffers for the hot path, growing per
 *    `brk_last_required_size` and **retrying exactly once** on
 *    `BRK_ERR_BUFFER_TOO_SMALL`.
 *  - `FinalizationRegistry` as the last safety net when the user forgets
 *    `disconnect()`.
 *  - Exposes each `brk_*` function type-safely for the callback layer above.
 *
 * This layer has NO EventEmitter and no business logic — that belongs to
 * `src/callback/`.
 *
 * ## ⚠ The shim's two-queue constraint (confirmed with native, M1)
 *
 * The shim uses **two** separate queues:
 *  - `main_q`     → {@link NativeClient.pollEvents} (DR, LOG, STATS, ERROR, THROTTLE,
 *                   OAUTH_REFRESH, ADMIN_RESULT)
 *  - `consumer_q` → {@link NativeClient.consumeBatch}
 *
 * The **REBALANCE** and **OFFSET_COMMIT** events travel via `consumer_q`: they
 * only get stashed for `brk_events_poll` to return WHEN `brk_consume_batch` is
 * called. Therefore, for a consumer that has `subscribe()`d, the upper layer
 * MUST keep calling `consumeBatch()` continuously — even when the user has not
 * called `run()`/`consume()` or is paused — otherwise rebalances never arrive
 * and the consumer is kicked from the group. Messages returned in the meantime
 * must be buffered, never dropped.
 */

import { Buffer } from "node:buffer";
import { CString, type Pointer } from "bun:ffi";
import { loadNative, type BrkNative } from "../ffi/loader.ts";
import {
  BRK_ASSIGN,
  BRK_CLIENT_CONSUMER,
  NO_LEADER_EPOCH,
  BRK_CLIENT_PRODUCER,
  BRK_ERR_BUFFER_TOO_SMALL,
  BRK_ERR_INVALID_HANDLE,
  BRK_ERR_INVALID_STATE,
  type BrkAdminOp,
  type BrkAssignMode,
  type BrkClientType,
} from "../ffi/types.ts";
import { BufWriter } from "./binary.ts";
import {
  TopicNameTable,
  decodeEventFrames,
  decodeMessageBatchWithSize,
  decodeStringList,
  decodeTplBuffer,
  encodeProduceBatch,
  encodeStringList,
  encodeTpl,
  type BrkEvent,
  type DecodedMessage,
  type ProduceRecord,
  type TopicPartitionEntry,
  type TopicPartitionInput,
} from "./batch-decoder.ts";
import { DEFAULT_JS_OPTIONS, type JsOptions } from "./config.ts";
import { LibrdKafkaError, throwOnError } from "./errors.ts";

/* ========================================================================== */
/* String/pointer utilities                                                    */
/* ========================================================================== */

const ENCODER = /* @__PURE__ */ new TextEncoder();
const DECODER = /* @__PURE__ */ new TextDecoder();

/** UTF-8 + NUL for `const char*` parameters. */
export function cstringBuffer(value: string): Uint8Array {
  const bytes = ENCODER.encode(value);
  const out = new Uint8Array(bytes.length + 1);
  out.set(bytes);
  return out;
}

/** Reads a NUL-terminated string C wrote into a JS buffer. */
function readCString(buf: Uint8Array): string {
  const end = buf.indexOf(0);
  return DECODER.decode(end < 0 ? buf : buf.subarray(0, end));
}

/* ========================================================================== */
/* State machine                                                               */
/* ========================================================================== */

export type ClientState = "INIT" | "CONNECTING" | "READY" | "DISCONNECTING" | "CLOSED";

/** Reusable buffers for hot/cold paths; each slot grows independently. */
type BufferSlot = "consume" | "event" | "scratch" | "name";

/** Pointer box for the FinalizationRegistry (holds NO back-reference to the client). */
interface HandleBox {
  handle: Pointer | null;
  native: BrkNative;
  destroyed: boolean;
  label: string;
  onLeak: ((label: string) => void) | undefined;
}

const leakRegistry = new FinalizationRegistry<HandleBox>((box) => {
  if (box.destroyed || box.handle === null) return;
  box.destroyed = true;
  const handle = box.handle;
  box.handle = null;
  box.native.brk_client_destroy(handle);
  const warn =
    box.onLeak ??
    ((label: string) =>
      console.warn(
        `bun-rdkafka: ${label} was GC'd without disconnect() — the handle was ` +
          `destroyed by the safety net. Call disconnect() for a clean shutdown.`,
      ));
  warn(box.label);
});

/* ========================================================================== */
/* Options                                                                     */
/* ========================================================================== */

export interface NativeClientOptions {
  type: BrkClientType;
  /** Stringified librdkafka properties (from `ConfigBuilder.build()`). */
  properties: readonly (readonly [string, string])[];
  /** JS-layer options (buffer sizes…). */
  js?: Partial<JsOptions>;
  /** Injects the native function table — test-only; defaults to lazy `loadNative()`. */
  native?: BrkNative;
  /** Label used in error/warning messages. */
  label?: string;
  /** Replaces `console.warn` when the FinalizationRegistry has to clean up. */
  onLeak?: (label: string) => void;
}

/** Result of `brk_produce_batch`. */
export interface ProduceBatchResult {
  /** Number of records the shim processed. */
  accepted: number;
  /** Per-record `rd_kafka_resp_err_t` (length `accepted`); 0 = enqueued. */
  errors: Int16Array;
}

export interface WatermarkOffsets {
  low: number;
  high: number;
}

/** Smallest fresh consume buffer (bytes) — see {@link NativeClient.consumeBatch}. */
const CONSUME_BUF_MIN = 4096;
/** First consume buffer; it then tracks the traffic (2× the last batch). */
const CONSUME_BUF_INITIAL = 64 * 1024;

/**
 * Consume buffers are Buffers (so message views come out as Buffers) and are
 * not zero-filled: C writes the prefix that gets decoded, nothing else is read.
 */
function allocConsumeBuffer(size: number): Uint8Array {
  return Buffer.allocUnsafeSlow(size);
}

/** Messages per prefetch frame (matches the callback layer's CONSUME_BATCH_MAX). */
const CONSUME_PREFETCH_MAX_MSGS = 500;
const ERRSTR_CAP = 512;

/* ========================================================================== */
/* NativeClient                                                                */
/* ========================================================================== */

export class NativeClient {
  readonly type: BrkClientType;
  readonly label: string;
  readonly js: JsOptions;

  private readonly properties: readonly (readonly [string, string])[];
  private nativeLib: BrkNative | undefined;
  private stateValue: ClientState = "INIT";
  private box: HandleBox | undefined;
  private readonly onLeak: ((label: string) => void) | undefined;

  /** `topic_id → name` intern table, living as long as the handle. */
  readonly topics: TopicNameTable;

  private consumeBuf: Uint8Array;
  private eventBuf: Uint8Array;
  private scratchBuf: Uint8Array = new Uint8Array(1024);
  /**
   * DEDICATED buffer for `brk_topic_name`. It must stay separate from
   * `scratchBuf`: `assignment()`/`position()`/`committed()` write the tpl into
   * `scratchBuf` and decode it in place, and decoding may call back into
   * `topicName()` on a cache miss — sharing the buffer would overwrite the very
   * data being read.
   */
  private nameBuf: Uint8Array = new Uint8Array(256);
  private readonly errBuf = new Uint8Array(ERRSTR_CAP);
  private readonly produceWriter = new BufWriter(64 * 1024);

  /** Intermediate buffers (string→UTF-8) that must live across the produce FFI call. */
  private readonly produceKeepAlive: unknown[] = [];
  private readonly tplWriter = new BufWriter(4096);
  private readonly listWriter = new BufWriter(1024);
  private produceErrOut = new Int16Array(1024);
  private readonly lowOut = new BigInt64Array(1);
  private readonly highOut = new BigInt64Array(1);
  private readonly ptrOut = new BigUint64Array(1);

  constructor(options: NativeClientOptions) {
    this.type = options.type;
    this.properties = options.properties;
    this.label = options.label ?? (options.type === BRK_CLIENT_PRODUCER ? "Producer" : "Consumer");
    this.js = { ...DEFAULT_JS_OPTIONS, ...options.js };
    this.nativeLib = options.native;
    this.onLeak = options.onLeak;
    this.consumeBuf = allocConsumeBuffer(
      this.js.consumePrefetch
        ? this.js.consumeBufferBytes
        : Math.min(CONSUME_BUF_INITIAL, this.js.consumeBufferBytes),
    );
    this.eventBuf = new Uint8Array(this.js.eventBufferBytes);
    this.topics = new TopicNameTable((topicId) => this.topicName(topicId));
  }

  /* --------------------------------------------------------------- state */

  get state(): ClientState {
    return this.stateValue;
  }

  /** Has the handle been created and not yet destroyed? */
  get isOpen(): boolean {
    return this.stateValue === "CONNECTING" || this.stateValue === "READY";
  }

  get isClosed(): boolean {
    return this.stateValue === "CLOSED";
  }

  /** The handle pointer — FFI use only (e.g. `brk_send_offsets_to_transaction`). */
  get handle(): Pointer {
    const handle = this.box?.handle;
    if (handle === undefined || handle === null) {
      throw new LibrdKafkaError(`bun-rdkafka: ${this.label} has no native handle yet`, {
        code: BRK_ERR_INVALID_HANDLE,
        origin: "shim",
      });
    }
    return handle;
  }

  private get native(): BrkNative {
    this.nativeLib ??= loadNative();
    return this.nativeLib;
  }

  private assertOpen(op: string): Pointer {
    if (!this.isOpen) {
      throw new LibrdKafkaError(
        `bun-rdkafka: cannot call ${op}() while ${this.label} is in state ${this.stateValue}`,
        { code: BRK_ERR_INVALID_STATE, origin: "shim", context: op },
      );
    }
    return this.handle;
  }

  private assertType(expected: BrkClientType, op: string): Pointer {
    const handle = this.assertOpen(op);
    if (this.type !== expected) {
      throw new LibrdKafkaError(
        `bun-rdkafka: ${op}() is only usable with a ` +
          `${expected === BRK_CLIENT_CONSUMER ? "consumer" : "producer"}`,
        { code: BRK_ERR_INVALID_STATE, origin: "shim", context: op },
      );
    }
    return handle;
  }

  /* ------------------------------------------------------------ lifecycle */

  /**
   * INIT → CONNECTING: builds the conf, `brk_conf_set`s each property, then
   * `brk_client_new`. Does not block, does not wait for metadata — the upper
   * layer decides the transition to READY via {@link markReady}.
   */
  connect(): void {
    if (this.stateValue !== "INIT") {
      throw new LibrdKafkaError(
        `bun-rdkafka: ${this.label} is already in state ${this.stateValue}, cannot connect() again`,
        { code: BRK_ERR_INVALID_STATE, origin: "shim", context: "connect" },
      );
    }
    const native = this.native;
    const conf = native.brk_conf_new();
    if (conf === null) {
      throw new LibrdKafkaError("bun-rdkafka: brk_conf_new() returned NULL (out of memory?)", {
        code: BRK_ERR_INVALID_HANDLE,
        origin: "shim",
        context: "connect",
      });
    }

    let handle: Pointer | null = null;
    try {
      for (const [key, value] of this.properties) {
        this.errBuf.fill(0);
        const ret = native.brk_conf_set(
          conf,
          cstringBuffer(key),
          cstringBuffer(value),
          this.errBuf,
          ERRSTR_CAP,
        );
        if (ret < 0) {
          const detail = readCString(this.errBuf);
          throw (
            LibrdKafkaError.fromReturn(
              ret,
              "brk_conf_set",
              `bun-rdkafka: config "${key}" = "${value}" was rejected: ${detail}`,
            ) ?? new LibrdKafkaError(`bun-rdkafka: config "${key}" was rejected`)
          );
        }
      }

      this.errBuf.fill(0);
      handle = native.brk_client_new(this.type, conf, this.errBuf, ERRSTR_CAP);
      if (handle === null) {
        throw new LibrdKafkaError(
          `bun-rdkafka: failed to create ${this.label}: ${readCString(this.errBuf)}`,
          { code: BRK_ERR_INVALID_HANDLE, origin: "shim", context: "brk_client_new" },
        );
      }
    } finally {
      // The conf is consumed when brk_client_new succeeds; otherwise it must be destroyed.
      if (handle === null) native.brk_conf_destroy(conf);
    }

    this.box = {
      handle,
      native,
      destroyed: false,
      label: this.label,
      onLeak: this.onLeak,
    };
    leakRegistry.register(this, this.box, this);
    this.stateValue = "CONNECTING";

    // EXPERIMENT (docs/notes/consumer-prefetch-thread.md): consume batches are
    // serialized on a shim thread; frames are sized like the JS consume buffer
    // so brk_consume_batch (now a memcpy) never reports BUFFER_TOO_SMALL.
    if (this.type === BRK_CLIENT_CONSUMER && this.js.consumePrefetch) {
      this.check(
        native.brk_consume_prefetch_start(
          handle,
          this.js.consumeBufferBytes,
          CONSUME_PREFETCH_MAX_MSGS,
          this.js.consumePrefetchFrames,
        ),
        "brk_consume_prefetch_start",
      );
    }
  }

  /** Frames filled by the prefetch thread so far (-1 when the experiment is off). */
  prefetchStats(): number {
    if (this.box === undefined || this.box.destroyed) return -1;
    return Number(this.native.brk_consume_prefetch_stats(this.handle));
  }

  /** CONNECTING → READY (when the first metadata arrives). */
  markReady(): void {
    if (this.stateValue !== "CONNECTING") return;
    this.stateValue = "READY";
  }

  /**
   * → DISCONNECTING → CLOSED. Idempotent at the TS layer: a second call never
   * touches native again (calling `brk_client_destroy` twice is undefined
   * behavior).
   */
  disconnect(): void {
    if (this.stateValue === "CLOSED" || this.stateValue === "DISCONNECTING") return;
    if (this.stateValue === "INIT") {
      this.stateValue = "CLOSED";
      return;
    }
    this.stateValue = "DISCONNECTING";
    const box = this.box;
    if (box && !box.destroyed && box.handle !== null) {
      const handle = box.handle;
      box.destroyed = true;
      box.handle = null;
      leakRegistry.unregister(this);
      box.native.brk_client_destroy(handle);
    }
    this.topics.clear();
    this.stateValue = "CLOSED";
  }

  /* --------------------------------------------------- buffers + errors */

  /**
   * Invokes a buffer-writing function; on `BRK_ERR_BUFFER_TOO_SMALL`, grows per
   * `brk_last_required_size` and **retries exactly once**.
   */
  private withGrowingBuffer(
    slot: BufferSlot,
    op: string,
    call: (buf: Uint8Array) => number,
  ): number {
    let buf = this.getBuffer(slot);
    let ret = call(buf);
    if (ret === BRK_ERR_BUFFER_TOO_SMALL) {
      const required = this.native.brk_last_required_size(this.handle);
      const next = Math.max(required > 0 ? required : 0, buf.length * 2);
      buf = slot === "consume" ? allocConsumeBuffer(next) : new Uint8Array(next);
      this.setBuffer(slot, buf);
      ret = call(buf);
    }
    return this.check(ret, op);
  }

  private getBuffer(slot: BufferSlot): Uint8Array {
    if (slot === "consume") return this.consumeBuf;
    if (slot === "event") return this.eventBuf;
    if (slot === "name") return this.nameBuf;
    return this.scratchBuf;
  }

  private setBuffer(slot: BufferSlot, buf: Uint8Array): void {
    if (slot === "consume") this.consumeBuf = buf;
    else if (slot === "event") this.eventBuf = buf;
    else if (slot === "name") this.nameBuf = buf;
    else this.scratchBuf = buf;
  }

  /** Throws `LibrdKafkaError` when `ret < 0`, with the `brk_last_error_string` detail. */
  private check(ret: number, op: string): number {
    if (ret >= 0) return ret;
    const detail = this.safeLastErrorString();
    return throwOnError(ret, op, detail ? `${op}: ${detail}` : undefined);
  }

  private safeLastErrorString(): string | undefined {
    const handle = this.box?.handle;
    if (!handle) return undefined;
    try {
      const n = this.native.brk_last_error_string(handle, this.errBuf, ERRSTR_CAP);
      if (n <= 0) return undefined;
      return DECODER.decode(this.errBuf.subarray(0, Math.min(n, ERRSTR_CAP)));
    } catch {
      return undefined;
    }
  }

  /* ------------------------------------------------------------- common */

  /** `brk_client_outq_len` — messages/requests still waiting in the out queue. */
  outqLen(): number {
    const handle = this.assertOpen("outqLen");
    return this.native.brk_client_outq_len(handle);
  }

  /** `brk_last_error` — most recent kafka error code on the handle. */
  lastError(): number {
    const handle = this.assertOpen("lastError");
    return this.native.brk_last_error(handle);
  }

  /** `brk_last_error_string` — description of the most recent error. */
  lastErrorString(): string {
    this.assertOpen("lastErrorString");
    return this.safeLastErrorString() ?? "";
  }

  /** `brk_topic_name` — topic name for an interned `topic_id`. */
  topicName(topicId: number): string {
    const handle = this.assertOpen("topicName");
    const n = this.withGrowingBuffer("name", "brk_topic_name", (buf) =>
      this.native.brk_topic_name(handle, topicId, buf, buf.length),
    );
    return DECODER.decode(this.nameBuf.subarray(0, n));
  }

  /* -------------------------------------------------------- event drain */

  /**
   * `brk_events_poll` — drains the main event queue into {@link BrkEvent}s.
   * `timeoutMs` is ALWAYS 0 when called from the main thread (NFR-2).
   *
   * Note: REBALANCE/OFFSET_COMMIT only show up here AFTER a
   * {@link consumeBatch} call (see the two-queue note at the top of this file).
   */
  pollEvents(maxEvents = 256, timeoutMs = 0): BrkEvent[] {
    const handle = this.assertOpen("pollEvents");
    const count = this.withGrowingBuffer("event", "brk_events_poll", (buf) =>
      this.native.brk_events_poll(handle, buf, buf.length, maxEvents, timeoutMs),
    );
    if (count === 0) return [];
    return decodeEventFrames(this.eventBuf, count, { topics: this.topics });
  }

  /* ----------------------------------------------------------- producer */

  /**
   * `brk_produce_batch` — encodes a PRODUCE BATCH and pushes it down to C in
   * exactly ONE FFI call for the whole batch (NFR-1).
   */
  produceBatch(records: readonly ProduceRecord[]): ProduceBatchResult {
    const handle = this.assertType(BRK_CLIENT_PRODUCER, "produceBatch");
    if (records.length === 0) return { accepted: 0, errors: new Int16Array(0) };

    this.produceWriter.reset();
    // Format 3 carries (ptr,len) for key/value: `records` + `produceKeepAlive`
    // must stay alive until the synchronous FFI call below returns (header §format 3).
    encodeProduceBatch(records, this.produceWriter, this.produceKeepAlive);
    const payload = this.produceWriter.unsafeBytes();

    if (this.produceErrOut.length < records.length) {
      this.produceErrOut = new Int16Array(Math.max(records.length, this.produceErrOut.length * 2));
    }

    try {
      const accepted = this.check(
        this.native.brk_produce_batch(
          handle,
          payload,
          payload.length,
          this.produceErrOut,
          records.length,
        ),
        "brk_produce_batch",
      );
      return { accepted, errors: this.produceErrOut.subarray(0, accepted) };
    } finally {
      this.produceKeepAlive.length = 0;
    }
  }

  /** `brk_flush` — blocks up to `timeoutMs`; only call with small, repeated timeouts. */
  flush(timeoutMs: number): number {
    const handle = this.assertType(BRK_CLIENT_PRODUCER, "flush");
    return this.check(this.native.brk_flush(handle, timeoutMs), "brk_flush");
  }

  /* ------------------------------------------------------- transactions */

  initTransactions(timeoutMs: number): void {
    const handle = this.assertType(BRK_CLIENT_PRODUCER, "initTransactions");
    this.withErrstr("brk_init_transactions", (buf, cap) =>
      this.native.brk_init_transactions(handle, timeoutMs, buf, cap),
    );
  }

  beginTransaction(): void {
    const handle = this.assertType(BRK_CLIENT_PRODUCER, "beginTransaction");
    this.withErrstr("brk_begin_transaction", (buf, cap) =>
      this.native.brk_begin_transaction(handle, buf, cap),
    );
  }

  commitTransaction(timeoutMs: number): void {
    const handle = this.assertType(BRK_CLIENT_PRODUCER, "commitTransaction");
    this.withErrstr("brk_commit_transaction", (buf, cap) =>
      this.native.brk_commit_transaction(handle, timeoutMs, buf, cap),
    );
  }

  abortTransaction(timeoutMs: number): void {
    const handle = this.assertType(BRK_CLIENT_PRODUCER, "abortTransaction");
    this.withErrstr("brk_abort_transaction", (buf, cap) =>
      this.native.brk_abort_transaction(handle, timeoutMs, buf, cap),
    );
  }

  /** `brk_send_offsets_to_transaction` — `consumer` is the client providing the offsets. */
  sendOffsetsToTransaction(
    offsets: readonly TopicPartitionInput[],
    consumer: NativeClient,
    timeoutMs: number,
  ): void {
    const handle = this.assertType(BRK_CLIENT_PRODUCER, "sendOffsetsToTransaction");
    const tpl = this.encodeTplPayload(offsets);
    const consumerHandle = consumer.handle;
    this.withErrstr("brk_send_offsets_to_transaction", (buf, cap) =>
      this.native.brk_send_offsets_to_transaction(
        handle,
        tpl,
        tpl.length,
        consumerHandle,
        timeoutMs,
        buf,
        cap,
      ),
    );
  }

  /* ----------------------------------------------------------- consumer */

  /** `brk_subscribe` — STRING LIST (format 1). */
  subscribe(topics: readonly string[]): void {
    const handle = this.assertType(BRK_CLIENT_CONSUMER, "subscribe");
    this.listWriter.reset();
    encodeStringList(topics, this.listWriter);
    const payload = this.listWriter.unsafeBytes();
    this.check(this.native.brk_subscribe(handle, payload, payload.length), "brk_subscribe");
  }

  unsubscribe(): void {
    const handle = this.assertType(BRK_CLIENT_CONSUMER, "unsubscribe");
    this.check(this.native.brk_unsubscribe(handle), "brk_unsubscribe");
  }

  /** `brk_subscription` — the currently subscribed topic list. */
  subscription(): string[] {
    const handle = this.assertType(BRK_CLIENT_CONSUMER, "subscription");
    this.withGrowingBuffer("scratch", "brk_subscription", (buf) =>
      this.native.brk_subscription(handle, buf, buf.length),
    );
    return decodeStringList(this.scratchBuf);
  }

  /**
   * `brk_consume_batch` — heart of the consume path: one FFI call returns up to
   * `maxMsgs` messages (NFR-1). `timeoutMs` = 0 from the main thread.
   *
   * Also the `consumer_q` pump: it must be called regularly once subscribed so
   * REBALANCE/OFFSET_COMMIT surface via `pollEvents()`.
   */
  consumeBatch(maxMsgs: number, timeoutMs = 0): DecodedMessage[] {
    const handle = this.assertType(BRK_CLIENT_CONSUMER, "consumeBatch");
    const count = this.withGrowingBuffer("consume", "brk_consume_batch", (buf) =>
      this.native.brk_consume_batch(handle, buf, buf.length, maxMsgs, timeoutMs),
    );
    if (count === 0) return [];
    // Zero per-message copies: key/value/headers are views into the batch
    // buffer, which is retired right here (never written to again) — so they
    // stay valid for as long as the app keeps them. Keeping one message alive
    // keeps its whole batch buffer alive; the next buffer is therefore sized
    // to the traffic (2× the last batch, within
    // [CONSUME_BUF_MIN, js.consume.buffer.bytes]) to bound that. Prefetch
    // frames are always `js.consume.buffer.bytes` (or grown), so keep the
    // full size there and avoid a BUFFER_TOO_SMALL round-trip per frame.
    const buf = this.consumeBuf;
    const { messages, byteLength } = decodeMessageBatchWithSize(buf, count, {
      topics: this.topics,
      copy: false,
    });
    this.consumeBuf = allocConsumeBuffer(
      this.js.consumePrefetch
        ? Math.max(this.js.consumeBufferBytes, buf.length)
        : Math.min(Math.max(CONSUME_BUF_MIN, byteLength * 2), this.js.consumeBufferBytes),
    );
    return messages;
  }

  /** `brk_commit`. Empty/`null` `offsets` = commit all current positions. */
  commit(offsets: readonly TopicPartitionInput[] | null, async: boolean): void {
    const handle = this.assertType(BRK_CLIENT_CONSUMER, "commit");
    if (offsets === null || offsets.length === 0) {
      this.check(this.native.brk_commit(handle, null, 0, async ? 1 : 0), "brk_commit");
      return;
    }
    const tpl = this.encodeTplPayload(offsets);
    this.check(this.native.brk_commit(handle, tpl, tpl.length, async ? 1 : 0), "brk_commit");
  }

  /** `brk_committed` — blocks up to `timeoutMs` (broker round-trip). */
  committed(
    partitions: readonly TopicPartitionInput[] | null,
    timeoutMs: number,
  ): TopicPartitionEntry[] {
    const handle = this.assertType(BRK_CLIENT_CONSUMER, "committed");
    const tpl =
      partitions === null || partitions.length === 0
        ? null
        : this.encodeTplPayload(partitions);
    const tplLen = tpl?.length ?? 0;
    this.withGrowingBuffer("scratch", "brk_committed", (buf) =>
      this.native.brk_committed(handle, tpl, tplLen, buf, buf.length, timeoutMs),
    );
    return decodeTplBuffer(this.scratchBuf, { topics: this.topics });
  }

  /**
   * `brk_offsets_for_times` — `offset` of each input entry is the timestamp
   * (ms); returns the same entries with `offset` = the earliest offset at or
   * after that timestamp. Blocks up to `timeoutMs` (broker round-trip).
   */
  offsetsForTimes(
    partitions: readonly TopicPartitionInput[],
    timeoutMs: number,
  ): TopicPartitionEntry[] {
    const handle = this.assertOpen("offsetsForTimes");
    const tpl = this.encodeTplPayload(partitions);
    this.withGrowingBuffer("scratch", "brk_offsets_for_times", (buf) =>
      this.native.brk_offsets_for_times(handle, tpl, tpl.length, buf, buf.length, timeoutMs),
    );
    return decodeTplBuffer(this.scratchBuf, { topics: this.topics });
  }

  /** `brk_seek` — by topic NAME (cold path, no interned id needed). */
  seek(topic: string, partition: number, offset: number | bigint, timeoutMs: number): void {
    const handle = this.assertType(BRK_CLIENT_CONSUMER, "seek");
    this.check(
      this.native.brk_seek(handle, cstringBuffer(topic), partition, offset, timeoutMs),
      "brk_seek",
    );
  }

  /**
   * `brk_assign` — answers a REBALANCE event or assigns manually.
   *
   * ⚠ An EMPTY array ≠ `null`: cooperative-sticky hands the second joiner an
   * empty incremental assign in the first rebalance round — an encoded tpl with
   * count=0 MUST be sent then; a NULL tpl is only valid for
   * `partitions === null` (BRK_UNASSIGN). Conflating the two once deadlocked
   * cooperative rebalancing permanently (M3).
   */
  assign(
    partitions: readonly TopicPartitionInput[] | null,
    mode: BrkAssignMode = BRK_ASSIGN,
  ): void {
    const handle = this.assertType(BRK_CLIENT_CONSUMER, "assign");
    if (partitions === null) {
      this.check(this.native.brk_assign(handle, null, 0, mode), "brk_assign");
      return;
    }
    const tpl = this.encodeTplPayload(partitions);
    this.check(this.native.brk_assign(handle, tpl, tpl.length, mode), "brk_assign");
  }

  /** `brk_assignment` — the current assignment. */
  assignment(): TopicPartitionEntry[] {
    const handle = this.assertType(BRK_CLIENT_CONSUMER, "assignment");
    this.withGrowingBuffer("scratch", "brk_assignment", (buf) =>
      this.native.brk_assignment(handle, buf, buf.length),
    );
    return decodeTplBuffer(this.scratchBuf, { topics: this.topics });
  }

  /** `brk_position` — current positions of the assignment. */
  position(): TopicPartitionEntry[] {
    const handle = this.assertType(BRK_CLIENT_CONSUMER, "position");
    this.withGrowingBuffer("scratch", "brk_position", (buf) =>
      this.native.brk_position(handle, buf, buf.length),
    );
    return decodeTplBuffer(this.scratchBuf, { topics: this.topics });
  }

  /** `brk_pause_resume` (tpl offsets are ignored). */
  pauseResume(partitions: readonly TopicPartitionInput[], resume: boolean): void {
    const handle = this.assertType(BRK_CLIENT_CONSUMER, "pauseResume");
    const tpl = this.encodeTplPayload(partitions);
    this.check(
      this.native.brk_pause_resume(handle, tpl, tpl.length, resume ? 1 : 0),
      "brk_pause_resume",
    );
  }

  /**
   * `brk_offsets_store` — workflow `enable.auto.offset.store=false`.
   * A single partition of an already-interned topic (the per-message store of
   * the KafkaJS `eachMessage` loop) takes `brk_offset_store_single`: no tpl
   * encode/decode, no list allocation on either side.
   */
  offsetsStore(partitions: readonly TopicPartitionInput[]): void {
    const handle = this.assertType(BRK_CLIENT_CONSUMER, "offsetsStore");
    if (partitions.length === 1) {
      const p = partitions[0] as TopicPartitionInput;
      const topicId =
        p.topicId !== undefined && p.topicId >= 0
          ? p.topicId
          : p.topic !== undefined
            ? this.topics.idOf(p.topic)
            : undefined;
      if (topicId !== undefined && p.offset !== undefined && (p.metadata ?? null) === null) {
        this.check(
          this.native.brk_offset_store_single(
            handle,
            topicId,
            p.partition,
            p.offset,
            p.leaderEpoch ?? NO_LEADER_EPOCH,
          ),
          "brk_offset_store_single",
        );
        return;
      }
    }
    const tpl = this.encodeTplPayload(partitions);
    this.check(this.native.brk_offsets_store(handle, tpl, tpl.length), "brk_offsets_store");
  }

  /** `brk_query_watermark` — asks the broker (blocks up to `timeoutMs`). */
  queryWatermark(topic: string, partition: number, timeoutMs: number): WatermarkOffsets {
    const handle = this.assertOpen("queryWatermark");
    this.check(
      this.native.brk_query_watermark(
        handle,
        cstringBuffer(topic),
        partition,
        this.lowOut,
        this.highOut,
        timeoutMs,
      ),
      "brk_query_watermark",
    );
    return this.readWatermarks();
  }

  /** `brk_get_watermark` — local cache, never touches the broker. */
  getWatermark(topic: string, partition: number): WatermarkOffsets {
    const handle = this.assertOpen("getWatermark");
    this.check(
      this.native.brk_get_watermark(
        handle,
        cstringBuffer(topic),
        partition,
        this.lowOut,
        this.highOut,
      ),
      "brk_get_watermark",
    );
    return this.readWatermarks();
  }

  private readWatermarks(): WatermarkOffsets {
    return { low: Number(this.lowOut[0] ?? 0n), high: Number(this.highOut[0] ?? 0n) };
  }

  /* ------------------------------------------------------- oauthbearer */

  /** `brk_oauthbearer_set_token` — answers an OAUTH_REFRESH event. */
  setOauthBearerToken(
    token: string,
    lifetimeMs: number | bigint,
    principal: string,
    extensions?: readonly string[],
  ): void {
    const handle = this.assertOpen("setOauthBearerToken");
    let ext: Uint8Array | null = null;
    if (extensions && extensions.length > 0) {
      this.listWriter.reset();
      encodeStringList(extensions, this.listWriter);
      ext = this.listWriter.toBytes();
    }
    this.withErrstr("brk_oauthbearer_set_token", (buf, cap) =>
      this.native.brk_oauthbearer_set_token(
        handle,
        cstringBuffer(token),
        lifetimeMs,
        cstringBuffer(principal),
        ext,
        ext?.length ?? 0,
        buf,
        cap,
      ),
    );
  }

  /**
   * `brk_sasl_set_credentials` — changes the SASL PLAIN/SCRAM credentials after
   * the handle was created; existing connections are not torn down, the new
   * credentials apply to the next authentication.
   */
  saslSetCredentials(username: string, password: string): void {
    const handle = this.assertOpen("saslSetCredentials");
    this.check(
      this.native.brk_sasl_set_credentials(
        handle,
        cstringBuffer(username),
        cstringBuffer(password),
      ),
      "brk_sasl_set_credentials",
    );
  }

  /** `brk_oauthbearer_set_token_failure`. */
  setOauthBearerTokenFailure(reason: string): void {
    const handle = this.assertOpen("setOauthBearerTokenFailure");
    this.check(
      this.native.brk_oauthbearer_set_token_failure(handle, cstringBuffer(reason)),
      "brk_oauthbearer_set_token_failure",
    );
  }

  /* --------------------------------------------------- metadata & admin */

  /**
   * `brk_metadata` — JSON malloc'd by the shim; copied into a string then
   * `brk_mem_free`d immediately (design §8).
   */
  metadata(topic: string | null, timeoutMs: number): string {
    const handle = this.assertOpen("metadata");
    this.ptrOut[0] = 0n;
    this.check(
      this.native.brk_metadata(
        handle,
        topic === null ? null : cstringBuffer(topic),
        timeoutMs,
        this.ptrOut,
      ),
      "brk_metadata",
    );
    const raw = this.ptrOut[0] ?? 0n;
    if (raw === 0n) return "";
    const pointer = Number(raw) as Pointer;
    try {
      return new CString(pointer).toString();
    } finally {
      this.native.brk_mem_free(pointer);
      this.ptrOut[0] = 0n;
    }
  }

  /** `brk_admin_request` — the result arrives asynchronously via an ADMIN_RESULT event. */
  adminRequest(opId: BrkAdminOp, correlationId: bigint, requestJson: string): void {
    const handle = this.assertOpen("adminRequest");
    this.check(
      this.native.brk_admin_request(handle, opId, correlationId, cstringBuffer(requestJson)),
      "brk_admin_request",
    );
  }

  /* -------------------------------------------------------------- helper */

  private encodeTplPayload(entries: readonly TopicPartitionInput[]): Uint8Array {
    this.tplWriter.reset();
    encodeTpl(entries, this.tplWriter);
    return this.tplWriter.unsafeBytes();
  }

  private withErrstr(op: string, call: (buf: Uint8Array, cap: number) => number): number {
    this.errBuf.fill(0);
    const ret = call(this.errBuf, ERRSTR_CAP);
    if (ret < 0) {
      // Transaction-group errstrs carry the [retriable]/[txn-requires-abort] markers.
      throw LibrdKafkaError.fromErrstr(ret, op, readCString(this.errBuf));
    }
    return ret;
  }
}
