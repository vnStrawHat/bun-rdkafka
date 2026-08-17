/**
 * callback/producer.ts — the Callback API's `Producer` (design §6, FR-1).
 *
 * Semantics cross-checked against node-rdkafka/confluent-kafka-javascript
 * (`lib/producer.js`): synchronous `produce()` (throws on failure), the
 * `delivery-report` event (err-first) when the config has `dr_cb`/`dr_msg_cb`,
 * `poll()`/`setPollInterval()`, `flush()`, transactions. Internals are free
 * (api-compat-not-internals):
 *
 *  - **Staging batch**: `produce()` does NOT make one FFI call per message.
 *    Records are collected into a staging buffer and pushed down to
 *    `brk_produce_batch` in ONE FFI call when (a) the current microtask ends,
 *    (b) staging is full ({@link STAGING_MAX_RECORDS}), or (c) the user calls
 *    `poll()`/`flush()`. For a sequential `for { produce() }` loop, the whole
 *    loop becomes one FFI call — the main source of G3 (NFR-1).
 *  - Per-record errors from the shim (e.g. `ERR__QUEUE_FULL`) surface as a
 *    **throw on the next `produce()`** — matching upstream's observable
 *    semantics (throw when produce fails) within staging's limits.
 *  - Delivery reports flow: DR frame → `onEventFrame` → {@link DeliveryLedger}
 *    → emit `delivery-report`. Backpressure via `js.producer.max.pending`
 *    (defaults to `queue.buffering.max.messages`) — beyond the threshold,
 *    `produce()` throws `ERR__QUEUE_FULL` immediately (synchronously).
 */

import { BRK_CLIENT_PRODUCER, BRK_EVENT_DR, RD_KAFKA_PARTITION_UA } from "../ffi/types.ts";
import type { BrkEvent, ProduceHeader, ProduceRecord } from "../core/batch-decoder.ts";
import type { ClientConfig } from "../core/config.ts";
import { DeliveryLedger, type DeliveryResult } from "../core/delivery-ledger.ts";
import { ERROR_CODES, LibrdKafkaError, errorDescription } from "../core/errors.ts";
import type { NativeClient } from "../core/native-client.ts";
import {
  Client,
  type ClientInternalOptions,
  type DisconnectCallback,
} from "./client.ts";
import { ProducerStream, type WriteStreamOptions } from "./producer-stream.ts";

/* ========================================================================== */
/* Public types                                                                */
/* ========================================================================== */

/** Value/key accepted by `produce()` — same as upstream. */
export type ProduceValue = Uint8Array | string | null;

/**
 * Upstream-style headers: an array of objects, each with one-or-more key→value
 * pairs (`[{ header: "value" }]`), or the explicit `{ key, value }` form.
 */
export type ProduceHeaders = readonly Record<string, Uint8Array | string | null>[];

/** The `delivery-report` event's report — upstream shape. */
export interface DeliveryReportPayload {
  topic: string;
  partition: number;
  offset: number;
  key: ProduceValue;
  opaque: unknown;
  timestamp: number;
  size: number;
  /** Only present with `dr_msg_cb` configured (upstream includes the payload). */
  value?: ProduceValue;
}

export type DeliveryReportListener = (
  err: LibrdKafkaError | null,
  report: DeliveryReportPayload,
) => void;

export type TransactionCallback = (err: LibrdKafkaError | null) => void;
export type FlushCallback = (err: LibrdKafkaError | null) => void;

/** Input offsets of `sendOffsetsToTransaction` — upstream shape. */
export interface TopicPartitionOffset {
  topic: string;
  partition: number;
  offset: number;
}

/**
 * The consumer providing offsets to `sendOffsetsToTransaction`: a
 * KafkaConsumer (M3) or any object exposing a NativeClient (duck typing — the
 * tests' SliceConsumer also works via its `client` field).
 */
interface ConsumerLike {
  native?: NativeClient;
  client?: NativeClient;
}

/* ========================================================================== */
/* Internal constants                                                          */
/* ========================================================================== */

/**
 * Staging cap before an in-tick forced flush (prevents staging bloat when the
 * caller produces hundreds of thousands of messages without yielding the event
 * loop). The microtask flush is the main path; this threshold is only a
 * backstop, so it needs no config.
 */
export const STAGING_MAX_RECORDS = 10_000;

/** Loop step for retriable transaction ops (keeps the event loop alive — NFR-2). */
const TXN_STEP_MS = 100;
const DEFAULT_TXN_TIMEOUT_MS = 30_000;
const FLUSH_POLL_STEP_MS = 5;
const DEFAULT_FLUSH_TIMEOUT_MS = 10_000;

/** Metadata of a record awaiting its DR (the {@link DeliveryLedger}'s record `R`). */
interface StagedMeta {
  topic: string;
  key: ProduceValue;
  value: ProduceValue;
  opaque: unknown;
  size: number;
  /** HighLevelProducer: per-message callback. */
  onDelivery?: ((err: LibrdKafkaError | null, report?: DeliveryReportPayload) => void) | undefined;
}

function toBytes(value: ProduceValue | undefined): Uint8Array | string | null {
  return value ?? null;
}

function byteLength(value: ProduceValue | undefined): number {
  if (value === null || value === undefined) return 0;
  return typeof value === "string" ? Buffer.byteLength(value) : value.byteLength;
}

/** Normalizes upstream-style headers into format 3. */
export function normalizeHeaders(headers: ProduceHeaders | undefined): ProduceHeader[] | undefined {
  if (!headers || headers.length === 0) return undefined;
  const out: ProduceHeader[] = [];
  for (const entry of headers) {
    if (entry === null || typeof entry !== "object") {
      throw new LibrdKafkaError(`bun-rdkafka: header must be an object, got ${typeof entry}`, {
        code: ERROR_CODES.ERR__INVALID_ARG,
        origin: "local",
        context: "produce",
      });
    }
    const keys = Object.keys(entry);
    // The explicit { key, value } form (exactly these two properties).
    if (keys.length === 2 && "key" in entry && "value" in entry) {
      out.push({
        key: String((entry as { key: unknown }).key),
        value: (entry as { value: Uint8Array | string | null }).value ?? null,
      });
      continue;
    }
    for (const k of keys) {
      out.push({ key: k, value: entry[k] ?? null });
    }
  }
  return out;
}

/* ========================================================================== */
/* Producer                                                                    */
/* ========================================================================== */

export class Producer extends Client {
  /**
   * Creates a `Producer` and wraps it in a Writable {@link ProducerStream}
   * (upstream `Producer.createWriteStream`). See `callback/producer-stream.ts`.
   */
  static createWriteStream(
    conf: ClientConfig,
    topicConf: ClientConfig | undefined,
    streamOptions: WriteStreamOptions | string,
  ): ProducerStream {
    return new ProducerStream(new Producer(conf, topicConf), streamOptions);
  }

  /** The DR ledger: its record type is {@link StagedMeta}. */
  protected readonly ledger: DeliveryLedger<StagedMeta>;

  private staging: ProduceRecord[] = [];
  private stagingScheduled = false;
  /** Per-record error from the previous batch, thrown on the next `produce()`. */
  private pendingProduceError: LibrdKafkaError | null = null;
  private readonly emitDeliveryReports: boolean;
  private readonly includeValueInReport: boolean;
  private pollIntervalMs = 0;
  private pollIntervalTimer: ReturnType<typeof setInterval> | undefined;

  constructor(
    globalConf?: ClientConfig,
    topicConf?: ClientConfig,
    internal: ClientInternalOptions = {},
  ) {
    super(globalConf, topicConf, BRK_CLIENT_PRODUCER, internal);
    this.ledger = new DeliveryLedger<StagedMeta>({
      maxPending: this.jsOptions.producerMaxPending,
    });

    const { dr_cb, dr_msg_cb } = this.configCallbacks;
    this.emitDeliveryReports = dr_cb !== undefined || dr_msg_cb !== undefined;
    this.includeValueInReport = dr_msg_cb !== undefined;
    // dr_cb as a function → registered as a listener (equivalent to upstream behavior).
    if (typeof dr_cb === "function") this.on("delivery-report", dr_cb as DeliveryReportListener);
    if (typeof dr_msg_cb === "function") {
      this.on("delivery-report", dr_msg_cb as DeliveryReportListener);
    }

    this.on("ready", () => this.applyPollInterval());
  }

  /* -------------------------------------------------------------- produce */

  /**
   * Enqueues one message for sending. Synchronous: returns `true` once staged,
   * throws `LibrdKafkaError` on failure (not connected, backpressure, previous
   * batch error). Messages actually reach librdkafka in batches — see the doc
   * at the top of this file.
   */
  produce(
    topic: string,
    partition?: number | null,
    message?: ProduceValue,
    key?: ProduceValue,
    timestamp?: number | null,
    opaque?: unknown,
    headers?: ProduceHeaders,
  ): boolean {
    this.produceInternal(topic, partition, message, key, timestamp, opaque, headers, undefined);
    return true;
  }

  /** Shared path for Producer.produce and HighLevelProducer (with per-message cb). */
  protected produceInternal(
    topic: string,
    partition: number | null | undefined,
    message: ProduceValue | undefined,
    key: ProduceValue | undefined,
    timestamp: number | null | undefined,
    opaque: unknown,
    headers: ProduceHeaders | undefined,
    onDelivery: StagedMeta["onDelivery"],
  ): bigint {
    const nc = this.native;
    if (nc === undefined || !nc.isOpen) {
      throw new LibrdKafkaError("Producer not connected", {
        code: ERROR_CODES.ERR__STATE,
        origin: "local",
        context: "produce",
      });
    }
    if (typeof topic !== "string" || topic.length === 0) {
      throw new LibrdKafkaError("bun-rdkafka: produce() requires a non-empty string topic", {
        code: ERROR_CODES.ERR__INVALID_ARG,
        origin: "local",
        context: "produce",
      });
    }
    if (this.pendingProduceError !== null) {
      const err = this.pendingProduceError;
      this.pendingProduceError = null;
      throw err;
    }

    const value = toBytes(message);
    const keyBytes = toBytes(key);
    const meta: StagedMeta = {
      topic,
      key: keyBytes,
      value,
      opaque,
      size: byteLength(message),
      onDelivery,
    };

    // track() throws ERR__QUEUE_FULL beyond js.producer.max.pending
    // (synchronously, matching librdkafka's semantics when the queue is full).
    const opaqueId = this.ledger.track(
      meta,
      (result) => this.handleDelivered(result),
      (error) => this.handleDeliveryFailed(error, meta),
    );

    const record: ProduceRecord = {
      topic,
      partition: partition ?? RD_KAFKA_PARTITION_UA,
      timestamp: timestamp ?? 0,
      opaqueId,
      key: keyBytes,
      value,
    };
    const normalized = normalizeHeaders(headers);
    if (normalized !== undefined) record.headers = normalized;

    this.staging.push(record);
    if (this.staging.length >= STAGING_MAX_RECORDS) {
      this.flushStaging();
    } else if (!this.stagingScheduled) {
      this.stagingScheduled = true;
      queueMicrotask(() => this.flushStaging());
    }
    return opaqueId;
  }

  /** Pushes all of staging down to `brk_produce_batch` (one FFI call). */
  protected flushStaging(): void {
    this.stagingScheduled = false;
    if (this.staging.length === 0) return;
    const nc = this.native;
    const records = this.staging;
    this.staging = [];

    if (nc === undefined || !nc.isOpen) {
      const err = new LibrdKafkaError("Producer not connected", {
        code: ERROR_CODES.ERR__STATE,
        origin: "local",
        context: "produce",
      });
      for (const record of records) this.ledger.fail(record.opaqueId, err);
      return;
    }

    let accepted = 0;
    let errors: Int16Array;
    try {
      const result = nc.produceBatch(records);
      accepted = result.accepted;
      errors = result.errors;
    } catch (error) {
      // The whole batch failed (handle/decode error): no record was enqueued.
      const err =
        error instanceof LibrdKafkaError
          ? error
          : new LibrdKafkaError(String(error), { context: "produce" });
      for (const record of records) this.ledger.fail(record.opaqueId, err);
      this.pendingProduceError ??= err;
      return;
    }

    for (let i = 0; i < records.length; i++) {
      const code = i < accepted ? (errors[i] ?? 0) : ERROR_CODES.ERR__FAIL;
      if (code === 0) continue;
      const err = LibrdKafkaError.fromKafkaCode(code, errorDescription(code), {
        context: "produce",
      });
      const record = records[i];
      if (record !== undefined) this.ledger.fail(record.opaqueId, err);
      // Surfaces as a throw on the next produce() — keep the FIRST error.
      this.pendingProduceError ??= err;
    }

    // DRs are about to arrive: kick the scheduler to HOT.
    this.scheduler?.kick();
  }

  /* ------------------------------------------------------ delivery report */

  protected override onEventFrame(event: BrkEvent): void {
    if (event.type === BRK_EVENT_DR) {
      this.ledger.settleAll(event.reports);
    }
  }

  private handleDelivered(result: DeliveryResult<StagedMeta>): void {
    const meta = result.record;
    const report = this.buildReport(meta, result.partition, result.offset, result.timestamp);
    meta.onDelivery?.(null, report);
    if (this.emitDeliveryReports) this.emit("delivery-report", null, report);
  }

  private handleDeliveryFailed(error: LibrdKafkaError, meta: StagedMeta): void {
    const report = this.buildReport(meta, RD_KAFKA_PARTITION_UA, -1, -1);
    meta.onDelivery?.(error, report);
    if (this.emitDeliveryReports) this.emit("delivery-report", error, report);
  }

  private buildReport(
    meta: StagedMeta,
    partition: number,
    offset: number,
    timestamp: number,
  ): DeliveryReportPayload {
    const report: DeliveryReportPayload = {
      topic: meta.topic,
      partition,
      offset,
      key: meta.key,
      opaque: meta.opaque,
      timestamp,
      size: meta.size,
    };
    if (this.includeValueInReport) report.value = meta.value;
    return report;
  }

  /* --------------------------------------------------------- poll / flush */

  /**
   * Pumps one manual poll round (upstream requires calling it to receive DRs;
   * here the PollScheduler already pumps — `poll()` just forces a staging flush
   * + one immediate round).
   */
  poll(): this {
    this.flushStaging();
    this.scheduler?.runOnce();
    return this;
  }

  /**
   * Periodic poll like upstream. The scheduler already pumps, so this timer is
   * only a compatibility safety net; `0` disables it.
   */
  setPollInterval(intervalMs: number): this {
    this.pollIntervalMs = intervalMs;
    this.applyPollInterval();
    return this;
  }

  private applyPollInterval(): void {
    if (this.pollIntervalTimer !== undefined) {
      clearInterval(this.pollIntervalTimer);
      this.pollIntervalTimer = undefined;
    }
    if (this.pollIntervalMs > 0 && this.isConnected()) {
      this.pollIntervalTimer = setInterval(() => this.poll(), this.pollIntervalMs);
      this.pollIntervalTimer.unref?.();
    }
  }

  /**
   * Waits until every produced message is delivered (empty outq + no pending
   * DRs). Does NOT block the event loop: checks in a timer loop (NFR-2).
   */
  flush(timeout?: number | FlushCallback, cb?: FlushCallback): this {
    let timeoutMs = DEFAULT_FLUSH_TIMEOUT_MS;
    if (typeof timeout === "function") cb = timeout;
    else if (timeout !== undefined) timeoutMs = timeout;

    this.flushStaging();
    const nc = this.native;
    if (nc === undefined || !nc.isOpen) {
      queueMicrotask(() => cb?.(notConnected("flush")));
      return this;
    }

    const deadline = Date.now() + timeoutMs;
    const step = (): void => {
      if (this.native !== nc || !nc.isOpen) {
        cb?.(notConnected("flush"));
        return;
      }
      this.flushStaging();
      this.scheduler?.runOnce();
      if (nc.outqLen() === 0 && this.ledger.pending === 0) {
        cb?.(null);
        return;
      }
      if (Date.now() >= deadline) {
        cb?.(
          LibrdKafkaError.fromKafkaCode(
            ERROR_CODES.ERR__TIMED_OUT,
            `flush: ${nc.outqLen()} messages still in the outq, ` +
              `${this.ledger.pending} DRs pending after ${timeoutMs}ms`,
            { context: "flush" },
          ),
        );
        return;
      }
      setTimeout(step, FLUSH_POLL_STEP_MS);
    };
    queueMicrotask(step);
    return this;
  }

  /** Records awaiting DRs (staged + already handed to librdkafka). */
  get pendingDeliveryReports(): number {
    return this.ledger.pending;
  }

  protected override isCold(): boolean {
    if (this.staging.length > 0 || this.ledger.pending > 0) return false;
    const nc = this.native;
    if (nc !== undefined && nc.isOpen && nc.outqLen() > 0) return false;
    return true;
  }

  override disconnect(cb?: DisconnectCallback): this {
    // Push out the remaining staging + pick up available DRs before destroying the handle.
    if (this.native !== undefined && this.native.isOpen) {
      this.flushStaging();
      this.scheduler?.runOnce();
    }
    if (this.pollIntervalTimer !== undefined) {
      clearInterval(this.pollIntervalTimer);
      this.pollIntervalTimer = undefined;
    }
    super.disconnect(cb);
    // The handle is destroyed — DRs will never arrive: reject so callbacks don't hang.
    if (this.ledger.pending > 0) {
      this.ledger.failAll(
        LibrdKafkaError.fromKafkaCode(
          ERROR_CODES.ERR__DESTROY,
          "bun-rdkafka: producer disconnected before receiving delivery reports",
          { context: "disconnect" },
        ),
      );
    }
    return this;
  }

  /* -------------------------------------------------------- transactions */

  /**
   * `rd_kafka_init_transactions`. A retriable op: called in ≤100ms steps to
   * avoid blocking the event loop for long (NFR-2) — librdkafka allows calling
   * again after `ERR__TIMED_OUT` to continue the op running in the background.
   */
  initTransactions(timeout?: number | TransactionCallback, cb?: TransactionCallback): this {
    const [timeoutMs, callback] = txnArgs(timeout, cb);
    this.runRetriableTxnOp("initTransactions", timeoutMs, callback, (nc, stepMs) =>
      nc.initTransactions(stepMs),
    );
    return this;
  }

  beginTransaction(cb?: TransactionCallback): this {
    queueMicrotask(() => {
      const nc = this.native;
      if (nc === undefined || !nc.isOpen) {
        cb?.(notConnected("beginTransaction"));
        return;
      }
      try {
        nc.beginTransaction();
        cb?.(null);
      } catch (error) {
        cb?.(asLibrdKafkaError(error, "beginTransaction"));
      }
    });
    return this;
  }

  commitTransaction(timeout?: number | TransactionCallback, cb?: TransactionCallback): this {
    const [timeoutMs, callback] = txnArgs(timeout, cb);
    // Staging must fully drain before commit — messages still in staging do
    // not belong to any transaction on librdkafka's side yet.
    this.flushStaging();
    this.runRetriableTxnOp("commitTransaction", timeoutMs, callback, (nc, stepMs) =>
      nc.commitTransaction(stepMs),
    );
    return this;
  }

  abortTransaction(timeout?: number | TransactionCallback, cb?: TransactionCallback): this {
    const [timeoutMs, callback] = txnArgs(timeout, cb);
    this.flushStaging();
    this.runRetriableTxnOp("abortTransaction", timeoutMs, callback, (nc, stepMs) =>
      nc.abortTransaction(stepMs),
    );
    return this;
  }

  /**
   * `rd_kafka_send_offsets_to_transaction` — `consumer` is the KafkaConsumer
   * (or NativeClient-exposing object) providing the group metadata.
   */
  sendOffsetsToTransaction(
    offsets: readonly TopicPartitionOffset[],
    consumer: ConsumerLike,
    timeout?: number | TransactionCallback,
    cb?: TransactionCallback,
  ): this {
    const [timeoutMs, callback] = txnArgs(timeout, cb);
    const consumerNative = extractNative(consumer);
    if (consumerNative === undefined) {
      queueMicrotask(() =>
        callback?.(
          new LibrdKafkaError(
            "bun-rdkafka: sendOffsetsToTransaction() requires a connected consumer",
            { code: ERROR_CODES.ERR__INVALID_ARG, origin: "local", context: "sendOffsetsToTransaction" },
          ),
        ),
      );
      return this;
    }
    this.flushStaging();
    this.runRetriableTxnOp("sendOffsetsToTransaction", timeoutMs, callback, (nc, stepMs) =>
      nc.sendOffsetsToTransaction(offsets, consumerNative, stepMs),
    );
    return this;
  }

  /** Transaction op loop: each step ≤{@link TXN_STEP_MS}, retrying while retriable. */
  private runRetriableTxnOp(
    op: string,
    timeoutMs: number,
    cb: TransactionCallback | undefined,
    call: (nc: NativeClient, stepMs: number) => void,
  ): void {
    const deadline = Date.now() + timeoutMs;
    const attempt = (): void => {
      const nc = this.native;
      if (nc === undefined || !nc.isOpen) {
        cb?.(notConnected(op));
        return;
      }
      const remaining = deadline - Date.now();
      const stepMs = Math.max(1, Math.min(TXN_STEP_MS, remaining));
      try {
        call(nc, stepMs);
        cb?.(null);
      } catch (error) {
        const err = asLibrdKafkaError(error, op);
        const retriable = err.isRetriable || err.code === ERROR_CODES.ERR__TIMED_OUT;
        if (retriable && Date.now() + 1 < deadline) {
          // Yield the event loop between steps so other schedulers/timers run.
          setTimeout(attempt, 0);
          return;
        }
        cb?.(err);
      }
    };
    queueMicrotask(attempt);
  }
}

/* ========================================================================== */
/* Helper                                                                      */
/* ========================================================================== */

function txnArgs(
  timeout: number | TransactionCallback | undefined,
  cb: TransactionCallback | undefined,
): [number, TransactionCallback | undefined] {
  if (typeof timeout === "function") return [DEFAULT_TXN_TIMEOUT_MS, timeout];
  return [timeout ?? DEFAULT_TXN_TIMEOUT_MS, cb];
}

function notConnected(context: string): LibrdKafkaError {
  return new LibrdKafkaError("Producer not connected", {
    code: ERROR_CODES.ERR__STATE,
    origin: "local",
    context,
  });
}

function asLibrdKafkaError(error: unknown, context: string): LibrdKafkaError {
  if (error instanceof LibrdKafkaError) return error;
  return new LibrdKafkaError(error instanceof Error ? error.message : String(error), {
    context,
  });
}

/**
 * Extracts the NativeClient from a consumer: `native` (protected on Client —
 * compile-time only, so readable via indexing) or `client` (SliceConsumer/tests).
 */
function extractNative(consumer: ConsumerLike): NativeClient | undefined {
  const holder = consumer as { native?: NativeClient; client?: NativeClient };
  const nc = holder.native ?? holder.client;
  if (nc === undefined || typeof nc !== "object") return undefined;
  return nc.isOpen ? nc : undefined;
}
