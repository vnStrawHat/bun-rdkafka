/**
 * callback/kafka-consumer.ts — `KafkaConsumer` (M3, FR-1).
 *
 * API + semantics cross-checked against node-rdkafka /
 * confluent-kafka-javascript's `KafkaConsumer` (`lib/kafka-consumer.js`,
 * `types/rdkafka.d.ts`). Internals are free (api-compat-not-internals): no
 * consume-loop thread — every message goes through {@link Client}'s
 * `pollTick()` in the pull model.
 *
 * ## The two-queue constraint (see the top of core/native-client.ts)
 *
 * REBALANCE/OFFSET_COMMIT only surface via `pollEvents()` AFTER
 * `consumeBatch()` is called. Therefore an "active" consumer (subscribed or
 * still holding an assignment) ALWAYS `consumeBatch()`es every poll round —
 * even when the user has not called `consume()` or is paused — and messages
 * are kept in an internal buffer awaiting dispatch:
 *
 *  - buffer full (`MAX_BUFFERED_MESSAGES`) → each round pulls only 1 message,
 *    so rebalancing keeps flowing while memory stays bounded (librdkafka stops
 *    fetching per `queued.max.messages.kbytes`, so the cap is rarely hit);
 *  - paused partitions: still `consumeBatch()` — librdkafka stops fetching
 *    paused partitions so the batches naturally empty out (verified by an
 *    integration test).
 *
 * ## Two consume modes (matching upstream)
 *
 *  - **flowing**: `consume()` / `consume(cb)` — emits `data` per message
 *    (the cb, if given, is additionally called with `(null, message)` for each
 *    message).
 *  - **non-flowing**: `consume(n, cb)` — one grab of up to `n` messages; when
 *    nothing is available, waits until `setDefaultConsumeTimeout()` (default
 *    1000ms) and then returns an empty array.
 */

import { Buffer } from "node:buffer";
import {
  BRK_ASSIGN,
  BRK_ASSIGN_INCREMENTAL,
  BRK_CLIENT_CONSUMER,
  BRK_EVENT_OFFSET_COMMIT,
  BRK_EVENT_REBALANCE,
  BRK_REBALANCE_PROTOCOL_COOPERATIVE,
  BRK_UNASSIGN,
  BRK_UNASSIGN_INCREMENTAL,
} from "../ffi/types.ts";
import type {
  BrkEvent,
  BrkOffsetCommitEvent,
  BrkRebalanceEvent,
  DecodedMessage,
  TopicPartitionEntry,
  TopicPartitionInput,
} from "../core/batch-decoder.ts";
import type { ClientConfig } from "../core/config.ts";
import { ERROR_CODES, LibrdKafkaError } from "../core/errors.ts";
import type { NativeClient } from "../core/native-client.ts";
import { Client, type ClientInternalOptions } from "./client.ts";

/* ========================================================================== */
/* Public types (shapes of upstream's types/rdkafka.d.ts)                      */
/* ========================================================================== */

export interface TopicPartition {
  topic: string;
  partition: number;
  leaderEpoch?: number;
}

export interface TopicPartitionOffset extends TopicPartition {
  offset: number;
}

export interface TopicPartitionOffsetAndMetadata extends TopicPartitionOffset {
  /** Commit metadata — travels with the commit and comes back verbatim via `committed()`. */
  metadata?: string | null;
}

export type Assignment = TopicPartition | TopicPartitionOffset;

/** Each header is a ONE-key object (exactly how node-rdkafka returns them). */
export type MessageHeader = { [key: string]: string | Buffer };

export type MessageValue = Buffer | null;
export type MessageKey = string | Buffer | null;

export interface Message extends TopicPartitionOffset {
  value: MessageValue;
  size: number;
  key?: MessageKey;
  timestamp?: number;
  headers?: MessageHeader[];
  opaque?: unknown;
}

export interface EofEvent extends TopicPartitionOffset {}

export type SubscribeTopicList = string[];

export type ConsumeCallback = (err: LibrdKafkaError | null, messages: Message[]) => void;
export type MessageCallback = (err: LibrdKafkaError | null, message: Message) => void;
export type CommittedCallback = (
  err: LibrdKafkaError | null,
  topicPartitions?: TopicPartitionOffsetAndMetadata[],
) => void;
export type SeekCallback = (err: LibrdKafkaError | null) => void;

export type RebalanceProtocolName = "NONE" | "EAGER" | "COOPERATIVE";

/* ========================================================================== */
/* Constants                                                                   */
/* ========================================================================== */

const DEFAULT_CONSUME_TIMEOUT_MS = 1000;
/** Internal buffer cap; beyond it, each poll round pulls only 1 message. */
const MAX_BUFFERED_MESSAGES = 10_000;
/** Max messages per `brk_consume_batch` call. */
const CONSUME_BATCH_MAX = 500;

/* ========================================================================== */
/* Shape-conversion utilities                                                  */
/* ========================================================================== */

/** Wraps a Uint8Array (already copied out of the C buffer) as a Buffer with NO extra copy. */
function asBuffer(bytes: Uint8Array): Buffer {
  return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

/** `DecodedMessage` (core) → `Message` (upstream shape, field for field). */
function toMessage(m: DecodedMessage): Message {
  const message: Message = {
    value: m.value === null ? null : asBuffer(m.value),
    size: m.value === null ? 0 : m.value.byteLength,
    topic: m.topic,
    partition: m.partition,
    offset: m.offset,
  };
  if (m.key !== null) message.key = asBuffer(m.key);
  if (m.timestamp >= 0) message.timestamp = m.timestamp;
  if (m.leaderEpoch >= 0) message.leaderEpoch = m.leaderEpoch;
  if (m.headers.length > 0) {
    message.headers = m.headers.map((h) => ({
      [h.key]: h.value === null ? Buffer.alloc(0) : asBuffer(h.value),
    }));
  }
  return message;
}

/** C→JS entry → `{topic, partition}` (+leaderEpoch when present). Drops invalid offsets. */
function toTopicPartition(entry: TopicPartitionEntry): TopicPartition {
  const out: TopicPartition = { topic: entry.topic, partition: entry.partition };
  if (entry.leaderEpoch >= 0) out.leaderEpoch = entry.leaderEpoch;
  return out;
}

function toTopicPartitionOffset(entry: TopicPartitionEntry): TopicPartitionOffset {
  return { ...toTopicPartition(entry), offset: entry.offset };
}

function toTopicPartitionOffsetAndMetadata(
  entry: TopicPartitionEntry,
): TopicPartitionOffsetAndMetadata {
  const out: TopicPartitionOffsetAndMetadata = toTopicPartitionOffset(entry);
  if (entry.metadata !== null) out.metadata = entry.metadata;
  return out;
}

/** User-supplied toppar → tpl encoder input (including commit metadata when present). */
function toInput(
  tp: TopicPartition | TopicPartitionOffset | TopicPartitionOffsetAndMetadata,
): TopicPartitionInput {
  const input: TopicPartitionInput = { topic: tp.topic, partition: tp.partition };
  if ("offset" in tp && tp.offset !== undefined) input.offset = tp.offset;
  if (tp.leaderEpoch !== undefined) input.leaderEpoch = tp.leaderEpoch;
  if ("metadata" in tp && tp.metadata !== undefined && tp.metadata !== null) {
    input.metadata = tp.metadata;
  }
  return input;
}

function toInputs(
  tps: TopicPartition | TopicPartitionOffset | readonly (TopicPartition | TopicPartitionOffset)[],
): TopicPartitionInput[] {
  return Array.isArray(tps) ? tps.map(toInput) : [toInput(tps as TopicPartition)];
}

/** A `consume(n, cb)` request waiting to be served. */
interface PendingConsume {
  n: number;
  cb: ConsumeCallback;
  deadline: number;
}

/* ========================================================================== */
/* KafkaConsumer                                                               */
/* ========================================================================== */

export class KafkaConsumer extends Client {
  /** Flowing mode enabled (`consume()` with no args). */
  #flowing = false;
  /** Per-message callback of `consume(cb)` (flowing). */
  #flowingCb: MessageCallback | undefined;
  /** Pending non-flowing requests. */
  readonly #pending: PendingConsume[] = [];
  /** Converted messages awaiting dispatch (see the buffer note at the top of this file). */
  readonly #buffer: Message[] = [];

  #subscribedTopics: SubscribeTopicList | null = null;
  /** Does the consumer still hold an assignment (manual or via rebalance)? */
  #hasAssignment = false;
  /** `assignment_lost` flag from the latest REBALANCE payload (see assignmentLost()). */
  #assignmentLost = false;
  #lastProtocol: RebalanceProtocolName = "NONE";
  #consumeTimeoutMs = DEFAULT_CONSUME_TIMEOUT_MS;

  constructor(
    conf?: ClientConfig,
    topicConf?: ClientConfig,
    internal?: ClientInternalOptions,
  ) {
    super(conf, topicConf, BRK_CLIENT_CONSUMER, internal ?? {});
  }

  /** The native client, with a connectedness check. */
  #nc(op: string): NativeClient {
    const nc = this.native;
    if (nc === undefined || !nc.isOpen) {
      throw new LibrdKafkaError(`bun-rdkafka: ${op}() requires a connected consumer`, {
        code: ERROR_CODES.ERR__STATE,
        origin: "local",
        context: op,
      });
    }
    return nc;
  }

  /* ------------------------------------------------------- subscription */

  subscribe(topics: SubscribeTopicList): this {
    this.#nc("subscribe").subscribe(topics);
    this.#subscribedTopics = [...topics];
    this.scheduler?.kick();
    this.emit("subscribed", topics);
    return this;
  }

  unsubscribe(): this {
    this.#nc("unsubscribe").unsubscribe();
    this.#subscribedTopics = null;
    this.emit("unsubscribed");
    return this;
  }

  subscription(): string[] {
    return this.#nc("subscription").subscription();
  }

  /* ------------------------------------------------------------ consume */

  /** Timeout for `consume(n, cb)` when no messages are available (upstream API). */
  setDefaultConsumeTimeout(timeoutMs: number): void {
    this.#consumeTimeoutMs = timeoutMs;
  }

  consume(): void;
  consume(cb: MessageCallback): void;
  consume(size: number, cb?: ConsumeCallback): void;
  consume(sizeOrCb?: number | MessageCallback, cb?: ConsumeCallback): void {
    this.#nc("consume");
    if (typeof sizeOrCb === "number") {
      // non-flowing: one grab of ≤ n messages.
      this.#pending.push({
        n: sizeOrCb,
        cb: cb ?? (() => {}),
        deadline: Date.now() + this.#consumeTimeoutMs,
      });
    } else {
      this.#flowing = true;
      this.#flowingCb = sizeOrCb;
    }
    this.scheduler?.kick();
  }

  /* --------------------------------------------------------- assignment */

  assign(assignments: readonly Assignment[]): this {
    this.#nc("assign").assign(toInputs(assignments), BRK_ASSIGN);
    this.#refreshHasAssignment();
    return this;
  }

  unassign(): this {
    this.#nc("unassign").assign(null, BRK_UNASSIGN);
    this.#refreshHasAssignment();
    return this;
  }

  incrementalAssign(assignments: readonly Assignment[]): this {
    this.#nc("incrementalAssign").assign(toInputs(assignments), BRK_ASSIGN_INCREMENTAL);
    this.#refreshHasAssignment();
    return this;
  }

  incrementalUnassign(assignments: readonly Assignment[]): this {
    this.#nc("incrementalUnassign").assign(toInputs(assignments), BRK_UNASSIGN_INCREMENTAL);
    this.#refreshHasAssignment();
    return this;
  }

  assignments(): Assignment[] {
    return this.#nc("assignments").assignment().map(toTopicPartition);
  }

  /**
   * Protocol of the latest rebalance ("NONE" before any) — upstream API.
   */
  rebalanceProtocol(): RebalanceProtocolName {
    return this.#lastProtocol;
  }

  /**
   * Whether the current assignment was "lost" (session timeout, fenced…).
   * The value comes from the `assignment_lost` flag of the latest REBALANCE
   * payload (set on REVOKE, cleared once an ASSIGN is handled) — matching
   * upstream's usage pattern: checked inside `rebalance_cb` on revoke.
   */
  assignmentLost(): boolean {
    return this.#assignmentLost;
  }

  /* ------------------------------------------------------------- offset */

  commit(): this;
  commit(
    topicPartition: TopicPartitionOffsetAndMetadata | TopicPartitionOffsetAndMetadata[],
  ): this;
  commit(
    topicPartition?: TopicPartitionOffsetAndMetadata | TopicPartitionOffsetAndMetadata[],
  ): this {
    this.#nc("commit").commit(topicPartition === undefined ? null : toInputs(topicPartition), true);
    return this;
  }

  commitSync(
    topicPartition?: TopicPartitionOffsetAndMetadata | TopicPartitionOffsetAndMetadata[],
  ): this {
    this.#nc("commitSync").commit(
      topicPartition === undefined ? null : toInputs(topicPartition),
      false,
    );
    return this;
  }

  /** Commits `msg.offset + 1` (the next offset to read) — unlike `commit(msg)`, which commits as-is. */
  commitMessage(msg: TopicPartitionOffset): this {
    return this.commit({ topic: msg.topic, partition: msg.partition, offset: msg.offset + 1 });
  }

  commitMessageSync(msg: TopicPartitionOffset): this {
    return this.commitSync({ topic: msg.topic, partition: msg.partition, offset: msg.offset + 1 });
  }

  committed(timeout: number, cb: CommittedCallback): this;
  committed(toppars: TopicPartition[], timeout: number, cb: CommittedCallback): this;
  committed(
    topparsOrTimeout: TopicPartition[] | number,
    timeoutOrCb: number | CommittedCallback,
    cb?: CommittedCallback,
  ): this {
    let toppars: TopicPartition[] | null = null;
    let timeout: number;
    let callback: CommittedCallback;
    if (typeof topparsOrTimeout === "number") {
      timeout = topparsOrTimeout;
      callback = timeoutOrCb as CommittedCallback;
    } else {
      toppars = topparsOrTimeout;
      timeout = timeoutOrCb as number;
      callback = cb as CommittedCallback;
    }
    // Cold path: brk_committed blocks up to `timeout` (broker round-trip) —
    // the same contract as metadata in connect() (design §5.1).
    queueMicrotask(() => {
      try {
        const entries = this.#nc("committed").committed(
          toppars === null ? null : toppars.map(toInput),
          timeout,
        );
        callback(null, entries.map(toTopicPartitionOffsetAndMetadata));
      } catch (error) {
        callback(KafkaConsumer.#asError(error, "committed"));
      }
    });
    return this;
  }

  seek(toppar: TopicPartitionOffset, timeout: number | null, cb: SeekCallback): this {
    queueMicrotask(() => {
      try {
        this.#nc("seek").seek(toppar.topic, toppar.partition, toppar.offset, timeout ?? 0);
        cb(null);
      } catch (error) {
        cb(KafkaConsumer.#asError(error, "seek"));
      }
    });
    return this;
  }

  offsetsStore(topicPartitions: TopicPartitionOffsetAndMetadata[]): this {
    this.#nc("offsetsStore").offsetsStore(topicPartitions.map(toInput));
    return this;
  }

  /* ------------------------------------------------------- flow control */

  pause(topicPartitions: TopicPartition[]): this {
    this.#nc("pause").pauseResume(topicPartitions.map(toInput), false);
    return this;
  }

  resume(topicPartitions: TopicPartition[]): this {
    this.#nc("resume").pauseResume(topicPartitions.map(toInput), true);
    this.scheduler?.kick();
    return this;
  }

  position(toppars?: TopicPartition[]): TopicPartitionOffset[] {
    const all = this.#nc("position").position().map(toTopicPartitionOffset);
    if (toppars === undefined) return all;
    const wanted = new Set(toppars.map((tp) => `${tp.topic}\x00${tp.partition}`));
    return all.filter((tp) => wanted.has(`${tp.topic}\x00${tp.partition}`));
  }

  /** Watermarks from the local cache (no broker round-trip) — may be absent right after connect. */
  getWatermarkOffsets(topic: string, partition: number): { lowOffset: number; highOffset: number } {
    const wm = this.#nc("getWatermarkOffsets").getWatermark(topic, partition);
    return { lowOffset: wm.low, highOffset: wm.high };
  }

  /* ------------------------------------------------------------ disconnect */

  override disconnect(cb?: Parameters<Client["disconnect"]>[0]): this {
    // Settle non-flowing requests before destroying the handle: hand over what
    // is buffered, the rest get an empty array.
    this.#flowing = false;
    this.#flowingCb = undefined;
    const pending = this.#pending.splice(0);
    for (const req of pending) {
      const batch = this.#buffer.splice(0, req.n);
      queueMicrotask(() => req.cb(null, batch));
    }
    this.#buffer.length = 0;
    this.#subscribedTopics = null;
    this.#hasAssignment = false;
    return super.disconnect(cb);
  }

  /* ---------------------------------------------------- poll integration */

  /** An active consumer is never COLD (rebalancing would lag by `js.poll.interval.ms`). */
  protected override isCold(): boolean {
    return !this.#active() && this.#pending.length === 0;
  }

  #active(): boolean {
    return this.#subscribedTopics !== null || this.#hasAssignment;
  }

  protected override pollTick(): number {
    let handled = 0;
    if (this.#active()) {
      const room = MAX_BUFFERED_MESSAGES - this.#buffer.length;
      const want = room <= 0 ? 1 : Math.min(CONSUME_BATCH_MAX, room);
      const nc = this.native;
      if (nc !== undefined && nc.isOpen) {
        const messages = nc.consumeBatch(want, 0);
        handled += messages.length;
        for (const message of messages) this.#route(message);
      }
    }
    handled += this.#servePending();
    if (this.#flowing) handled += this.#drainFlowing();
    return handled;
  }

  /** Classifies one record from `brk_consume_batch`. */
  #route(m: DecodedMessage): void {
    if (m.err === 0) {
      this.#buffer.push(toMessage(m));
      return;
    }
    if (m.err === ERROR_CODES.ERR__PARTITION_EOF) {
      this.emit("partition.eof", {
        topic: m.topic,
        partition: m.partition,
        offset: m.offset,
      } satisfies EofEvent);
      return;
    }
    this.emit(
      "event.error",
      LibrdKafkaError.fromKafkaCode(m.err, undefined, { context: "consume" }),
    );
  }

  /** Serves `consume(n, cb)` requests in FIFO order. */
  #servePending(): number {
    let served = 0;
    const now = Date.now();
    while (this.#pending.length > 0) {
      const req = this.#pending[0] as PendingConsume;
      if (this.#buffer.length === 0) {
        if (now < req.deadline) break; // time remains — wait for the next round
        this.#pending.shift();
        queueMicrotask(() => req.cb(null, []));
        continue;
      }
      this.#pending.shift();
      const batch = this.#buffer.splice(0, req.n);
      served += batch.length;
      queueMicrotask(() => req.cb(null, batch));
    }
    return served;
  }

  /** Flowing: emits `data` per message (+ the per-message cb when set). */
  #drainFlowing(): number {
    let emitted = 0;
    while (this.#flowing) {
      const message = this.#buffer.shift();
      if (message === undefined) break;
      emitted++;
      this.emit("data", message);
      this.#flowingCb?.(null, message);
    }
    return emitted;
  }

  /* -------------------------------------------------------- event frames */

  protected override onEventFrame(event: BrkEvent): void {
    if (event.type === BRK_EVENT_REBALANCE) {
      this.#handleRebalance(event);
      return;
    }
    if (event.type === BRK_EVENT_OFFSET_COMMIT) {
      this.#handleOffsetCommit(event);
      return;
    }
    super.onEventFrame(event);
  }

  #handleRebalance(event: BrkRebalanceEvent): void {
    const cooperative = event.protocol === BRK_REBALANCE_PROTOCOL_COOPERATIVE;
    this.#lastProtocol = cooperative ? "COOPERATIVE" : "EAGER";
    const isAssign = event.code === ERROR_CODES.ERR__ASSIGN_PARTITIONS;
    // Set on revoke (read by the user in rebalance_cb), cleared on a new assign.
    this.#assignmentLost = isAssign ? false : event.assignmentLost;
    const err = LibrdKafkaError.fromKafkaCode(event.code, undefined, { context: "rebalance" });
    const parts = event.partitions.map(toTopicPartition);

    const cb = this.configCallbacks.rebalance_cb;
    if (typeof cb === "function") {
      // The user is responsible for calling (incremental)assign/unassign — matching upstream.
      try {
        cb.call(this, err, parts);
      } catch (error) {
        this.emit("rebalance.error", error);
      }
    } else {
      try {
        if (isAssign) {
          if (cooperative) this.incrementalAssign(parts);
          else this.assign(parts);
        } else if (cooperative) {
          this.incrementalUnassign(parts);
        } else {
          this.unassign();
        }
      } catch (error) {
        this.emit("rebalance.error", error);
      }
    }
    this.emit("rebalance", err, parts);
  }

  #handleOffsetCommit(event: BrkOffsetCommitEvent): void {
    const err =
      event.code === 0
        ? null
        : LibrdKafkaError.fromKafkaCode(event.code, undefined, { context: "offset.commit" });
    const parts = event.partitions.map(toTopicPartitionOffset);
    const cb = this.configCallbacks.offset_commit_cb;
    if (typeof cb === "function") cb.call(this, err, parts);
    this.emit("offset.commit", err, parts);
  }

  /** Refreshes the assignment flag after each assign operation (cold path, 1 FFI call). */
  #refreshHasAssignment(): void {
    try {
      this.#hasAssignment = this.#nc("assignments").assignment().length > 0;
    } catch {
      this.#hasAssignment = false;
    }
  }

  static #asError(error: unknown, context: string): LibrdKafkaError {
    if (error instanceof LibrdKafkaError) return error;
    return new LibrdKafkaError(error instanceof Error ? error.message : String(error), {
      code: ERROR_CODES.ERR__FAIL,
      origin: "local",
      context,
    });
  }
}
