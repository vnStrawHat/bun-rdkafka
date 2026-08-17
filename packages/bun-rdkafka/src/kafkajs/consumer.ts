/**
 * kafkajs/consumer.ts — the promisified Consumer (KafkaJS namespace), FR-2.
 *
 * Built on the callback {@link KafkaConsumer} (composition). API + observable
 * semantics compatible with confluent-kafka-javascript
 * (`lib/kafkajs/_consumer.js` is a semantics reference only — code is NOT
 * ported, see ADR-4b).
 *
 * ## The run() scheduler (designed specifically for Bun — design §7)
 *
 * ```
 *   fetch pump ──consume(n)──▶ per-partition queue ◀──claim── worker × N
 * ```
 *
 *  - **Fetch pump**: a self-chaining `inner.consume(FETCH_CHUNK, cb)` loop,
 *    routing messages into per-partition queues. It stops itself when the
 *    total pending messages exceed {@link MAX_QUEUED_TOTAL} (backpressure);
 *    workers wake it again once below half the cap.
 *  - **Worker pool**: N = `partitionsConsumedConcurrently` workers; each
 *    worker claims one partition EXCLUSIVELY (round-robin) and processes at
 *    most one "turn" (`maxBatchSize` messages or 1 batch) before releasing —
 *    absolute per-partition ordering, parallelism across partitions.
 *  - **Partition epoch**: seek/pause/rebalance/handler-error bumps the
 *    partition's `epoch` and clears its queue; messages carrying an old epoch
 *    are dropped on the spot, and a running handler detects staleness by
 *    comparing epochs — no global cache-stale flag or pending-operation queue
 *    like upstream.
 *
 * ## Offset semantics (matching upstream, at-least-once by default)
 *
 *  - `enable.auto.offset.store` is forced to `false` (regardless of config):
 *    this layer stores `offset + 1` itself AFTER the handler succeeds;
 *    librdkafka auto-commits the stored offsets per `auto.commit.interval.ms`
 *    (when autoCommit is on).
 *  - eachMessage throw → seek back to that exact offset (the message is
 *    redelivered), the partition is NOT auto-paused — like upstream.
 *  - eachBatch: `resolveOffset(o)` stores `o + 1` immediately;
 *    `eachBatchAutoResolve` (default true) resolves the last offset when the
 *    payload is not stale; whatever is unresolved when the handler
 *    finishes/throws → seek back to `lastResolved + 1`.
 *  - The payload's `pause()`: pauses the partition + seeks to the next
 *    unprocessed offset; a message mid-processing does NOT count as processed
 *    (redelivered after `resume()` — the same mechanism as upstream).
 *  - `storeOffsets()`: the user's stored offset becomes the partition's
 *    "next unprocessed" reference (`storedNext`) — pause()/rewind seek there —
 *    until the scheduler stores again after the next successfully processed
 *    message (which then takes over, exactly like a `commitOffsets` after a
 *    manual store would). librdkafka itself keeps only the latest store per
 *    partition, so the two sources never conflict at commit time.
 *
 * ## Rebalance callback (upstream `#rebalanceCallback` semantics)
 *
 * This layer installs its own `rebalance_cb` on the callback consumer, so the
 * callback layer no longer assigns by itself. The user's `rebalance_cb` (from
 * the config) is awaited as `cb(err, assignment, {assign, unassign,
 * assignmentLost})`: calling `assignmentFns.assign(x)` uses `x` (EAGER →
 * assign, COOPERATIVE → incrementalAssign) and skips the default; returning a
 * truthy alternate assignment uses it (and skips pending seeks); a throwing
 * user cb is logged and the default behavior continues. Pending seeks are
 * folded into the assign call as offsets (no separate seek round-trip). The
 * callback layer emits `rebalance` on the internal client right after the cb
 * returns — i.e. before the (async) assign lands, unlike upstream (after).
 * Partition epochs are bumped synchronously on revoke, before any await.
 *
 * ## Deliberate upstream deviations
 *
 *  - `stop()`: upstream throws notImplemented; we implement it for real (stops
 *    run(), keeps the connection, allows run() again) — a safe superset.
 *  - `disconnect()`/`stop()` must NOT be called from inside
 *    eachMessage/eachBatch (the same upstream constraint; here it deadlocks
 *    instead of throwing — documented).
 */

import { Buffer } from "node:buffer";
import {
  KafkaConsumer,
  type Assignment as CbAssignment,
  type Message as CbMessage,
  type MessageHeader as CbMessageHeader,
  type TopicPartition as CbTopicPartition,
  type TopicPartitionOffsetAndMetadata as CbTopicPartitionOffsetAndMetadata,
} from "../callback/kafka-consumer.ts";
import type { ClientConfig } from "../core/config.ts";
import { ERROR_CODES, LibrdKafkaError } from "../core/errors.ts";
import {
  createBindingMessageMetadata,
  loggerTrampoline,
  mapCommonConfig,
  mapConsumerConfig,
  resolveLogger,
  type CommonRawConfig,
  type Logger,
  type LogMessage,
} from "./config-mapper.ts";
import { KafkaJSError, fromLibrdKafkaError } from "./errors.ts";
import { Admin } from "./admin.ts";

/* ========================================================================== */
/* Public types (KafkaJS shapes)                                               */
/* ========================================================================== */

export interface TopicPartitionOffset {
  topic: string;
  partition: number;
  offset: string | number;
  /** Commit metadata (KafkaJS-compatible) — round-trips through committed offsets. */
  metadata?: string | null;
}

export interface TopicPartition {
  topic: string;
  partition: number;
}

/** Input of storeOffsets() / result of committed() (KafkaJS shape, string offsets). */
export interface TopicPartitionOffsetAndMetadata {
  topic: string;
  partition: number;
  offset: string | number;
  leaderEpoch?: number | null;
  metadata?: string | null;
}

/** committed() result entry — always fully populated (upstream shape). */
export interface CommittedOffset {
  topic: string;
  partition: number;
  offset: string;
  leaderEpoch: number | null;
  metadata: string | null;
}

/**
 * The third argument of a KafkaJS `rebalance_cb` (upstream `assignmentFns`):
 * lets the callback take over the (un)assignment for this rebalance.
 */
export interface AssignmentFns {
  /** Assigns exactly `assignment` (EAGER → assign, COOPERATIVE → incrementalAssign) instead of the default. */
  assign: (assignment: RebalanceAssignment[]) => void;
  /** Unassigns instead of the default (EAGER → unassign all, COOPERATIVE → incrementalUnassign). */
  unassign: (assignment: RebalanceAssignment[]) => void;
  /** Whether the current assignment was lost (session timeout, fenced…). */
  assignmentLost: () => boolean;
}

/** An entry of the rebalance assignment — the user may add `offset` before assigning. */
export interface RebalanceAssignment {
  topic: string;
  partition: number;
  offset?: number;
  leaderEpoch?: number;
}

export type RebalanceCallback = (
  err: LibrdKafkaError,
  assignment: RebalanceAssignment[],
  assignmentFns: AssignmentFns,
) => unknown;

/**
 * Input of pause()/resume(): the KafkaJS shape `{topic, partitions?: number[]}`
 * OR an assignment() entry `{topic, partition}` (upstream's example passes
 * `consumer.pause(consumer.assignment())` directly).
 */
export interface TopicPartitions {
  topic: string;
  partitions?: number[];
  partition?: number;
}

export interface KafkaJSHeaders {
  [key: string]: Buffer | string | (Buffer | string)[] | undefined;
}

export interface KafkaJSMessage {
  key: Buffer | null;
  value: Buffer | null;
  timestamp: string;
  attributes: number;
  offset: string;
  size: number;
  headers?: KafkaJSHeaders;
  leaderEpoch?: number;
}

export interface EachMessagePayload {
  topic: string;
  partition: number;
  message: KafkaJSMessage;
  heartbeat: () => Promise<void>;
  pause: () => () => void;
}

export interface Batch {
  topic: string;
  partition: number;
  highWatermark: string;
  messages: KafkaJSMessage[];
  isEmpty: () => boolean;
  firstOffset: () => string | null;
  lastOffset: () => string;
  offsetLag: () => string;
  offsetLagLow: () => string;
}

export interface EachBatchPayload {
  batch: Batch;
  resolveOffset: (offset: string | number) => void;
  heartbeat: () => Promise<void>;
  isRunning: () => boolean;
  isStale: () => boolean;
  pause: () => () => void;
  commitOffsetsIfNecessary: () => Promise<void>;
}

export type EachMessageHandler = (payload: EachMessagePayload) => Promise<void> | void;
export type EachBatchHandler = (payload: EachBatchPayload) => Promise<void> | void;

export interface ConsumerRunConfig {
  eachMessage?: EachMessageHandler;
  eachBatch?: EachBatchHandler;
  eachBatchAutoResolve?: boolean;
  partitionsConsumedConcurrently?: number;
}

export interface ConsumerSubscribeTopics {
  topics: (string | RegExp)[];
  replace?: boolean;
}

interface MappedConsumerConfig {
  conf: ClientConfig;
  topicConf: ClientConfig;
  autoCommit: boolean;
  maxBatchSize: number;
  rebalanceCb: RebalanceCallback | undefined;
  logger: Logger;
}

/** kafkaJS block → librdkafka props (M5a's mapper) + this layer's own options. */
function mapConfig(raw: unknown): MappedConsumerConfig {
  if (raw === null || typeof raw !== "object") {
    throw new KafkaJSError("bun-rdkafka: the consumer config must be an object");
  }
  const rawConfig = raw as CommonRawConfig;
  const mapped = mapConsumerConfig(rawConfig, mapCommonConfig(rawConfig));
  const conf: ClientConfig = { ...mapped.globalConf };
  const topicConf: ClientConfig = { ...mapped.topicConf };
  const logger = resolveLogger(rawConfig, mapped);

  // KafkaJS layer: a `rebalance_cb` in the config is the user's hook (see the
  // "Rebalance callback" section at the top) — this layer owns the actual
  // assign/unassign calls, matching upstream _consumer.js. Pulled out of the
  // conf; the layer installs its own trampoline in the constructor.
  const rebalanceCb = conf["rebalance_cb"];
  delete conf["rebalance_cb"];

  const rawAuto = conf["enable.auto.commit"];
  const autoCommit =
    rawAuto === undefined ? true : !(rawAuto === false || rawAuto === "false");
  const rawBatch = Number(conf["js.consumer.max.batch.size"]);
  const maxBatchSize = Number.isFinite(rawBatch) && rawBatch > 0 ? rawBatch : 32;

  // This layer's invariant (see the doc at the top): the JS layer stores
  // offsets itself after the handler succeeds — always forced, even when the
  // user/mapper set otherwise.
  conf["enable.auto.commit"] = autoCommit;
  conf["enable.auto.offset.store"] = false;

  return {
    conf,
    topicConf,
    autoCommit,
    maxBatchSize,
    rebalanceCb: typeof rebalanceCb === "function" ? (rebalanceCb as RebalanceCallback) : undefined,
    logger,
  };
}

/* ========================================================================== */
/* Internals                                                                   */
/* ========================================================================== */

/** Max messages per fetch-pump `inner.consume()` call. */
const FETCH_CHUNK = 500;
/** Cap on total messages pending across the per-partition queues (backpressure). */
const MAX_QUEUED_TOTAL = 10_000;

interface QueuedEntry {
  msg: CbMessage;
  epoch: number;
}

interface PartitionState {
  readonly key: string;
  readonly topic: string;
  readonly partition: number;
  /** Bumped on seek/pause/rebalance/handler-error — invalidates all older messages. */
  epoch: number;
  queue: QueuedEntry[];
  /** A worker currently holds this partition exclusively. */
  claimed: boolean;
  pausedByUser: boolean;
  /** Next STORED offset (last successfully processed offset + 1); -1 = none yet. */
  storedNext: number;
}

function partKey(topic: string, partition: number): string {
  return `${topic} ${partition}`;
}

const noopHeartbeat = async (): Promise<void> => {};

/** Callback-shape headers (array of one-key objects) → a KafkaJS object (dups → array). */
function toKafkaJSHeaders(headers: CbMessageHeader[] | undefined): KafkaJSHeaders | undefined {
  if (headers === undefined || headers.length === 0) return undefined;
  const out: KafkaJSHeaders = {};
  for (const header of headers) {
    for (const [key, value] of Object.entries(header)) {
      const prev = out[key];
      if (prev === undefined) out[key] = value;
      else if (Array.isArray(prev)) prev.push(value);
      else out[key] = [prev, value];
    }
  }
  return out;
}

function toKafkaJSMessage(m: CbMessage): KafkaJSMessage {
  const message: KafkaJSMessage = {
    key: m.key === undefined || m.key === null ? null : Buffer.isBuffer(m.key) ? m.key : Buffer.from(m.key),
    value: m.value,
    timestamp: m.timestamp === undefined ? "" : String(m.timestamp),
    attributes: 0,
    offset: String(m.offset),
    size: m.size,
  };
  const headers = toKafkaJSHeaders(m.headers);
  if (headers !== undefined) message.headers = headers;
  if (m.leaderEpoch !== undefined) message.leaderEpoch = m.leaderEpoch;
  return message;
}

/** Normalizes `[{topic, partitions?}]` → a concrete partition list. */
function expandTopicPartitions(
  input: TopicPartitions[],
  assigned: CbTopicPartition[],
): CbTopicPartition[] {
  const out: CbTopicPartition[] = [];
  for (const spec of input) {
    if (spec.partitions !== undefined) {
      for (const partition of spec.partitions) out.push({ topic: spec.topic, partition });
    } else if (spec.partition !== undefined) {
      out.push({ topic: spec.topic, partition: spec.partition });
    } else {
      for (const tp of assigned) if (tp.topic === spec.topic) out.push(tp);
    }
  }
  return out;
}

/* ========================================================================== */
/* Consumer                                                                    */
/* ========================================================================== */

/**
 * Internal test option: inject a fake KafkaConsumer — either an instance, or a
 * factory receiving the mapped confs (so a fake can capture the `rebalance_cb`
 * trampoline this layer installs).
 */
export interface ConsumerInternalOptions {
  inner?: KafkaConsumer | ((conf: ClientConfig, topicConf: ClientConfig) => KafkaConsumer);
}

export class Consumer {
  readonly #inner: KafkaConsumer;
  readonly #autoCommit: boolean;
  readonly #maxBatchSize: number;
  readonly #logger: Logger;
  readonly #userRebalanceCb: RebalanceCallback | undefined;

  #connected = false;
  #running = false;
  #runEpoch = 0;

  #eachMessage: EachMessageHandler | undefined;
  #eachBatch: EachBatchHandler | undefined;
  #eachBatchAutoResolve = true;
  #concurrency = 1;

  /** partition key → state. Holds only currently assigned partitions. */
  readonly #parts = new Map<string, PartitionState>();
  /** Round-robin claim order. */
  #claimCursor = 0;
  #queuedTotal = 0;
  /** Seeks for not-yet-assigned partitions — applied on rebalance (upstream semantics). */
  readonly #pendingSeeks = new Map<string, number>();

  #pumpActive = false;
  #workers: Promise<void>[] = [];
  #waiters: (() => void)[] = [];
  #wakeVersion = 0;

  #subscribedSpecs: string[] = [];

  constructor(rawMergedConfig: unknown, internal?: ConsumerInternalOptions) {
    const mapped = mapConfig(rawMergedConfig);
    this.#autoCommit = mapped.autoCommit;
    this.#maxBatchSize = mapped.maxBatchSize;
    this.#logger = mapped.logger;
    this.#userRebalanceCb = mapped.rebalanceCb;
    // This layer owns (un)assignment — see "Rebalance callback" at the top.
    mapped.conf["rebalance_cb"] = (err: LibrdKafkaError, parts: CbTopicPartition[]) => {
      this.#rebalanceCallback(err, parts).catch((error: unknown) => {
        this.#logger.error(`Error from rebalance callback: ${String(error)}`, this.#metadata());
      });
    };
    const injected = internal?.inner;
    this.#inner =
      injected === undefined
        ? new KafkaConsumer(mapped.conf, mapped.topicConf)
        : typeof injected === "function"
          ? injected(mapped.conf, mapped.topicConf)
          : injected;
    this.#inner.on("event.log", (msg: LogMessage) => loggerTrampoline(msg, this.#logger));
    this.#inner.on("event.error", (err: LibrdKafkaError) => {
      this.#logger.error(`Error: ${err.message}`, this.#metadata());
    });
  }

  #metadata(): object {
    return createBindingMessageMetadata(this.#inner.name);
  }

  /** @internal The underlying Callback-API client (null before connect). */
  _getInternalClient(): KafkaConsumer | null {
    return this.#connected ? this.#inner : null;
  }

  /** The logger of this consumer (default logger, or the `kafkaJS.logger` given in the config). */
  logger(): Logger {
    return this.#logger;
  }

  /**
   * An admin client riding this consumer's connection (the consumer must be
   * connected before the admin's connect(); shares this consumer's logger).
   */
  dependentAdmin(): Admin {
    return new Admin(null, this);
  }

  /* -------------------------------------------------------------- lifecycle */

  async connect(): Promise<void> {
    if (this.#connected) return;
    await new Promise<void>((resolve, reject) => {
      this.#inner.connect(undefined, (err) => {
        if (err) reject(fromLibrdKafkaError(err));
        else resolve();
      });
    });
    this.#connected = true;
    this.#logger.info("Consumer connected", this.#metadata());
  }

  /**
   * Stops run() (waits for running handlers to finish), commits the stored
   * offsets one last time, and disconnects. Must NOT be called from inside
   * eachMessage/eachBatch (deadlock — the same constraint as upstream).
   */
  async disconnect(): Promise<void> {
    await this.#haltRun();
    if (!this.#connected) return;
    if (this.#autoCommit) this.#commitStoredBestEffort();
    this.#connected = false;
    await new Promise<void>((resolve) => {
      // A failed disconnect still counts as disconnected — nothing more the user can do.
      this.#inner.disconnect(() => resolve());
    });
    this.#logger.info("Consumer disconnected", this.#metadata());
  }

  /** Stops run() but keeps the connection; run() may be called again (upstream superset). */
  async stop(): Promise<void> {
    await this.#haltRun();
  }

  async #haltRun(): Promise<void> {
    if (!this.#running) return;
    this.#running = false;
    this.#runEpoch++;
    this.#signal();
    const workers = this.#workers;
    this.#workers = [];
    await Promise.allSettled(workers);
    for (const state of this.#parts.values()) {
      state.epoch++;
      state.queue = [];
    }
    this.#queuedTotal = 0;
  }

  /** Commits the stored offsets (best-effort, used at disconnect). */
  #commitStoredBestEffort(): void {
    try {
      // NULL tpl → librdkafka fills in the current assignment's stored offsets.
      this.#inner.commitSync();
    } catch (error) {
      if (error instanceof LibrdKafkaError && error.code === ERROR_CODES.ERR__NO_OFFSET) return;
      // Never throw from disconnect — logging suffices (offsets get redelivered, at-least-once).
      this.#logger.error(`Commit during disconnect failed: ${String(error)}`, this.#metadata());
    }
  }

  /* ------------------------------------------------------------ subscribe */

  async subscribe(subscription: ConsumerSubscribeTopics): Promise<void> {
    if (!this.#connected) {
      throw new KafkaJSError("bun-rdkafka: subscribe() requires a connected consumer");
    }
    const specs: string[] = [];
    for (const topic of subscription.topics) {
      if (topic instanceof RegExp) {
        if (topic.flags !== "") {
          throw new KafkaJSError("bun-rdkafka: RegExp topics do not support flags");
        }
        if (!topic.source.startsWith("^")) {
          throw new KafkaJSError('bun-rdkafka: RegExp topics must start with "^"');
        }
        specs.push(topic.source);
      } else {
        specs.push(topic);
      }
    }
    this.#subscribedSpecs = subscription.replace
      ? specs
      : [...new Set([...this.#subscribedSpecs, ...specs])];
    this.#inner.subscribe(this.#subscribedSpecs);
  }

  /* ------------------------------------------------------------------ run */

  async run(config: ConsumerRunConfig = {}): Promise<void> {
    if (this.#running) throw new KafkaJSError("Consumer is already running.");
    if (config.eachMessage === undefined && config.eachBatch === undefined) {
      throw new KafkaJSError("bun-rdkafka: run() requires eachMessage or eachBatch");
    }
    this.#eachMessage = config.eachMessage;
    this.#eachBatch = config.eachMessage === undefined ? config.eachBatch : undefined;
    this.#eachBatchAutoResolve = config.eachBatchAutoResolve ?? true;
    this.#concurrency = Math.max(1, config.partitionsConsumedConcurrently ?? 1);

    this.#running = true;
    const runEpoch = ++this.#runEpoch;
    this.#pump(runEpoch);
    this.#workers = Array.from({ length: this.#concurrency }, () => this.#worker(runEpoch));
  }

  /* ----------------------------------------------------- offset / seeking */

  async commitOffsets(offsets?: TopicPartitionOffset[]): Promise<void> {
    if (!this.#connected) {
      throw new KafkaJSError("bun-rdkafka: commitOffsets() requires a connected consumer");
    }
    await new Promise<void>((resolve, reject) => {
      queueMicrotask(() => {
        try {
          if (offsets === undefined) this.#inner.commitSync();
          else {
            this.#inner.commitSync(
              offsets.map((o) => ({
                topic: o.topic,
                partition: o.partition,
                offset: Number(o.offset),
                ...(o.metadata !== undefined && o.metadata !== null
                  ? { metadata: o.metadata }
                  : {}),
              })),
            );
          }
          resolve();
        } catch (error) {
          if (error instanceof LibrdKafkaError && error.code === ERROR_CODES.ERR__NO_OFFSET) {
            resolve(); // nothing to commit — matching upstream (not an error)
            return;
          }
          reject(
            error instanceof LibrdKafkaError
              ? fromLibrdKafkaError(error)
              : (error as Error),
          );
        }
      });
    });
  }

  /**
   * Fetches the committed offsets of `topicPartitions` (default: the current
   * assignment). `timeout` -1 = infinite (upstream default). Cold path: the
   * broker round-trip blocks like the callback layer's committed().
   */
  async committed(
    topicPartitions: TopicPartition[] | null = null,
    timeout = -1,
  ): Promise<CommittedOffset[]> {
    if (!this.#connected) {
      throw new KafkaJSError("Committed can only be called while connected.", {
        code: ERROR_CODES.ERR__STATE,
      });
    }
    const targets: CbTopicPartition[] = (topicPartitions ?? this.assignment()).map((tp) => ({
      topic: tp.topic,
      partition: tp.partition,
    }));
    return new Promise((resolve, reject) => {
      this.#inner.committed(targets, timeout, (err, offsets) => {
        if (err) {
          reject(fromLibrdKafkaError(err));
          return;
        }
        resolve(
          (offsets ?? []).map((o) => ({
            topic: o.topic,
            partition: o.partition,
            offset: String(o.offset),
            leaderEpoch: o.leaderEpoch ?? null,
            metadata: o.metadata ?? null,
          })),
        );
      });
    });
  }

  /**
   * Stores offsets for the next (auto or manual) commit — `offset` is the
   * NEXT offset to read, exactly as for commitOffsets(). Only assigned
   * partitions may be stored (ERR__STATE otherwise). Interplay with run() is
   * described in the "Offset semantics" section at the top of this file.
   */
  storeOffsets(topicPartitions: TopicPartitionOffsetAndMetadata[]): void {
    if (!this.#connected) {
      throw new KafkaJSError("storeOffsets can only be called while connected.", {
        code: ERROR_CODES.ERR__STATE,
      });
    }
    if (!Array.isArray(topicPartitions)) {
      throw new KafkaJSError("storeOffsets requires an array of {topic, partition, offset}.", {
        code: ERROR_CODES.ERR__INVALID_ARG,
      });
    }
    const entries: CbTopicPartitionOffsetAndMetadata[] = topicPartitions.map((tpo) => {
      const offset = Number(tpo.offset);
      if (typeof tpo.topic !== "string" || !Number.isInteger(tpo.partition) || !Number.isFinite(offset)) {
        throw new KafkaJSError("storeOffsets: each entry needs {topic: string, partition: number, offset}.", {
          code: ERROR_CODES.ERR__INVALID_ARG,
        });
      }
      const entry: CbTopicPartitionOffsetAndMetadata = { topic: tpo.topic, partition: tpo.partition, offset };
      if (typeof tpo.leaderEpoch === "number") entry.leaderEpoch = tpo.leaderEpoch;
      if (typeof tpo.metadata === "string") entry.metadata = tpo.metadata;
      return entry;
    });
    if (entries.length === 0) return;
    try {
      this.#inner.offsetsStore(entries);
    } catch (error) {
      throw error instanceof LibrdKafkaError ? fromLibrdKafkaError(error) : error;
    }
    for (const entry of entries) {
      const state = this.#parts.get(partKey(entry.topic, entry.partition));
      if (state !== undefined) state.storedNext = entry.offset;
    }
  }

  /**
   * Seeks the partition to `offset`. Not-yet-assigned partition → remembered
   * and applied when a rebalance grants the partition (upstream semantics).
   * With autoCommit on → the seeked offset is committed immediately (upstream
   * semantics).
   */
  seek(tpo: TopicPartitionOffset): void {
    if (!this.#connected) {
      throw new KafkaJSError("bun-rdkafka: seek() requires a connected consumer");
    }
    const offset = Number(tpo.offset);
    const key = partKey(tpo.topic, tpo.partition);
    const state = this.#parts.get(key);
    if (state === undefined) {
      this.#pendingSeeks.set(key, offset);
      return;
    }
    this.#rewind(state, offset);
    state.storedNext = offset;
    if (this.#autoCommit) {
      try {
        this.#inner.commit({ topic: tpo.topic, partition: tpo.partition, offset });
      } catch {
        /* best-effort async commit — the result arrives via the offset.commit event */
      }
    }
  }

  /* --------------------------------------------------------- flow control */

  /**
   * Pauses the partitions; returns a function resuming exactly the paused
   * partitions (upstream: `const resume = consumer.pause([...])`).
   */
  pause(topicPartitions: TopicPartitions[]): () => void {
    if (!this.#connected) {
      throw new KafkaJSError("Pause can only be called while connected.", {
        code: ERROR_CODES.ERR__STATE,
      });
    }
    const targets = expandTopicPartitions(topicPartitions, this.#assignedList());
    const resumeFn = (): void => {
      this.resume(targets.map((tp) => ({ topic: tp.topic, partitions: [tp.partition] })));
    };
    if (targets.length === 0) return resumeFn;
    this.#logger.debug(`Pausing ${targets.length} partition(s)`, this.#metadata());
    this.#inner.pause(targets);
    for (const tp of targets) {
      const state = this.#parts.get(partKey(tp.topic, tp.partition));
      if (state === undefined) continue;
      state.pausedByUser = true;
      // Fetched-but-unprocessed messages get dropped — seek to the next
      // unprocessed offset so resume() continues at the right spot.
      const next = this.#nextUnprocessedOffset(state);
      if (next !== null) this.#rewind(state, next);
      else state.epoch++; // still invalidates the running handler (stale)
    }
    return resumeFn;
  }

  resume(topicPartitions: TopicPartitions[]): void {
    if (!this.#connected) {
      throw new KafkaJSError("Resume can only be called while connected.", {
        code: ERROR_CODES.ERR__STATE,
      });
    }
    const targets = expandTopicPartitions(topicPartitions, this.#assignedList());
    if (targets.length === 0) return;
    this.#logger.debug(`Resuming ${targets.length} partition(s)`, this.#metadata());
    this.#inner.resume(targets);
    for (const tp of targets) {
      const state = this.#parts.get(partKey(tp.topic, tp.partition));
      if (state !== undefined) state.pausedByUser = false;
    }
    this.#signal();
  }

  /** KafkaJS shape: grouped by topic. */
  paused(): TopicPartitions[] {
    const byTopic = new Map<string, number[]>();
    for (const state of this.#parts.values()) {
      if (!state.pausedByUser) continue;
      const list = byTopic.get(state.topic) ?? [];
      list.push(state.partition);
      byTopic.set(state.topic, list);
    }
    return [...byTopic.entries()].map(([topic, partitions]) => ({
      topic,
      partitions: partitions.sort((a, b) => a - b),
    }));
  }

  assignment(): TopicPartition[] {
    if (!this.#connected) return [];
    return this.#assignedList().map((tp) => ({ topic: tp.topic, partition: tp.partition }));
  }

  #assignedList(): CbTopicPartition[] {
    try {
      return this.#inner.assignments();
    } catch {
      return [];
    }
  }

  /* ============================== scheduler ============================== */

  /** Fetch pump: a self-chaining consume(n) loop, stopping on backpressure/halt. */
  #pump(runEpoch: number): void {
    if (!this.#running || runEpoch !== this.#runEpoch || this.#pumpActive) return;
    if (this.#queuedTotal >= MAX_QUEUED_TOTAL) return; // a worker calls again once drained
    this.#pumpActive = true;
    try {
      this.#inner.consume(FETCH_CHUNK, (err, messages) => {
        this.#pumpActive = false;
        if (!this.#running || runEpoch !== this.#runEpoch) return;
        if (err) this.#logger.error(`consume() inside run() failed: ${err.message}`, this.#metadata());
        else if (messages.length > 0) {
          for (const msg of messages) this.#route(msg);
          this.#signal();
        }
        this.#pump(runEpoch);
      });
    } catch (error) {
      this.#pumpActive = false;
      this.#logger.error(`Fetch pump stopped: ${String(error)}`, this.#metadata());
    }
  }

  #route(msg: CbMessage): void {
    const key = partKey(msg.topic, msg.partition);
    let state = this.#parts.get(key);
    if (state === undefined) {
      // The message arrived before we saw the rebalance-assign (the default
      // assign runs in the callback layer before the event surfaces) — create
      // the state on the spot.
      state = this.#ensurePart(msg.topic, msg.partition);
    }
    state.queue.push({ msg, epoch: state.epoch });
    this.#queuedTotal++;
  }

  /**
   * Creates the partition state if missing. `startOffset` (a real offset the
   * partition was assigned at — pending seek or a user-modified assignment)
   * seeds `storedNext`, exactly like seek() does.
   */
  #ensurePart(topic: string, partition: number, startOffset?: number): PartitionState {
    const key = partKey(topic, partition);
    let state = this.#parts.get(key);
    if (state === undefined) {
      state = {
        key,
        topic,
        partition,
        epoch: 0,
        queue: [],
        claimed: false,
        pausedByUser: false,
        storedNext: -1,
      };
      this.#parts.set(key, state);
    }
    if (startOffset !== undefined && startOffset >= 0) state.storedNext = startOffset;
    return state;
  }

  /** The partition's next unprocessed offset (null when undeterminable). */
  #nextUnprocessedOffset(state: PartitionState): number | null {
    if (state.storedNext >= 0) return state.storedNext;
    const first = state.queue.find((e) => e.epoch === state.epoch);
    return first === undefined ? null : first.msg.offset;
  }

  /** Bumps the epoch, clears the queue, seeks the partition to `offset`. */
  #rewind(state: PartitionState, offset: number): void {
    state.epoch++;
    this.#queuedTotal -= state.queue.length;
    state.queue = [];
    this.#seekWithRetry(state, offset, state.epoch, 0);
  }

  /**
   * Seek via the callback layer with timeout 0 → `rd_kafka_seek_partitions`
   * returning ERR__IN_PROGRESS means "started, completing asynchronously" —
   * SUCCESS, not an error. ERR__PREV_IN_PROGRESS (a previous seek still in
   * flight) → retry with an epoch guard: only while no newer rewind has
   * replaced this seek's target.
   */
  #seekWithRetry(state: PartitionState, offset: number, epochAtSeek: number, attempt: number): void {
    this.#inner.seek({ topic: state.topic, partition: state.partition, offset }, null, (err) => {
      if (err === null || err.code === ERROR_CODES.ERR__IN_PROGRESS) return;
      if (
        err.code === ERROR_CODES.ERR__PREV_IN_PROGRESS &&
        attempt < 40 &&
        state.epoch === epochAtSeek
      ) {
        setTimeout(() => this.#seekWithRetry(state, offset, epochAtSeek, attempt + 1), 25);
        return;
      }
      if (state.epoch === epochAtSeek) {
        this.#logger.error(`Seek ${state.key}@${offset} failed: ${err.message}`, this.#metadata());
      }
    });
  }

  /* ----------------------------------------------------------- rebalance */

  /**
   * The `rebalance_cb` trampoline installed on the callback consumer — see
   * "Rebalance callback" at the top of this file for the semantics.
   */
  async #rebalanceCallback(err: LibrdKafkaError, parts: CbTopicPartition[]): Promise<void> {
    const isAssign = err.code === ERROR_CODES.ERR__ASSIGN_PARTITIONS;
    const isLost = this.#inner.assignmentLost();
    let assignment: RebalanceAssignment[] = parts.map((tp) => ({ ...tp }));
    let assignmentFnCalled = false;
    let assignmentModified = false;

    // REVOKE bookkeeping runs synchronously (before any await): running
    // handlers go stale and queued messages are dropped right away.
    if (!isAssign) this.#forgetPartitions(parts);

    this.#logger.info(
      `Received rebalance event with message: '${err.message}' and ${parts.length} partition(s), isLost: ${isLost}`,
      this.#metadata(),
    );

    const assignFn = (userAssignment: RebalanceAssignment[]): void => {
      if (assignmentFnCalled) return;
      assignmentFnCalled = true;
      const list = userAssignment as CbAssignment[];
      if (this.#inner.rebalanceProtocol() === "COOPERATIVE") this.#inner.incrementalAssign(list);
      else this.#inner.assign(list);
      for (const tp of userAssignment) this.#ensurePart(tp.topic, tp.partition, tp.offset);
      this.#signal();
    };
    const unassignFn = (userAssignment: RebalanceAssignment[]): void => {
      if (assignmentFnCalled) return;
      assignmentFnCalled = true;
      this.#forgetPartitions(userAssignment);
      if (this.#inner.rebalanceProtocol() === "COOPERATIVE") {
        this.#inner.incrementalUnassign(userAssignment as CbAssignment[]);
      } else {
        this.#inner.unassign();
      }
    };

    try {
      const userCb = this.#userRebalanceCb;
      if (userCb !== undefined) {
        const assignmentFns: AssignmentFns = {
          assign: assignFn,
          unassign: unassignFn,
          assignmentLost: () => isLost,
        };
        let alternate: unknown = null;
        try {
          alternate = await userCb(err, assignment, assignmentFns);
        } catch (error) {
          this.#logger.error(
            `Error from user's rebalance callback: ${error instanceof Error ? (error.stack ?? error.message) : String(error)}, ` +
              "continuing with the default rebalance behavior.",
            this.#metadata(),
          );
        }
        if (alternate) {
          assignment = alternate as RebalanceAssignment[];
          assignmentModified = true;
        }
      } else if (!isAssign && err.code !== ERROR_CODES.ERR__REVOKE_PARTITIONS) {
        throw new Error(`Unexpected rebalance_cb error code ${err.code}`);
      }
    } finally {
      try {
        if (isAssign) {
          if (this.#pendingSeeks.size > 0 && !assignmentModified && !assignmentFnCalled) {
            assignment = this.#applyPendingSeeks(assignment);
          }
          assignFn(assignment);
        } else {
          unassignFn(assignment);
        }
      } catch (error) {
        // A disconnect racing the rebalance is not an error worth reporting.
        if (this.#inner.isConnected()) this.#inner.emit("rebalance.error", error);
      }
    }
  }

  /** Folds pending seeks into the assignment as start offsets (consumed here). */
  #applyPendingSeeks(assignment: RebalanceAssignment[]): RebalanceAssignment[] {
    for (const tp of assignment) {
      const key = partKey(tp.topic, tp.partition);
      const offset = this.#pendingSeeks.get(key);
      if (offset === undefined) continue;
      this.#pendingSeeks.delete(key);
      tp.offset = offset;
    }
    return assignment;
  }

  /** REVOKE bookkeeping: bump the epoch (invalidating running handlers), drop the state. */
  #forgetPartitions(parts: readonly TopicPartition[]): void {
    for (const tp of parts) {
      const key = partKey(tp.topic, tp.partition);
      const state = this.#parts.get(key);
      if (state === undefined) continue;
      state.epoch++;
      this.#queuedTotal -= state.queue.length;
      state.queue = [];
      this.#parts.delete(key);
    }
  }

  /* ------------------------------------------------------------- workers */

  #signal(): void {
    this.#wakeVersion++;
    const waiters = this.#waiters.splice(0);
    for (const wake of waiters) wake();
  }

  async #waitSignal(): Promise<void> {
    await new Promise<void>((resolve) => this.#waiters.push(resolve));
  }

  /** Claims the next partition with work (round-robin, exclusive). */
  #claimNext(): PartitionState | undefined {
    const states = [...this.#parts.values()];
    if (states.length === 0) return undefined;
    for (let i = 0; i < states.length; i++) {
      const state = states[(this.#claimCursor + i) % states.length] as PartitionState;
      if (state.claimed || state.pausedByUser) continue;
      if (!state.queue.some((e) => e.epoch === state.epoch)) continue;
      this.#claimCursor = (this.#claimCursor + i + 1) % states.length;
      state.claimed = true;
      return state;
    }
    return undefined;
  }

  async #worker(runEpoch: number): Promise<void> {
    while (this.#running && runEpoch === this.#runEpoch) {
      const seenWake = this.#wakeVersion;
      const state = this.#claimNext();
      if (state === undefined) {
        if (this.#wakeVersion === seenWake) await this.#waitSignal();
        continue;
      }
      try {
        if (this.#eachMessage !== undefined) await this.#processMessages(state, runEpoch);
        else await this.#processBatch(state, runEpoch);
      } catch (error) {
        // Never let a worker die on an internal error — handler errors are handled inside.
        this.#logger.error(`Internal worker error: ${String(error)}`, this.#metadata());
      } finally {
        state.claimed = false;
        this.#signal();
      }
      this.#maybeResumePump();
    }
  }

  #maybeResumePump(): void {
    if (this.#queuedTotal < MAX_QUEUED_TOTAL / 2) this.#pump(this.#runEpoch);
  }

  /** Pulls one valid entry (matching epoch) off the queue. */
  #take(state: PartitionState): QueuedEntry | undefined {
    while (state.queue.length > 0) {
      const entry = state.queue.shift() as QueuedEntry;
      this.#queuedTotal--;
      if (entry.epoch === state.epoch) return entry;
    }
    return undefined;
  }

  #store(state: PartitionState, nextOffset: number, leaderEpoch: number | undefined): void {
    try {
      this.#inner.offsetsStore([
        leaderEpoch === undefined
          ? { topic: state.topic, partition: state.partition, offset: nextOffset }
          : { topic: state.topic, partition: state.partition, offset: nextOffset, leaderEpoch },
      ]);
      state.storedNext = nextOffset;
    } catch (error) {
      // The partition may have just been revoked — the new owner handles the offset.
      if (!(error instanceof LibrdKafkaError && error.code === ERROR_CODES.ERR__STATE)) {
        this.#logger.error(`offsetsStore ${state.key} failed: ${String(error)}`, this.#metadata());
      }
    }
  }

  /** payload.pause() — pauses exactly this partition, returning a resume function (upstream). */
  #payloadPause(state: PartitionState): () => () => void {
    return () => {
      this.pause([{ topic: state.topic, partitions: [state.partition] }]);
      return () => this.resume([{ topic: state.topic, partitions: [state.partition] }]);
    };
  }

  /* eachMessage: sequential within a partition, at most one "turn" per claim. */
  async #processMessages(state: PartitionState, runEpoch: number): Promise<void> {
    const handler = this.#eachMessage as EachMessageHandler;
    for (let i = 0; i < this.#maxBatchSize; i++) {
      if (!this.#running || runEpoch !== this.#runEpoch) return;
      const entry = this.#take(state);
      if (entry === undefined) return;
      const epochAtStart = state.epoch;
      const payload: EachMessagePayload = {
        topic: state.topic,
        partition: state.partition,
        message: toKafkaJSMessage(entry.msg),
        heartbeat: noopHeartbeat,
        pause: this.#payloadPause(state),
      };
      try {
        await handler(payload);
      } catch (error) {
        this.#logger.error(
          `eachMessage failed at ${state.key}@${entry.msg.offset}; ` +
            `the message may be redelivered. ${String(error)}`,
          this.#metadata(),
        );
        if (state.epoch === epochAtStart) this.#rewind(state, entry.msg.offset);
        return;
      }
      if (state.epoch !== epochAtStart) return; // pause/seek inside the handler — no store
      this.#store(state, entry.msg.offset + 1, entry.msg.leaderEpoch);
    }
  }

  /* eachBatch: one batch (≤ maxBatchSize, same epoch) per claim. */
  async #processBatch(state: PartitionState, runEpoch: number): Promise<void> {
    const handler = this.#eachBatch as EachBatchHandler;
    const entries: QueuedEntry[] = [];
    while (entries.length < this.#maxBatchSize) {
      const entry = this.#take(state);
      if (entry === undefined) break;
      entries.push(entry);
    }
    if (entries.length === 0 || !this.#running || runEpoch !== this.#runEpoch) return;

    const epochAtStart = state.epoch;
    const first = (entries[0] as QueuedEntry).msg;
    const last = (entries[entries.length - 1] as QueuedEntry).msg;
    let highWatermark = -1001; // RD_KAFKA_OFFSET_INVALID while the local cache is empty
    try {
      highWatermark = this.#inner.getWatermarkOffsets(state.topic, state.partition).highOffset;
    } catch {
      /* local cache has no watermark yet — keep -1001 like upstream */
    }
    let resolvedNext = -1;
    const resolveOffset = (offset: string | number): void => {
      const next = Number(offset) + 1;
      if (next <= resolvedNext) return;
      resolvedNext = next;
      if (state.epoch === epochAtStart) this.#store(state, next, last.leaderEpoch);
    };

    const lag = (from: number): string =>
      highWatermark < 0 ? "-1" : String(Math.max(0, highWatermark - 1 - from));
    const payload: EachBatchPayload = {
      batch: {
        topic: state.topic,
        partition: state.partition,
        highWatermark: String(highWatermark),
        messages: entries.map((e) => toKafkaJSMessage(e.msg)),
        isEmpty: () => entries.length === 0,
        firstOffset: () => String(first.offset),
        lastOffset: () => String(last.offset),
        offsetLag: () => lag(last.offset),
        offsetLagLow: () => lag(first.offset),
      },
      resolveOffset,
      heartbeat: noopHeartbeat,
      isRunning: () => this.#running,
      isStale: () => state.epoch !== epochAtStart,
      pause: this.#payloadPause(state),
      commitOffsetsIfNecessary: async () => {
        if (!this.#autoCommit) await this.commitOffsets();
      },
    };

    let threw = false;
    try {
      await handler(payload);
    } catch (error) {
      threw = true;
      this.#logger.error(
        `eachBatch failed at ${state.key}[${first.offset}..${last.offset}]; ` +
          `the unresolved part will be redelivered. ${String(error)}`,
        this.#metadata(),
      );
    }
    if (state.epoch !== epochAtStart) return; // stale — pause/seek/rebalance took care of it

    if (!threw && this.#eachBatchAutoResolve && resolvedNext <= last.offset) {
      resolveOffset(last.offset);
    }
    if (resolvedNext <= last.offset) {
      // Something is still unresolved (a throw, or autoResolve=false with the
      // user not resolving everything) → redeliver from the next unresolved
      // offset.
      this.#rewind(state, resolvedNext >= 0 ? resolvedNext : first.offset);
    }
  }
}
