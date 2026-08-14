/**
 * test/integration/slice-harness.ts — minimal wiring between `core/` and a
 * real broker, shared by the integration tests (M1) and `bench/`.
 *
 * This is **not** public API: the `src/callback/` layer (M2–M3) does this
 * properly with EventEmitter, staging buffers, commit semantics… At M1 the
 * harness only needs enough to prove the vertical slice:
 *
 * ```
 * ConfigBuilder → NativeClient → PollScheduler ─┬→ brk_events_poll  → DeliveryLedger
 *                                               └→ brk_consume_batch → BatchDecoder
 * ```
 *
 * Two points tracking the design constraints:
 *  1. **Every consumer poll round calls BOTH `consumeBatch()` AND
 *     `pollEvents()`**, in that order — REBALANCE/OFFSET_COMMIT live on
 *     `consumer_q` and only surface via `brk_events_poll` after a
 *     `brk_consume_batch` (see the top of `core/poll-scheduler.ts`).
 *  2. Every FFI call uses `timeout_ms = 0` → never blocks the event loop (NFR-2).
 */

import {
  buildConfig,
  type BuiltConfig,
  type ClientConfig,
} from "../../packages/bun-rdkafka/src/core/config.ts";
import {
  DeliveryLedger,
  type DeliveryResult,
} from "../../packages/bun-rdkafka/src/core/delivery-ledger.ts";
import { ERROR_CODES, LibrdKafkaError } from "../../packages/bun-rdkafka/src/core/errors.ts";
import { NativeClient } from "../../packages/bun-rdkafka/src/core/native-client.ts";
import { PollScheduler } from "../../packages/bun-rdkafka/src/core/poll-scheduler.ts";
import type {
  BrkErrorEvent,
  BrkEvent,
  BrkLogEvent,
  BrkOffsetCommitEvent,
  BrkRebalanceEvent,
  DecodedMessage,
  ProduceRecord,
  TopicPartitionEntry,
} from "../../packages/bun-rdkafka/src/core/batch-decoder.ts";

export type {
  BrkErrorEvent,
  BrkEvent,
  BrkLogEvent,
  BrkOffsetCommitEvent,
  BrkRebalanceEvent,
  DecodedMessage,
  TopicPartitionEntry,
};
export type { DeliveryResult };
import {
  BRK_ASSIGN,
  BRK_ASSIGN_INCREMENTAL,
  BRK_CLIENT_CONSUMER,
  BRK_CLIENT_PRODUCER,
  BRK_EVENT_DR,
  BRK_EVENT_ERROR,
  BRK_EVENT_LOG,
  BRK_EVENT_OFFSET_COMMIT,
  BRK_EVENT_REBALANCE,
  BRK_REBALANCE_PROTOCOL_COOPERATIVE,
  BRK_UNASSIGN,
  BRK_UNASSIGN_INCREMENTAL,
} from "../../packages/bun-rdkafka/src/ffi/types.ts";

/* ========================================================================== */
/* Chung                                                                       */
/* ========================================================================== */

export interface HarnessOptions {
  /** librdkafka + `js.*` config (through `ConfigBuilder`). */
  config: ClientConfig;
  /** A label for error messages. */
  label?: string;
  /**
   * Waits for the first metadata inside `connect()` (simulating upstream's
   * `connect()` semantics). Default 0 = skipped: `brk_metadata` was still an M4
   * TODO in the shim, so at M1 the client goes straight to READY.
   */
  waitMetadataMs?: number;
  /** Max events per `brk_events_poll`. */
  maxEvents?: number;
  /** Receives every LOG event (default: dropped). */
  onLog?: (event: BrkLogEvent) => void;
  /** Receives client-level ERROR events. */
  onError?: (event: BrkErrorEvent) => void;
}

/** Returns `true` once the condition holds, yielding to the event loop between tries. */
export async function waitFor(
  predicate: () => boolean,
  timeoutMs: number,
  describe: () => string,
  stepMs = 1,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) {
      throw new Error(`harness: ${timeoutMs}ms elapsed without reaching: ${describe()}`);
    }
    await Bun.sleep(stepMs);
  }
}

abstract class SliceClient {
  readonly client: NativeClient;
  readonly scheduler: PollScheduler;
  /** The config split into properties/js/callbacks. */
  readonly built: BuiltConfig;

  /** Client-level ERROR events received (fatal or not). */
  readonly errorEvents: BrkErrorEvent[] = [];
  /** Exceptions thrown inside poll rounds (never swallowed silently). */
  readonly pollErrors: unknown[] = [];

  protected readonly maxEvents: number;
  protected readonly onLogHook: ((event: BrkLogEvent) => void) | undefined;
  protected readonly onErrorHook: ((event: BrkErrorEvent) => void) | undefined;
  private readonly waitMetadataMs: number;

  constructor(type: typeof BRK_CLIENT_PRODUCER | typeof BRK_CLIENT_CONSUMER, options: HarnessOptions) {
    const built = buildConfig(options.config);
    this.built = built;
    this.client = new NativeClient({
      type,
      properties: built.properties,
      js: built.js,
      ...(options.label === undefined ? {} : { label: options.label }),
    });
    this.maxEvents = options.maxEvents ?? 256;
    this.onLogHook = options.onLog;
    this.onErrorHook = options.onError;
    this.waitMetadataMs = options.waitMetadataMs ?? 0;
    this.scheduler = new PollScheduler({
      poll: () => this.pollOnce(),
      idleMaxMs: built.js.pollIdleMaxMs,
      coldIntervalMs: built.js.pollIntervalMs,
      isCold: () => this.isCold(),
      onError: (error) => {
        this.pollErrors.push(error);
      },
    });
  }

  /** Creates the native handle, (optionally) waits for first metadata, then runs the scheduler. */
  connect(): void {
    this.client.connect();
    if (this.waitMetadataMs > 0) {
      // A one-off cold path: proves the handle can talk to the real broker.
      this.client.metadata(null, this.waitMetadataMs);
    }
    this.client.markReady();
    this.scheduler.start();
  }

  /** Stops the scheduler + `brk_client_destroy` (blocks until internal threads join). */
  disconnect(): void {
    this.scheduler.stop();
    this.client.disconnect();
  }

  /** Manually runs one poll round (no follow-up scheduling) — for tests forcing the pace. */
  pollNow(): number {
    return this.scheduler.runOnce();
  }

  /** Rethrows the first exception caught in a poll round (if any). */
  throwPollErrors(): void {
    const first = this.pollErrors[0];
    if (first !== undefined) throw first;
  }

  protected abstract pollOnce(): number;
  protected abstract isCold(): boolean;

  /** Handles the events shared by producer and consumer. */
  protected handleCommonEvent(event: BrkEvent): void {
    if (event.type === BRK_EVENT_ERROR) {
      this.errorEvents.push(event);
      this.onErrorHook?.(event);
      return;
    }
    if (event.type === BRK_EVENT_LOG) {
      this.onLogHook?.(event);
    }
  }
}

/* ========================================================================== */
/* Producer                                                                    */
/* ========================================================================== */

/** A produce record without an `opaque_id` — the harness assigns one from the {@link DeliveryLedger}. */
export type SliceProduceInput = Omit<ProduceRecord, "opaqueId"> & { opaqueId?: bigint };

export interface SliceProducerOptions extends HarnessOptions {
  /** The ledger's backpressure threshold (default `js.producer.max.pending`). */
  maxPending?: number;
  /** Called per successful DR (avoids a closure/promise per record). */
  onDelivery?: (result: DeliveryResult<number>) => void;
  /** Called per failed DR. */
  onDeliveryError?: (error: LibrdKafkaError) => void;
}

export class SliceProducer extends SliceClient {
  /** The DR ledger — `record` is a caller-assigned index (or 0). */
  readonly ledger: DeliveryLedger<number>;

  /** Successful DRs received. */
  delivered = 0;
  /** Failed DRs received. */
  deliveryFailures = 0;
  /** Records `brk_produce_batch` rejected immediately (per-record err != 0). */
  rejected = 0;
  /** DR frames decoded. */
  drFrames = 0;
  lastDeliveryError: LibrdKafkaError | undefined;

  private readonly onDeliveryHook: ((result: DeliveryResult<number>) => void) | undefined;
  private readonly onDeliveryErrorHook: ((error: LibrdKafkaError) => void) | undefined;
  /** Two closures reused for EVERY record — no hot-path allocation. */
  private readonly resolveOne: (result: DeliveryResult<number>) => void;
  private readonly rejectOne: (error: LibrdKafkaError) => void;

  constructor(options: SliceProducerOptions) {
    super(BRK_CLIENT_PRODUCER, options);
    this.ledger = new DeliveryLedger<number>({
      maxPending: options.maxPending ?? this.built.js.producerMaxPending,
    });
    this.onDeliveryHook = options.onDelivery;
    this.onDeliveryErrorHook = options.onDeliveryError;
    this.resolveOne = (result) => {
      this.delivered++;
      this.onDeliveryHook?.(result);
    };
    this.rejectOne = (error) => {
      this.deliveryFailures++;
      this.lastDeliveryError = error;
      this.onDeliveryErrorHook?.(error);
    };
  }

  /** Records awaiting delivery reports. */
  get pending(): number {
    return this.ledger.pending;
  }

  /**
   * Tracks the records then pushes the whole batch down to C in **one**
   * `brk_produce_batch` call.
   *
   * ⚠ Overwrites each element's `opaqueId` field in `records` (the harness
   * reuses the caller's objects to avoid hot-path copies).
   *
   * @param tags the `record` values stored in the ledger per element (defaults
   *   to indexes).
   * @returns the number of records enqueued into librdkafka.
   */
  produce(records: SliceProduceInput[], tags?: readonly number[]): number {
    for (let i = 0; i < records.length; i++) {
      const rec = records[i] as ProduceRecord;
      rec.opaqueId = this.ledger.track(tags?.[i] ?? i, this.resolveOne, this.rejectOne);
    }
    const result = this.client.produceBatch(records as ProduceRecord[]);

    // Records the shim did not process (accepted < length) or rejected per-record.
    for (let i = result.accepted; i < records.length; i++) {
      this.rejected++;
      this.ledger.fail(
        (records[i] as ProduceRecord).opaqueId,
        LibrdKafkaError.fromKafkaCode(
          ERROR_CODES.ERR__QUEUE_FULL,
          "harness: brk_produce_batch did not accept this record",
        ),
      );
    }
    let queued = 0;
    for (let i = 0; i < result.accepted; i++) {
      const err = result.errors[i] ?? 0;
      if (err === 0) {
        queued++;
        continue;
      }
      this.rejected++;
      this.ledger.fail(
        (records[i] as ProduceRecord).opaqueId,
        LibrdKafkaError.fromKafkaCode(err, "harness: brk_produce_batch rejected the record"),
      );
    }
    this.scheduler.kick(); // DRs incoming → back to HOT now
    return queued;
  }

  /** Waits for every produced record to settle (all DRs in). */
  async waitIdle(timeoutMs = 120_000): Promise<void> {
    await waitFor(
      () => this.ledger.pending === 0,
      timeoutMs,
      () => `producer still has ${this.ledger.pending} records awaiting DRs`,
    );
  }

  /** Waits until the records awaiting DRs drop below `limit` (backpressure). */
  async waitBelow(limit: number, timeoutMs = 120_000): Promise<void> {
    await waitFor(
      () => this.ledger.pending < limit,
      timeoutMs,
      () => `pending ${this.ledger.pending} >= ${limit}`,
    );
  }

  protected override pollOnce(): number {
    const events = this.client.pollEvents(this.maxEvents);
    let items = 0;
    for (const event of events) {
      if (event.type === BRK_EVENT_DR) {
        this.drFrames++;
        items += event.reports.length;
        this.ledger.settleAll(event.reports);
        continue;
      }
      items++;
      this.handleCommonEvent(event);
    }
    return items;
  }

  protected override isCold(): boolean {
    return this.ledger.pending === 0 && this.client.outqLen() === 0;
  }
}

/* ========================================================================== */
/* Consumer                                                                    */
/* ========================================================================== */

export interface SliceConsumerOptions extends HarnessOptions {
  /** Max messages per `brk_consume_batch` (default 500). */
  maxMessages?: number;
  /** Receives each message; without it, messages buffer into {@link SliceConsumer.messages}. */
  onMessage?: (message: DecodedMessage) => void;
  /**
   * Replaces the default rebalance behavior. The handler **must** call
   * `brk_assign` itself (the shim never assigns — see the ABI header).
   */
  onRebalance?: (event: BrkRebalanceEvent, consumer: SliceConsumer) => void;
  onOffsetCommit?: (event: BrkOffsetCommitEvent) => void;
}

export class SliceConsumer extends SliceClient {
  /** Ordinary messages (err === 0) when `onMessage` is absent. */
  readonly messages: DecodedMessage[] = [];
  /** Records carrying err != 0 (e.g. `_PARTITION_EOF`). */
  readonly errorMessages: DecodedMessage[] = [];
  readonly rebalanceEvents: BrkRebalanceEvent[] = [];
  readonly offsetCommitEvents: BrkOffsetCommitEvent[] = [];

  /** Total ordinary messages received (even when using `onMessage`). */
  consumed = 0;
  /** The current assignment per the REBALANCE events handled. */
  assigned: TopicPartitionEntry[] = [];
  /** How many REBALANCEs were answered via `brk_assign`. */
  assignReplies = 0;

  private subscribedValue = false;
  private readonly maxMessages: number;
  private readonly onMessageHook: ((message: DecodedMessage) => void) | undefined;
  private readonly onRebalanceHook:
    | ((event: BrkRebalanceEvent, consumer: SliceConsumer) => void)
    | undefined;
  private readonly onOffsetCommitHook: ((event: BrkOffsetCommitEvent) => void) | undefined;

  constructor(options: SliceConsumerOptions) {
    super(BRK_CLIENT_CONSUMER, options);
    this.maxMessages = options.maxMessages ?? 500;
    this.onMessageHook = options.onMessage;
    this.onRebalanceHook = options.onRebalance;
    this.onOffsetCommitHook = options.onOffsetCommit;
  }

  get subscribed(): boolean {
    return this.subscribedValue;
  }

  subscribe(topics: readonly string[]): void {
    this.client.subscribe(topics);
    this.subscribedValue = true;
    this.scheduler.kick();
  }

  unsubscribe(): void {
    this.client.unsubscribe();
    this.subscribedValue = false;
    this.scheduler.kick();
  }

  /** Answers one REBALANCE event by default (eager or cooperative). */
  applyRebalance(event: BrkRebalanceEvent): void {
    const cooperative = event.protocol === BRK_REBALANCE_PROTOCOL_COOPERATIVE;
    if (event.code === ERROR_CODES.ERR__ASSIGN_PARTITIONS) {
      this.client.assign(event.partitions, cooperative ? BRK_ASSIGN_INCREMENTAL : BRK_ASSIGN);
      this.assigned = cooperative ? [...this.assigned, ...event.partitions] : [...event.partitions];
    } else {
      this.client.assign(
        cooperative ? event.partitions : null,
        cooperative ? BRK_UNASSIGN_INCREMENTAL : BRK_UNASSIGN,
      );
      if (cooperative) {
        const revoked = new Set(event.partitions.map((p) => `${p.topic}/${p.partition}`));
        this.assigned = this.assigned.filter((p) => !revoked.has(`${p.topic}/${p.partition}`));
      } else {
        this.assigned = [];
      }
    }
    this.assignReplies++;
  }

  /** Waits until `n` ordinary messages are received. */
  async waitForMessages(n: number, timeoutMs = 120_000): Promise<void> {
    await waitFor(
      () => this.consumed >= n,
      timeoutMs,
      () => `only ${this.consumed}/${n} messages received`,
    );
  }

  /** Waits for an assignment (the first REBALANCE assign answered). */
  async waitForAssignment(timeoutMs = 60_000): Promise<void> {
    await waitFor(
      () => this.assigned.length > 0,
      timeoutMs,
      () => "no REBALANCE assign received yet",
    );
  }

  /**
   * One poll round: `brk_consume_batch` FIRST, `brk_events_poll` SECOND —
   * mandatory, since REBALANCE/OFFSET_COMMIT only surface on the main queue
   * after consumer_q is pumped.
   */
  protected override pollOnce(): number {
    const messages = this.client.consumeBatch(this.maxMessages, 0);
    let items = messages.length;
    for (const message of messages) {
      if (message.err !== 0) {
        this.errorMessages.push(message);
        continue;
      }
      this.consumed++;
      if (this.onMessageHook) this.onMessageHook(message);
      else this.messages.push(message);
    }

    const events = this.client.pollEvents(this.maxEvents);
    items += events.length;
    for (const event of events) {
      if (event.type === BRK_EVENT_REBALANCE) {
        this.rebalanceEvents.push(event);
        if (this.onRebalanceHook) this.onRebalanceHook(event, this);
        else this.applyRebalance(event);
        continue;
      }
      if (event.type === BRK_EVENT_OFFSET_COMMIT) {
        this.offsetCommitEvents.push(event);
        this.onOffsetCommitHook?.(event);
        continue;
      }
      this.handleCommonEvent(event);
    }
    return items;
  }

  /** A subscribed consumer must NEVER go COLD (rebalances would lag 500ms). */
  protected override isCold(): boolean {
    return !this.subscribedValue && this.assigned.length === 0;
  }
}
