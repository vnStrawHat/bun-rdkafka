/**
 * core/delivery-ledger.ts — the delivery-report ledger (design §5.4).
 *
 * The `opaque_id` (u64) assigned by JS at produce time is returned verbatim by
 * C in the DR event; the ledger uses it to resolve/reject the matching
 * record's callback/promise, and counts in-flight records for backpressure
 * (`js.producer.max.pending`).
 */

import type { DeliveryReport } from "./batch-decoder.ts";
import { ERROR_CODES, LibrdKafkaError, errorDescription } from "./errors.ts";

const U64_WRAP = 1n << 64n;

/** Successful delivery result for one record. */
export interface DeliveryResult<R> {
  opaqueId: bigint;
  partition: number;
  offset: number;
  timestamp: number;
  /** The very record object the caller registered (carries topic, key…). */
  record: R;
}

export type DeliveryResolve<R> = (result: DeliveryResult<R>) => void;
export type DeliveryReject = (error: LibrdKafkaError) => void;

interface PendingEntry<R> {
  record: R;
  resolve: DeliveryResolve<R>;
  reject: DeliveryReject;
}

export interface DeliveryLedgerOptions {
  /**
   * Maximum number of records allowed to await a DR. Beyond the threshold,
   * {@link DeliveryLedger.track} throws `ERR__QUEUE_FULL` — matching
   * librdkafka's semantics (`queue.buffering.max.messages`).
   */
  maxPending?: number;
  /** First opaque_id (default 1; 0 is reserved for "untracked"). */
  startId?: bigint;
}

/** Default of `js.producer.max.pending` = `queue.buffering.max.messages`. */
export const DEFAULT_MAX_PENDING = 100_000;

export class DeliveryLedger<R = unknown> {
  private readonly entries = new Map<bigint, PendingEntry<R>>();
  private nextIdValue: bigint;

  /** Backpressure threshold. */
  readonly maxPending: number;

  constructor(options: DeliveryLedgerOptions = {}) {
    this.maxPending = options.maxPending ?? DEFAULT_MAX_PENDING;
    this.nextIdValue = options.startId ?? 1n;
  }

  /** Number of records awaiting a delivery report. */
  get pending(): number {
    return this.entries.size;
  }

  get isFull(): boolean {
    return this.entries.size >= this.maxPending;
  }

  /** opaque_id that the next {@link track} will assign (not consumed). */
  get nextId(): bigint {
    return this.nextIdValue;
  }

  has(opaqueId: bigint): boolean {
    return this.entries.has(opaqueId);
  }

  /** Pending record for `opaqueId` (for debugging/inspection). */
  peek(opaqueId: bigint): R | undefined {
    return this.entries.get(opaqueId)?.record;
  }

  /**
   * Assigns an opaque_id and records the entry.
   * @throws LibrdKafkaError `ERR__QUEUE_FULL` when {@link maxPending} is exceeded.
   */
  track(record: R, resolve: DeliveryResolve<R>, reject: DeliveryReject): bigint {
    if (this.isFull) {
      throw LibrdKafkaError.fromKafkaCode(
        ERROR_CODES.ERR__QUEUE_FULL,
        `bun-rdkafka: ${this.entries.size} records are awaiting delivery reports ` +
          `(js.producer.max.pending = ${this.maxPending})`,
      );
    }
    const id = this.nextIdValue;
    this.nextIdValue = (this.nextIdValue + 1n) % U64_WRAP;
    this.entries.set(id, { record, resolve, reject });
    return id;
  }

  /** Promise variant for the KafkaJS layer (`send()` awaits the DR). */
  trackPromise(record: R): { opaqueId: bigint; promise: Promise<DeliveryResult<R>> } {
    let resolve!: DeliveryResolve<R>;
    let reject!: DeliveryReject;
    const promise = new Promise<DeliveryResult<R>>((res, rej) => {
      resolve = res;
      reject = rej;
    });
    const opaqueId = this.track(record, resolve, reject);
    return { opaqueId, promise };
  }

  /**
   * Settles one record from a DR received in an event frame.
   * @returns `false` when the opaque_id is not in the ledger (stray/duplicate DR).
   */
  settle(report: DeliveryReport): boolean {
    const entry = this.entries.get(report.opaqueId);
    if (!entry) return false;
    this.entries.delete(report.opaqueId);
    if (report.err !== ERROR_CODES.ERR_NO_ERROR) {
      entry.reject(
        LibrdKafkaError.fromKafkaCode(report.err, errorDescription(report.err), {
          context: "delivery-report",
        }),
      );
    } else {
      entry.resolve({
        opaqueId: report.opaqueId,
        partition: report.partition,
        offset: report.offset,
        timestamp: report.timestamp,
        record: entry.record,
      });
    }
    return true;
  }

  /** Settles a frame's whole DR array; returns the number of matched DRs. */
  settleAll(reports: readonly DeliveryReport[]): number {
    let matched = 0;
    for (const report of reports) {
      if (this.settle(report)) matched++;
    }
    return matched;
  }

  /** Rejects one specific record (e.g. produce failed right in `brk_produce_batch`). */
  fail(opaqueId: bigint, error: LibrdKafkaError): boolean {
    const entry = this.entries.get(opaqueId);
    if (!entry) return false;
    this.entries.delete(opaqueId);
    entry.reject(error);
    return true;
  }

  /**
   * Rejects every pending record (disconnect / fatal error / purge).
   * @returns the number of rejected records.
   */
  failAll(error: LibrdKafkaError): number {
    const pending = [...this.entries.values()];
    this.entries.clear();
    for (const entry of pending) entry.reject(error);
    return pending.length;
  }
}
