/**
 * core/poll-scheduler.ts — the client's "heartbeat" (design §5.2).
 *
 * Since the shim never calls back into JS, the TS layer must actively call
 * `brk_events_poll` / `brk_consume_batch`. The scheduler is adaptive:
 *
 * ```
 * HOT   : the last poll had data      → poll again on the next microtask,
 *         yielding a macrotask every `macrotaskEvery` rounds to avoid starving I/O
 * WARM  : data just ran out           → poll every idleMinMs (1ms) while data
 *         was seen within the last `idleHoldMs` (100ms — keeps latency at
 *         timer granularity for steady traffic), then back off
 *         1 → 2 → 4 … → idleMaxMs (default 50ms, option `js.poll.idle.max.ms`)
 * COLD  : `isCold()` = true (no consumer, producer outq empty)
 *         → a `coldIntervalMs` timer (default 500ms), `unref()`ed so it does
 *           not keep the process alive
 * ```
 *
 * Every FFI call inside `poll` must use `timeout_ms = 0` (NFR-2).
 *
 * ## ⚠ Shim constraint: consumers must always be pumped
 *
 * The shim has two queues: `main_q` (serving `brk_events_poll`) and
 * `consumer_q` (serving `brk_consume_batch`). The **REBALANCE** and
 * **OFFSET_COMMIT** events live on `consumer_q` and only surface to
 * `brk_events_poll` WHEN `brk_consume_batch` gets called. Consequences for the
 * scheduler:
 *
 *  - For a consumer that has `subscribe()`d, `poll` must call BOTH
 *    `consumeBatch()` and `pollEvents()` on **every** round — even while the
 *    upper layer is not consuming messages (the user has not called
 *    `run()`/`consume()`, or every partition is paused). Messages fetched in
 *    the meantime must be buffered, never dropped.
 *  - Therefore `isCold()` for a subscribed consumer must ALWAYS return `false`:
 *    falling into COLD (500ms) is enough for rebalances to lag until the
 *    consumer is kicked from the group. COLD is reserved for idle producers and
 *    consumers that have not subscribed.
 *  - The WARM backoff ceiling (`js.poll.idle.max.ms`, default 50ms) is exactly
 *    the maximum rebalance/offset-commit latency — do not raise it too far for
 *    consumers.
 *
 * Testable by design: `poll` is a function returning the number of items
 * handled, and all timers go through {@link SchedulerTimers}, so unit tests can
 * inject a fake clock and tick manually.
 */

/** Scheduler state. */
export type PollPhase = "STOPPED" | "HOT" | "WARM" | "COLD";

/** Timer handle issued by {@link SchedulerTimers} (opaque to the scheduler). */
export type TimerHandle = unknown;

/** Timer injection point — defaults to the runtime's `setTimeout`/`queueMicrotask`. */
export interface SchedulerTimers {
  setTimer(fn: () => void, ms: number, unref: boolean): TimerHandle;
  clearTimer(handle: TimerHandle): void;
  setMicrotask(fn: () => void): void;
  /**
   * Macrotask that still lets the event loop run its I/O phase. Optional:
   * without it the scheduler falls back to `setTimer(fn, 0)` — which in Bun
   * (like Node) has ~1 ms of granularity, versus a few µs for `setImmediate`.
   */
  setImmediate?(fn: () => void): TimerHandle;
  clearImmediate?(handle: TimerHandle): void;
}

export const defaultTimers: SchedulerTimers = {
  setTimer(fn, ms, unref) {
    const t = setTimeout(fn, ms);
    if (unref) (t as { unref?: () => void }).unref?.();
    return t;
  },
  clearTimer(handle) {
    clearTimeout(handle as ReturnType<typeof setTimeout>);
  },
  setMicrotask(fn) {
    queueMicrotask(fn);
  },
  setImmediate(fn) {
    return setImmediate(fn);
  },
  clearImmediate(handle) {
    clearImmediate(handle as ReturnType<typeof setImmediate>);
  },
};

export interface PollSchedulerOptions {
  /** Performs one poll round; returns the number of items handled (0 = empty). */
  poll: () => number;
  /** Initial backoff when data just ran out (ms). Default 1. */
  idleMinMs?: number;
  /** Backoff ceiling (`js.poll.idle.max.ms`). Default 50. */
  idleMaxMs?: number;
  /**
   * After the last non-empty poll, keep polling at `idleMinMs` for this long
   * before the exponential backoff kicks in. Default 100.
   */
  idleHoldMs?: number;
  /** Clock used for `idleHoldMs` (tests inject a fake one). Default `Date.now`. */
  now?: () => number;
  /** Poll interval while COLD (ms). Default 500. */
  coldIntervalMs?: number;
  /** Whether falling into COLD is allowed (e.g. producer outq empty, no consumer). */
  isCold?: () => boolean;
  /** Every N HOT rounds, use a macrotask instead of a microtask. Default 8. */
  macrotaskEvery?: number;
  timers?: SchedulerTimers;
  /** Receives exceptions thrown by `poll` (default: rethrown on a macrotask). */
  onError?: (error: unknown) => void;
}

export class PollScheduler {
  private readonly pollFn: () => number;
  private readonly idleMinMs: number;
  private readonly idleMaxMs: number;
  private readonly idleHoldMs: number;
  private readonly now: () => number;
  private readonly coldIntervalMs: number;
  private readonly macrotaskEvery: number;
  private readonly isColdFn: (() => boolean) | undefined;
  private readonly timers: SchedulerTimers;
  private readonly onError: ((error: unknown) => void) | undefined;

  private phaseValue: PollPhase = "STOPPED";
  private delay = 0;
  private timer: TimerHandle | undefined;
  private scheduled = false;
  /** Invalidates callbacks scheduled before a cancellation. */
  private epoch = 0;
  private hotTicks = 0;
  private immediate: TimerHandle | undefined;
  private emptyPollsValue = 0;
  private pollCountValue = 0;
  /** Timestamp of the last non-empty poll (-Infinity = never). */
  private lastDataAt = -Infinity;

  constructor(options: PollSchedulerOptions) {
    this.pollFn = options.poll;
    this.idleMinMs = options.idleMinMs ?? 1;
    this.idleMaxMs = options.idleMaxMs ?? 50;
    this.idleHoldMs = options.idleHoldMs ?? 100;
    this.now = options.now ?? Date.now;
    this.coldIntervalMs = options.coldIntervalMs ?? 500;
    this.macrotaskEvery = Math.max(1, options.macrotaskEvery ?? 8);
    this.isColdFn = options.isCold;
    this.timers = options.timers ?? defaultTimers;
    this.onError = options.onError;
  }

  get phase(): PollPhase {
    return this.phaseValue;
  }

  get running(): boolean {
    return this.phaseValue !== "STOPPED";
  }

  /** Delay (ms) for the next scheduling; 0 = immediate. */
  get nextDelayMs(): number {
    if (this.phaseValue === "COLD") return this.coldIntervalMs;
    if (this.phaseValue === "WARM") return this.delay;
    return 0;
  }

  /** Consecutive empty polls. */
  get emptyPolls(): number {
    return this.emptyPollsValue;
  }

  /** Total poll rounds run (for tests/metrics). */
  get pollCount(): number {
    return this.pollCountValue;
  }

  /** Starts the poll loop (idempotent). Enters HOT directly to pick up data early. */
  start(): void {
    if (this.running) return;
    this.phaseValue = "HOT";
    this.delay = 0;
    this.hotTicks = 0;
    this.emptyPollsValue = 0;
    this.schedule();
  }

  /** Stops completely; all scheduled callbacks are invalidated. */
  stop(): void {
    this.cancelPending();
    this.phaseValue = "STOPPED";
  }

  /**
   * Kicks the scheduler back to HOT immediately (after `produce()`,
   * `subscribe()`, `commit()`…). No-op while STOPPED.
   */
  kick(): void {
    if (!this.running) return;
    this.cancelPending();
    this.phaseValue = "HOT";
    this.delay = 0;
    this.emptyPollsValue = 0;
    this.schedule();
  }

  /**
   * Runs exactly one poll round and updates state — does NOT schedule the next
   * round. Used by manual `producer.poll()` and by unit tests.
   * @returns the number of items handled.
   */
  runOnce(): number {
    let n = 0;
    try {
      n = this.pollFn();
    } catch (error) {
      this.pollCountValue++;
      if (this.onError) {
        this.onError(error);
      } else {
        this.timers.setTimer(
          () => {
            throw error;
          },
          0,
          false,
        );
      }
      return 0;
    }
    this.pollCountValue++;
    this.applyResult(n);
    return n;
  }

  private applyResult(n: number): void {
    if (this.phaseValue === "STOPPED") return;
    if (n > 0) {
      this.emptyPollsValue = 0;
      this.delay = 0;
      this.phaseValue = "HOT";
      this.lastDataAt = this.now();
      return;
    }
    this.emptyPollsValue++;
    if (this.isColdFn?.() === true) {
      this.delay = this.coldIntervalMs;
      this.phaseValue = "COLD";
      return;
    }
    if (this.delay === 0 || this.now() - this.lastDataAt < this.idleHoldMs) {
      // Recent traffic: stay at the finest cadence (a burst is probably coming).
      this.delay = this.idleMinMs;
    } else {
      this.delay = Math.min(this.delay * 2, this.idleMaxMs);
    }
    this.phaseValue = "WARM";
  }

  private cancelPending(): void {
    this.epoch++;
    this.scheduled = false;
    if (this.timer !== undefined) {
      this.timers.clearTimer(this.timer);
      this.timer = undefined;
    }
    if (this.immediate !== undefined) {
      this.timers.clearImmediate?.(this.immediate);
      this.immediate = undefined;
    }
  }

  private schedule(): void {
    if (!this.running || this.scheduled) return;
    this.scheduled = true;
    const epoch = this.epoch;
    const tick = (): void => {
      if (epoch !== this.epoch || !this.running) return;
      this.scheduled = false;
      this.timer = undefined;
      this.immediate = undefined;
      this.runOnce();
      this.schedule();
    };

    if (this.phaseValue === "HOT") {
      this.hotTicks++;
      // Interleave macrotasks so the event loop's I/O is not starved (design §5.2).
      // setImmediate when available: it yields to I/O like a timer but costs
      // µs, not the ~1 ms timer granularity that would cap the hot loop.
      if (this.hotTicks % this.macrotaskEvery === 0) {
        if (this.timers.setImmediate !== undefined) {
          this.immediate = this.timers.setImmediate(tick);
        } else {
          this.timer = this.timers.setTimer(tick, 0, false);
        }
      } else {
        this.timers.setMicrotask(tick);
      }
      return;
    }

    const unref = this.phaseValue === "COLD";
    this.timer = this.timers.setTimer(tick, this.nextDelayMs, unref);
  }
}
