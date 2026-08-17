import { describe, expect, test } from "bun:test";
import {
  PollScheduler,
  type SchedulerTimers,
  type TimerHandle,
} from "../../packages/bun-rdkafka/src/core/poll-scheduler.ts";

interface FakeTimer {
  id: number;
  fn: () => void;
  ms: number;
  unref: boolean;
}

/** A minimal fake clock: tests "tick" manually, independent of real time. */
class FakeTimers implements SchedulerTimers {
  microtasks: (() => void)[] = [];
  timers: FakeTimer[] = [];
  cleared: number[] = [];
  private nextId = 1;

  setTimer(fn: () => void, ms: number, unref: boolean): TimerHandle {
    const timer: FakeTimer = { id: this.nextId++, fn, ms, unref };
    this.timers.push(timer);
    return timer.id;
  }

  clearTimer(handle: TimerHandle): void {
    this.cleared.push(handle as number);
    this.timers = this.timers.filter((t) => t.id !== handle);
  }

  setMicrotask(fn: () => void): void {
    this.microtasks.push(fn);
  }

  /** Modelled as a 0 ms timer: it runs on the next event-loop turn, after microtasks. */
  setImmediate(fn: () => void): TimerHandle {
    return this.setTimer(fn, 0, false);
  }

  clearImmediate(handle: TimerHandle): void {
    this.clearTimer(handle);
  }

  /** Runs one scheduled round (microtasks first, like the real event loop). */
  step(): void {
    if (this.microtasks.length > 0) {
      const queue = this.microtasks;
      this.microtasks = [];
      for (const fn of queue) fn();
      return;
    }
    const queue = this.timers;
    this.timers = [];
    for (const timer of queue) timer.fn();
  }

  get pending(): number {
    return this.microtasks.length + this.timers.length;
  }

  lastTimer(): FakeTimer | undefined {
    return this.timers[this.timers.length - 1];
  }
}

function makeScheduler(results: number[], extra: Record<string, unknown> = {}) {
  const timers = new FakeTimers();
  let index = 0;
  const scheduler = new PollScheduler({
    poll: () => results[Math.min(index++, results.length - 1)] ?? 0,
    timers,
    ...extra,
  });
  return { scheduler, timers, calls: () => index };
}

describe("lifecycle", () => {
  test("starts in STOPPED; start() enters HOT and schedules immediately", () => {
    const { scheduler, timers } = makeScheduler([1]);
    expect(scheduler.phase).toBe("STOPPED");
    expect(scheduler.running).toBe(false);

    scheduler.start();
    expect(scheduler.phase).toBe("HOT");
    expect(timers.microtasks).toHaveLength(1);
    expect(timers.timers).toHaveLength(0);
  });

  test("calling start() twice does not double-schedule", () => {
    const { scheduler, timers } = makeScheduler([1]);
    scheduler.start();
    scheduler.start();
    expect(timers.pending).toBe(1);
  });

  test("stop() cancels everything and stale callbacks become no-ops", () => {
    const { scheduler, timers, calls } = makeScheduler([0]);
    scheduler.start();
    timers.step(); // empty poll → WARM + timer
    expect(scheduler.phase).toBe("WARM");
    const before = calls();

    scheduler.stop();
    expect(scheduler.phase).toBe("STOPPED");
    expect(timers.cleared).toHaveLength(1);

    timers.step(); // nothing left to run
    expect(calls()).toBe(before);
  });
});

describe("HOT / WARM / COLD transitions", () => {
  test("data available → stays HOT, scheduling via microtask", () => {
    const { scheduler, timers } = makeScheduler([5, 5, 5]);
    scheduler.start();
    timers.step();
    expect(scheduler.phase).toBe("HOT");
    expect(timers.microtasks).toHaveLength(1);
    timers.step();
    expect(scheduler.phase).toBe("HOT");
    expect(scheduler.pollCount).toBe(2);
  });

  test("every macrotaskEvery HOT rounds yields one macrotask", () => {
    const { scheduler, timers } = makeScheduler([1], { macrotaskEvery: 3 });
    scheduler.start(); // schedule #1: microtask
    timers.step(); // schedule #2: microtask
    timers.step(); // schedule #3: macrotask (3 % 3 === 0)
    expect(timers.microtasks).toHaveLength(0);
    expect(timers.timers).toHaveLength(1);
    expect(timers.lastTimer()).toMatchObject({ ms: 0, unref: false });
  });

  test("recent data holds the backoff at idleMin for idleHoldMs", () => {
    let clock = 1000;
    const results = [5, 0, 0, 0, 0, 0, 0];
    let i = 0;
    const timers = new FakeTimers();
    const scheduler = new PollScheduler({
      poll: () => results[Math.min(i++, results.length - 1)]!,
      timers,
      idleMaxMs: 8,
      idleHoldMs: 100,
      now: () => clock,
    });
    scheduler.start();
    timers.step(); // data → HOT (lastDataAt = 1000)
    const delays: number[] = [];
    for (let k = 0; k < 3; k++) {
      timers.step();
      delays.push(scheduler.nextDelayMs);
      clock += 30; // 90ms of emptiness: still inside the hold window
    }
    expect(delays).toEqual([1, 1, 1]);
    clock += 20; // 110ms since data → backoff resumes
    timers.step();
    expect(scheduler.nextDelayMs).toBe(2);
    timers.step();
    expect(scheduler.nextDelayMs).toBe(4);
  });

  test("data exhausted → WARM with backoff 1 → 2 → 4 … → idleMax", () => {
    const { scheduler, timers } = makeScheduler([0], { idleMaxMs: 8 });
    scheduler.start();

    const delays: number[] = [];
    for (let i = 0; i < 6; i++) {
      timers.step();
      expect(scheduler.phase).toBe("WARM");
      delays.push(scheduler.nextDelayMs);
    }
    expect(delays).toEqual([1, 2, 4, 8, 8, 8]); // capped at idleMaxMs
    expect(timers.lastTimer()).toMatchObject({ ms: 8, unref: false });
    expect(scheduler.emptyPolls).toBe(6);
  });

  test("idleMaxMs defaults to 50 (js.poll.idle.max.ms)", () => {
    const { scheduler, timers } = makeScheduler([0]);
    scheduler.start();
    for (let i = 0; i < 12; i++) timers.step();
    expect(scheduler.nextDelayMs).toBe(50);
  });

  test("data returning → HOT with the backoff reset", () => {
    const { scheduler, timers } = makeScheduler([0, 0, 3]);
    scheduler.start();
    timers.step();
    timers.step();
    expect(scheduler.nextDelayMs).toBe(2);
    timers.step();
    expect(scheduler.phase).toBe("HOT");
    expect(scheduler.nextDelayMs).toBe(0);
    expect(scheduler.emptyPolls).toBe(0);
  });

  test("isCold() → COLD, with a long, unref'd timer", () => {
    const { scheduler, timers } = makeScheduler([0], {
      isCold: () => true,
      coldIntervalMs: 500,
    });
    scheduler.start();
    timers.step();
    expect(scheduler.phase).toBe("COLD");
    expect(scheduler.nextDelayMs).toBe(500);
    expect(timers.lastTimer()).toMatchObject({ ms: 500, unref: true });
  });

  test("a subscribed consumer (isCold=false) never falls into COLD", () => {
    const { scheduler, timers } = makeScheduler([0], { isCold: () => false });
    scheduler.start();
    for (let i = 0; i < 10; i++) timers.step();
    expect(scheduler.phase).toBe("WARM");
    expect(timers.lastTimer()?.unref).toBe(false);
  });
});

describe("kick()", () => {
  test("brings WARM back to HOT immediately, cancelling the pending timer", () => {
    const { scheduler, timers } = makeScheduler([0, 0, 0]);
    scheduler.start();
    timers.step();
    timers.step();
    expect(scheduler.phase).toBe("WARM");
    expect(scheduler.nextDelayMs).toBe(2);

    scheduler.kick();
    expect(scheduler.phase).toBe("HOT");
    expect(scheduler.nextDelayMs).toBe(0);
    expect(scheduler.emptyPolls).toBe(0);
    expect(timers.timers).toHaveLength(0);
    expect(timers.microtasks).toHaveLength(1);
  });

  test("kick() while STOPPED does nothing", () => {
    const { scheduler, timers } = makeScheduler([1]);
    scheduler.kick();
    expect(scheduler.phase).toBe("STOPPED");
    expect(timers.pending).toBe(0);
  });

  test("kick() from COLD also returns to HOT", () => {
    const { scheduler, timers } = makeScheduler([0], { isCold: () => true });
    scheduler.start();
    timers.step();
    expect(scheduler.phase).toBe("COLD");
    scheduler.kick();
    expect(scheduler.phase).toBe("HOT");
  });
});

describe("runOnce() and errors", () => {
  test("runOnce runs exactly one round, scheduling nothing further", () => {
    const { scheduler, timers } = makeScheduler([7]);
    expect(scheduler.runOnce()).toBe(7);
    expect(scheduler.pollCount).toBe(1);
    expect(timers.pending).toBe(0);
  });

  test("a poll exception goes to onError; the loop survives", () => {
    const timers = new FakeTimers();
    const errors: unknown[] = [];
    let n = 0;
    const scheduler = new PollScheduler({
      poll: () => {
        n++;
        if (n === 1) throw new Error("boom");
        return 0;
      },
      timers,
      onError: (err) => errors.push(err),
    });
    scheduler.start();
    timers.step();
    expect(errors).toHaveLength(1);
    expect((errors[0] as Error).message).toBe("boom");
    expect(scheduler.phase).toBe("HOT"); // errors do not change the phase
    timers.step();
    expect(scheduler.phase).toBe("WARM");
  });
});
