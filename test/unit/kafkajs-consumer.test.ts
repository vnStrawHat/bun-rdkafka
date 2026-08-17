/**
 * Unit tests for kafkajs/consumer.ts — the per-partition queue + epoch scheduler +
 * worker pool (ADR-4b), running on a FAKE KafkaConsumer (no .so/broker needed).
 */

import { describe, expect, test } from "bun:test";
import { EventEmitter } from "node:events";
import { Buffer } from "node:buffer";
import { Consumer } from "../../packages/bun-rdkafka/src/kafkajs/consumer.ts";
import type { KafkaConsumer, Message } from "../../packages/bun-rdkafka/src/callback/kafka-consumer.ts";
import { ERROR_CODES, LibrdKafkaError } from "../../packages/bun-rdkafka/src/core/errors.ts";

/* ------------------------------------------------------------------ fake */

interface PendingConsume {
  n: number;
  cb: (err: null, messages: Message[]) => void;
}

type RebalanceCb = (err: LibrdKafkaError, parts: { topic: string; partition: number }[]) => void;

class FakeInner extends EventEmitter {
  /** The `rebalance_cb` trampoline the KafkaJS layer installs on the conf. */
  rebalanceCb: RebalanceCb | undefined;
  assigns: unknown[][] = [];
  incrementalAssigns: unknown[][] = [];
  unassigns = 0;
  incrementalUnassigns: unknown[][] = [];
  protocol: "NONE" | "EAGER" | "COOPERATIVE" = "EAGER";
  lost = false;
  name = "consumer#fake";
  feed: Message[] = [];
  stored: { topic: string; partition: number; offset: number }[] = [];
  seeks: { topic: string; partition: number; offset: number }[] = [];
  pauses: { topic: string; partition: number }[][] = [];
  resumes: { topic: string; partition: number }[][] = [];
  commits: unknown[] = [];
  commitSyncs: unknown[] = [];
  subscribes: string[][] = [];
  assigned: { topic: string; partition: number }[] = [];
  highWatermark = -1001;
  #pending: PendingConsume[] = [];

  connect(_opts: unknown, cb?: (err: null) => void): this {
    queueMicrotask(() => cb?.(null));
    return this;
  }

  disconnect(cb?: () => void): this {
    queueMicrotask(() => cb?.());
    return this;
  }

  subscribe(topics: string[]): this {
    this.subscribes.push([...topics]);
    return this;
  }

  consume(n: number, cb: (err: null, messages: Message[]) => void): void {
    this.#pending.push({ n, cb });
    queueMicrotask(() => this.#serve());
  }

  /** Tests pump messages then call this to serve waiting consume()s. */
  push(...messages: Message[]): void {
    this.feed.push(...messages);
    queueMicrotask(() => this.#serve());
  }

  #serve(): void {
    while (this.#pending.length > 0 && this.feed.length > 0) {
      const req = this.#pending.shift() as PendingConsume;
      req.cb(null, this.feed.splice(0, req.n));
    }
  }

  /** Answers waiting consume()s with an empty array (simulating a timeout). */
  flushEmpty(): void {
    const pending = this.#pending.splice(0);
    for (const req of pending) req.cb(null, []);
  }

  offsetsStore(list: { topic: string; partition: number; offset: number }[]): this {
    this.stored.push(...list);
    return this;
  }

  seek(
    tpo: { topic: string; partition: number; offset: number },
    _timeout: number | null,
    cb: (err: null) => void,
  ): this {
    this.seeks.push(tpo);
    queueMicrotask(() => cb(null));
    return this;
  }

  pause(list: { topic: string; partition: number }[]): this {
    this.pauses.push(list);
    return this;
  }

  resume(list: { topic: string; partition: number }[]): this {
    this.resumes.push(list);
    return this;
  }

  commit(arg?: unknown): this {
    this.commits.push(arg ?? null);
    return this;
  }

  commitSync(arg?: unknown): this {
    this.commitSyncs.push(arg ?? null);
    return this;
  }

  assignments(): { topic: string; partition: number }[] {
    return this.assigned;
  }

  isConnected(): boolean {
    return true;
  }

  rebalanceProtocol(): "NONE" | "EAGER" | "COOPERATIVE" {
    return this.protocol;
  }

  assignmentLost(): boolean {
    return this.lost;
  }

  assign(list: { topic: string; partition: number; offset?: number }[]): this {
    this.assigns.push(list);
    this.assigned = list.map((tp) => ({ topic: tp.topic, partition: tp.partition }));
    return this;
  }

  incrementalAssign(list: { topic: string; partition: number; offset?: number }[]): this {
    this.incrementalAssigns.push(list);
    this.assigned.push(...list.map((tp) => ({ topic: tp.topic, partition: tp.partition })));
    return this;
  }

  unassign(): this {
    this.unassigns++;
    this.assigned = [];
    return this;
  }

  incrementalUnassign(list: { topic: string; partition: number }[]): this {
    this.incrementalUnassigns.push(list);
    return this;
  }

  /** Simulates a rebalance event reaching the KafkaJS layer through its rebalance_cb. */
  rebalance(code: number, parts: { topic: string; partition: number }[]): void {
    const err = new LibrdKafkaError(
      code === ERROR_CODES.ERR__ASSIGN_PARTITIONS ? "assign" : "revoke",
      { code, origin: "local" },
    );
    this.rebalanceCb?.(err, parts);
  }

  getWatermarkOffsets(_topic: string, _partition: number): { lowOffset: number; highOffset: number } {
    return { lowOffset: 0, highOffset: this.highWatermark };
  }
}

function msg(topic: string, partition: number, offset: number, value = `v${offset}`): Message {
  return {
    topic,
    partition,
    offset,
    value: Buffer.from(value),
    size: value.length,
    key: Buffer.from(`k${offset}`),
    timestamp: 1000 + offset,
  };
}

function makeConsumer(
  opts: { autoCommit?: boolean; maxBatch?: number; raw?: Record<string, unknown> } = {},
): { consumer: Consumer; inner: FakeInner } {
  const inner = new FakeInner();
  const raw: Record<string, unknown> = {
    kafkaJS: { groupId: "g", brokers: ["b:9092"], autoCommit: opts.autoCommit ?? true },
    ...opts.raw,
  };
  if (opts.maxBatch !== undefined) raw["js.consumer.max.batch.size"] = opts.maxBatch;
  const consumer = new Consumer(raw, {
    inner: (conf) => {
      inner.rebalanceCb = conf["rebalance_cb"] as RebalanceCb;
      return inner as unknown as KafkaConsumer;
    },
  });
  return { consumer, inner };
}

async function settle(ms = 20): Promise<void> {
  await Bun.sleep(ms);
}

/* ------------------------------------------------------------------ tests */

describe("KafkaJS Consumer — scheduler", () => {
  test("absolute per-partition ordering, parallel across partitions (concurrency 2)", async () => {
    const { consumer, inner } = makeConsumer();
    await consumer.connect();
    const seen = new Map<number, number[]>();
    let overlap = false;
    const active = new Set<number>();
    await consumer.run({
      partitionsConsumedConcurrently: 2,
      eachMessage: async ({ partition, message }) => {
        active.add(partition);
        if (active.size > 1) overlap = true;
        await Bun.sleep(2);
        const list = seen.get(partition) ?? [];
        list.push(Number(message.offset));
        seen.set(partition, list);
        active.delete(partition);
      },
    });
    for (let o = 0; o < 8; o++) inner.push(msg("t", 0, o), msg("t", 1, o));
    await settle(150);
    expect(seen.get(0)).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
    expect(seen.get(1)).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
    expect(overlap).toBe(true);
    await consumer.stop();
  });

  test("concurrency 1: never two handlers running at once", async () => {
    const { consumer, inner } = makeConsumer();
    await consumer.connect();
    let inFlight = 0;
    let maxInFlight = 0;
    await consumer.run({
      partitionsConsumedConcurrently: 1,
      eachMessage: async () => {
        inFlight++;
        maxInFlight = Math.max(maxInFlight, inFlight);
        await Bun.sleep(1);
        inFlight--;
      },
    });
    for (let o = 0; o < 5; o++) inner.push(msg("t", 0, o), msg("t", 1, o));
    await settle(100);
    expect(maxInFlight).toBe(1);
    await consumer.stop();
  });

  test("offset store = offset + 1 after each successful eachMessage", async () => {
    const { consumer, inner } = makeConsumer();
    await consumer.connect();
    await consumer.run({ eachMessage: async () => {} });
    inner.push(msg("t", 0, 0), msg("t", 0, 1), msg("t", 0, 2));
    await settle();
    expect(inner.stored.map((s) => s.offset)).toEqual([1, 2, 3]);
    await consumer.stop();
  });

  test("an eachMessage throw → seek to that offset, no store, the message is redelivered", async () => {
    const { consumer, inner } = makeConsumer();
    await consumer.connect();
    let failedOnce = false;
    const processed: number[] = [];
    await consumer.run({
      eachMessage: async ({ message }) => {
        const o = Number(message.offset);
        if (o === 1 && !failedOnce) {
          failedOnce = true;
          throw new Error("boom");
        }
        processed.push(o);
      },
    });
    inner.push(msg("t", 0, 0), msg("t", 0, 1), msg("t", 0, 2));
    await settle();
    // seek to 1; message 2 (fetched earlier, old epoch) gets dropped
    expect(inner.seeks).toContainEqual({ topic: "t", partition: 0, offset: 1 });
    expect(processed).toEqual([0]);
    // the broker "redelivers" from offset 1
    inner.push(msg("t", 0, 1), msg("t", 0, 2));
    await settle();
    expect(processed).toEqual([0, 1, 2]);
    expect(inner.stored.map((s) => s.offset)).toEqual([1, 2, 3]);
    await consumer.stop();
  });

  test("the payload's pause(): pauses the partition, no store for the in-flight message, resume continues in place", async () => {
    const { consumer, inner } = makeConsumer();
    inner.assigned = [{ topic: "t", partition: 0 }];
    await consumer.connect();
    const processed: number[] = [];
    let resumeFn: (() => void) | undefined;
    await consumer.run({
      eachMessage: async ({ message, pause }) => {
        const o = Number(message.offset);
        if (o === 1 && resumeFn === undefined) {
          resumeFn = pause();
          return;
        }
        processed.push(o);
      },
    });
    inner.push(msg("t", 0, 0), msg("t", 0, 1), msg("t", 0, 2));
    await settle();
    expect(processed).toEqual([0]);
    expect(inner.pauses.flat()).toContainEqual({ topic: "t", partition: 0 });
    // seek to the unprocessed offset (message 1 does NOT count as processed)
    expect(inner.seeks).toContainEqual({ topic: "t", partition: 0, offset: 1 });
    expect(consumer.paused()).toEqual([{ topic: "t", partitions: [0] }]);

    resumeFn?.();
    inner.push(msg("t", 0, 1), msg("t", 0, 2));
    await settle();
    expect(inner.resumes.flat()).toContainEqual({ topic: "t", partition: 0 });
    expect(processed).toEqual([0, 1, 2]);
    expect(consumer.paused()).toEqual([]);
    await consumer.stop();
  });

  test("seek() outside a handler: the epoch drops old messages, reprocessing from the new offset", async () => {
    const { consumer, inner } = makeConsumer();
    await consumer.connect();
    const processed: number[] = [];
    let block = true;
    await consumer.run({
      eachMessage: async ({ message }) => {
        while (block) await Bun.sleep(1); // hold the worker so the queue backs up
        processed.push(Number(message.offset));
      },
    });
    inner.push(msg("t", 0, 0), msg("t", 0, 1), msg("t", 0, 2));
    await settle(10);
    consumer.seek({ topic: "t", partition: 0, offset: "5" });
    block = false;
    await settle();
    // message 0 was mid-flight at seek time → stale, no store; 1,2 dropped
    expect(inner.seeks).toContainEqual({ topic: "t", partition: 0, offset: 5 });
    inner.push(msg("t", 0, 5), msg("t", 0, 6));
    await settle();
    expect(processed.filter((o) => o >= 5)).toEqual([5, 6]);
    expect(processed).not.toContain(1);
    expect(processed).not.toContain(2);
    // autoCommit on → the seeked offset gets committed (upstream semantics)
    expect(inner.commits).toContainEqual({ topic: "t", partition: 0, offset: 5 });
    await consumer.stop();
  });

  test("seek() on an unassigned partition → pending, applied on the rebalance assign", async () => {
    const { consumer, inner } = makeConsumer();
    await consumer.connect();
    await consumer.run({ eachMessage: async () => {} });
    consumer.seek({ topic: "t", partition: 3, offset: 7 });
    expect(inner.seeks).toHaveLength(0);
    inner.rebalance(ERROR_CODES.ERR__ASSIGN_PARTITIONS, [{ topic: "t", partition: 3 }]);
    await settle(5);
    // The pending seek is folded into the assign call as the start offset (no seek round-trip).
    expect(inner.assigns).toEqual([[{ topic: "t", partition: 3, offset: 7 }]]);
    expect(inner.seeks).toHaveLength(0);
    await consumer.stop();
  });

  test("a mid-flight rebalance revoke: the running handler goes stale, no store", async () => {
    const { consumer, inner } = makeConsumer();
    await consumer.connect();
    let release = false;
    await consumer.run({
      eachMessage: async () => {
        while (!release) await Bun.sleep(1);
      },
    });
    inner.push(msg("t", 0, 0));
    await settle(10);
    inner.rebalance(ERROR_CODES.ERR__REVOKE_PARTITIONS, [{ topic: "t", partition: 0 }]);
    release = true;
    await settle();
    expect(inner.stored).toHaveLength(0);
    await consumer.stop();
  });
});

describe("KafkaJS Consumer — eachBatch", () => {
  test("autoResolve=true: stores last+1, no re-seek", async () => {
    const { consumer, inner } = makeConsumer({ maxBatch: 10 });
    inner.highWatermark = 3;
    await consumer.connect();
    const batches: string[][] = [];
    await consumer.run({
      eachBatch: async ({ batch }) => {
        batches.push(batch.messages.map((m) => m.offset));
        expect(batch.topic).toBe("t");
        expect(batch.highWatermark).toBe("3");
        expect(batch.firstOffset()).toBe("0");
        expect(batch.lastOffset()).toBe("2");
        expect(batch.offsetLag()).toBe("0");
      },
    });
    inner.push(msg("t", 0, 0), msg("t", 0, 1), msg("t", 0, 2));
    await settle();
    expect(batches).toEqual([["0", "1", "2"]]);
    expect(inner.stored.map((s) => s.offset)).toEqual([3]);
    expect(inner.seeks).toHaveLength(0);
    await consumer.stop();
  });

  test("autoResolve=false + a partial resolve → stores the resolved part, re-seeks the rest", async () => {
    const { consumer, inner } = makeConsumer({ maxBatch: 10 });
    await consumer.connect();
    let first = true;
    await consumer.run({
      eachBatchAutoResolve: false,
      eachBatch: async ({ batch, resolveOffset }) => {
        if (first) {
          first = false;
          resolveOffset(batch.messages[0]!.offset); // resolve only offset 0
        }
      },
    });
    inner.push(msg("t", 0, 0), msg("t", 0, 1), msg("t", 0, 2));
    await settle();
    expect(inner.stored.map((s) => s.offset)).toEqual([1]);
    expect(inner.seeks).toContainEqual({ topic: "t", partition: 0, offset: 1 });
    await consumer.stop();
  });

  test("a handler throw: keeps the resolved offsets, seeks to lastResolved+1", async () => {
    const { consumer, inner } = makeConsumer({ maxBatch: 10 });
    await consumer.connect();
    let threwOnce = false;
    await consumer.run({
      eachBatch: async ({ batch, resolveOffset }) => {
        if (!threwOnce) {
          threwOnce = true;
          resolveOffset(batch.messages[1]!.offset); // resolve up to offset 1
          throw new Error("batch boom");
        }
      },
    });
    inner.push(msg("t", 0, 0), msg("t", 0, 1), msg("t", 0, 2));
    await settle();
    expect(inner.stored.map((s) => s.offset)).toEqual([2]);
    expect(inner.seeks).toContainEqual({ topic: "t", partition: 0, offset: 2 });
    await consumer.stop();
  });

  test("isStale after an in-handler seek; autoResolve skips a stale batch", async () => {
    const { consumer, inner } = makeConsumer({ maxBatch: 10 });
    await consumer.connect();
    let staleAfterSeek: boolean | undefined;
    await consumer.run({
      eachBatch: async ({ isStale }) => {
        expect(isStale()).toBe(false);
        consumer.seek({ topic: "t", partition: 0, offset: 9 });
        staleAfterSeek = isStale();
      },
    });
    inner.push(msg("t", 0, 0), msg("t", 0, 1));
    await settle();
    expect(staleAfterSeek).toBe(true);
    expect(inner.stored).toHaveLength(0); // stale → no auto-resolve
    await consumer.stop();
  });

  test("commitOffsetsIfNecessary: commits when autoCommit=false, a no-op when true", async () => {
    for (const autoCommit of [false, true]) {
      const { consumer, inner } = makeConsumer({ autoCommit, maxBatch: 10 });
      await consumer.connect();
      await consumer.run({
        eachBatch: async ({ commitOffsetsIfNecessary }) => {
          await commitOffsetsIfNecessary();
        },
      });
      inner.push(msg("t", 0, 0));
      await settle();
      expect(inner.commitSyncs.length).toBe(autoCommit ? 0 : 1);
      await consumer.stop();
    }
  });
});

describe("KafkaJS Consumer — API surface", () => {
  test("run() twice → 'Consumer is already running.'; a missing handler → throw", async () => {
    const { consumer } = makeConsumer();
    await consumer.connect();
    await consumer.run({ eachMessage: async () => {} });
    expect(consumer.run({ eachMessage: async () => {} })).rejects.toThrow(
      "Consumer is already running.",
    );
    await consumer.stop();
    expect(consumer.run({})).rejects.toThrow();
  });

  test("stop() then run() again works; running handlers are awaited", async () => {
    const { consumer, inner } = makeConsumer();
    await consumer.connect();
    let finished = false;
    await consumer.run({
      eachMessage: async () => {
        await Bun.sleep(15);
        finished = true;
      },
    });
    inner.push(msg("t", 0, 0));
    await settle(5);
    await consumer.stop();
    expect(finished).toBe(true); // graceful: waits for the handler
    await consumer.run({ eachMessage: async () => {} });
    await consumer.stop();
  });

  test("subscribe: RegExps must start with ^ and carry no flags; merged unless replace", async () => {
    const { consumer, inner } = makeConsumer();
    await consumer.connect();
    expect(consumer.subscribe({ topics: [/foo/] })).rejects.toThrow('"^"');
    expect(consumer.subscribe({ topics: [/^foo/i] })).rejects.toThrow("flags");
    await consumer.subscribe({ topics: ["a", /^pre-.*/] });
    expect(inner.subscribes.at(-1)).toEqual(["a", "^pre-.*"]);
    await consumer.subscribe({ topics: ["b"] });
    expect(inner.subscribes.at(-1)).toEqual(["a", "^pre-.*", "b"]);
    await consumer.subscribe({ topics: ["c"], replace: true });
    expect(inner.subscribes.at(-1)).toEqual(["c"]);
  });

  test("subscribe/commitOffsets/seek before connect → KafkaJSError", async () => {
    const { consumer } = makeConsumer();
    expect(consumer.subscribe({ topics: ["x"] })).rejects.toThrow("connect");
    expect(consumer.commitOffsets()).rejects.toThrow("connect");
    expect(() => consumer.seek({ topic: "x", partition: 0, offset: 0 })).toThrow("connect");
  });

  test("disconnect with autoCommit: commitSync() gets called", async () => {
    const { consumer, inner } = makeConsumer();
    await consumer.connect();
    await consumer.run({ eachMessage: async () => {} });
    inner.push(msg("t", 0, 0));
    await settle();
    await consumer.disconnect();
    expect(inner.commitSyncs).toContainEqual(null);
  });

  test("the KafkaJS message shape: string offset/timestamp, headers merged into an object, dups into arrays", async () => {
    const { consumer, inner } = makeConsumer();
    await consumer.connect();
    let got: unknown;
    await consumer.run({
      eachMessage: async ({ message }) => {
        got = message;
      },
    });
    const m = msg("t", 0, 42, "hello");
    m.headers = [{ a: Buffer.from("1") }, { b: Buffer.from("x") }, { a: Buffer.from("2") }];
    m.leaderEpoch = 5;
    inner.push(m);
    await settle();
    const message = got as {
      offset: string;
      timestamp: string;
      attributes: number;
      size: number;
      leaderEpoch: number;
      headers: Record<string, Buffer | Buffer[]>;
      value: Buffer;
    };
    expect(message.offset).toBe("42");
    expect(message.timestamp).toBe("1042");
    expect(message.attributes).toBe(0);
    expect(message.size).toBe(5);
    expect(message.leaderEpoch).toBe(5);
    expect(message.value.toString()).toBe("hello");
    expect((message.headers["a"] as Buffer[]).map((b) => b.toString())).toEqual(["1", "2"]);
    expect((message.headers["b"] as Buffer).toString()).toBe("x");
    await consumer.stop();
  });

  test("backpressure: the pump stops past the cap; workers wake it again", async () => {
    const { consumer, inner } = makeConsumer({ maxBatch: 5000 });
    await consumer.connect();
    let processed = 0;
    await consumer.run({
      eachMessage: async () => {
        processed++;
      },
    });
    // 12k messages — past the 10k cap; all must eventually be processed
    const batch: Message[] = [];
    for (let o = 0; o < 12_000; o++) batch.push(msg("t", 0, o, "x"));
    inner.push(...batch);
    await settle(300);
    expect(processed).toBe(12_000);
    await consumer.stop();
  });
});
