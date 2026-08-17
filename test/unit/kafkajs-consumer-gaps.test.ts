/**
 * Unit tests for the KafkaJS Consumer API gaps closed after the audit:
 * pause() → resume fn, committed(), storeOffsets(), logger(), dependentAdmin(),
 * _getInternalClient(), and the rebalance_cb `assignmentFns` plumbing — all on
 * a FAKE KafkaConsumer (no .so/broker needed).
 */

import { describe, expect, test } from "bun:test";
import { EventEmitter } from "node:events";
import { Buffer } from "node:buffer";
import { Consumer } from "../../packages/bun-rdkafka/src/kafkajs/consumer.ts";
import { Admin } from "../../packages/bun-rdkafka/src/kafkajs/admin.ts";
import {
  DefaultLogger,
  logLevel,
  type Logger,
} from "../../packages/bun-rdkafka/src/kafkajs/config-mapper.ts";
import type { KafkaConsumer, Message } from "../../packages/bun-rdkafka/src/callback/kafka-consumer.ts";
import { ERROR_CODES, LibrdKafkaError } from "../../packages/bun-rdkafka/src/core/errors.ts";

/* ------------------------------------------------------------------ fake */

type TP = { topic: string; partition: number };
type TPO = TP & { offset?: number; leaderEpoch?: number; metadata?: string };
type RebalanceCb = (err: LibrdKafkaError, parts: TP[]) => void;

interface PendingConsume {
  n: number;
  cb: (err: null, messages: Message[]) => void;
}

class FakeInner extends EventEmitter {
  name = "consumer#fake";
  rebalanceCb: RebalanceCb | undefined;
  protocol: "NONE" | "EAGER" | "COOPERATIVE" = "EAGER";
  lost = false;
  assigned: TP[] = [];
  assigns: TPO[][] = [];
  incrementalAssigns: TPO[][] = [];
  unassigns = 0;
  incrementalUnassigns: TP[][] = [];
  stored: TPO[] = [];
  seeks: TPO[] = [];
  pauses: TP[][] = [];
  resumes: TP[][] = [];
  committedCalls: { toppars: TP[]; timeout: number }[] = [];
  committedAnswer: TPO[] | LibrdKafkaError = [];
  storeError: LibrdKafkaError | null = null;
  feed: Message[] = [];
  #pending: PendingConsume[] = [];

  connect(_opts: unknown, cb?: (err: null) => void): this {
    queueMicrotask(() => cb?.(null));
    return this;
  }
  disconnect(cb?: () => void): this {
    queueMicrotask(() => cb?.());
    return this;
  }
  isConnected(): boolean {
    return true;
  }
  subscribe(): this {
    return this;
  }
  consume(n: number, cb: (err: null, messages: Message[]) => void): void {
    this.#pending.push({ n, cb });
    queueMicrotask(() => this.#serve());
  }
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
  offsetsStore(list: TPO[]): this {
    if (this.storeError !== null) throw this.storeError;
    this.stored.push(...list);
    return this;
  }
  seek(tpo: TPO, _timeout: number | null, cb: (err: null) => void): this {
    this.seeks.push(tpo);
    queueMicrotask(() => cb(null));
    return this;
  }
  pause(list: TP[]): this {
    this.pauses.push(list);
    return this;
  }
  resume(list: TP[]): this {
    this.resumes.push(list);
    return this;
  }
  commit(): this {
    return this;
  }
  commitSync(): this {
    return this;
  }
  assignments(): TP[] {
    return this.assigned;
  }
  rebalanceProtocol(): "NONE" | "EAGER" | "COOPERATIVE" {
    return this.protocol;
  }
  assignmentLost(): boolean {
    return this.lost;
  }
  assign(list: TPO[]): this {
    this.assigns.push(list);
    this.assigned = list.map((tp) => ({ topic: tp.topic, partition: tp.partition }));
    return this;
  }
  incrementalAssign(list: TPO[]): this {
    this.incrementalAssigns.push(list);
    this.assigned.push(...list.map((tp) => ({ topic: tp.topic, partition: tp.partition })));
    return this;
  }
  unassign(): this {
    this.unassigns++;
    this.assigned = [];
    return this;
  }
  incrementalUnassign(list: TP[]): this {
    this.incrementalUnassigns.push(list);
    return this;
  }
  committed(
    toppars: TP[],
    timeout: number,
    cb: (err: LibrdKafkaError | null, offsets?: TPO[]) => void,
  ): this {
    this.committedCalls.push({ toppars, timeout });
    const answer = this.committedAnswer;
    queueMicrotask(() => {
      if (answer instanceof LibrdKafkaError) cb(answer);
      else cb(null, answer);
    });
    return this;
  }
  getWatermarkOffsets(): { lowOffset: number; highOffset: number } {
    return { lowOffset: 0, highOffset: -1001 };
  }
  /** Simulates a rebalance reaching the KafkaJS layer through its rebalance_cb trampoline. */
  rebalance(code: number, parts: TP[]): void {
    const err = new LibrdKafkaError(code === ERROR_CODES.ERR__ASSIGN_PARTITIONS ? "assign" : "revoke", {
      code,
      origin: "local",
    });
    this.rebalanceCb?.(err, parts);
  }
}

class RecordingLogger implements Logger {
  level: number = logLevel.INFO;
  lines: { level: string; message: string }[] = [];
  setLogLevel(level: number): void {
    this.level = level;
  }
  info(message: string): void {
    this.lines.push({ level: "info", message });
  }
  error(message: string): void {
    this.lines.push({ level: "error", message });
  }
  warn(message: string): void {
    this.lines.push({ level: "warn", message });
  }
  debug(message: string): void {
    this.lines.push({ level: "debug", message });
  }
}

function msg(topic: string, partition: number, offset: number): Message {
  const value = `v${offset}`;
  return { topic, partition, offset, value: Buffer.from(value), size: value.length, key: null, timestamp: 1 };
}

function makeConsumer(raw: Record<string, unknown> = {}): { consumer: Consumer; inner: FakeInner } {
  const inner = new FakeInner();
  const consumer = new Consumer(
    { kafkaJS: { groupId: "g", brokers: ["b:9092"] }, ...raw },
    {
      inner: (conf) => {
        inner.rebalanceCb = conf["rebalance_cb"] as RebalanceCb;
        return inner as unknown as KafkaConsumer;
      },
    },
  );
  return { consumer, inner };
}

const settle = (ms = 10): Promise<void> => Bun.sleep(ms);
const ASSIGN = ERROR_CODES.ERR__ASSIGN_PARTITIONS;
const REVOKE = ERROR_CODES.ERR__REVOKE_PARTITIONS;

/* ------------------------------------------------------------------ tests */

describe("KafkaJS Consumer — state checks", () => {
  test("pause/resume/committed/storeOffsets before connect → ERR__STATE", async () => {
    const { consumer } = makeConsumer();
    expect(() => consumer.pause([{ topic: "t" }])).toThrow(expect.objectContaining({ code: ERROR_CODES.ERR__STATE }));
    expect(() => consumer.resume([{ topic: "t" }])).toThrow(expect.objectContaining({ code: ERROR_CODES.ERR__STATE }));
    expect(() => consumer.storeOffsets([{ topic: "t", partition: 0, offset: "1" }])).toThrow(
      expect.objectContaining({ code: ERROR_CODES.ERR__STATE }),
    );
    await expect(consumer.committed()).rejects.toMatchObject({ code: ERROR_CODES.ERR__STATE });
    expect(consumer._getInternalClient()).toBeNull();
  });

  test("_getInternalClient() returns the callback consumer once connected", async () => {
    const { consumer, inner } = makeConsumer();
    await consumer.connect();
    expect(consumer._getInternalClient()).toBe(inner as unknown as KafkaConsumer);
    await consumer.disconnect();
    expect(consumer._getInternalClient()).toBeNull();
  });
});

describe("KafkaJS Consumer — pause() returns a resume function", () => {
  test("the returned function resumes exactly the paused partitions", async () => {
    const { consumer, inner } = makeConsumer();
    await consumer.connect();
    inner.assigned = [
      { topic: "t", partition: 0 },
      { topic: "t", partition: 1 },
      { topic: "t", partition: 2 },
    ];
    const resume = consumer.pause([{ topic: "t", partitions: [0, 2] }]);
    expect(typeof resume).toBe("function");
    expect(inner.pauses).toEqual([
      [
        { topic: "t", partition: 0 },
        { topic: "t", partition: 2 },
      ],
    ]);
    resume();
    expect(inner.resumes.flat()).toEqual([
      { topic: "t", partition: 0 },
      { topic: "t", partition: 2 },
    ]);
    await consumer.disconnect();
  });
});

describe("KafkaJS Consumer — committed()", () => {
  test("defaults to the current assignment, maps to the KafkaJS shape (string offsets, null-filled)", async () => {
    const { consumer, inner } = makeConsumer();
    await consumer.connect();
    inner.assigned = [{ topic: "t", partition: 0 }];
    inner.committedAnswer = [
      { topic: "t", partition: 0, offset: 42, leaderEpoch: 3, metadata: "m" },
      { topic: "t", partition: 1, offset: -1001 },
    ];
    const out = await consumer.committed();
    expect(inner.committedCalls).toEqual([{ toppars: [{ topic: "t", partition: 0 }], timeout: -1 }]);
    expect(out).toEqual([
      { topic: "t", partition: 0, offset: "42", leaderEpoch: 3, metadata: "m" },
      { topic: "t", partition: 1, offset: "-1001", leaderEpoch: null, metadata: null },
    ]);
    await consumer.committed([{ topic: "x", partition: 5 }], 1234);
    expect(inner.committedCalls[1]).toEqual({ toppars: [{ topic: "x", partition: 5 }], timeout: 1234 });
    await consumer.disconnect();
  });

  test("a librdkafka error → KafkaJSError with the same code", async () => {
    const { consumer, inner } = makeConsumer();
    await consumer.connect();
    inner.committedAnswer = new LibrdKafkaError("nope", { code: ERROR_CODES.ERR__TIMED_OUT, origin: "local" });
    await expect(consumer.committed([{ topic: "t", partition: 0 }])).rejects.toMatchObject({
      code: ERROR_CODES.ERR__TIMED_OUT,
    });
    await consumer.disconnect();
  });
});

describe("KafkaJS Consumer — storeOffsets()", () => {
  test("stores numeric offsets (+ leaderEpoch/metadata) via the callback layer", async () => {
    const { consumer, inner } = makeConsumer();
    await consumer.connect();
    consumer.storeOffsets([
      { topic: "t", partition: 0, offset: "10" },
      { topic: "t", partition: 1, offset: 20, leaderEpoch: 2, metadata: "meta" },
    ]);
    expect(inner.stored).toEqual([
      { topic: "t", partition: 0, offset: 10 },
      { topic: "t", partition: 1, offset: 20, leaderEpoch: 2, metadata: "meta" },
    ]);
    await consumer.disconnect();
  });

  test("argument validation → ERR__INVALID_ARG; ERR__STATE from librdkafka is mapped", async () => {
    const { consumer, inner } = makeConsumer();
    await consumer.connect();
    expect(() => consumer.storeOffsets("x" as never)).toThrow(
      expect.objectContaining({ code: ERROR_CODES.ERR__INVALID_ARG }),
    );
    expect(() => consumer.storeOffsets([{ topic: "t", partition: 0, offset: "abc" }])).toThrow(
      expect.objectContaining({ code: ERROR_CODES.ERR__INVALID_ARG }),
    );
    inner.storeError = new LibrdKafkaError("not assigned", { code: ERROR_CODES.ERR__STATE, origin: "local" });
    expect(() => consumer.storeOffsets([{ topic: "t", partition: 0, offset: "1" }])).toThrow(
      expect.objectContaining({ code: ERROR_CODES.ERR__STATE }),
    );
    await consumer.disconnect();
  });

  test("a user-stored offset is the partition's next-unprocessed reference until the scheduler stores again", async () => {
    const { consumer, inner } = makeConsumer();
    await consumer.connect();
    let block = true;
    await consumer.run({
      eachMessage: async () => {
        while (block) await Bun.sleep(1);
      },
    });
    inner.assigned = [{ topic: "t", partition: 0 }];
    inner.rebalance(ASSIGN, [{ topic: "t", partition: 0 }]);
    inner.push(msg("t", 0, 0), msg("t", 0, 1));
    await settle();
    // Handler is stuck on offset 0; the user stores 7 (skipping ahead), then pauses:
    // the pause seeks to the user's stored offset, not to 0.
    consumer.storeOffsets([{ topic: "t", partition: 0, offset: 7 }]);
    consumer.pause([{ topic: "t", partitions: [0] }]);
    expect(inner.seeks).toContainEqual({ topic: "t", partition: 0, offset: 7 });
    block = false;
    await settle();
    // The stale handler (paused mid-flight) does not store 1 over the user's 7.
    expect(inner.stored.map((s) => s.offset)).toEqual([7]);
    await consumer.disconnect();
  });
});

describe("KafkaJS Consumer — logger()", () => {
  test("a DefaultLogger by default; `debug` config → DEBUG level", () => {
    const { consumer } = makeConsumer();
    expect(consumer.logger()).toBeInstanceOf(DefaultLogger);
    const { consumer: debugConsumer } = makeConsumer({ debug: "all" });
    expect((debugConsumer.logger() as unknown as { level: number }).level).toBe(logLevel.DEBUG);
  });

  test("a custom kafkaJS.logger receives librdkafka logs (event.log) and internal lines", async () => {
    const logger = new RecordingLogger();
    const { consumer, inner } = makeConsumer({ kafkaJS: { groupId: "g", brokers: ["b:9092"], logger } });
    expect(consumer.logger()).toBe(logger);
    await consumer.connect();
    inner.emit("event.log", { severity: 6, fac: "FAC", message: "hello from librdkafka" });
    inner.emit("event.error", new LibrdKafkaError("boom", { code: ERROR_CODES.ERR__TRANSPORT, origin: "local" }));
    inner.rebalance(ASSIGN, [{ topic: "t", partition: 0 }]);
    await settle();
    expect(logger.lines.some((l) => l.level === "info" && l.message.includes("hello from librdkafka"))).toBe(true);
    expect(logger.lines.some((l) => l.level === "error" && l.message.includes("boom"))).toBe(true);
    expect(logger.lines.some((l) => l.level === "info" && l.message.includes("Received rebalance event"))).toBe(true);
    await consumer.disconnect();
  });

  test("kafkaJS.logLevel is honored by the custom logger", () => {
    const logger = new RecordingLogger();
    makeConsumer({ kafkaJS: { groupId: "g", brokers: ["b:9092"], logger, logLevel: logLevel.ERROR } });
    expect(logger.level).toBe(logLevel.ERROR);
  });
});

describe("KafkaJS Consumer — dependentAdmin()", () => {
  test("returns an Admin that refuses to connect while the consumer is not connected", async () => {
    const { consumer } = makeConsumer();
    const admin = consumer.dependentAdmin();
    expect(admin).toBeInstanceOf(Admin);
    await expect(admin.connect()).rejects.toMatchObject({ code: ERROR_CODES.ERR__STATE });
  });

  test("shares the consumer's logger once connected", async () => {
    const logger = new RecordingLogger();
    const { consumer } = makeConsumer({ kafkaJS: { groupId: "g", brokers: ["b:9092"], logger } });
    await consumer.connect();
    const admin = consumer.dependentAdmin();
    await admin.connect();
    expect(admin.logger()).toBe(logger);
    expect(logger.lines.some((l) => l.message.includes("Admin client connected"))).toBe(true);
    await admin.disconnect();
    await consumer.disconnect();
  });
});

describe("KafkaJS Consumer — rebalance_cb with assignmentFns", () => {
  test("no user cb: default assign/unassign per protocol", async () => {
    const { consumer, inner } = makeConsumer();
    await consumer.connect();
    inner.rebalance(ASSIGN, [{ topic: "t", partition: 0 }]);
    await settle();
    expect(inner.assigns).toEqual([[{ topic: "t", partition: 0 }]]);
    inner.rebalance(REVOKE, [{ topic: "t", partition: 0 }]);
    await settle();
    expect(inner.unassigns).toBe(1);

    inner.protocol = "COOPERATIVE";
    inner.rebalance(ASSIGN, [{ topic: "t", partition: 1 }]);
    await settle();
    expect(inner.incrementalAssigns).toEqual([[{ topic: "t", partition: 1 }]]);
    inner.rebalance(REVOKE, [{ topic: "t", partition: 1 }]);
    await settle();
    expect(inner.incrementalUnassigns).toEqual([[{ topic: "t", partition: 1 }]]);
    await consumer.disconnect();
  });

  test("the user cb receives (err, assignment, {assign, unassign, assignmentLost}) — awaited", async () => {
    const seen: unknown[] = [];
    const { consumer, inner } = makeConsumer({
      rebalance_cb: async (err: LibrdKafkaError, assignment: unknown, fns: Record<string, unknown>) => {
        await Bun.sleep(5);
        seen.push([err.code, assignment, Object.keys(fns).sort(), (fns["assignmentLost"] as () => boolean)()]);
      },
    });
    await consumer.connect();
    inner.lost = true;
    inner.rebalance(REVOKE, [{ topic: "t", partition: 0 }]);
    // Not yet assigned/unassigned: the user cb is still awaited.
    expect(inner.unassigns).toBe(0);
    await settle(20);
    expect(seen).toEqual([[REVOKE, [{ topic: "t", partition: 0 }], ["assign", "assignmentLost", "unassign"], true]]);
    expect(inner.unassigns).toBe(1);
    await consumer.disconnect();
  });

  test("assignmentFns.assign(modified) is used and the default assign is skipped", async () => {
    const { consumer, inner } = makeConsumer({
      rebalance_cb: (
        err: LibrdKafkaError,
        assignment: { topic: string; partition: number; offset?: number }[],
        fns: { assign: (a: unknown[]) => void },
      ) => {
        if (err.code === ASSIGN) {
          assignment[0]!.offset = 34;
          fns.assign(assignment);
        }
      },
    });
    await consumer.connect();
    consumer.seek({ topic: "t", partition: 0, offset: 99 }); // pending — must NOT be applied over the user's choice
    inner.rebalance(ASSIGN, [{ topic: "t", partition: 0 }]);
    await settle();
    expect(inner.assigns).toEqual([[{ topic: "t", partition: 0, offset: 34 }]]);
    await consumer.disconnect();
  });

  test("COOPERATIVE: assignmentFns.assign → incrementalAssign", async () => {
    const { consumer, inner } = makeConsumer({
      rebalance_cb: (_err: LibrdKafkaError, assignment: unknown[], fns: { assign: (a: unknown[]) => void }) =>
        fns.assign(assignment),
    });
    await consumer.connect();
    inner.protocol = "COOPERATIVE";
    inner.rebalance(ASSIGN, [{ topic: "t", partition: 2 }]);
    await settle();
    expect(inner.incrementalAssigns).toEqual([[{ topic: "t", partition: 2 }]]);
    expect(inner.assigns).toEqual([]);
    await consumer.disconnect();
  });

  test("a truthy return value is used as the alternate assignment (pending seeks skipped)", async () => {
    const { consumer, inner } = makeConsumer({
      rebalance_cb: (err: LibrdKafkaError) =>
        err.code === ASSIGN ? [{ topic: "t", partition: 0, offset: 5 }] : undefined,
    });
    await consumer.connect();
    consumer.seek({ topic: "t", partition: 0, offset: 99 });
    inner.rebalance(ASSIGN, [{ topic: "t", partition: 0 }]);
    await settle();
    expect(inner.assigns).toEqual([[{ topic: "t", partition: 0, offset: 5 }]]);
    await consumer.disconnect();
  });

  test("a throwing user cb is logged and the default behavior continues (pending seek applied)", async () => {
    const logger = new RecordingLogger();
    const { consumer, inner } = makeConsumer({
      kafkaJS: { groupId: "g", brokers: ["b:9092"], logger },
      rebalance_cb: () => {
        throw new Error("user cb exploded");
      },
    });
    await consumer.connect();
    consumer.seek({ topic: "t", partition: 0, offset: 7 });
    inner.rebalance(ASSIGN, [{ topic: "t", partition: 0 }]);
    await settle();
    expect(inner.assigns).toEqual([[{ topic: "t", partition: 0, offset: 7 }]]);
    expect(logger.lines.some((l) => l.level === "error" && l.message.includes("user cb exploded"))).toBe(true);
    await consumer.disconnect();
  });

  test("assignmentFns.unassign on revoke: epochs bump — a mid-flight handler stores nothing", async () => {
    let unassignCalls = 0;
    const { consumer, inner } = makeConsumer({
      rebalance_cb: (err: LibrdKafkaError, assignment: unknown[], fns: { unassign: (a: unknown[]) => void }) => {
        if (err.code === REVOKE) {
          unassignCalls++;
          fns.unassign(assignment);
        }
      },
    });
    await consumer.connect();
    let release = false;
    await consumer.run({
      eachMessage: async () => {
        while (!release) await Bun.sleep(1);
      },
    });
    inner.rebalance(ASSIGN, [{ topic: "t", partition: 0 }]);
    inner.push(msg("t", 0, 0));
    await settle();
    inner.rebalance(REVOKE, [{ topic: "t", partition: 0 }]);
    release = true;
    await settle(20);
    expect(unassignCalls).toBe(1);
    expect(inner.unassigns).toBe(1);
    expect(inner.stored).toHaveLength(0);
    await consumer.disconnect();
  });
});
