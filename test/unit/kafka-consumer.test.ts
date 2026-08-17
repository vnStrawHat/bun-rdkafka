/**
 * test/unit/kafka-consumer.test.ts — KafkaConsumer (M3) with a fake native.
 *
 * The fake simulates "the C side": MESSAGE BATCH / EVENT FRAME queues built
 * with helpers/c-side-encoders (byte-exact per the header ABI), a topic intern
 * table, and brk_* call recording for semantics asserts.
 */

import { describe, expect, test } from "bun:test";
import { Buffer } from "node:buffer";
import { ptr } from "bun:ffi";

import { KafkaConsumer, type Message } from "../../packages/bun-rdkafka/src/callback/kafka-consumer.ts";
import type { LibrdKafkaError } from "../../packages/bun-rdkafka/src/core/errors.ts";
import { ERROR_CODES } from "../../packages/bun-rdkafka/src/core/errors.ts";
import {
  encodeStringList,
  encodeTpl,
  type TopicPartitionInput,
} from "../../packages/bun-rdkafka/src/core/batch-decoder.ts";
import type { BrkNative } from "../../packages/bun-rdkafka/src/ffi/loader.ts";
import {
  BRK_ASSIGN,
  BRK_ASSIGN_INCREMENTAL,
  BRK_EVENT_OFFSET_COMMIT,
  BRK_EVENT_REBALANCE,
  BRK_REBALANCE_PROTOCOL_COOPERATIVE,
  BRK_REBALANCE_PROTOCOL_EAGER,
  BRK_UNASSIGN,
  BRK_UNASSIGN_INCREMENTAL,
} from "../../packages/bun-rdkafka/src/ffi/types.ts";
import {
  encodeEventFrames,
  encodeMessageBatch,
  offsetCommitPayload,
  rebalancePayload,
  type RawMessage,
} from "./helpers/c-side-encoders.ts";

/* ========================================================================== */
/* Fake native                                                                 */
/* ========================================================================== */

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyFn = (...args: any[]) => unknown;

const META_JSON = JSON.stringify({
  orig_broker_id: 1,
  orig_broker_name: "localhost:9092/1",
  brokers: [{ id: 1, host: "localhost", port: 9092 }],
  topics: [],
});

const keepAlive: Uint8Array[] = [];
const ENC = new TextEncoder();

interface AssignCall {
  mode: number;
  /** tpl bytes COPIED at call time (the original buffer is reused). */
  tpl: Uint8Array | null;
}

interface CommitCall {
  tpl: Uint8Array | null;
  async: number;
}

/** A simulated "C side" for one consumer. */
class FakeWorld {
  /** The result queue for brk_consume_batch. */
  readonly batches: { bytes: Uint8Array; count: number }[] = [];
  /** The result queue for brk_events_poll. */
  readonly events: { bytes: Uint8Array; count: number }[] = [];
  readonly topicNames = new Map<number, string>();
  /** The current assignment (written by brk_assign, read by brk_assignment). */
  assignment: TopicPartitionInput[] = [];

  readonly assignCalls: AssignCall[] = [];
  readonly commitCalls: CommitCall[] = [];
  readonly seekCalls: { topic: string; partition: number; offset: number; timeout: number }[] = [];
  readonly pauseResumeCalls: { resume: number }[] = [];
  subscribedLists: string[][] = [];
  unsubscribeCount = 0;

  pushMessages(messages: RawMessage[]): void {
    this.batches.push({ bytes: encodeMessageBatch(messages), count: messages.length });
  }

  pushEventFrames(frames: { type: number; payload: Uint8Array }[]): void {
    this.events.push({ bytes: encodeEventFrames(frames), count: frames.length });
  }

  native(): BrkNative {
    const overrides: Record<string, AnyFn> = {
      brk_conf_new: () => 1,
      brk_client_new: () => 2,
      brk_metadata: (_h: unknown, _t: unknown, _ms: unknown, out: BigUint64Array) => {
        const buf = ENC.encode(`${META_JSON}\0`);
        keepAlive.push(buf);
        out[0] = BigInt(ptr(buf));
        return buf.length - 1;
      },
      brk_consume_batch: (_h: unknown, buf: Uint8Array, cap: number, maxMsgs: number) => {
        const next = this.batches[0];
        if (!next) return 0;
        if (next.count > maxMsgs) return 0; // tests always push batches ≤ maxMsgs
        if (next.bytes.length > cap) return -1; // BUFFER_TOO_SMALL unused here
        this.batches.shift();
        buf.set(next.bytes);
        return next.count;
      },
      brk_events_poll: (_h: unknown, buf: Uint8Array, cap: number) => {
        const next = this.events[0];
        if (!next) return 0;
        if (next.bytes.length > cap) return -1;
        this.events.shift();
        buf.set(next.bytes);
        return next.count;
      },
      brk_topic_name: (_h: unknown, topicId: number, buf: Uint8Array) => {
        const name = this.topicNames.get(topicId);
        if (name === undefined) return ERROR_CODES.ERR__UNKNOWN_TOPIC ?? -1;
        const bytes = ENC.encode(name);
        buf.set(bytes);
        return bytes.length;
      },
      brk_subscribe: (_h: unknown, payload: Uint8Array, len: number) => {
        // decode the STRING LIST for asserts
        const view = payload.subarray(0, len);
        const dv = new DataView(view.buffer, view.byteOffset, view.byteLength);
        const count = dv.getUint32(0, true);
        const topics: string[] = [];
        let off = 4;
        for (let i = 0; i < count; i++) {
          const n = dv.getUint16(off, true);
          off += 2;
          topics.push(new TextDecoder().decode(view.subarray(off, off + n)));
          off += n;
        }
        this.subscribedLists.push(topics);
        return 0;
      },
      brk_unsubscribe: () => {
        this.unsubscribeCount++;
        return 0;
      },
      brk_subscription: (_h: unknown, buf: Uint8Array) => {
        const w = encodeStringList(this.subscribedLists.at(-1) ?? []);
        const bytes = w.toBytes();
        buf.set(bytes);
        return this.subscribedLists.at(-1)?.length ?? 0;
      },
      brk_assign: (_h: unknown, tplBuf: Uint8Array | null, len: number, mode: number) => {
        this.assignCalls.push({
          mode,
          tpl: tplBuf === null ? null : tplBuf.slice(0, len),
        });
        // Update the fake assignment so brk_assignment reflects it (enough for #refreshHasAssignment).
        if (mode === BRK_ASSIGN) this.assignment = tplBuf === null ? [] : [{ topic: "x", partition: 0 }];
        else if (mode === BRK_UNASSIGN) this.assignment = [];
        else if (mode === BRK_ASSIGN_INCREMENTAL) this.assignment.push({ topic: "x", partition: this.assignment.length });
        else if (mode === BRK_UNASSIGN_INCREMENTAL) this.assignment.pop();
        return 0;
      },
      brk_assignment: (_h: unknown, buf: Uint8Array) => {
        const bytes = encodeTpl(this.assignment).toBytes();
        buf.set(bytes);
        return this.assignment.length;
      },
      brk_position: (_h: unknown, buf: Uint8Array) => {
        const bytes = encodeTpl(this.assignment).toBytes();
        buf.set(bytes);
        return this.assignment.length;
      },
      brk_commit: (_h: unknown, tplBuf: Uint8Array | null, len: number, async: number) => {
        this.commitCalls.push({ tpl: tplBuf === null ? null : tplBuf.slice(0, len), async });
        return 0;
      },
      brk_seek: (
        _h: unknown,
        topicC: Uint8Array,
        partition: number,
        offset: number | bigint,
        timeout: number,
      ) => {
        const end = topicC.indexOf(0);
        this.seekCalls.push({
          topic: new TextDecoder().decode(topicC.subarray(0, end < 0 ? undefined : end)),
          partition,
          offset: Number(offset),
          timeout,
        });
        return 0;
      },
      brk_pause_resume: (_h: unknown, _tpl: Uint8Array, _len: number, resume: number) => {
        this.pauseResumeCalls.push({ resume });
        return 0;
      },
    };
    const proxy = new Proxy(
      {},
      {
        get:
          (_t, prop: string) =>
          (...args: unknown[]) =>
            overrides[prop] ? overrides[prop](...args) : 0,
      },
    );
    return proxy as unknown as BrkNative;
  }
}

/** Decodes the tpl bytes (JS→C, format 2) the fake captured — for content asserts. */
function decodeCapturedTpl(bytes: Uint8Array): { topic: string; partition: number; offset: number }[] {
  const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const count = dv.getUint32(0, true);
  let off = 4;
  const out: { topic: string; partition: number; offset: number }[] = [];
  for (let i = 0; i < count; i++) {
    off += 4; // topic_id (always -1 JS→C in these tests)
    const nameLen = dv.getUint16(off, true);
    off += 2;
    const topic = new TextDecoder().decode(bytes.subarray(off, off + nameLen));
    off += nameLen;
    const partition = dv.getInt32(off, true);
    off += 4;
    const offset = Number(dv.getBigInt64(off, true));
    off += 8 + 4; // offset + leader_epoch
    const metadataLen = dv.getInt16(off, true);
    off += 2 + (metadataLen > 0 ? metadataLen : 0);
    out.push({ topic, partition, offset });
  }
  return out;
}

async function makeConsumer(
  world: FakeWorld,
  conf: Record<string, unknown> = {},
): Promise<KafkaConsumer> {
  const consumer = new KafkaConsumer({ "group.id": "m3-unit", ...conf }, undefined, {
    native: world.native(),
    onLeak: () => {},
  });
  await new Promise<void>((resolve, reject) => {
    consumer.connect({}, (err) => (err ? reject(err) : resolve()));
  });
  return consumer;
}

async function waitUntil(fn: () => boolean, ms = 2000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!fn()) {
    if (Date.now() > deadline) throw new Error("waitUntil: timed out");
    await Bun.sleep(5);
  }
}

const TOPIC_ID = 7;

function raw(partial: Partial<RawMessage> & { offset: number }): RawMessage {
  return {
    topicId: TOPIC_ID,
    partition: 0,
    timestampMs: 1_700_000_000_123,
    timestampType: 1,
    err: 0,
    key: null,
    value: `v${partial.offset}`,
    ...partial,
  } as RawMessage;
}

/* ========================================================================== */
/* Flowing                                                                     */
/* ========================================================================== */

describe("KafkaConsumer flowing", () => {
  test("consume() emits 'data' per message with the upstream shape field for field", async () => {
    const world = new FakeWorld();
    world.topicNames.set(TOPIC_ID, "m3-topic");
    world.pushMessages([
      raw({
        offset: 0,
        key: "k0",
        value: "hello",
        headers: [{ key: "h1", value: "x" }, { key: "h2", value: null }],
        leaderEpoch: 3,
      }),
      raw({ offset: 1, value: null, timestampMs: -1 }),
    ]);
    const consumer = await makeConsumer(world);
    const got: Message[] = [];
    const cbGot: Message[] = [];
    consumer.on("data", (m: Message) => got.push(m));
    consumer.subscribe(["m3-topic"]);
    consumer.consume((_err, m) => cbGot.push(m));
    await waitUntil(() => got.length === 2);

    const m0 = got[0] as Message;
    expect(m0.topic).toBe("m3-topic");
    expect(m0.partition).toBe(0);
    expect(m0.offset).toBe(0);
    expect(Buffer.isBuffer(m0.value)).toBe(true);
    expect((m0.value as Buffer).toString()).toBe("hello");
    expect(m0.size).toBe(5);
    expect((m0.key as Buffer).toString()).toBe("k0");
    expect(m0.timestamp).toBe(1_700_000_000_123);
    expect(m0.leaderEpoch).toBe(3);
    // headers: an array of ONE-key objects with Buffer values.
    expect(m0.headers).toHaveLength(2);
    const h0 = (m0.headers as Record<string, Buffer>[])[0] as Record<string, Buffer>;
    expect(Object.keys(h0)).toEqual(["h1"]);
    expect(h0["h1"]!.toString()).toBe("x");

    const m1 = got[1] as Message;
    expect(m1.value).toBeNull();
    expect(m1.size).toBe(0);
    expect(m1.key).toBeUndefined();
    expect(m1.timestamp).toBeUndefined();
    expect(m1.headers).toBeUndefined();

    // consume(cb) flowing: the cb receives each message like 'data'.
    expect(cbGot.map((m) => m.offset)).toEqual([0, 1]);
    consumer.disconnect();
  });

  test("partition.eof → its own event, not 'data'; other errs → event.error", async () => {
    const world = new FakeWorld();
    world.topicNames.set(TOPIC_ID, "m3-topic");
    world.pushMessages([
      raw({ offset: 5 }),
      raw({ offset: 6, err: ERROR_CODES.ERR__PARTITION_EOF, value: null }),
      raw({ offset: 0, err: ERROR_CODES.ERR__TRANSPORT, value: null }),
    ]);
    const consumer = await makeConsumer(world);
    const data: Message[] = [];
    const eofs: { topic: string; partition: number; offset: number }[] = [];
    const errors: LibrdKafkaError[] = [];
    consumer.on("data", (m: Message) => data.push(m));
    consumer.on("partition.eof", (e: (typeof eofs)[0]) => eofs.push(e));
    consumer.on("event.error", (e: LibrdKafkaError) => errors.push(e));
    consumer.subscribe(["m3-topic"]);
    consumer.consume();
    await waitUntil(() => data.length === 1 && eofs.length === 1 && errors.length === 1);
    expect(eofs[0]).toEqual({ topic: "m3-topic", partition: 0, offset: 6 });
    expect(errors[0]?.code).toBe(ERROR_CODES.ERR__TRANSPORT);
    // getLastError() tracks event.error.
    expect(consumer.getLastError()?.code).toBe(ERROR_CODES.ERR__TRANSPORT);
    consumer.disconnect();
  });

  test("flowing: UNKNOWN_TOPIC_OR_PART / TOPIC_AUTHORIZATION_FAILED → 'warning', not 'event.error'", async () => {
    const world = new FakeWorld();
    world.topicNames.set(TOPIC_ID, "m3-topic");
    world.pushMessages([
      raw({ offset: 0, err: ERROR_CODES.ERR_UNKNOWN_TOPIC_OR_PART, value: null }),
      raw({ offset: 0, err: ERROR_CODES.ERR_TOPIC_AUTHORIZATION_FAILED, value: null }),
      raw({ offset: 0, err: ERROR_CODES.ERR__TRANSPORT, value: null }),
    ]);
    const consumer = await makeConsumer(world);
    const warnings: LibrdKafkaError[] = [];
    const errors: LibrdKafkaError[] = [];
    consumer.on("warning", (e: LibrdKafkaError) => warnings.push(e));
    consumer.on("event.error", (e: LibrdKafkaError) => errors.push(e));
    consumer.subscribe(["m3-topic"]);
    consumer.consume();
    await waitUntil(() => warnings.length === 2 && errors.length === 1);
    expect(warnings.map((w) => w.code)).toEqual([
      ERROR_CODES.ERR_UNKNOWN_TOPIC_OR_PART,
      ERROR_CODES.ERR_TOPIC_AUTHORIZATION_FAILED,
    ]);
    expect(errors[0]?.code).toBe(ERROR_CODES.ERR__TRANSPORT);
    consumer.disconnect();
  });

  test("non-flowing: the same codes stay 'event.error' (upstream only warns in flowing mode)", async () => {
    const world = new FakeWorld();
    world.topicNames.set(TOPIC_ID, "m3-topic");
    world.pushMessages([raw({ offset: 0, err: ERROR_CODES.ERR_UNKNOWN_TOPIC_OR_PART, value: null })]);
    const consumer = await makeConsumer(world);
    const warnings: LibrdKafkaError[] = [];
    const errors: LibrdKafkaError[] = [];
    consumer.on("warning", (e: LibrdKafkaError) => warnings.push(e));
    consumer.on("event.error", (e: LibrdKafkaError) => errors.push(e));
    consumer.subscribe(["m3-topic"]);
    await waitUntil(() => errors.length === 1);
    expect(warnings).toHaveLength(0);
    consumer.disconnect();
  });
});

/* ========================================================================== */
/* Non-flowing                                                                 */
/* ========================================================================== */

describe("KafkaConsumer non-flowing", () => {
  test("consume(n, cb): returns ≤ n, FIFO, an empty array after the timeout when drained", async () => {
    const world = new FakeWorld();
    world.topicNames.set(TOPIC_ID, "m3-topic");
    world.pushMessages([raw({ offset: 0 }), raw({ offset: 1 }), raw({ offset: 2 })]);
    const consumer = await makeConsumer(world);
    consumer.subscribe(["m3-topic"]);

    const first = await new Promise<Message[]>((resolve) => consumer.consume(2, (_e, ms) => resolve(ms)));
    expect(first.map((m) => m.offset)).toEqual([0, 1]);

    const second = await new Promise<Message[]>((resolve) => consumer.consume(5, (_e, ms) => resolve(ms)));
    expect(second.map((m) => m.offset)).toEqual([2]);

    consumer.setDefaultConsumeTimeout(30);
    const start = Date.now();
    const empty = await new Promise<Message[]>((resolve) => consumer.consume(1, (_e, ms) => resolve(ms)));
    expect(empty).toEqual([]);
    expect(Date.now() - start).toBeGreaterThanOrEqual(25);
    consumer.disconnect();
  });
});

/* ========================================================================== */
/* Rebalance                                                                   */
/* ========================================================================== */

const ASSIGN_PARTS: TopicPartitionInput[] = [
  { topic: "m3-topic", partition: 0 },
  { topic: "m3-topic", partition: 1 },
];

function rebalanceFrame(code: number, protocol: number, assignmentLost = false) {
  return {
    type: BRK_EVENT_REBALANCE,
    payload: rebalancePayload(code, protocol, ASSIGN_PARTS, assignmentLost),
  };
}

describe("KafkaConsumer rebalance", () => {
  test("eager default: assign → BRK_ASSIGN, revoke → BRK_UNASSIGN(null); emits 'rebalance'", async () => {
    const world = new FakeWorld();
    const consumer = await makeConsumer(world);
    const rebalances: { err: LibrdKafkaError; parts: unknown[] }[] = [];
    consumer.on("rebalance", (err: LibrdKafkaError, parts: unknown[]) =>
      rebalances.push({ err, parts }),
    );
    consumer.subscribe(["m3-topic"]);

    world.pushEventFrames([
      rebalanceFrame(ERROR_CODES.ERR__ASSIGN_PARTITIONS, BRK_REBALANCE_PROTOCOL_EAGER),
    ]);
    await waitUntil(() => world.assignCalls.length === 1);
    expect(world.assignCalls[0]?.mode).toBe(BRK_ASSIGN);
    expect(decodeCapturedTpl(world.assignCalls[0]?.tpl as Uint8Array).map((p) => p.partition)).toEqual([0, 1]);
    expect(consumer.rebalanceProtocol()).toBe("EAGER");

    world.pushEventFrames([
      rebalanceFrame(ERROR_CODES.ERR__REVOKE_PARTITIONS, BRK_REBALANCE_PROTOCOL_EAGER),
    ]);
    await waitUntil(() => world.assignCalls.length === 2);
    expect(world.assignCalls[1]?.mode).toBe(BRK_UNASSIGN);
    expect(world.assignCalls[1]?.tpl).toBeNull();

    await waitUntil(() => rebalances.length === 2);
    expect(rebalances[0]?.err.code).toBe(ERROR_CODES.ERR__ASSIGN_PARTITIONS);
    expect(rebalances[0]?.parts).toEqual([
      { topic: "m3-topic", partition: 0 },
      { topic: "m3-topic", partition: 1 },
    ]);
    consumer.disconnect();
  });

  test("assignmentLost(): set from the REVOKE flag, cleared on a new ASSIGN", async () => {
    const world = new FakeWorld();
    const consumer = await makeConsumer(world);
    consumer.subscribe(["m3-topic"]);
    expect(consumer.assignmentLost()).toBe(false);

    world.pushEventFrames([
      rebalanceFrame(ERROR_CODES.ERR__REVOKE_PARTITIONS, BRK_REBALANCE_PROTOCOL_EAGER, true),
    ]);
    await waitUntil(() => world.assignCalls.length === 1);
    expect(consumer.assignmentLost()).toBe(true);

    world.pushEventFrames([
      rebalanceFrame(ERROR_CODES.ERR__ASSIGN_PARTITIONS, BRK_REBALANCE_PROTOCOL_EAGER),
    ]);
    await waitUntil(() => world.assignCalls.length === 2);
    expect(consumer.assignmentLost()).toBe(false);
    consumer.disconnect();
  });

  test("cooperative: an empty incremental assign → an EMPTY tpl (count=0), NOT NULL", async () => {
    // M3 regression: cooperative-sticky hands the second joiner an empty
    // incremental assign in the first rebalance round; sending NULL made C
    // return BRK_ERR_DECODE and the group deadlocked.
    const world = new FakeWorld();
    const consumer = await makeConsumer(world);
    consumer.subscribe(["m3-topic"]);
    world.pushEventFrames([
      {
        type: BRK_EVENT_REBALANCE,
        payload: rebalancePayload(
          ERROR_CODES.ERR__ASSIGN_PARTITIONS,
          BRK_REBALANCE_PROTOCOL_COOPERATIVE,
          [],
        ),
      },
    ]);
    await waitUntil(() => world.assignCalls.length === 1);
    expect(world.assignCalls[0]?.mode).toBe(BRK_ASSIGN_INCREMENTAL);
    const tpl = world.assignCalls[0]?.tpl as Uint8Array;
    expect(tpl).not.toBeNull();
    expect(decodeCapturedTpl(tpl)).toEqual([]); // count=0
    consumer.disconnect();
  });

  test("cooperative: incremental assign/unassign", async () => {
    const world = new FakeWorld();
    const consumer = await makeConsumer(world);
    consumer.subscribe(["m3-topic"]);
    world.pushEventFrames([
      rebalanceFrame(ERROR_CODES.ERR__ASSIGN_PARTITIONS, BRK_REBALANCE_PROTOCOL_COOPERATIVE),
    ]);
    await waitUntil(() => world.assignCalls.length === 1);
    expect(world.assignCalls[0]?.mode).toBe(BRK_ASSIGN_INCREMENTAL);
    expect(consumer.rebalanceProtocol()).toBe("COOPERATIVE");

    world.pushEventFrames([
      rebalanceFrame(ERROR_CODES.ERR__REVOKE_PARTITIONS, BRK_REBALANCE_PROTOCOL_COOPERATIVE),
    ]);
    await waitUntil(() => world.assignCalls.length === 2);
    expect(world.assignCalls[1]?.mode).toBe(BRK_UNASSIGN_INCREMENTAL);
    expect(world.assignCalls[1]?.tpl).not.toBeNull();
    consumer.disconnect();
  });

  test("the user's rebalance_cb: this = consumer, responsible for assigning", async () => {
    const world = new FakeWorld();
    const seen: { code: number; self: boolean }[] = [];
    const consumer = await makeConsumer(world, {
      rebalance_cb(this: KafkaConsumer, err: LibrdKafkaError, parts: never[]) {
        seen.push({ code: err.code, self: this instanceof KafkaConsumer });
        if (err.code === ERROR_CODES.ERR__ASSIGN_PARTITIONS) this.assign(parts);
      },
    });
    consumer.subscribe(["m3-topic"]);
    world.pushEventFrames([
      rebalanceFrame(ERROR_CODES.ERR__ASSIGN_PARTITIONS, BRK_REBALANCE_PROTOCOL_EAGER),
    ]);
    await waitUntil(() => seen.length === 1);
    expect(seen[0]).toEqual({ code: ERROR_CODES.ERR__ASSIGN_PARTITIONS, self: true });
    // Exactly ONE assign call — from the user, with no auto-assign on top.
    await waitUntil(() => world.assignCalls.length === 1);
    await Bun.sleep(30);
    expect(world.assignCalls).toHaveLength(1);
    expect(world.assignCalls[0]?.mode).toBe(BRK_ASSIGN);
    consumer.disconnect();
  });

  test("offset.commit frame → offset_commit_cb + emit 'offset.commit'", async () => {
    const world = new FakeWorld();
    const cbCalls: { err: LibrdKafkaError | null; parts: { offset: number }[] }[] = [];
    const consumer = await makeConsumer(world, {
      offset_commit_cb: (err: LibrdKafkaError | null, parts: { offset: number }[]) =>
        cbCalls.push({ err, parts }),
    });
    const emitted: { offset: number }[][] = [];
    consumer.on("offset.commit", (_e: unknown, parts: { offset: number }[]) => emitted.push(parts));
    consumer.subscribe(["m3-topic"]);
    world.pushEventFrames([
      {
        type: BRK_EVENT_OFFSET_COMMIT,
        payload: offsetCommitPayload(0, [{ topic: "m3-topic", partition: 0, offset: 42 }]),
      },
    ]);
    await waitUntil(() => cbCalls.length === 1 && emitted.length === 1);
    expect(cbCalls[0]?.err).toBeNull();
    expect(cbCalls[0]?.parts[0]?.offset).toBe(42);
    consumer.disconnect();
  });
});

/* ========================================================================== */
/* Offset & flow control API                                                   */
/* ========================================================================== */

describe("KafkaConsumer offset/flow API", () => {
  test("commit/commitSync/commitMessage(+1)/commit() semantics", async () => {
    const world = new FakeWorld();
    const consumer = await makeConsumer(world);
    consumer.subscribe(["m3-topic"]);

    consumer.commit({ topic: "t", partition: 1, offset: 10 });
    consumer.commitSync({ topic: "t", partition: 1, offset: 11 });
    consumer.commitMessage({ topic: "t", partition: 1, offset: 20 });
    consumer.commitMessageSync({ topic: "t", partition: 1, offset: 30 });
    consumer.commit();

    expect(world.commitCalls).toHaveLength(5);
    expect(world.commitCalls[0]?.async).toBe(1);
    expect(decodeCapturedTpl(world.commitCalls[0]?.tpl as Uint8Array)[0]?.offset).toBe(10);
    expect(world.commitCalls[1]?.async).toBe(0);
    expect(decodeCapturedTpl(world.commitCalls[1]?.tpl as Uint8Array)[0]?.offset).toBe(11);
    // commitMessage: offset + 1.
    expect(decodeCapturedTpl(world.commitCalls[2]?.tpl as Uint8Array)[0]?.offset).toBe(21);
    expect(world.commitCalls[3]?.async).toBe(0);
    expect(decodeCapturedTpl(world.commitCalls[3]?.tpl as Uint8Array)[0]?.offset).toBe(31);
    // commit() with no args → a NULL tpl (commit every position).
    expect(world.commitCalls[4]?.tpl).toBeNull();
    consumer.disconnect();
  });

  test("seek: null timeout → 0; the pause/resume flag is right; subscribed/unsubscribed", async () => {
    const world = new FakeWorld();
    const consumer = await makeConsumer(world);
    const events: string[] = [];
    consumer.on("subscribed", () => events.push("subscribed"));
    consumer.on("unsubscribed", () => events.push("unsubscribed"));

    consumer.subscribe(["m3-a", "m3-b"]);
    expect(world.subscribedLists[0]).toEqual(["m3-a", "m3-b"]);
    expect(consumer.subscription()).toEqual(["m3-a", "m3-b"]);

    await new Promise<void>((resolve, reject) =>
      consumer.seek({ topic: "m3-a", partition: 2, offset: 99 }, null, (e) =>
        e ? reject(e) : resolve(),
      ),
    );
    expect(world.seekCalls[0]).toEqual({ topic: "m3-a", partition: 2, offset: 99, timeout: 0 });

    consumer.pause([{ topic: "m3-a", partition: 0 }]);
    consumer.resume([{ topic: "m3-a", partition: 0 }]);
    expect(world.pauseResumeCalls.map((c) => c.resume)).toEqual([0, 1]);

    consumer.unsubscribe();
    expect(world.unsubscribeCount).toBe(1);
    expect(events).toEqual(["subscribed", "unsubscribed"]);
    consumer.disconnect();
  });
});
