/**
 * Callback API — `Producer` + `HighLevelProducer`: staging batch (3 trigger),
 * backpressure, DR correlation, per-record error, flush, transactions retriable
 * loop, serializers. All on a FAKE native function table — no .so/broker needed.
 */

import { describe, expect, test } from "bun:test";
import { ptr } from "bun:ffi";
import type { BrkNative } from "../../packages/bun-rdkafka/src/ffi/loader.ts";
import {
  BRK_ERR_KAFKA_OFFSET,
  BRK_EVENT_DR,
} from "../../packages/bun-rdkafka/src/ffi/types.ts";
import {
  Producer,
  STAGING_MAX_RECORDS,
  normalizeHeaders,
  type DeliveryReportPayload,
} from "../../packages/bun-rdkafka/src/callback/producer.ts";
import { HighLevelProducer } from "../../packages/bun-rdkafka/src/callback/high-level-producer.ts";
import { ERROR_CODES, LibrdKafkaError } from "../../packages/bun-rdkafka/src/core/errors.ts";
import type { ClientConfig } from "../../packages/bun-rdkafka/src/core/config.ts";
import {
  decodeProduceBatch,
  drPayload,
  encodeEventFrames,
  type DecodedProduceRecord,
  type RawDeliveryReport,
} from "./helpers/c-side-encoders.ts";

type AnyFn = (...args: any[]) => any;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Waits for the staging-flush microtask to finish. */
const flushTick = () => sleep(1);

const META_JSON = JSON.stringify({
  orig_broker_id: 1,
  orig_broker_name: "localhost:9092/1",
  brokers: [{ id: 1, host: "localhost", port: 9092 }],
  topics: [],
});

const metaKeepAlive: Uint8Array[] = [];

/* ========================================================================== */
/* A fake native for the producer                                              */
/* ========================================================================== */

interface ProducerFake {
  native: BrkNative;
  /** Each element = one brk_produce_batch call (decoded). */
  batches: DecodedProduceRecord[][];
  /** err_out filled for the next produce_batch call (by record index). */
  nextErrOut: number[] | null;
  /** The DR frame queue for brk_events_poll. */
  queueDr(reports: RawDeliveryReport[]): void;
  outqLen: number;
  calls: string[];
  overrides: Record<string, AnyFn>;
}

function producerFake(overrides: Record<string, AnyFn> = {}): ProducerFake {
  const eventQueue: Uint8Array[] = [];
  const state: ProducerFake = {
    native: undefined as unknown as BrkNative,
    batches: [],
    nextErrOut: null,
    queueDr(reports) {
      eventQueue.push(encodeEventFrames([{ type: BRK_EVENT_DR, payload: drPayload(reports) }]));
    },
    outqLen: 0,
    calls: [],
    overrides,
  };

  const builtin: Record<string, AnyFn> = {
    brk_conf_new: () => 1,
    brk_client_new: () => 2,
    brk_metadata: (_h: unknown, _t: unknown, _timeout: unknown, out: BigUint64Array) => {
      const buf = new TextEncoder().encode(`${META_JSON}\0`);
      metaKeepAlive.push(buf);
      out[0] = BigInt(ptr(buf));
      return buf.length - 1;
    },
    brk_client_outq_len: () => state.outqLen,
    brk_produce_batch: (
      _h: unknown,
      inBuf: Uint8Array,
      inLen: number,
      errOut: Int16Array,
      maxRecords: number,
    ) => {
      const records = decodeProduceBatch(inBuf.subarray(0, inLen));
      state.batches.push(records);
      expect(records.length).toBeLessThanOrEqual(maxRecords);
      errOut.fill(0);
      if (state.nextErrOut) {
        state.nextErrOut.forEach((code, i) => {
          errOut[i] = code;
        });
        state.nextErrOut = null;
      }
      return records.length;
    },
    brk_events_poll: (_h: unknown, buf: Uint8Array) => {
      const next = eventQueue.shift();
      if (!next) return 0;
      buf.set(next);
      return 1; // one DR frame per call (a frame may bundle many reports)
    },
  };

  state.native = new Proxy(
    {},
    {
      get(_target, prop: string) {
        return (...args: unknown[]) => {
          state.calls.push(prop);
          const fn = state.overrides[prop] ?? builtin[prop];
          return fn ? fn(...args) : 0;
        };
      },
    },
  ) as unknown as BrkNative;
  return state;
}

function makeProducer(
  config: ClientConfig = {},
  overrides: Record<string, AnyFn> = {},
): { producer: Producer; fake: ProducerFake } {
  const fake = producerFake(overrides);
  const producer = new Producer(
    { "bootstrap.servers": "localhost:9092", ...config },
    undefined,
    { native: fake.native, onLeak: () => {} },
  );
  return { producer, fake };
}

function connect(producer: Producer): Promise<void> {
  return new Promise((resolve, reject) => {
    producer.connect({}, (err) => (err ? reject(err) : resolve()));
  });
}

function disconnect(producer: Producer): Promise<void> {
  return new Promise((resolve) => {
    producer.disconnect(() => resolve());
  });
}

/* ========================================================================== */
/* Staging                                                                     */
/* ========================================================================== */

describe("Producer staging batch", () => {
  test("many produce()s in one tick → ONE brk_produce_batch", async () => {
    const { producer, fake } = makeProducer();
    await connect(producer);

    producer.produce("t", 0, Buffer.from("v0"), "k0", 1111, "op0");
    producer.produce("t", null, "v1", null, undefined, undefined, [{ h1: "x" }]);
    producer.produce("u", 2, null, Buffer.from("k2"));
    expect(fake.batches).toHaveLength(0); // not yet flushed within the tick

    await flushTick();
    expect(fake.batches).toHaveLength(1);
    const batch = fake.batches[0]!;
    expect(batch).toHaveLength(3);

    expect(batch[0]).toMatchObject({ topic: "t", partition: 0, timestampMs: 1111n });
    expect(Buffer.from(batch[0]!.value!).toString()).toBe("v0");
    expect(Buffer.from(batch[0]!.key!).toString()).toBe("k0");

    // null partition → -1 (UA), default timestamp 0, normalized headers.
    expect(batch[1]).toMatchObject({ partition: -1, timestampMs: 0n });
    expect(batch[1]!.headers).toEqual([{ key: "h1", value: new TextEncoder().encode("x") }]);

    // value null (tombstone), key Buffer.
    expect(batch[2]!.value).toBeNull();
    expect(Buffer.from(batch[2]!.key!).toString()).toBe("k2");

    // opaque_ids increase and differ.
    const ids = batch.map((r) => r.opaqueId);
    expect(new Set(ids).size).toBe(3);

    await disconnect(producer);
  });

  test("full staging (STAGING_MAX_RECORDS) → forced flush within the tick", async () => {
    const { producer, fake } = makeProducer();
    await connect(producer);
    for (let i = 0; i < STAGING_MAX_RECORDS; i++) {
      producer.produce("t", 0, "v", null);
    }
    // The flush happened synchronously, no microtask wait needed.
    expect(fake.batches).toHaveLength(1);
    expect(fake.batches[0]).toHaveLength(STAGING_MAX_RECORDS);
    await disconnect(producer);
  });

  test("poll() forces an immediate flush", async () => {
    const { producer, fake } = makeProducer();
    await connect(producer);
    producer.produce("t", 0, "v", null);
    expect(fake.batches).toHaveLength(0);
    producer.poll();
    expect(fake.batches).toHaveLength(1);
    await disconnect(producer);
  });

  test("produce before connect → throws ERR__STATE 'Producer not connected'", () => {
    const { producer } = makeProducer();
    expect(() => producer.produce("t", 0, "v")).toThrow(LibrdKafkaError);
    try {
      producer.produce("t", 0, "v");
    } catch (error) {
      expect((error as LibrdKafkaError).code).toBe(ERROR_CODES.ERR__STATE);
      expect((error as LibrdKafkaError).message).toBe("Producer not connected");
    }
  });
});

/* ========================================================================== */
/* Backpressure + per-record errors                                            */
/* ========================================================================== */

describe("backpressure & per-record errors", () => {
  test("exceeding js.producer.max.pending → a synchronous ERR__QUEUE_FULL throw", async () => {
    const { producer } = makeProducer({ "js.producer.max.pending": 2 });
    await connect(producer);
    producer.produce("t", 0, "a");
    producer.produce("t", 0, "b");
    expect(() => producer.produce("t", 0, "c")).toThrow(/QUEUE_FULL|awaiting delivery reports/);
    await disconnect(producer);
  });

  test("a per-record shim err → a throw on the NEXT produce() + a DR err", async () => {
    const { producer, fake } = makeProducer({ dr_cb: true });
    await connect(producer);
    const reports: [LibrdKafkaError | null, DeliveryReportPayload][] = [];
    producer.on("delivery-report", (err: LibrdKafkaError | null, report: DeliveryReportPayload) =>
      reports.push([err, report]),
    );

    fake.nextErrOut = [0, ERROR_CODES.ERR__QUEUE_FULL];
    producer.produce("t", 0, "ok");
    producer.produce("t", 0, "full", "key-full");
    await flushTick();

    // The broken record settles immediately with a DR err.
    expect(reports).toHaveLength(1);
    expect(reports[0]![0]?.code).toBe(ERROR_CODES.ERR__QUEUE_FULL);
    expect(reports[0]![1].topic).toBe("t");

    // The error surfaces as a throw on the next produce (then clears).
    expect(() => producer.produce("t", 0, "next")).toThrow(LibrdKafkaError);
    producer.produce("t", 0, "after"); // no longer throws
    await disconnect(producer);
  });
});

/* ========================================================================== */
/* Delivery report                                                             */
/* ========================================================================== */

describe("delivery-report", () => {
  test("DR correlation via opaque_id; the report shape is right; opaque passes through", async () => {
    const { producer, fake } = makeProducer({ dr_cb: true });
    await connect(producer);
    const reports: [LibrdKafkaError | null, DeliveryReportPayload][] = [];
    producer.on("delivery-report", (err: LibrdKafkaError | null, r: DeliveryReportPayload) =>
      reports.push([err, r]),
    );

    const myOpaque = { tag: "hello" };
    producer.produce("t", 0, Buffer.from("value-x"), "key-x", 999, myOpaque);
    producer.produce("t", 1, "value-y", null);
    await flushTick();
    const batch = fake.batches[0]!;

    fake.queueDr([
      { opaqueId: batch[0]!.opaqueId, err: 0, partition: 0, offset: 41, timestampMs: 999 },
      { opaqueId: batch[1]!.opaqueId, err: 0, partition: 1, offset: 7, timestampMs: 1000 },
    ]);
    await sleep(20); // the scheduler pumps events_poll itself

    expect(reports).toHaveLength(2);
    const [err0, r0] = reports[0]!;
    expect(err0).toBeNull();
    expect(r0).toMatchObject({
      topic: "t",
      partition: 0,
      offset: 41,
      key: "key-x",
      opaque: myOpaque,
      timestamp: 999,
      size: 7, // "value-x"
    });
    expect("value" in r0).toBe(false); // no dr_msg_cb → no payload attached
    expect(producer.pendingDeliveryReports).toBe(0);
    await disconnect(producer);
  });

  test("dr_msg_cb → the report carries the value; a dr_cb function is invoked as a listener", async () => {
    const seen: DeliveryReportPayload[] = [];
    const { producer, fake } = makeProducer({
      dr_msg_cb: (_err: LibrdKafkaError | null, report: DeliveryReportPayload) => {
        seen.push(report);
      },
    });
    await connect(producer);
    producer.produce("t", 0, "payload-here", null);
    await flushTick();
    fake.queueDr([
      { opaqueId: fake.batches[0]![0]!.opaqueId, err: 0, partition: 0, offset: 1, timestampMs: 5 },
    ]);
    await sleep(20);
    expect(seen).toHaveLength(1);
    expect(seen[0]!.value).toBe("payload-here");
    await disconnect(producer);
  });

  test("no dr_cb/dr_msg_cb → NO delivery-report emit (like upstream)", async () => {
    const { producer, fake } = makeProducer();
    await connect(producer);
    let emitted = 0;
    producer.on("delivery-report", () => emitted++);
    producer.produce("t", 0, "v");
    await flushTick();
    fake.queueDr([
      { opaqueId: fake.batches[0]![0]!.opaqueId, err: 0, partition: 0, offset: 1, timestampMs: 5 },
    ]);
    await sleep(20);
    expect(emitted).toBe(0);
    expect(producer.pendingDeliveryReports).toBe(0); // the ledger still settles
    await disconnect(producer);
  });

  test("disconnect with pending DRs → rejects with ERR__DESTROY", async () => {
    const { producer } = makeProducer({ dr_cb: true });
    await connect(producer);
    const errors: (LibrdKafkaError | null)[] = [];
    producer.on("delivery-report", (err: LibrdKafkaError | null) => errors.push(err));
    producer.produce("t", 0, "v");
    await disconnect(producer);
    expect(errors).toHaveLength(1);
    expect(errors[0]?.code).toBe(ERROR_CODES.ERR__DESTROY);
  });
});

/* ========================================================================== */
/* flush                                                                       */
/* ========================================================================== */

describe("flush", () => {
  test("empty outq + no pending DRs → cb(null)", async () => {
    const { producer, fake } = makeProducer();
    await connect(producer);
    producer.produce("t", 0, "v");
    // DRs arrive right after the staging flush.
    queueMicrotask(() => {
      const batch = fake.batches[0];
      if (batch) {
        fake.queueDr([
          { opaqueId: batch[0]!.opaqueId, err: 0, partition: 0, offset: 1, timestampMs: 1 },
        ]);
      }
    });
    const err = await new Promise<LibrdKafkaError | null>((resolve) => {
      producer.flush(2000, resolve);
    });
    expect(err).toBeNull();
    await disconnect(producer);
  });

  test("never finishing → cb(ERR__TIMED_OUT) after the timeout", async () => {
    const { producer, fake } = makeProducer();
    await connect(producer);
    fake.outqLen = 5; // pretend the outq never drains
    const t0 = Date.now();
    const err = await new Promise<LibrdKafkaError | null>((resolve) => {
      producer.flush(60, resolve);
    });
    expect(err?.code).toBe(ERROR_CODES.ERR__TIMED_OUT);
    expect(Date.now() - t0).toBeGreaterThanOrEqual(55);
    fake.outqLen = 0;
    await disconnect(producer);
  });
});

/* ========================================================================== */
/* Transactions                                                                */
/* ========================================================================== */

describe("transactions", () => {
  const TIMED_OUT_RET = BRK_ERR_KAFKA_OFFSET + ERROR_CODES.ERR__TIMED_OUT;

  function writeErrstr(buf: Uint8Array, text: string): void {
    buf.fill(0);
    buf.set(new TextEncoder().encode(`${text}\0`));
  }

  test("initTransactions retriable: TIMED_OUT [retriable] ×2 rồi OK", async () => {
    let attempts = 0;
    const { producer, fake } = makeProducer(
      { "transactional.id": "txn-1" },
      {
        brk_init_transactions: (_h: unknown, _t: unknown, errBuf: Uint8Array) => {
          attempts++;
          if (attempts <= 2) {
            writeErrstr(errBuf, "Transaction op timed out [retriable]");
            return TIMED_OUT_RET;
          }
          return 0;
        },
      },
    );
    await connect(producer);
    const err = await new Promise<LibrdKafkaError | null>((resolve) => {
      producer.initTransactions(2000, resolve);
    });
    expect(err).toBeNull();
    expect(attempts).toBe(3);
    expect(fake.calls.filter((c) => c === "brk_init_transactions")).toHaveLength(3);
    await disconnect(producer);
  });

  test("a non-retriable error → cb(err) immediately, keeping the txn-requires-abort flag", async () => {
    const FENCED_RET = BRK_ERR_KAFKA_OFFSET + ERROR_CODES.ERR_INVALID_PRODUCER_EPOCH;
    const { producer } = makeProducer(
      { "transactional.id": "txn-2" },
      {
        brk_commit_transaction: (_h: unknown, _t: unknown, errBuf: Uint8Array) => {
          writeErrstr(errBuf, "Producer fenced [txn-requires-abort]");
          return FENCED_RET;
        },
      },
    );
    await connect(producer);
    const err = await new Promise<LibrdKafkaError | null>((resolve) => {
      producer.commitTransaction(500, resolve);
    });
    expect(err?.code).toBe(ERROR_CODES.ERR_INVALID_PRODUCER_EPOCH);
    expect(err?.isTxnRequiresAbort).toBe(true);
    await disconnect(producer);
  });

  test("an exhausted timeout → returns the last retriable error instead of looping forever", async () => {
    const { producer } = makeProducer(
      { "transactional.id": "txn-3" },
      {
        brk_init_transactions: (_h: unknown, _t: unknown, errBuf: Uint8Array) => {
          writeErrstr(errBuf, "still timed out [retriable]");
          return TIMED_OUT_RET;
        },
      },
    );
    await connect(producer);
    const err = await new Promise<LibrdKafkaError | null>((resolve) => {
      producer.initTransactions(120, resolve);
    });
    expect(err?.code).toBe(ERROR_CODES.ERR__TIMED_OUT);
    await disconnect(producer);
  });

  test("commitTransaction flushes staging before committing", async () => {
    const order: string[] = [];
    const { producer, fake } = makeProducer(
      { "transactional.id": "txn-4" },
      {
        brk_commit_transaction: () => {
          order.push("commit");
          return 0;
        },
      },
    );
    await connect(producer);
    producer.beginTransaction();
    await sleep(1);
    producer.produce("t", 0, "in-txn");
    const origBatches = fake.batches.length;
    await new Promise<void>((resolve, reject) => {
      producer.commitTransaction(1000, (err) => (err ? reject(err) : resolve()));
    });
    expect(fake.batches.length).toBe(origBatches + 1); // staging went down before the commit
    expect(order).toEqual(["commit"]);
    await disconnect(producer);
  });
});

/* ========================================================================== */
/* HighLevelProducer                                                           */
/* ========================================================================== */

describe("HighLevelProducer", () => {
  function makeHlp(overrides: Record<string, AnyFn> = {}) {
    const fake = producerFake(overrides);
    const producer = new HighLevelProducer(
      { "bootstrap.servers": "localhost:9092" },
      undefined,
      { native: fake.native, onLeak: () => {} },
    );
    return { producer, fake };
  }

  test("produce(topic, null, msg, key, ts, cb) → cb(null, offset) sau DR", async () => {
    const { producer, fake } = makeHlp();
    await connect(producer);
    const offset = await new Promise<number | undefined>((resolve, reject) => {
      producer.produce(
        "t",
        null,
        Buffer.from("hello"),
        "k",
        Date.now(),
        (err: LibrdKafkaError | null, off?: number) => (err ? reject(err) : resolve(off)),
      );
      // DRs after the staging flush.
      queueMicrotask(() => {
        queueMicrotask(() => {
          const batch = fake.batches[0];
          if (batch) {
            fake.queueDr([
              { opaqueId: batch[0]!.opaqueId, err: 0, partition: 0, offset: 123, timestampMs: 1 },
            ]);
          }
        });
      });
    });
    expect(offset).toBe(123);
    await disconnect(producer);
  });

  test("serializers: a sync value, a Promise-returning key (the upstream example)", async () => {
    const { producer, fake } = makeHlp();
    producer.setValueSerializer((v) => Buffer.from((v as { message: string }).message));
    producer.setKeySerializer(
      () => new Promise((resolve) => setTimeout(() => resolve(null), 5)),
    );
    await connect(producer);
    await new Promise<void>((resolve, reject) => {
      producer.produce(
        "t",
        null,
        { message: "alliance4ever" },
        "discarded",
        Date.now(),
        (err: LibrdKafkaError | null) => (err ? reject(err) : resolve()),
      );
      // wait for the serializer promise before the batch → DR exists.
      const feed = () => {
        const batch = fake.batches[0];
        if (!batch) {
          setTimeout(feed, 2);
          return;
        }
        fake.queueDr([
          { opaqueId: batch[0]!.opaqueId, err: 0, partition: 0, offset: 9, timestampMs: 1 },
        ]);
      };
      feed();
    });
    const record = fake.batches[0]![0]!;
    expect(Buffer.from(record.value!).toString()).toBe("alliance4ever");
    expect(record.key).toBeNull(); // the key serializer discards the key
    await disconnect(producer);
  });

  test("serializer callback-style (fn 2 tham số)", async () => {
    const { producer, fake } = makeHlp();
    producer.setValueSerializer((v, cb) => {
      setTimeout(() => cb!(null, `ser:${String(v)}`), 2);
    });
    await connect(producer);
    await new Promise<void>((resolve, reject) => {
      producer.produce("t", null, "raw", null, Date.now(), (err: LibrdKafkaError | null) =>
        err ? reject(err) : resolve(),
      );
      const feed = () => {
        const batch = fake.batches[0];
        if (!batch) {
          setTimeout(feed, 2);
          return;
        }
        fake.queueDr([
          { opaqueId: batch[0]!.opaqueId, err: 0, partition: 0, offset: 1, timestampMs: 1 },
        ]);
      };
      feed();
    });
    expect(Buffer.from(fake.batches[0]![0]!.value!).toString()).toBe("ser:raw");
    await disconnect(producer);
  });

  test("a throwing serializer → cb(ERR__VALUE_SERIALIZATION), no throw", async () => {
    const { producer } = makeHlp();
    producer.setValueSerializer(() => {
      throw new Error("bad value");
    });
    await connect(producer);
    const err = await new Promise<LibrdKafkaError | null>((resolve) => {
      producer.produce("t", null, "x", null, Date.now(), (e: LibrdKafkaError | null) =>
        resolve(e),
      );
    });
    expect(err?.code).toBe(ERROR_CODES.ERR__VALUE_SERIALIZATION);
    expect(err?.message).toContain("bad value");
    await disconnect(producer);
  });

  test("produce with headers as the 7th argument", async () => {
    const { producer, fake } = makeHlp();
    await connect(producer);
    await new Promise<void>((resolve, reject) => {
      producer.produce(
        "t",
        null,
        "v",
        null,
        Date.now(),
        [{ trace: "abc" }],
        (err: LibrdKafkaError | null) => (err ? reject(err) : resolve()),
      );
      const feed = () => {
        const batch = fake.batches[0];
        if (!batch) {
          setTimeout(feed, 2);
          return;
        }
        fake.queueDr([
          { opaqueId: batch[0]!.opaqueId, err: 0, partition: 0, offset: 1, timestampMs: 1 },
        ]);
      };
      feed();
    });
    expect(fake.batches[0]![0]!.headers).toEqual([
      { key: "trace", value: new TextEncoder().encode("abc") },
    ]);
    await disconnect(producer);
  });
});

/* ========================================================================== */
/* normalizeHeaders                                                            */
/* ========================================================================== */

describe("normalizeHeaders", () => {
  test("the upstream {k: v} form and the explicit {key, value} form", () => {
    expect(
      normalizeHeaders([{ a: "1" }, { b: "2", c: "3" }, { key: "d", value: "4" }]),
    ).toEqual([
      { key: "a", value: "1" },
      { key: "b", value: "2" },
      { key: "c", value: "3" },
      { key: "d", value: "4" },
    ]);
    expect(normalizeHeaders(undefined)).toBeUndefined();
    expect(normalizeHeaders([])).toBeUndefined();
  });
});
