/**
 * M2 — `Producer`/`HighLevelProducer` against a real Kafka broker:
 * full DRs + content roundtrips, every compression codec, idempotence,
 * transactions (commit visible / abort invisible under read_committed), flush,
 * HLP serializers.
 *
 * The docker broker is SHARED with other agents/tests running in parallel:
 * NO stopKafka() in afterAll; topic prefix "m2-" (see docker-kafka.ts).
 */

import { beforeAll, describe, expect, test } from "bun:test";
import { createTopic, integrationAvailable, startKafka } from "./docker-kafka.ts";
import { SliceConsumer, waitFor } from "./slice-harness.ts";
import {
  HighLevelProducer,
  Producer,
  type DeliveryReportPayload,
} from "../../packages/bun-rdkafka/src/index.ts";
import type { LibrdKafkaError } from "../../packages/bun-rdkafka/src/index.ts";
import type { ClientConfig } from "../../packages/bun-rdkafka/src/core/config.ts";

const AVAILABLE = await integrationAvailable();
const RUN_ID = Date.now().toString(36);
const DECODER = new TextDecoder();

let brokers = "localhost:9092";

if (AVAILABLE) {
  beforeAll(async () => {
    ({ brokers } = await startKafka());
  }, 180_000);
}

/* ------------------------------------------------------------- promisify */

function connectP(p: Producer): Promise<void> {
  return new Promise((resolve, reject) => p.connect({}, (err) => (err ? reject(err) : resolve())));
}
function disconnectP(p: Producer): Promise<void> {
  return new Promise((resolve) => p.disconnect(() => resolve()));
}
function flushP(p: Producer, timeoutMs: number): Promise<void> {
  return new Promise((resolve, reject) =>
    p.flush(timeoutMs, (err) => (err ? reject(err) : resolve())),
  );
}
function txnP(fn: (cb: (err: LibrdKafkaError | null) => void) => void): Promise<void> {
  return new Promise((resolve, reject) => fn((err) => (err ? reject(err) : resolve())));
}

function makeProducer(extra: ClientConfig = {}): Producer {
  return new Producer({
    "bootstrap.servers": brokers,
    "socket.timeout.ms": 10_000,
    ...extra,
  });
}

/** Reads `count` messages of a topic from the start via slice-harness (independent of M3). */
async function readAll(topic: string, count: number, extra: ClientConfig = {}) {
  const consumer = new SliceConsumer({
    config: {
      "bootstrap.servers": brokers,
      "group.id": `m2-read-${topic}-${Math.random().toString(36).slice(2)}`,
      "auto.offset.reset": "earliest",
      "enable.auto.commit": false,
      "fetch.queue.backoff.ms": 10,
      ...extra,
    },
  });
  consumer.connect();
  try {
    consumer.subscribe([topic]);
    await consumer.waitForMessages(count, 60_000);
    // A short quiet window to ensure no extra messages (e.g. leaked aborts).
    await new Promise((r) => setTimeout(r, 300));
    consumer.throwPollErrors();
    return [...consumer.messages];
  } finally {
    consumer.disconnect();
  }
}

/* ========================================================================== */

describe.skipIf(!AVAILABLE)("M2 Producer (real broker)", () => {
  test("produce 100 → all DRs, increasing offsets, key/value/headers/timestamp roundtrip", async () => {
    const topic = `m2-basic-${RUN_ID}`;
    await createTopic(topic, 1);
    const producer = makeProducer({ dr_cb: true });
    await connectP(producer);

    const reports: DeliveryReportPayload[] = [];
    const drErrors: LibrdKafkaError[] = [];
    producer.on("delivery-report", (err: LibrdKafkaError | null, r: DeliveryReportPayload) => {
      if (err) drErrors.push(err);
      else reports.push(r);
    });

    const TS = 1_700_000_000_000;
    for (let i = 0; i < 100; i++) {
      producer.produce(topic, 0, Buffer.from(`value-${i}`), `key-${i}`, TS + i, `op-${i}`, [
        { index: String(i) },
        { fixed: "hdr" },
      ]);
    }
    await flushP(producer, 30_000);

    expect(drErrors).toHaveLength(0);
    expect(reports).toHaveLength(100);
    const offsets = reports.map((r) => r.offset).sort((a, b) => a - b);
    expect(offsets[0]).toBe(0);
    expect(offsets[99]).toBe(99);
    expect(new Set(offsets).size).toBe(100);
    expect(reports[0]!.opaque).toMatch(/^op-\d+$/);
    expect(producer.pendingDeliveryReports).toBe(0);
    await disconnectP(producer);

    const messages = await readAll(topic, 100);
    expect(messages).toHaveLength(100);
    for (const message of messages) {
      const index = Number(/value-(\d+)/.exec(DECODER.decode(message.value!))?.[1]);
      expect(DECODER.decode(message.key!)).toBe(`key-${index}`);
      expect(message.timestamp).toBe(TS + index);
      expect(message.headers).toHaveLength(2);
      expect(DECODER.decode(message.headers[0]!.value!)).toBe(String(index));
      expect(message.headers[1]!.key).toBe("fixed");
    }
  }, 120_000);

  for (const codec of ["gzip", "snappy", "lz4", "zstd"] as const) {
    test(`codec ${codec}: 50 message roundtrip`, async () => {
      const topic = `m2-codec-${codec}-${RUN_ID}`;
      await createTopic(topic, 1);
      const producer = makeProducer({ "compression.codec": codec, dr_cb: true });
      await connectP(producer);
      let delivered = 0;
      producer.on("delivery-report", (err: LibrdKafkaError | null) => {
        if (!err) delivered++;
      });
      // A repetitive payload — gives the codec something to compress.
      for (let i = 0; i < 50; i++) {
        producer.produce(topic, 0, `${codec}-${i}-`.repeat(20), `k${i}`);
      }
      await flushP(producer, 30_000);
      expect(delivered).toBe(50);
      await disconnectP(producer);

      const messages = await readAll(topic, 50);
      expect(messages).toHaveLength(50);
      const seen = new Set(
        messages.map((m) => Number(new RegExp(`${codec}-(\\d+)-`).exec(DECODER.decode(m.value!))?.[1])),
      );
      expect(seen.size).toBe(50);
    }, 120_000);
  }

  test("idempotent producer: 200 messages, no errors, order preserved", async () => {
    const topic = `m2-idem-${RUN_ID}`;
    await createTopic(topic, 1);
    const producer = makeProducer({ "enable.idempotence": true, dr_cb: true });
    await connectP(producer);
    const offsets: number[] = [];
    const drErrors: LibrdKafkaError[] = [];
    producer.on("delivery-report", (err: LibrdKafkaError | null, r: DeliveryReportPayload) =>
      err ? drErrors.push(err) : offsets.push(r.offset),
    );
    for (let i = 0; i < 200; i++) producer.produce(topic, 0, `idem-${i}`);
    await flushP(producer, 30_000);
    expect(drErrors).toHaveLength(0);
    expect(offsets).toHaveLength(200);
    await disconnectP(producer);

    const messages = await readAll(topic, 200);
    // Idempotent + 1 partition ⇒ content in produce order.
    messages.forEach((m, i) => {
      expect(DECODER.decode(m.value!)).toBe(`idem-${i}`);
    });
  }, 120_000);

  test("transactions: commit visible, abort invisible (read_committed)", async () => {
    const topic = `m2-txn-${RUN_ID}`;
    await createTopic(topic, 1);
    const producer = makeProducer({
      "transactional.id": `m2-txn-id-${RUN_ID}`,
      dr_cb: true,
    });
    await connectP(producer);

    await txnP((cb) => producer.initTransactions(30_000, cb));

    // Transaction 1: commit.
    await txnP((cb) => producer.beginTransaction(cb));
    for (let i = 0; i < 10; i++) producer.produce(topic, 0, `c-${i}`);
    await txnP((cb) => producer.commitTransaction(30_000, cb));

    // Transaction 2: abort.
    await txnP((cb) => producer.beginTransaction(cb));
    for (let i = 0; i < 10; i++) producer.produce(topic, 0, `a-${i}`);
    await txnP((cb) => producer.abortTransaction(30_000, cb));

    await disconnectP(producer);

    const committed = await readAll(topic, 10, { "isolation.level": "read_committed" });
    const values = committed.map((m) => DECODER.decode(m.value!));
    expect(values).toHaveLength(10);
    expect(values.every((v) => v.startsWith("c-"))).toBe(true);
  }, 180_000);

  test("HighLevelProducer: serializers + callback (err, offset) per-message", async () => {
    const topic = `m2-hlp-${RUN_ID}`;
    await createTopic(topic, 1);
    const producer = new HighLevelProducer({ "bootstrap.servers": brokers });
    producer.setValueSerializer((v) => Buffer.from(JSON.stringify(v)));
    producer.setKeySerializer(
      () => new Promise((resolve) => setTimeout(() => resolve(null), 2)),
    );
    await connectP(producer);

    const offsets = await Promise.all(
      Array.from({ length: 5 }, (_, i) =>
        new Promise<number | undefined>((resolve, reject) => {
          producer.produce(
            topic,
            null,
            { index: i, source: "hlp" },
            "to-be-discarded",
            Date.now(),
            (err: LibrdKafkaError | null, offset?: number) =>
              err ? reject(err) : resolve(offset),
          );
        }),
      ),
    );
    expect(offsets).toHaveLength(5);
    expect(new Set(offsets).size).toBe(5); // real, distinct offsets
    await disconnectP(producer);

    const messages = await readAll(topic, 5);
    const parsed = messages.map((m) => JSON.parse(DECODER.decode(m.value!)) as { index: number });
    expect(new Set(parsed.map((p) => p.index)).size).toBe(5);
    expect(messages.every((m) => m.key === null)).toBe(true);
  }, 120_000);

  test("a real flush: 500 messages fully drained within the timeout", async () => {
    const topic = `m2-flush-${RUN_ID}`;
    await createTopic(topic, 1);
    const producer = makeProducer();
    await connectP(producer);
    for (let i = 0; i < 500; i++) producer.produce(topic, 0, `f-${i}`);
    await flushP(producer, 30_000);
    expect(producer.pendingDeliveryReports).toBe(0);
    await waitFor(() => true, 1, () => "");
    await disconnectP(producer);
  }, 60_000);
});
