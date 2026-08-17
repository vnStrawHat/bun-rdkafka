/**
 * M8 — Callback API gaps against a real broker: `features()` / default export,
 * `KafkaConsumer.offsetsForTimes`, JS-side `partitioner_cb`, `event_cb` →
 * `event.event`, `connection.failure` + `getLastError()`, and the flowing-mode
 * `warning` event.
 *
 * The broker is SHARED with other tests (see docker-kafka.ts): no stopKafka(),
 * unique "m8-" topic names per run.
 */

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Buffer } from "node:buffer";
import { integrationAvailable, startKafka } from "./docker-kafka.ts";
import Kafka, {
  AdminClient,
  KafkaConsumer,
  Producer,
  features,
  librdkafkaVersion,
  type BrokerMetadata,
  type ClientMetrics,
  type DeliveryReportPayload,
  type LibrdKafkaError,
  type TopicPartitionOffset,
} from "../../packages/bun-rdkafka/src/index.ts";
import { ERROR_CODES } from "../../packages/bun-rdkafka/src/core/errors.ts";
import { waitFor } from "./slice-harness.ts";

const AVAILABLE = await integrationAvailable();
const RUN_ID = Date.now().toString(36);

let brokers = "localhost:9092";
let admin: AdminClient;

function call<T>(run: (cb: (err: LibrdKafkaError | null, result?: T) => void) => void): Promise<T> {
  return new Promise((resolve, reject) => {
    run((err, result) => (err ? reject(err) : resolve(result as T)));
  });
}

function connectAsync(client: Producer | KafkaConsumer): Promise<void> {
  return new Promise((resolve, reject) => {
    client.connect({ timeout: 15_000 }, (err) => (err ? reject(err) : resolve()));
  });
}

function disconnectAsync(client: Producer | KafkaConsumer): Promise<void> {
  return new Promise((resolve) => {
    client.disconnect(() => resolve());
  });
}

async function createTopic(topic: string, partitions: number): Promise<void> {
  await call<void>((cb) =>
    admin.createTopic(
      { topic, num_partitions: partitions, replication_factor: 1 },
      15_000,
      cb as (e: LibrdKafkaError | null) => void,
    ),
  );
}

describe.skipIf(!AVAILABLE)("M8 callback gaps (real broker)", () => {
  beforeAll(async () => {
    brokers = (await startKafka()).brokers;
    admin = AdminClient.create({ "bootstrap.servers": brokers });
  }, 180_000);

  afterAll(() => {
    admin?.disconnect();
  });

  test("features() / default export match librdkafka's builtin.features", () => {
    const list = features();
    expect(list).toContain("ssl");
    expect(list).toContain("sasl_scram");
    expect(Kafka.features).toEqual(list);
    expect(Kafka.librdkafkaVersion).toBe(librdkafkaVersion());
    expect(Kafka.Producer).toBe(Producer);
  });

  test(
    "offsetsForTimes: the middle timestamp maps to the middle offset; a future timestamp → -1",
    async () => {
      const topic = `m8-oft-${RUN_ID}`;
      await createTopic(topic, 1);
      const base = Date.now() - 60_000;
      const producer = new Producer({ "bootstrap.servers": brokers, "linger.ms": 0 });
      await connectAsync(producer);
      for (let i = 0; i < 3; i++) {
        producer.produce(topic, 0, Buffer.from(`m-${i}`), null, base + i * 1000);
      }
      await call<void>((cb) => producer.flush(15_000, cb as (e: LibrdKafkaError | null) => void));
      await disconnectAsync(producer);

      const consumer = new KafkaConsumer({
        "bootstrap.servers": brokers,
        "group.id": `m8-oft-${RUN_ID}`,
      });
      await connectAsync(consumer);
      try {
        const result = await call<TopicPartitionOffset[]>((cb) =>
          consumer.offsetsForTimes([{ topic, partition: 0, offset: base + 1000 }], 10_000, cb),
        );
        expect(result).toHaveLength(1);
        expect(result[0]?.topic).toBe(topic);
        expect(result[0]?.partition).toBe(0);
        expect(result[0]?.offset).toBe(1);

        // A timestamp before the first message → offset 0; the (toppars, cb)
        // overload uses the 1000ms upstream default.
        const first = await call<TopicPartitionOffset[]>((cb) =>
          consumer.offsetsForTimes([{ topic, partition: 0, offset: base - 5000 }], cb),
        );
        expect(first[0]?.offset).toBe(0);
        // Exactly the last message's timestamp → its offset.
        const last = await call<TopicPartitionOffset[]>((cb) =>
          consumer.offsetsForTimes([{ topic, partition: 0, offset: base + 2000 }], 10_000, cb),
        );
        expect(last[0]?.offset).toBe(2);

        // A timestamp after every message: RD_KAFKA_OFFSET_END (-1).
        const future = await call<TopicPartitionOffset[]>((cb) =>
          consumer.offsetsForTimes([{ topic, partition: 0, offset: Date.now() + 3_600_000 }], 10_000, cb),
        );
        expect(future[0]?.offset).toBe(-1);

        // Unknown partition → err-first callback + getLastError().
        const err = await new Promise<LibrdKafkaError | null>((resolve) =>
          consumer.offsetsForTimes([{ topic, partition: 42, offset: base }], 5_000, (e) => resolve(e)),
        );
        expect(err).not.toBeNull();
        expect(consumer.getLastError()).toBe(err);
      } finally {
        await disconnectAsync(consumer);
      }
    },
    120_000,
  );

  test(
    "partitioner_cb: a constant partitioner lands every message on that partition",
    async () => {
      const topic = `m8-part-${RUN_ID}`;
      await createTopic(topic, 4);
      const seen: { topic: string; key: unknown; count: number }[] = [];
      const producer = new Producer({
        "bootstrap.servers": brokers,
        "linger.ms": 0,
        dr_cb: true,
        partitioner_cb: (t: string, key: unknown, count: number) => {
          seen.push({ topic: t, key, count });
          return 3;
        },
      });
      const reports: DeliveryReportPayload[] = [];
      producer.on("delivery-report", (err: LibrdKafkaError | null, report: DeliveryReportPayload) => {
        expect(err).toBeNull();
        reports.push(report);
      });
      // Connect with the topic in the metadata request so the count is known immediately.
      await new Promise<void>((resolve, reject) =>
        producer.connect({ topic, timeout: 15_000 }, (err) => (err ? reject(err) : resolve())),
      );
      try {
        for (let i = 0; i < 10; i++) producer.produce(topic, null, Buffer.from(`v-${i}`), `k-${i}`);
        // An explicit partition bypasses the partitioner.
        producer.produce(topic, 1, Buffer.from("explicit"), "k-x");
        await call<void>((cb) => producer.flush(15_000, cb as (e: LibrdKafkaError | null) => void));
        await waitFor(() => reports.length === 11, 30_000, () => `DRs: ${reports.length}`);
        const viaPartitioner = reports.filter((r) => String(r.key) !== "k-x");
        expect(viaPartitioner.every((r) => r.partition === 3)).toBe(true);
        expect(reports.find((r) => String(r.key) === "k-x")?.partition).toBe(1);
        expect(seen).toHaveLength(10);
        expect(seen[0]?.count).toBe(4);
        expect(seen[0]?.key).toBe("k-0");
      } finally {
        await disconnectAsync(producer);
      }
    },
    120_000,
  );

  test(
    "event_cb: true → raw frames on 'event.event' / 'event' (statistics)",
    async () => {
      const producer = new Producer({
        "bootstrap.servers": brokers,
        "statistics.interval.ms": 100,
        event_cb: true,
      });
      const raw: { type: number }[] = [];
      const alias: { type: number }[] = [];
      const stats: unknown[] = [];
      producer.on("event.event", (e: { type: number }) => raw.push(e));
      producer.on("event", (e: { type: number }) => alias.push(e));
      producer.on("event.stats", (s: unknown) => stats.push(s));
      await connectAsync(producer);
      try {
        await waitFor(() => stats.length >= 2, 15_000, () => `stats: ${stats.length}`);
        expect(raw.length).toBeGreaterThanOrEqual(2);
        expect(alias.length).toBe(raw.length);
        expect(raw.some((e) => "json" in e)).toBe(true);
      } finally {
        await disconnectAsync(producer);
      }
    },
    60_000,
  );

  test(
    "connection.failure: an unreachable broker → event.error + connection.failure + getLastError()",
    async () => {
      const producer = new Producer({
        "bootstrap.servers": "127.0.0.1:1",
        "socket.timeout.ms": 1000,
        "log.connection.close": false,
      });
      const failures: { err: LibrdKafkaError; metrics: ClientMetrics }[] = [];
      const errors: LibrdKafkaError[] = [];
      producer.on("connection.failure", (err: LibrdKafkaError, metrics: ClientMetrics) =>
        failures.push({ err, metrics }),
      );
      producer.on("event.error", (err: LibrdKafkaError) => errors.push(err));
      const err = await new Promise<LibrdKafkaError | null>((resolve) =>
        producer.connect({ timeout: 1500 }, (e) => resolve(e)),
      );
      expect(err).not.toBeNull();
      expect(failures).toHaveLength(1);
      expect(failures[0]?.err).toBe(err as LibrdKafkaError);
      expect(typeof failures[0]?.metrics.connectionOpened).toBe("number");
      expect(errors).toContain(err as LibrdKafkaError);
      expect(producer.getLastError()).toBe(err as LibrdKafkaError);
      expect(producer.isConnected()).toBe(false);
    },
    30_000,
  );

  test(
    "warning: flowing consume of a nonexistent topic emits 'warning' (UNKNOWN_TOPIC_OR_PART), not event.error",
    async () => {
      const consumer = new KafkaConsumer({
        "bootstrap.servers": brokers,
        "group.id": `m8-warn-${RUN_ID}`,
        "allow.auto.create.topics": false,
        "topic.metadata.refresh.interval.ms": 1000,
      });
      const warnings: LibrdKafkaError[] = [];
      const errors: LibrdKafkaError[] = [];
      consumer.on("warning", (e: LibrdKafkaError) => warnings.push(e));
      consumer.on("event.error", (e: LibrdKafkaError) => errors.push(e));
      await connectAsync(consumer);
      try {
        consumer.subscribe([`m8-does-not-exist-${RUN_ID}`]);
        consumer.consume();
        await waitFor(() => warnings.length >= 1, 30_000, () => `warnings: ${warnings.length}, errors: ${errors.map((e) => e.code)}`);
        expect(warnings[0]?.code).toBe(ERROR_CODES.ERR_UNKNOWN_TOPIC_OR_PART);
        expect(errors.filter((e) => e.code === ERROR_CODES.ERR_UNKNOWN_TOPIC_OR_PART)).toHaveLength(0);
      } finally {
        await disconnectAsync(consumer);
      }
    },
    60_000,
  );

  test("getMetadata still works after the gaps (sanity: brokers listed)", async () => {
    const producer = new Producer({ "bootstrap.servers": brokers });
    await connectAsync(producer);
    try {
      const md = await call<{ brokers: BrokerMetadata[] }>((cb) => producer.getMetadata({}, cb));
      expect(md.brokers.length).toBeGreaterThan(0);
    } finally {
      await disconnectAsync(producer);
    }
  });
});
