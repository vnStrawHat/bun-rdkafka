/**
 * test/integration/m6-gaps.integration.test.ts — the ABI gaps closed in M6,
 * against a real broker:
 *  1. listOffsets (op BRK_ADMIN_LIST_OFFSETS): EARLIEST / LATEST / theo
 *     timestamp, qua Callback AdminClient.
 *  2. Commit metadata round-trip: commit(offset + metadata) → committed()
 *     returns it verbatim (the extended TPL format 2).
 *  3. KafkaJS admin: fetchTopicOffsets(READ_COMMITTED) + fetchTopicOffsetsByTimestamp.
 *
 * ⚠ The docker broker is SHARED (KEEP_KAFKA=1 when run with the suite): do not
 * stopKafka() ở afterAll, topic prefix "m6-".
 */

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Buffer } from "node:buffer";
import { integrationAvailable, startKafka } from "./docker-kafka.ts";
import { AdminClient, OffsetSpec, type ListOffsetsReport } from "../../packages/bun-rdkafka/src/callback/admin.ts";
import { Producer } from "../../packages/bun-rdkafka/src/callback/producer.ts";
import {
  KafkaConsumer,
  type TopicPartitionOffsetAndMetadata,
} from "../../packages/bun-rdkafka/src/callback/kafka-consumer.ts";
import { Kafka } from "../../packages/bun-rdkafka/src/kafkajs/kafka.ts";
import { IsolationLevel } from "../../packages/bun-rdkafka/src/kafkajs/admin.ts";
import type { LibrdKafkaError } from "../../packages/bun-rdkafka/src/core/errors.ts";

const AVAILABLE = await integrationAvailable();
const RUN_ID = Date.now().toString(36);
const TOPIC = `m6-listoffsets-${RUN_ID}`;

let brokers = "localhost:9092";
let admin: AdminClient;
/** The timestamp just BEFORE the 6th message (index 5) — the ByTimestamp mark. */
let midTimestamp = 0;

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

describe.skipIf(!AVAILABLE)("M6 ABI gaps (real broker)", () => {
  beforeAll(async () => {
    const kafka = await startKafka();
    brokers = kafka.brokers;
    admin = AdminClient.create({ "bootstrap.servers": brokers });
    await call<void>((cb) =>
      admin.createTopic(
        { topic: TOPIC, num_partitions: 1, replication_factor: 1 },
        10_000,
        cb as (e: LibrdKafkaError | null) => void,
      ),
    );

    // 10 messages: the first 5, then a pause so timestamps split into two batches.
    const producer = new Producer({ "bootstrap.servers": brokers, "linger.ms": 0 });
    await connectAsync(producer);
    for (let i = 0; i < 5; i++) producer.produce(TOPIC, 0, Buffer.from(`lo-${i}`));
    await call<void>((cb) => producer.flush(10_000, cb as (e: LibrdKafkaError | null) => void));
    await new Promise((r) => setTimeout(r, 50));
    midTimestamp = Date.now();
    await new Promise((r) => setTimeout(r, 50));
    for (let i = 5; i < 10; i++) producer.produce(TOPIC, 0, Buffer.from(`hi-${i}`));
    await call<void>((cb) => producer.flush(10_000, cb as (e: LibrdKafkaError | null) => void));
    await disconnectAsync(producer);
  }, 180_000);

  afterAll(() => {
    admin?.disconnect();
    // shared broker — do NOT stopKafka().
  });

  test("listOffsets: EARLIEST=0, LATEST=10, the midpoint timestamp → offset 5", async () => {
    const earliest = await call<ListOffsetsReport[]>((cb) =>
      admin.listOffsets([{ topic: TOPIC, partition: 0, offsetSpec: OffsetSpec.EARLIEST }], { timeout: 10_000 }, cb),
    );
    expect(earliest).toHaveLength(1);
    expect(earliest[0]?.error).toBeNull();
    expect(earliest[0]?.offset).toBe(0);

    const latest = await call<ListOffsetsReport[]>((cb) =>
      admin.listOffsets([{ topic: TOPIC, partition: 0, offsetSpec: OffsetSpec.LATEST }], { timeout: 10_000 }, cb),
    );
    expect(latest[0]?.error).toBeNull();
    expect(latest[0]?.offset).toBe(10);

    // The first offset with timestamp >= midTimestamp = message "hi-5" (offset 5).
    const byTs = await call<ListOffsetsReport[]>((cb) =>
      admin.listOffsets([{ topic: TOPIC, partition: 0, offsetSpec: midTimestamp }], { timeout: 10_000 }, cb),
    );
    expect(byTs[0]?.error).toBeNull();
    expect(byTs[0]?.offset).toBe(5);
    expect(byTs[0]?.timestamp).toBeGreaterThanOrEqual(midTimestamp);

    // isolation_level passes through (a txn-free topic → same result as RU).
    const rc = await call<ListOffsetsReport[]>((cb) =>
      admin.listOffsets(
        [{ topic: TOPIC, partition: 0, offsetSpec: OffsetSpec.LATEST }],
        { timeout: 10_000, isolationLevel: 1 },
        cb,
      ),
    );
    expect(rc[0]?.offset).toBe(10);
  }, 60_000);

  test("commit metadata round-trip: commit(offset+metadata) → committed() returns it verbatim", async () => {
    const consumer = new KafkaConsumer({
      "bootstrap.servers": brokers,
      "group.id": `m6-md-${RUN_ID}`,
      "enable.auto.commit": false,
    });
    await connectAsync(consumer);

    const metadata = "checkpoint-α-β/42";
    consumer.commitSync({ topic: TOPIC, partition: 0, offset: 7, metadata });

    const committed = await new Promise<TopicPartitionOffsetAndMetadata[]>((resolve, reject) => {
      consumer.committed([{ topic: TOPIC, partition: 0 }], 10_000, (err, tps) =>
        err ? reject(err) : resolve(tps ?? []),
      );
    });
    expect(committed).toHaveLength(1);
    expect(committed[0]?.offset).toBe(7);
    expect(committed[0]?.metadata).toBe(metadata);

    // A commit WITHOUT metadata → committed carries no metadata field (null ≠ "").
    consumer.commitSync({ topic: TOPIC, partition: 0, offset: 9 });
    const committed2 = await new Promise<TopicPartitionOffsetAndMetadata[]>((resolve, reject) => {
      consumer.committed([{ topic: TOPIC, partition: 0 }], 10_000, (err, tps) =>
        err ? reject(err) : resolve(tps ?? []),
      );
    });
    expect(committed2[0]?.offset).toBe(9);
    expect(committed2[0]?.metadata).toBeUndefined();

    await disconnectAsync(consumer);
  }, 60_000);

  test("KafkaJS: fetchTopicOffsets(READ_COMMITTED) + fetchTopicOffsetsByTimestamp", async () => {
    const kafka = new Kafka({ "bootstrap.servers": brokers });
    const kjsAdmin = kafka.admin();
    await kjsAdmin.connect();

    const rc = await kjsAdmin.fetchTopicOffsets(TOPIC, {
      isolationLevel: IsolationLevel.READ_COMMITTED,
      timeout: 10_000,
    });
    expect(rc).toEqual([{ partition: 0, offset: "10", high: "10", low: "0" }]);

    const byTs = await kjsAdmin.fetchTopicOffsetsByTimestamp(TOPIC, midTimestamp, {
      timeout: 10_000,
    });
    expect(byTs).toEqual([{ partition: 0, offset: "5" }]);

    // an omitted timestamp → LATEST (matching upstream).
    const latest = await kjsAdmin.fetchTopicOffsetsByTimestamp(TOPIC, undefined, { timeout: 10_000 });
    expect(latest).toEqual([{ partition: 0, offset: "10" }]);

    await kjsAdmin.disconnect();
  }, 60_000);
});
