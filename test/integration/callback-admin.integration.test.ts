/**
 * test/integration/callback-admin.integration.test.ts — the AdminClient (M4)
 * against a real broker: the full topic lifecycle + createFrom(Producer).
 *
 * ⚠ The docker broker is SHARED (KEEP_KAFKA=1 when run with the suite): do not
 * stopKafka() ở afterAll, topic prefix "m4-".
 */

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Buffer } from "node:buffer";
import { integrationAvailable, startKafka } from "./docker-kafka.ts";
import { AdminClient } from "../../packages/bun-rdkafka/src/callback/admin.ts";
import { Producer } from "../../packages/bun-rdkafka/src/callback/producer.ts";
import type { LibrdKafkaError } from "../../packages/bun-rdkafka/src/core/errors.ts";

const AVAILABLE = await integrationAvailable();
const RUN_ID = Date.now().toString(36);
const TOPIC = `m4-admin-ts-${RUN_ID}`;

let brokers = "localhost:9092";
let admin: AdminClient;

function call<T>(run: (cb: (err: LibrdKafkaError | null, result?: T) => void) => void): Promise<T> {
  return new Promise((resolve, reject) => {
    run((err, result) => (err ? reject(err) : resolve(result as T)));
  });
}

describe.skipIf(!AVAILABLE)("AdminClient (real broker)", () => {
  beforeAll(async () => {
    const kafka = await startKafka();
    brokers = kafka.brokers;
    admin = AdminClient.create({ "bootstrap.servers": brokers });
  }, 180_000);

  afterAll(() => {
    admin?.disconnect();
    // the shared broker — NO stopKafka() (other tests/agents are using it).
  });

  test("createTopic → describeTopics → createPartitions → listTopics", async () => {
    await call<void>((cb) =>
      admin.createTopic(
        { topic: TOPIC, num_partitions: 2, replication_factor: 1, config: { "cleanup.policy": "delete" } },
        10_000,
        cb as (e: LibrdKafkaError | null) => void,
      ),
    );

    const desc = await call<any>((cb) => admin.describeTopics([TOPIC], { timeout: 10_000 }, cb));
    expect(desc).toHaveLength(1);
    expect(desc[0].name).toBe(TOPIC);
    expect(desc[0].error).toBeNull();
    expect(desc[0].isInternal).toBe(false);
    expect(desc[0].partitions).toHaveLength(2);
    expect(desc[0].partitions[0].leader?.id).toBeGreaterThanOrEqual(0);

    await call<void>((cb) =>
      admin.createPartitions(TOPIC, 4, 10_000, cb as (e: LibrdKafkaError | null) => void),
    );
    const desc2 = await call<any>((cb) => admin.describeTopics([TOPIC], { timeout: 10_000 }, cb));
    expect(desc2[0].partitions).toHaveLength(4);

    const topics = await call<string[]>((cb) => admin.listTopics({ timeout: 10_000 }, cb));
    expect(topics).toContain(TOPIC);
  }, 60_000);

  test("a duplicate createTopic → LibrdKafkaError TOPIC_ALREADY_EXISTS (per-item)", async () => {
    const err = await call<void>((cb) =>
      admin.createTopic({ topic: TOPIC, num_partitions: 1 }, 10_000, cb as (e: LibrdKafkaError | null) => void),
    ).catch((e: LibrdKafkaError) => e);
    expect((err as LibrdKafkaError).code).toBe(36); // TOPIC_ALREADY_EXISTS
  }, 30_000);

  test("deleteRecords (a 2-partition fan-out) + listGroups works", async () => {
    // pump 3 messages into partitions 0 and 1 with the real Producer
    const producer = new Producer({ "bootstrap.servers": brokers, "linger.ms": 5 });
    await new Promise<void>((resolve, reject) =>
      producer.connect({}, (e) => (e ? reject(e) : resolve())),
    );
    for (let p = 0; p < 2; p++) {
      for (let i = 0; i < 3; i++) producer.produce(TOPIC, p, Buffer.from(`m4-${p}-${i}`));
    }
    await new Promise<void>((resolve, reject) =>
      producer.flush(10_000, (e) => (e ? reject(e) : resolve())),
    );

    const reports = await call<any>((cb) =>
      admin.deleteRecords(
        [
          { topic: TOPIC, partition: 0, offset: 2 },
          { topic: TOPIC, partition: 1, offset: 1 },
        ],
        { timeout: 10_000 },
        cb,
      ),
    );
    expect(reports).toEqual([
      { topic: TOPIC, partition: 0, lowWatermark: 2, error: null },
      { topic: TOPIC, partition: 1, lowWatermark: 1, error: null },
    ]);

    const groups = await call<any>((cb) => admin.listGroups({ timeout: 10_000 }, cb));
    expect(Array.isArray(groups.groups)).toBe(true);

    // createFrom on an open Producer: rides the same handle, no new connect
    const adminFrom = AdminClient.createFrom(producer);
    const topics = await call<string[]>((cb) => adminFrom.listTopics(cb));
    expect(topics).toContain(TOPIC);
    adminFrom.disconnect();
    // the host producer survives the admin "closing"
    expect(producer.isConnected()).toBe(true);
    await new Promise<void>((resolve) => {
      producer.disconnect(() => resolve());
    });
  }, 60_000);

  test("deleteTopic cleans up", async () => {
    await call<void>((cb) =>
      admin.deleteTopic(TOPIC, 10_000, cb as (e: LibrdKafkaError | null) => void),
    );
  }, 30_000);
});
