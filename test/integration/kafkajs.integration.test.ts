/**
 * M5a — the KafkaJS namespace against a real broker: Producer
 * send/sendBatch/transaction, the full Admin flow. Content cross-checked via
 * slice-harness (independent of M5b's KafkaJS Consumer).
 *
 * The docker broker is SHARED with other agents/tests: NO stopKafka() in afterAll,
 * topic prefix "m5a-".
 */

import { beforeAll, describe, expect, test } from "bun:test";
import { createTopic, integrationAvailable, startKafka } from "./docker-kafka.ts";
import { SliceConsumer, SliceProducer, waitFor } from "./slice-harness.ts";
import { KafkaJS } from "../../packages/bun-rdkafka/src/index.ts";
import { ERROR_CODES } from "../../packages/bun-rdkafka/src/core/errors.ts";

const AVAILABLE = await integrationAvailable();
const RUN_ID = Date.now().toString(36);
const DECODER = new TextDecoder();

let brokers = "localhost:9092";

if (AVAILABLE) {
  beforeAll(async () => {
    ({ brokers } = await startKafka());
  }, 180_000);
}

function newKafka(): InstanceType<typeof KafkaJS.Kafka> {
  return new KafkaJS.Kafka({
    kafkaJS: { brokers: [brokers], clientId: `m5a-${RUN_ID}` },
    "socket.timeout.ms": 10_000,
  });
}

/** Reads `count` messages of a topic from the start (independent of the KafkaJS layer). */
async function readBack(
  topic: string,
  count: number,
  extra: Record<string, unknown> = {},
): Promise<SliceConsumer> {
  const consumer = new SliceConsumer({
    config: {
      "bootstrap.servers": brokers,
      "group.id": `m5a-read-${topic}-${Math.random().toString(36).slice(2)}`,
      "auto.offset.reset": "earliest",
      "enable.auto.commit": false,
      "fetch.queue.backoff.ms": 10,
      ...extra,
    },
    label: `readback-${topic}`,
  });
  consumer.connect();
  consumer.subscribe([topic]);
  await consumer.waitForMessages(count);
  consumer.throwPollErrors();
  return consumer;
}

describe.skipIf(!AVAILABLE)("KafkaJS.Producer (real broker)", () => {
  test("send: per-partition-merged RecordMetadata + content/headers roundtrip", async () => {
    const topic = `m5a-send-${RUN_ID}`;
    await createTopic(topic, 1);

    const producer = newKafka().producer();
    await producer.connect();
    try {
      const metadata = await producer.send({
        topic,
        messages: [
          { value: "v0", key: "k0", headers: { h1: ["a", "b"], h2: "c" } },
          { value: "v1", key: "k1" },
          { value: Buffer.from("v2"), timestamp: "1723600000000" },
        ],
      });
      // 3 messages on one topic-partition → merged into 1 RecordMetadata with the smallest baseOffset
      expect(metadata).toHaveLength(1);
      const record = metadata[0]!;
      expect(record.topicName).toBe(topic);
      expect(record.partition).toBe(0);
      expect(record.errorCode).toBe(0);
      expect(record.baseOffset).toBe("0");
      expect(record.logAppendTime).toBe("-1");

      const consumer = await readBack(topic, 3);
      const values = consumer.messages.map((m) => DECODER.decode(m.value ?? undefined));
      expect(values).toEqual(["v0", "v1", "v2"]);
      const first = consumer.messages[0]!;
      expect(first.headers?.map((h) => [h.key, DECODER.decode(h.value ?? undefined)])).toEqual([
        ["h1", "a"],
        ["h1", "b"],
        ["h2", "c"],
      ]);
      expect(consumer.messages[2]!.timestamp).toBe(1723600000000);
      consumer.disconnect();
    } finally {
      await producer.disconnect();
    }
  }, 60_000);

  test("send with per-call acks/timeout/compression → ERR__INVALID_ARG", async () => {
    const producer = newKafka().producer();
    await producer.connect();
    try {
      for (const key of ["acks", "timeout", "compression"]) {
        try {
          await producer.send({ topic: "t", messages: [], [key]: 1 } as never);
          throw new Error("expected a reject");
        } catch (e) {
          expect((e as KafkaJS.KafkaJSError).code).toBe(ERROR_CODES.ERR__INVALID_ARG);
        }
      }
    } finally {
      await producer.disconnect();
    }
  }, 30_000);

  test("sendBatch across two topics → merged metadata, flush works", async () => {
    const t1 = `m5a-batch1-${RUN_ID}`;
    const t2 = `m5a-batch2-${RUN_ID}`;
    await createTopic(t1, 1);
    await createTopic(t2, 1);

    const producer = newKafka().producer();
    await producer.connect();
    try {
      const pending = producer.sendBatch({
        topicMessages: [
          { topic: t1, messages: [{ value: "a1" }, { value: "a2" }] },
          { topic: t2, messages: [{ value: "b1" }] },
        ],
      });
      await producer.flush({ timeout: 10_000 });
      const metadata = await pending;
      expect(metadata).toHaveLength(2);
      expect(new Set(metadata.map((m) => m.topicName))).toEqual(new Set([t1, t2]));

      (await readBack(t1, 2)).disconnect();
      (await readBack(t2, 1)).disconnect();
    } finally {
      await producer.disconnect();
    }
  }, 60_000);

  test("transaction: commit visible, abort invisible (read_committed)", async () => {
    const topic = `m5a-txn-${RUN_ID}`;
    await createTopic(topic, 1);

    const producer = newKafka().producer({
      kafkaJS: { transactionalId: `m5a-txn-id-${RUN_ID}` },
    });
    await producer.connect();
    try {
      expect(producer.isActive()).toBe(false);
      const txn = await producer.transaction();
      expect(producer.isActive()).toBe(true);
      const send1 = txn.send({ topic, messages: [{ value: "committed-1" }] });
      await txn.commit();
      await send1;
      expect(producer.isActive()).toBe(false);

      const txn2 = await producer.transaction();
      txn2.send({ topic, messages: [{ value: "aborted-1" }] }).catch(() => {});
      await txn2.abort();

      const txn3 = await producer.transaction();
      const send3 = txn3.send({ topic, messages: [{ value: "committed-2" }] });
      await txn3.commit();
      await send3;

      const consumer = await readBack(topic, 2, { "isolation.level": "read_committed" });
      // wait one more beat to be sure no aborted messages slip in
      await Bun.sleep(500);
      consumer.pollNow();
      const values = consumer.messages.map((m) => DECODER.decode(m.value ?? undefined));
      expect(values).toEqual(["committed-1", "committed-2"]);
      consumer.disconnect();
    } finally {
      await producer.disconnect();
    }
  }, 90_000);

  test("nested transactions / a commit without a transaction → ERR__STATE", async () => {
    const producer = newKafka().producer({
      kafkaJS: { transactionalId: `m5a-txn-state-${RUN_ID}` },
    });
    await producer.connect();
    try {
      await expect(producer.commit()).rejects.toMatchObject({ code: ERROR_CODES.ERR__STATE });
      await producer.transaction();
      await expect(producer.transaction()).rejects.toMatchObject({
        code: ERROR_CODES.ERR__STATE,
      });
      await producer.abort();
    } finally {
      await producer.disconnect();
    }
  }, 30_000);
});

describe.skipIf(!AVAILABLE)("KafkaJS.Admin (real broker)", () => {
  test("full flow: createTopics/listTopics/fetchTopicMetadata/fetchTopicOffsets/deleteTopicRecords/deleteTopics", async () => {
    const topic = `m5a-admin-${RUN_ID}`;
    const admin = newKafka().admin();
    await admin.connect();
    try {
      // createTopics: new → true; already existing → false
      expect(
        await admin.createTopics({
          topics: [
            {
              topic,
              numPartitions: 2,
              replicationFactor: 1,
              configEntries: [{ name: "retention.ms", value: "3600000" }],
            },
          ],
        }),
      ).toBe(true);
      expect(await admin.createTopics({ topics: [{ topic }] })).toBe(false);

      expect(await admin.listTopics()).toContain(topic);

      const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
      expect(metadata).toHaveLength(1);
      expect(metadata[0]!.name).toBe(topic);
      expect(metadata[0]!.partitions).toHaveLength(2);
      const partition0 = metadata[0]!.partitions.find((p) => p.partitionId === 0)!;
      expect(partition0.leader).toBeGreaterThanOrEqual(0);
      expect(partition0.leaderNode?.id).toBe(partition0.leader);
      expect(partition0.replicas.length).toBeGreaterThan(0);
      expect(partition0.isr.length).toBeGreaterThan(0);

      // Pump 5 messages into partition 0 so offsets exist
      const slice = new SliceProducer({
        config: { "bootstrap.servers": brokers },
        label: "m5a-admin-fill",
      });
      slice.connect();
      slice.produce(
        Array.from({ length: 5 }, (_, i) => ({
          topic,
          partition: 0,
          timestamp: 0,
          key: null,
          value: `m-${i}`,
        })),
      );
      await slice.waitIdle();
      slice.disconnect();

      const offsets = await admin.fetchTopicOffsets(topic);
      expect(offsets).toHaveLength(2);
      const partitionOffsets = offsets.find((o) => o.partition === 0)!;
      expect(partitionOffsets.low).toBe("0");
      expect(partitionOffsets.high).toBe("5");
      expect(partitionOffsets.offset).toBe("5");

      const reports = await admin.deleteTopicRecords({
        topic,
        partitions: [{ partition: 0, offset: "2" }],
      });
      expect(reports).toHaveLength(1);
      expect(reports[0]!.lowWatermark).toBe(2);
      expect((await admin.fetchTopicOffsets(topic)).find((o) => o.partition === 0)!.low).toBe(
        "2",
      );

      await admin.deleteTopics({ topics: [topic] });
    } finally {
      await admin.disconnect();
    }
  }, 120_000);

  test("fetchOffsets after a group commit; listGroups/describeGroups/deleteGroups", async () => {
    const topic = `m5a-groups-${RUN_ID}`;
    const groupId = `m5a-group-${RUN_ID}`;
    await createTopic(topic, 1);

    const slice = new SliceProducer({ config: { "bootstrap.servers": brokers } });
    slice.connect();
    slice.produce([
      { topic, partition: 0, timestamp: 0, key: null, value: "x1" },
      { topic, partition: 0, timestamp: 0, key: null, value: "x2" },
    ]);
    await slice.waitIdle();
    slice.disconnect();

    const consumer = new SliceConsumer({
      config: {
        "bootstrap.servers": brokers,
        "group.id": groupId,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": false,
        "fetch.queue.backoff.ms": 10,
      },
    });
    consumer.connect();
    consumer.subscribe([topic]);
    await consumer.waitForMessages(2);
    consumer.client.commit([{ topic, partition: 0, offset: 2 }], false);

    const admin = newKafka().admin();
    await admin.connect();
    try {
      const offsets = await admin.fetchOffsets({ groupId, topics: [topic] });
      expect(offsets).toHaveLength(1);
      expect(offsets[0]!.topic).toBe(topic);
      expect(offsets[0]!.partitions[0]!.offset).toBe("2");

      const groups = await admin.listGroups();
      expect(groups.groups.map((g) => g.groupId)).toContain(groupId);

      const described = await admin.describeGroups([groupId]);
      expect(described.groups).toHaveLength(1);
      expect(described.groups[0]!.groupId).toBe(groupId);
      expect(described.groups[0]!.members.length).toBeGreaterThan(0);

      // the group still has members → deleteGroups must reject with KafkaJSDeleteGroupsError
      consumer.disconnect();
      await Bun.sleep(500); // wait for the broker to register the member leaving
      const reports = await admin.deleteGroups([groupId]);
      expect(reports[0]!.groupId).toBe(groupId);
      expect(reports[0]!.error).toBeNull();
    } finally {
      await admin.disconnect();
    }
  }, 120_000);

  test("dependentAdmin on a connected producer", async () => {
    const producer = newKafka().producer();
    await producer.connect();
    try {
      const admin = producer.dependentAdmin();
      await admin.connect();
      expect(Array.isArray(await admin.listTopics())).toBe(true);
      await admin.disconnect();
      // the host client survives the dependent admin disconnecting
      await producer.send({ topic: `m5a-dep-${RUN_ID}`, messages: [{ value: "ok" }] });
    } finally {
      await producer.disconnect();
    }
  }, 60_000);
});
