/**
 * Integration tests for the KafkaJS Consumer (M5b) — a real broker via
 * docker-kafka.
 *
 * The broker container is SHARED between agents/suites: no stopKafka() in
 * afterAll. Topics/groups all use the "m5b-" prefix.
 */

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Buffer } from "node:buffer";
import { integrationAvailable, startKafka, createTopic } from "./docker-kafka.ts";
import { Consumer } from "../../packages/bun-rdkafka/src/kafkajs/consumer.ts";
import { Producer } from "../../packages/bun-rdkafka/src/callback/producer.ts";

const available = await integrationAvailable();
let brokers = "localhost:9092";

const RUN = `${Date.now().toString(36)}`; // unique topics/groups per run
const t = (name: string): string => `m5b-${name}-${RUN}`;

function newConsumer(groupId: string, extra: Record<string, unknown> = {}): Consumer {
  const kafkaJS = {
    groupId,
    brokers: [brokers],
    fromBeginning: true,
    ...(extra["kafkaJS"] as Record<string, unknown> | undefined),
  };
  const flat = { ...extra };
  delete flat["kafkaJS"];
  return new Consumer({
    kafkaJS,
    // The operational recommendation from bench/RESULTS.md — low consume latency.
    "fetch.queue.backoff.ms": 10,
    ...flat,
  });
}

async function produceTo(topic: string, perPartition: number, partitions: number): Promise<void> {
  const producer = new Producer({ "bootstrap.servers": brokers });
  await new Promise<void>((res, rej) => producer.connect({}, (e) => (e ? rej(e) : res())));
  for (let p = 0; p < partitions; p++) {
    for (let i = 0; i < perPartition; i++) {
      producer.produce(topic, p, Buffer.from(`v-${p}-${i}`), `k-${p}-${i}`);
    }
  }
  await new Promise<void>((res, rej) => producer.flush(30_000, (e) => (e ? rej(e) : res())));
  await new Promise<void>((res) => producer.disconnect(() => res()));
}

async function until(cond: () => boolean, timeoutMs: number, what: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for: ${what}`);
    await Bun.sleep(25);
  }
}

const consumers: Consumer[] = [];

function track(c: Consumer): Consumer {
  consumers.push(c);
  return c;
}

beforeAll(async () => {
  if (!available) return;
  ({ brokers } = await startKafka());
});

afterAll(async () => {
  // NO stopKafka() — the broker is shared. Only clean up live consumers.
  await Promise.allSettled(consumers.map((c) => c.disconnect()));
});

describe.skipIf(!available)("KafkaJS Consumer (real broker)", () => {
  test(
    "eachMessage: all messages, per-partition ordering, 3 partitions interleaved",
    async () => {
      const topic = t("order");
      await createTopic(topic, 3);
      await produceTo(topic, 30, 3);

      const consumer = track(newConsumer(t("g-order")));
      await consumer.connect();
      await consumer.subscribe({ topics: [topic] });

      const perPartition = new Map<number, string[]>();
      let overlap = false;
      const active = new Set<number>();
      let total = 0;
      await consumer.run({
        partitionsConsumedConcurrently: 3,
        eachMessage: async ({ partition, message }) => {
          active.add(partition);
          if (active.size > 1) overlap = true;
          await Bun.sleep(1); // force an overlap window between partitions
          const list = perPartition.get(partition) ?? [];
          list.push(message.value!.toString());
          perPartition.set(partition, list);
          active.delete(partition);
          total++;
        },
      });

      await until(() => total === 90, 60_000, "all 90 messages");
      for (let p = 0; p < 3; p++) {
        expect(perPartition.get(p)).toEqual(
          Array.from({ length: 30 }, (_, i) => `v-${p}-${i}`),
        );
      }
      expect(overlap).toBe(true); // concurrency 3: partitions run in parallel
      await consumer.disconnect();
    },
    90_000,
  );

  test(
    "pause() from inside eachMessage + resume: the in-flight message redelivered, nothing lost/duplicated after resume",
    async () => {
      const topic = t("pause");
      await createTopic(topic, 1);
      await produceTo(topic, 10, 1);

      const consumer = track(newConsumer(t("g-pause")));
      await consumer.connect();
      await consumer.subscribe({ topics: [topic] });

      const processed: number[] = [];
      let resumeFn: (() => void) | undefined;
      await consumer.run({
        eachMessage: async ({ message, pause }) => {
          const o = Number(message.offset);
          if (o === 3 && resumeFn === undefined) {
            resumeFn = pause();
            return; // message 3 does NOT count as processed
          }
          processed.push(o);
        },
      });

      await until(() => resumeFn !== undefined, 30_000, "paused at offset 3");
      await Bun.sleep(300); // the pause must hold: no further messages flow
      expect(processed).toEqual([0, 1, 2]);
      expect(consumer.paused()).toEqual([{ topic, partitions: [0] }]);

      resumeFn!();
      await until(() => processed.length === 10, 30_000, "all 10 after resume");
      expect(processed).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]); // 3 redelivered exactly once
      await consumer.disconnect();
    },
    60_000,
  );

  test(
    "a mid-run seek(): rereads from the seeked offset",
    async () => {
      const topic = t("seek");
      await createTopic(topic, 1);
      await produceTo(topic, 8, 1);

      const consumer = track(newConsumer(t("g-seek")));
      await consumer.connect();
      await consumer.subscribe({ topics: [topic] });

      const processed: number[] = [];
      let sought = false;
      await consumer.run({
        eachMessage: async ({ message }) => {
          processed.push(Number(message.offset));
        },
      });

      await until(() => processed.length === 8, 30_000, "the first full read");
      sought = true;
      consumer.seek({ topic, partition: 0, offset: "2" });
      await until(() => processed.length === 8 + 6, 30_000, "the reread from 2");
      expect(processed.slice(8)).toEqual([2, 3, 4, 5, 6, 7]);
      expect(sought).toBe(true);
      await consumer.disconnect();
    },
    60_000,
  );

  test(
    "autoCommit: stored+committed offsets survive a disconnect/reconnect in the same group",
    async () => {
      const topic = t("autocommit");
      const group = t("g-autocommit");
      await createTopic(topic, 1);
      await produceTo(topic, 10, 1);

      const first = track(newConsumer(group));
      await first.connect();
      await first.subscribe({ topics: [topic] });
      let count = 0;
      await first.run({ eachMessage: async () => void count++ });
      await until(() => count === 10, 30_000, "consumer 1 read all 10");
      await first.disconnect(); // autoCommit → stored offsets committed before the break

      await produceTo(topic, 5, 1); // offset 10..14
      const second = track(newConsumer(group));
      await second.connect();
      await second.subscribe({ topics: [topic] });
      const offsets: number[] = [];
      await second.run({ eachMessage: async ({ message }) => void offsets.push(Number(message.offset)) });
      await until(() => offsets.length >= 5, 30_000, "consumer 2 reads the new part");
      expect(offsets).toEqual([10, 11, 12, 13, 14]); // 0..9 NOT reread
      await second.disconnect();
    },
    90_000,
  );

  test(
    "autoCommit=false + a manual commitOffsets()",
    async () => {
      const topic = t("manual");
      const group = t("g-manual");
      await createTopic(topic, 1);
      await produceTo(topic, 10, 1);

      const first = track(newConsumer(group, { kafkaJS: { autoCommit: false } }));
      await first.connect();
      await first.subscribe({ topics: [topic] });
      let count = 0;
      await first.run({ eachMessage: async () => void count++ });
      await until(() => count === 10, 30_000, "all 10 read");
      await first.commitOffsets([{ topic, partition: 0, offset: "7" }]); // manual commit up to 7
      await first.disconnect();

      const second = track(newConsumer(group, { kafkaJS: { autoCommit: false } }));
      await second.connect();
      await second.subscribe({ topics: [topic] });
      const offsets: number[] = [];
      await second.run({ eachMessage: async ({ message }) => void offsets.push(Number(message.offset)) });
      await until(() => offsets.length >= 3, 30_000, "reread from the committed offset");
      expect(offsets.slice(0, 3)).toEqual([7, 8, 9]); // exactly the manual commit position
      await second.disconnect();
    },
    90_000,
  );

  test(
    "a rebalance while run() is live: 2 consumers split partitions, nothing lost/duplicated",
    async () => {
      const topic = t("rebal");
      const group = t("g-rebal");
      await createTopic(topic, 4);

      const seen = new Map<string, number>(); // "partition:offset" → times received
      const record = (partition: number, offset: string): void => {
        const key = `${partition}:${offset}`;
        seen.set(key, (seen.get(key) ?? 0) + 1);
      };

      const a = track(newConsumer(group));
      await a.connect();
      await a.subscribe({ topics: [topic] });
      await a.run({
        partitionsConsumedConcurrently: 2,
        eachMessage: async ({ partition, message }) => record(partition, message.offset),
      });
      await until(() => a.assignment().length === 4, 30_000, "A holds all 4 partitions");

      const b = track(newConsumer(group));
      await b.connect();
      await b.subscribe({ topics: [topic] });
      await b.run({
        partitionsConsumedConcurrently: 2,
        eachMessage: async ({ partition, message }) => record(partition, message.offset),
      });
      await until(
        () => a.assignment().length === 2 && b.assignment().length === 2,
        60_000,
        "split 2/2 after B joins",
      );

      await produceTo(topic, 10, 4); // 40 messages spread over 4 partitions
      await until(() => seen.size === 40, 60_000, "all 40 messages across both consumers");
      expect([...seen.values()].every((n) => n === 1)).toBe(true); // no duplicates

      await b.disconnect();
      await until(() => a.assignment().length === 4, 60_000, "A reclaims all 4");
      await produceTo(topic, 3, 4);
      await until(() => seen.size === 40 + 12, 60_000, "A picks up the new part");
      await a.disconnect();
    },
    180_000,
  );

  test(
    "graceful disconnect: the running handler finishes, resume lands in place, processed messages never repeat",
    async () => {
      const topic = t("graceful");
      const group = t("g-graceful");
      await createTopic(topic, 1);
      await produceTo(topic, 6, 1);

      const first = track(newConsumer(group));
      await first.connect();
      await first.subscribe({ topics: [topic] });
      const processed: number[] = [];
      await first.run({
        eachMessage: async ({ message }) => {
          await Bun.sleep(40); // a slow handler
          processed.push(Number(message.offset));
        },
      });
      await until(() => processed.length >= 2, 30_000, "a few messages processed");
      await first.disconnect(); // graceful: waits for the in-flight handler + commits stored
      const doneCount = processed.length;
      expect(doneCount).toBeGreaterThanOrEqual(2);

      const second = track(newConsumer(group));
      await second.connect();
      await second.subscribe({ topics: [topic] });
      const offsets: number[] = [];
      await second.run({ eachMessage: async ({ message }) => void offsets.push(Number(message.offset)) });
      await until(() => offsets.length === 6 - doneCount, 30_000, "the remainder");
      // Continues exactly from the unprocessed message — processed never redelivered, unprocessed never lost.
      expect(offsets[0]).toBe(doneCount);
      expect(offsets).toEqual(Array.from({ length: 6 - doneCount }, (_, i) => doneCount + i));
      await second.disconnect();
    },
    90_000,
  );
});
