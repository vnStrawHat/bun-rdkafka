/**
 * Integration tests for the KafkaJS API gaps closed after the audit — a real
 * broker via docker-kafka:
 *  - Producer.sendOffsets() with a KafkaJS Consumer (the INTRODUCTION.md EOS flow)
 *  - Consumer.committed(), storeOffsets(), logger(), dependentAdmin()
 *  - rebalance_cb with assignmentFns.assign(modified offsets)
 *  - pause() returning a resume function
 *
 * The broker container is SHARED between agents/suites: no stopKafka() in
 * afterAll. Topics/groups all use the "kjsgap-" prefix.
 */

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { createTopic, integrationAvailable, startKafka } from "./docker-kafka.ts";
import { KafkaJS } from "../../packages/bun-rdkafka/src/index.ts";
import type { Consumer } from "../../packages/bun-rdkafka/src/kafkajs/consumer.ts";
import type { Logger } from "../../packages/bun-rdkafka/src/kafkajs/config-mapper.ts";
import { ERROR_CODES, LibrdKafkaError } from "../../packages/bun-rdkafka/src/core/errors.ts";

const AVAILABLE = await integrationAvailable();
let brokers = "localhost:9092";

const RUN = Date.now().toString(36);
const t = (name: string): string => `kjsgap-${name}-${RUN}`;

function kafka(): InstanceType<typeof KafkaJS.Kafka> {
  return new KafkaJS.Kafka({
    kafkaJS: { brokers: [brokers], clientId: `kjsgap-${RUN}` },
    "socket.timeout.ms": 10_000,
  });
}

function newConsumer(groupId: string, extra: Record<string, unknown> = {}): Consumer {
  const kafkaJS = {
    groupId,
    fromBeginning: true,
    ...(extra["kafkaJS"] as Record<string, unknown> | undefined),
  };
  const flat = { ...extra };
  delete flat["kafkaJS"];
  return kafka().consumer({ kafkaJS, "fetch.queue.backoff.ms": 10, ...flat });
}

async function produce(topic: string, values: string[], partition = 0): Promise<void> {
  const producer = kafka().producer();
  await producer.connect();
  try {
    await producer.send({ topic, messages: values.map((value) => ({ value, partition })) });
  } finally {
    await producer.disconnect();
  }
}

async function until(cond: () => boolean, timeoutMs: number, what: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for: ${what}`);
    await Bun.sleep(25);
  }
}

class RecordingLogger implements Logger {
  level = 4;
  lines: { level: string; message: string; extra: Record<string, unknown> }[] = [];
  setLogLevel(level: number): void {
    this.level = level;
  }
  info(message: string, extra?: object): void {
    this.lines.push({ level: "info", message, extra: (extra ?? {}) as Record<string, unknown> });
  }
  error(message: string, extra?: object): void {
    this.lines.push({ level: "error", message, extra: (extra ?? {}) as Record<string, unknown> });
  }
  warn(message: string, extra?: object): void {
    this.lines.push({ level: "warn", message, extra: (extra ?? {}) as Record<string, unknown> });
  }
  debug(message: string, extra?: object): void {
    this.lines.push({ level: "debug", message, extra: (extra ?? {}) as Record<string, unknown> });
  }
}

const consumers: Consumer[] = [];
function track(c: Consumer): Consumer {
  consumers.push(c);
  return c;
}

beforeAll(async () => {
  if (!AVAILABLE) return;
  ({ brokers } = await startKafka());
}, 180_000);

afterAll(async () => {
  await Promise.allSettled(consumers.map((c) => c.disconnect()));
});

describe.skipIf(!AVAILABLE)("KafkaJS API gaps (real broker)", () => {
  test(
    "EOS: consume → transaction → send → sendOffsets(consumer) → commit; the offset is committed for the group",
    async () => {
      const input = t("eos-in");
      const output = t("eos-out");
      await createTopic(input, 1);
      await createTopic(output, 1);
      await produce(input, ["a", "b", "c", "d", "e"]);

      const groupId = t("g-eos");
      const consumer = track(newConsumer(groupId, { kafkaJS: { autoCommit: false } }));
      await consumer.connect();
      await consumer.subscribe({ topics: [input] });
      const seen: { offset: number; value: string }[] = [];
      await consumer.run({
        eachMessage: async ({ message }) => {
          seen.push({ offset: Number(message.offset), value: message.value!.toString() });
        },
      });
      await until(() => seen.length === 5, 30_000, "5 input messages");
      // Nothing committed yet (autoCommit off, no manual commit): -1001 = RD_KAFKA_OFFSET_INVALID.
      const before = await consumer.committed([{ topic: input, partition: 0 }], 10_000);
      expect(before).toEqual([{ topic: input, partition: 0, offset: "-1001", leaderEpoch: null, metadata: null }]);

      const producer = kafka().producer({ kafkaJS: { transactionalId: t("txn-eos") } });
      await producer.connect();
      try {
        const txn = await producer.transaction();
        const sent = txn.send({ topic: output, messages: [{ value: seen.map((m) => m.value).join("") }] });
        await txn.sendOffsets({
          consumer,
          topics: [{ topic: input, partitions: [{ partition: 0, offset: String(seen.length) }] }],
        });
        await txn.commit();
        await sent;
      } finally {
        await producer.disconnect();
      }

      // Verified through the consumer that took part...
      const after = await consumer.committed([{ topic: input, partition: 0 }], 10_000);
      expect(after[0]).toMatchObject({ topic: input, partition: 0, offset: "5" });
      await consumer.disconnect();

      // ...and through a fresh consumer of the same group.
      const fresh = track(newConsumer(groupId, { kafkaJS: { autoCommit: false } }));
      await fresh.connect();
      const freshView = await fresh.committed([{ topic: input, partition: 0 }], 10_000);
      expect(freshView[0]).toMatchObject({ topic: input, partition: 0, offset: "5" });
      await fresh.disconnect();

      // The transactional output is visible to a read_committed reader.
      const reader = track(newConsumer(t("g-eos-read"), { "isolation.level": "read_committed" }));
      await reader.connect();
      await reader.subscribe({ topics: [output] });
      const outValues: string[] = [];
      await reader.run({ eachMessage: async ({ message }) => void outValues.push(message.value!.toString()) });
      await until(() => outValues.length === 1, 30_000, "the transactional output message");
      expect(outValues).toEqual(["abcde"]);
      await reader.disconnect();
    },
    120_000,
  );

  test(
    "committed() defaults to the current assignment; storeOffsets() + commitOffsets() commits the user's offset",
    async () => {
      const topic = t("store");
      await createTopic(topic, 1);
      await produce(topic, ["0", "1", "2"]);

      const consumer = track(newConsumer(t("g-store"), { kafkaJS: { autoCommit: false } }));
      await consumer.connect();
      await consumer.subscribe({ topics: [topic] });
      let processed = 0;
      await consumer.run({ eachMessage: async () => void processed++ });
      await until(() => processed === 3, 30_000, "3 messages");
      await until(() => consumer.assignment().length === 1, 10_000, "assignment");

      // The scheduler stored 3 (all processed); the user overrides with 1 and commits the stores.
      consumer.storeOffsets([{ topic, partition: 0, offset: "1", metadata: "user-store" }]);
      await consumer.commitOffsets();
      const committed = await consumer.committed(undefined, 10_000); // default: the assignment
      expect(committed).toEqual([{ topic, partition: 0, offset: "1", leaderEpoch: null, metadata: "user-store" }]);

      // Storing for a partition this consumer does not own → ERR__STATE.
      expect(() => consumer.storeOffsets([{ topic, partition: 7, offset: "1" }])).toThrow(
        expect.objectContaining({ code: ERROR_CODES.ERR__STATE }),
      );
      await consumer.disconnect();
    },
    60_000,
  );

  test(
    "logger(): a custom kafkaJS.logger receives internal + librdkafka lines; debug config → DEBUG level",
    async () => {
      const topic = t("logger");
      await createTopic(topic, 1);
      const logger = new RecordingLogger();
      const consumer = track(newConsumer(t("g-logger"), { kafkaJS: { logger }, debug: "cgrp" }));
      expect(consumer.logger()).toBe(logger);
      expect(logger.level).toBe(KafkaJS.logLevel.DEBUG);
      await consumer.connect();
      await consumer.subscribe({ topics: [topic] });
      await consumer.run({ eachMessage: async () => {} });
      await until(() => consumer.assignment().length === 1, 30_000, "assignment");
      await until(
        () => logger.lines.some((l) => l.level === "info" && l.message.includes("Received rebalance event")),
        10_000,
        "the rebalance info line",
      );
      // librdkafka's own debug output (fac "CGRP…") reaches the same logger through event.log.
      await until(
        () => logger.lines.some((l) => l.level === "debug" && typeof l.extra["fac"] === "string" && l.extra["fac"] !== "BINDING"),
        10_000,
        "a librdkafka debug line",
      );
      await consumer.disconnect();
    },
    60_000,
  );

  test(
    "dependentAdmin(): rides the consumer's connection — listTopics() sees the topic",
    async () => {
      const topic = t("depadmin");
      await createTopic(topic, 1);
      const consumer = track(newConsumer(t("g-depadmin")));
      const admin = consumer.dependentAdmin();
      await expect(admin.connect()).rejects.toMatchObject({ code: ERROR_CODES.ERR__STATE }); // consumer not connected yet
      await consumer.connect();
      const admin2 = consumer.dependentAdmin();
      await admin2.connect();
      expect(admin2.logger()).toBe(consumer.logger());
      const topics = await admin2.listTopics({ timeout: 30_000 });
      expect(topics).toContain(topic);
      await admin2.disconnect();
      await consumer.disconnect();
    },
    60_000,
  );

  test(
    "rebalance_cb + assignmentFns.assign(modified offsets): consumption starts from the modified offset",
    async () => {
      const topic = t("rebalance-fns");
      await createTopic(topic, 1);
      await produce(topic, Array.from({ length: 10 }, (_, i) => `m${i}`));

      const events: { code: number; lost: boolean; count: number }[] = [];
      const consumer = track(
        newConsumer(t("g-rebalance-fns"), {
          rebalance_cb: (
            err: LibrdKafkaError,
            assignment: { topic: string; partition: number; offset?: number }[],
            fns: { assign: (a: unknown[]) => void; assignmentLost: () => boolean },
          ) => {
            events.push({ code: err.code, lost: fns.assignmentLost(), count: assignment.length });
            if (err.code === ERROR_CODES.ERR__ASSIGN_PARTITIONS) {
              for (const tp of assignment) tp.offset = 7;
              fns.assign(assignment);
            }
          },
        }),
      );
      await consumer.connect();
      await consumer.subscribe({ topics: [topic] });
      const offsets: number[] = [];
      await consumer.run({ eachMessage: async ({ message }) => void offsets.push(Number(message.offset)) });
      await until(() => offsets.length === 3, 30_000, "offsets 7..9");
      await Bun.sleep(300);
      expect(offsets).toEqual([7, 8, 9]);
      expect(events[0]).toEqual({ code: ERROR_CODES.ERR__ASSIGN_PARTITIONS, lost: false, count: 1 });
      // Note: the final revoke of disconnect() is answered inside the native
      // close (shim) and does not reach the JS rebalance_cb — unlike upstream.
      await consumer.disconnect();
    },
    60_000,
  );

  test(
    "pause() returns a function that resumes exactly the paused partitions",
    async () => {
      const topic = t("pause-fn");
      await createTopic(topic, 1);
      await produce(topic, ["a", "b"]);
      const consumer = track(newConsumer(t("g-pause-fn")));
      await consumer.connect();
      await consumer.subscribe({ topics: [topic] });
      const values: string[] = [];
      await consumer.run({ eachMessage: async ({ message }) => void values.push(message.value!.toString()) });
      await until(() => values.length === 2, 30_000, "the first 2 messages");

      const resume = consumer.pause([{ topic }]);
      expect(typeof resume).toBe("function");
      expect(consumer.paused()).toEqual([{ topic, partitions: [0] }]);
      await produce(topic, ["c", "d"]);
      await Bun.sleep(500);
      expect(values).toEqual(["a", "b"]); // paused: nothing flows

      resume();
      expect(consumer.paused()).toEqual([]);
      await until(() => values.length === 4, 30_000, "the messages after resume");
      expect(values).toEqual(["a", "b", "c", "d"]);
      await consumer.disconnect();
    },
    60_000,
  );
});
