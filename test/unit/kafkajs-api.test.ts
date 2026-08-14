/**
 * M5a — Kafka / Producer / Admin (promisified): broker-free validation —
 * state errors, banned per-send keys, sendOffsets validation, KafkaJS headers
 * → Callback API shape, kafkaJS keys misplaced outside the block.
 */

import { describe, expect, test } from "bun:test";
import { ERROR_CODES } from "../../packages/bun-rdkafka/src/core/errors.ts";
import { Kafka } from "../../packages/bun-rdkafka/src/kafkajs/kafka.ts";
import { KafkaJSError, KafkaJSNotImplemented } from "../../packages/bun-rdkafka/src/kafkajs/errors.ts";
import {
  Producer,
  convertToRdKafkaHeaders,
} from "../../packages/bun-rdkafka/src/kafkajs/producer.ts";
import { Admin } from "../../packages/bun-rdkafka/src/kafkajs/admin.ts";

async function expectCode(promise: Promise<unknown>, code: number): Promise<void> {
  try {
    await promise;
    throw new Error("expected a reject but it resolved");
  } catch (e) {
    expect(e).toBeInstanceOf(KafkaJSError);
    expect((e as KafkaJSError).code).toBe(code);
  }
}

describe("Kafka", () => {
  test("a kafkaJS key outside the block → a throw with guidance", () => {
    expect(() => new Kafka({ brokers: ["x:9092"] } as never)).toThrow(/kafkaJS block/);
    const kafka = new Kafka({ kafkaJS: { brokers: ["x:9092"] } });
    expect(() => kafka.producer({ idempotent: true } as never)).toThrow(/kafkaJS block/);
    expect(() => kafka.consumer({ groupId: "g" } as never)).toThrow(/kafkaJS block/);
    // 'acks' shares a librdkafka property name → allowed outside the block
    expect(() => kafka.producer({ acks: 1 } as never)).not.toThrow();
  });

  test("factories return the right instance kinds", () => {
    const kafka = new Kafka({ kafkaJS: { brokers: ["x:9092"] } });
    expect(kafka.producer()).toBeInstanceOf(Producer);
    expect(kafka.admin()).toBeInstanceOf(Admin);
  });
});

describe("Producer (not connected)", () => {
  const producer = new Kafka({ kafkaJS: { brokers: ["x:9092"] } }).producer();

  test("send/sendBatch/flush/transaction before connect → ERR__STATE", async () => {
    await expectCode(producer.send({ topic: "t", messages: [] }), ERROR_CODES.ERR__STATE);
    await expectCode(producer.sendBatch({ topicMessages: [] }), ERROR_CODES.ERR__STATE);
    await expectCode(producer.flush(), ERROR_CODES.ERR__STATE);
    await expectCode(producer.transaction(), ERROR_CODES.ERR__STATE);
  });

  test("sendOffsets: consumerGroupId, or a missing consumer → ERR__INVALID_ARG (even before state)", async () => {
    await expectCode(
      producer.sendOffsets({ consumerGroupId: "g", topics: [], consumer: {} as never }),
      ERROR_CODES.ERR__INVALID_ARG,
    );
    await expectCode(
      producer.sendOffsets({ topics: [{ topic: "t", partitions: [] }] } as never),
      ERROR_CODES.ERR__INVALID_ARG,
    );
  });

  test("setSaslCredentials before connect: edits the config, no throw", () => {
    const p = new Kafka({}).producer({
      kafkaJS: { sasl: { mechanism: "PLAIN", username: "old", password: "old" } },
    });
    expect(() => p.setSaslCredentials({ username: "u2", password: "p2" })).not.toThrow();
    expect(() => p.setSaslCredentials({ username: "u2" } as never)).toThrow(/password/);
  });

  test("config errors surface in connect() (not the constructor)", async () => {
    const p = new Kafka({}).producer({ kafkaJS: { retry: { factor: 0.5 } } });
    await expectCode(p.connect(), ERROR_CODES.ERR__INVALID_ARG);
  });
});

describe("convertToRdKafkaHeaders", () => {
  test("a map → one-key objects; array values split per element; undefined → null", () => {
    expect(
      convertToRdKafkaHeaders({
        h1: ["a", "b"],
        h2: "c",
        h3: Buffer.from("d"),
        h4: undefined,
      }),
    ).toEqual([
      { h1: "a" },
      { h1: "b" },
      { h2: "c" },
      { h3: Buffer.from("d") },
      { h4: null },
    ]);
    expect(convertToRdKafkaHeaders(undefined)).toBeNull();
  });
});

describe("Admin (not connected)", () => {
  const admin = new Kafka({ kafkaJS: { brokers: ["x:9092"] } }).admin();

  test("methods before connect → ERR__STATE", async () => {
    await expectCode(admin.createTopics({ topics: [] }), ERROR_CODES.ERR__STATE);
    await expectCode(admin.listTopics(), ERROR_CODES.ERR__STATE);
    await expectCode(admin.listGroups(), ERROR_CODES.ERR__STATE);
    await expectCode(admin.fetchOffsets({ groupId: "g" }), ERROR_CODES.ERR__STATE);
    await expectCode(admin.fetchTopicOffsets("t"), ERROR_CODES.ERR__STATE);
  });

  test("fetchTopicOffsetsByTimestamp before connect → ERR__STATE (implemented since M6)", async () => {
    // Before M6 this method was KafkaJSNotImplemented; it now uses admin
    // ListOffsets and behaves like any other method: not connected → ERR__STATE.
    await expectCode(admin.fetchTopicOffsetsByTimestamp("t", 123), ERROR_CODES.ERR__STATE);
  });

  test("dependent admin: connecting while the host client is unconnected → ERR__STATE", async () => {
    const p = new Kafka({}).producer();
    await expectCode(p.dependentAdmin().connect(), ERROR_CODES.ERR__STATE);
  });
});
