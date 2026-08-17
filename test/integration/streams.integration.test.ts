/**
 * test/integration/streams.integration.test.ts — the Stream API against a
 * real broker: createWriteStream (buffer + objectMode), createReadStream
 * (objectMode, streamAsBatch, byte mode), a pipe() round trip, and
 * `stream.consumer.commit()`.
 *
 * The docker broker is SHARED with other tests: no stopKafka() in afterAll;
 * every topic/group carries the "streams-" prefix + a per-run id.
 */

import { beforeAll, describe, expect, test } from "bun:test";
import { Buffer } from "node:buffer";
import { once } from "node:events";
import { Readable, Transform, pipeline } from "node:stream";

import { createTopic, integrationAvailable, startKafka } from "./docker-kafka.ts";
import {
  ConsumerStream,
  KafkaConsumer,
  Producer,
  ProducerStream,
  createReadStream,
  createWriteStream,
  type Message,
  type ReadStreamOptions,
} from "../../packages/bun-rdkafka/src/index.ts";
import type { ClientConfig } from "../../packages/bun-rdkafka/src/core/config.ts";

const AVAILABLE = await integrationAvailable();
const RUN_ID = Date.now().toString(36);

let brokers = "localhost:9092";

if (AVAILABLE) {
  beforeAll(async () => {
    ({ brokers } = await startKafka());
  }, 180_000);
}

/* ------------------------------------------------------------------ utils */

function topicName(tag: string): string {
  return `streams-${tag}-${RUN_ID}`;
}

function producerConf(extra: ClientConfig = {}): ClientConfig {
  return { "bootstrap.servers": brokers, "socket.timeout.ms": 10_000, "linger.ms": 5, ...extra };
}

function consumerConf(groupId: string, extra: ClientConfig = {}): ClientConfig {
  return {
    "bootstrap.servers": brokers,
    "group.id": groupId,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": false,
    "fetch.wait.max.ms": 10,
    "fetch.queue.backoff.ms": 10,
    "session.timeout.ms": 6000,
    "heartbeat.interval.ms": 1500,
    ...extra,
  };
}

/** Writes `count` messages through a ProducerStream and waits for its `close`. */
async function writeAll(stream: ProducerStream, chunks: unknown[]): Promise<void> {
  const closed = once(stream, "close");
  const errors: Error[] = [];
  stream.on("error", (e) => errors.push(e));
  for (const chunk of chunks) {
    if (!stream.write(chunk)) await once(stream, "drain");
  }
  stream.end();
  await closed;
  expect(errors).toEqual([]);
}

/** Reads `count` chunks from a ConsumerStream, then closes it. */
async function readN<T>(stream: ConsumerStream, count: number, timeoutMs = 60_000): Promise<T[]> {
  const out: T[] = [];
  const timer = setTimeout(() => stream.destroy(new Error(`timeout after ${out.length}/${count}`)), timeoutMs);
  try {
    for await (const chunk of stream) {
      out.push(chunk as T);
      if (out.length >= count) break;
    }
  } finally {
    clearTimeout(timer);
  }
  return out;
}

function readStream(topic: string, group: string, opts: Partial<ReadStreamOptions> = {}): ConsumerStream {
  return createReadStream(consumerConf(group), {}, { topics: topic, waitInterval: 50, ...opts });
}

/* ==================================================================== tests */

describe.skipIf(!AVAILABLE)("Stream API (integration)", () => {
  test("createWriteStream (buffer mode) → createReadStream (objectMode) round trip", async () => {
    const topic = topicName("buf");
    await createTopic(topic, 1);
    const N = 200;

    const ws = createWriteStream(producerConf(), {}, { topic });
    expect(ws).toBeInstanceOf(ProducerStream);
    expect(ws.producer).toBeInstanceOf(Producer);
    await writeAll(ws, Array.from({ length: N }, (_, i) => Buffer.from(`buf-${i}`)));
    // autoClose flushed + disconnected the producer.
    expect(ws.producer.isConnected()).toBe(false);

    const rs = readStream(topic, `${topic}-g1`, { fetchSize: 16 });
    expect(rs).toBeInstanceOf(ConsumerStream);
    expect(rs.consumer).toBeInstanceOf(KafkaConsumer);
    const messages = await readN<Message>(rs, N);
    expect(messages).toHaveLength(N);
    expect(messages.map((m) => m.value?.toString())).toEqual(
      Array.from({ length: N }, (_, i) => `buf-${i}`),
    );
    expect(messages[0]?.topic).toBe(topic);
    expect(messages.map((m) => m.offset)).toEqual(Array.from({ length: N }, (_, i) => i));
    // Breaking out of `for await` destroyed the stream → consumer released.
    expect(rs.destroyed).toBe(true);
    expect(rs.consumer.isConnected()).toBe(false);
  }, 120_000);

  test("createWriteStream (objectMode) routes partition/key/headers; streamAsBatch reads arrays", async () => {
    const topic = topicName("obj");
    await createTopic(topic, 1);
    const N = 60;

    const ws = Producer.createWriteStream(producerConf(), {}, { objectMode: true });
    await writeAll(
      ws,
      Array.from({ length: N }, (_, i) => ({
        topic,
        partition: 0,
        value: Buffer.from(`obj-${i}`),
        key: `key-${i}`,
        timestamp: 1_700_000_000_000 + i,
        headers: [{ idx: String(i) }],
      })),
    );

    const rs = KafkaConsumer.createReadStream(consumerConf(`${topic}-g1`), {}, {
      topics: [topic],
      waitInterval: 50,
      fetchSize: 8,
      streamAsBatch: true,
    });
    const batches: Message[][] = [];
    let total = 0;
    const timer = setTimeout(() => rs.destroy(new Error("timeout")), 60_000);
    for await (const batch of rs) {
      batches.push(batch as Message[]);
      total += (batch as Message[]).length;
      if (total >= N) break;
    }
    clearTimeout(timer);
    expect(total).toBe(N);
    expect(batches.every((b) => Array.isArray(b) && b.length >= 1 && b.length <= 8)).toBe(true);
    const all = batches.flat().sort((a, b) => Number(a.key) - Number(b.key) || 0);
    const byIdx = new Map(all.map((m) => [Number(m.headers?.[0]?.["idx"]?.toString()), m]));
    for (let i = 0; i < N; i++) {
      const m = byIdx.get(i);
      expect(m).toBeDefined();
      expect(m?.partition).toBe(0);
      expect(m?.key?.toString()).toBe(`key-${i}`);
      expect(m?.value?.toString()).toBe(`obj-${i}`);
      expect(m?.timestamp).toBe(1_700_000_000_000 + i);
    }
  }, 120_000);

  test("byte-mode read stream pushes values; pipe() read → transform → write round trip", async () => {
    const src = topicName("pipe-src");
    const dst = topicName("pipe-dst");
    await createTopic(src, 1);
    await createTopic(dst, 1);
    const N = 100;

    await writeAll(
      createWriteStream(producerConf(), {}, { topic: src }),
      Array.from({ length: N }, (_, i) => Buffer.from(`p-${i}`)),
    );

    // Byte-mode consumer stream: chunks are the raw values.
    const rs = readStream(src, `${src}-g1`, { objectMode: false, fetchSize: 10 });
    expect(rs.readableObjectMode).toBe(false);
    const upper = new Transform({
      transform(chunk: Buffer, _enc, cb) {
        cb(null, Buffer.from(chunk.toString().toUpperCase()));
      },
    });
    const ws = createWriteStream(producerConf(), {}, { topic: dst });

    // A consumer stream only ends when the consumer unsubscribes (upstream
    // semantics): do that once every message went through the transform.
    let piped = 0;
    upper.on("data", () => {
      piped++;
      if (piped === N) rs.consumer.unsubscribe();
    });
    await new Promise<void>((resolve, reject) => {
      pipeline(rs, upper, ws, (err) => (err ? reject(err) : resolve()));
    });
    expect(ws.producer.isConnected()).toBe(false);

    const check = readStream(dst, `${dst}-g1`, { fetchSize: 20 });
    const got = await readN<Message>(check, N);
    expect(got.map((m) => m.value?.toString())).toEqual(
      Array.from({ length: N }, (_, i) => `P-${i}`),
    );
  }, 120_000);

  test("Readable.from(...).pipe(writeStream) and stream.consumer.commit() persist offsets", async () => {
    const topic = topicName("commit");
    await createTopic(topic, 1);
    const N = 30;
    const group = `${topic}-g1`;

    const ws = createWriteStream(producerConf(), {}, { topic });
    const closed = once(ws, "close");
    Readable.from(Array.from({ length: N }, (_, i) => Buffer.from(`c-${i}`))).pipe(ws);
    await closed;

    // Read the first 10, commit through the exposed consumer, close.
    const rs1 = readStream(topic, group, { fetchSize: 5 });
    const first = await new Promise<Message[]>((resolve, reject) => {
      const out: Message[] = [];
      rs1.on("error", reject);
      rs1.on("data", (m: Message) => {
        out.push(m);
        if (out.length === 10) {
          rs1.pause();
          resolve(out);
        }
      });
    });
    expect(first.map((m) => m.offset)).toEqual(Array.from({ length: 10 }, (_, i) => i));
    const last = first[9] as Message;
    rs1.consumer.commitSync({ topic, partition: 0, offset: last.offset + 1 });
    await new Promise<void>((resolve) => rs1.close(resolve));
    expect(rs1.consumer.isConnected()).toBe(false);

    // A new stream in the same group resumes after the committed offset.
    const rs2 = readStream(topic, group, { fetchSize: 5 });
    const rest = await readN<Message>(rs2, N - 10);
    expect(rest.map((m) => m.offset)).toEqual(Array.from({ length: N - 10 }, (_, i) => 10 + i));
    expect(rest.map((m) => m.value?.toString())).toEqual(
      Array.from({ length: N - 10 }, (_, i) => `c-${10 + i}`),
    );
  }, 120_000);

  test("topics as a function receives the broker metadata", async () => {
    const topic = topicName("fn");
    await createTopic(topic, 1);
    await writeAll(createWriteStream(producerConf(), {}, { topic }), [Buffer.from("one")]);

    let seenTopics: string[] = [];
    const rs = createReadStream(consumerConf(`${topic}-g1`), {}, {
      topics: (metadata) => {
        seenTopics = (metadata?.topics ?? []).map((t) => t.name);
        return seenTopics.filter((t) => t === topic);
      },
      waitInterval: 50,
    });
    const got = await readN<Message>(rs, 1);
    expect(seenTopics).toContain(topic);
    expect(got[0]?.value?.toString()).toBe("one");
  }, 120_000);
});
