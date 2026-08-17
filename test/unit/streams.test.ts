/**
 * test/unit/streams.test.ts — Stream API (`ProducerStream` / `ConsumerStream`)
 * against in-memory fakes of Producer/KafkaConsumer. Hermetic: no broker, no
 * native library — only the node:stream integration is under test.
 */

import { describe, expect, test } from "bun:test";
import { Buffer } from "node:buffer";
import { EventEmitter, once } from "node:events";
import { Readable, Writable } from "node:stream";
import { setTimeout as sleep } from "node:timers/promises";

import { ProducerStream } from "../../packages/bun-rdkafka/src/callback/producer-stream.ts";
import {
  ConsumerStream,
  KafkaConsumerStream,
  normalizeTopics,
} from "../../packages/bun-rdkafka/src/callback/consumer-stream.ts";
import { ERROR_CODES, LibrdKafkaError } from "../../packages/bun-rdkafka/src/core/errors.ts";
import type { Producer } from "../../packages/bun-rdkafka/src/callback/producer.ts";
import type { KafkaConsumer, Message } from "../../packages/bun-rdkafka/src/callback/kafka-consumer.ts";

/* ========================================================================== */
/* Fakes                                                                       */
/* ========================================================================== */

interface ProducedRecord {
  topic: string;
  partition: unknown;
  value: unknown;
  key: unknown;
  timestamp: unknown;
  opaque: unknown;
  headers: unknown;
}

class FakeProducer extends EventEmitter {
  connected = false;
  connectCalls = 0;
  disconnectCalls = 0;
  flushCalls = 0;
  pollCalls = 0;
  pollInterval: number | undefined;
  produced: ProducedRecord[] = [];
  /** Throw this many ERR__QUEUE_FULL errors before accepting. */
  queueFullBudget = 0;
  /** Throw this error on the next produce(). */
  nextError: Error | null = null;
  /** Fail connect() with this error. */
  connectError: Error | null = null;
  /** Complete connect() only when release() is called. */
  manualReady = false;
  #release: (() => void) | undefined;

  isConnected(): boolean {
    return this.connected;
  }
  connect(_opts: unknown, cb: (err: Error | null, metadata?: unknown) => void): this {
    this.connectCalls++;
    const finish = (): void => {
      if (this.connectError) {
        cb(this.connectError);
        return;
      }
      this.connected = true;
      this.emit("ready", { name: "producer#1" }, { topics: [] });
      cb(null, { topics: [] });
    };
    if (this.manualReady) this.#release = finish;
    else setImmediate(finish);
    return this;
  }
  release(): void {
    this.#release?.();
    this.#release = undefined;
  }
  disconnect(cb?: (err: null) => void): this {
    this.disconnectCalls++;
    this.connected = false;
    setImmediate(() => {
      this.emit("disconnected", {});
      cb?.(null);
    });
    return this;
  }
  flush(_timeout: number, cb: (err: null) => void): this {
    this.flushCalls++;
    setImmediate(() => cb(null));
    return this;
  }
  poll(): this {
    this.pollCalls++;
    return this;
  }
  setPollInterval(ms: number): this {
    this.pollInterval = ms;
    return this;
  }
  produce(
    topic: string,
    partition?: unknown,
    value?: unknown,
    key?: unknown,
    timestamp?: unknown,
    opaque?: unknown,
    headers?: unknown,
  ): boolean {
    if (this.nextError) {
      const err = this.nextError;
      this.nextError = null;
      throw err;
    }
    if (this.queueFullBudget > 0) {
      this.queueFullBudget--;
      throw new LibrdKafkaError("queue full", { code: ERROR_CODES.ERR__QUEUE_FULL });
    }
    this.produced.push({ topic, partition, value, key, timestamp, opaque, headers });
    return true;
  }
}

function msg(i: number, topic = "t"): Message {
  return {
    topic,
    partition: 0,
    offset: i,
    value: Buffer.from(`m${i}`),
    key: null,
    size: 2,
    timestamp: 1000 + i,
  } as Message;
}

class FakeConsumer extends EventEmitter {
  connected = false;
  connectCalls = 0;
  disconnectCalls = 0;
  unsubscribeCalls = 0;
  subscribed: string[] | undefined;
  metadataCalls = 0;
  /** Messages served in order by consume(n). */
  queue: Message[] = [];
  consumeRequests: number[] = [];
  /** Return this error on the next consume(). */
  nextError: Error | null = null;
  connectError: Error | null = null;
  metadata: unknown = { topics: [{ name: "alpha" }, { name: "beta" }] };

  isConnected(): boolean {
    return this.connected;
  }
  connect(_opts: unknown, cb: (err: Error | null, metadata?: unknown) => void): this {
    this.connectCalls++;
    setImmediate(() => {
      if (this.connectError) {
        cb(this.connectError);
        return;
      }
      this.connected = true;
      this.emit("ready", { name: "consumer#1" }, this.metadata);
      cb(null, this.metadata);
    });
    return this;
  }
  getMetadata(_opts: unknown, cb: (err: Error | null, metadata?: unknown) => void): this {
    this.metadataCalls++;
    setImmediate(() => cb(null, this.metadata));
    return this;
  }
  disconnect(cb?: (err: null) => void): this {
    this.disconnectCalls++;
    this.connected = false;
    setImmediate(() => cb?.(null));
    return this;
  }
  subscribe(topics: string[]): this {
    if (!this.connected) throw new Error("subscribe() requires a connected consumer");
    this.subscribed = [...topics];
    this.emit("subscribed", topics);
    return this;
  }
  unsubscribe(): this {
    this.unsubscribeCalls++;
    this.subscribed = undefined;
    this.emit("unsubscribed");
    return this;
  }
  consume(n: number, cb: (err: Error | null, messages: Message[]) => void): void {
    this.consumeRequests.push(n);
    const err = this.nextError;
    this.nextError = null;
    const batch = err ? [] : this.queue.splice(0, n);
    setImmediate(() => cb(err, batch));
  }
}

const asProducer = (p: FakeProducer): Producer => p as unknown as Producer;
const asConsumer = (c: FakeConsumer): KafkaConsumer => c as unknown as KafkaConsumer;

function collect<T>(stream: Readable, count: number): Promise<T[]> {
  return new Promise((resolve, reject) => {
    const out: T[] = [];
    stream.on("data", (chunk: T) => {
      out.push(chunk);
      if (out.length === count) {
        stream.pause();
        resolve(out);
      }
    });
    stream.on("error", reject);
  });
}

/* ========================================================================== */
/* ProducerStream                                                              */
/* ========================================================================== */

describe("ProducerStream", () => {
  test("non-objectMode requires a topic; a string option is the encoding", () => {
    const p = new FakeProducer();
    expect(() => new ProducerStream(asProducer(p))).toThrow(TypeError);
    expect(() => new ProducerStream(asProducer(p), {})).toThrow(/topic/);
    expect(() => new ProducerStream(asProducer(p), 42 as never)).toThrow(TypeError);
    const s = new ProducerStream(asProducer(p), { objectMode: true, autoClose: false });
    expect(s.producer).toBe(asProducer(p));
    expect(s.autoClose).toBe(false);
    s.destroy();
  });

  test("sets the poll interval and auto-connects an unconnected producer", async () => {
    const p = new FakeProducer();
    const s = new ProducerStream(asProducer(p), { topic: "t", pollInterval: 250 });
    expect(p.pollInterval).toBe(250);
    expect(p.connectCalls).toBe(1);
    await once(p, "ready");
    expect(p.connected).toBe(true);
    s.destroy();
  });

  test("does not connect an already connected producer; default pollInterval 1000", () => {
    const p = new FakeProducer();
    p.connected = true;
    const s = new ProducerStream(asProducer(p), { topic: "t" });
    expect(p.connectCalls).toBe(0);
    expect(p.pollInterval).toBe(1000);
    s.destroy();
  });

  test("writes are queued until the producer is ready", async () => {
    const p = new FakeProducer();
    p.manualReady = true;
    const s = new ProducerStream(asProducer(p), { topic: "t" });
    let flushed = false;
    s.write(Buffer.from("a"), () => {
      flushed = true;
    });
    s.write(Buffer.from("b"));
    await sleep(10);
    expect(p.produced).toHaveLength(0);
    expect(flushed).toBe(false);
    p.release();
    await sleep(10);
    expect(flushed).toBe(true);
    expect(p.produced.map((r) => r.topic)).toEqual(["t", "t"]);
    expect(p.produced.map((r) => (r.value as Buffer).toString())).toEqual(["a", "b"]);
    expect(p.produced[0]?.partition).toBeNull();
    s.destroy();
  });

  test("non-objectMode: only buffers are produced; strings go through the encoding", async () => {
    const p = new FakeProducer();
    p.connected = true;
    const s = new ProducerStream(asProducer(p), { topic: "t", encoding: "utf8" });
    s.write("hello");
    s.write(new Uint8Array([1, 2, 3]));
    await sleep(5);
    expect(p.produced).toHaveLength(2);
    expect(Buffer.from(p.produced[0]?.value as Uint8Array).toString()).toBe("hello");
    expect(Buffer.from(p.produced[1]?.value as Uint8Array)).toEqual(Buffer.from([1, 2, 3]));
    s.destroy();
  });

  test("non-objectMode: a non-buffer chunk errors the stream", async () => {
    const p = new FakeProducer();
    p.connected = true;
    const s = new ProducerStream(asProducer(p), { topic: "t" });
    const errored = once(s, "error");
    // Writable itself may throw synchronously for non-buffer chunks (Node/Bun),
    // or defer to _write — either way the stream must surface an error.
    try {
      s.write({ not: "a buffer" } as never);
    } catch (e) {
      expect(e).toBeInstanceOf(Error);
      s.destroy();
      return;
    }
    const [err] = await errored;
    expect(err).toBeInstanceOf(Error);
    expect(p.produced).toHaveLength(0);
  });

  test("objectMode: every message field is routed to produce()", async () => {
    const p = new FakeProducer();
    p.connected = true;
    const s = new ProducerStream(asProducer(p), { objectMode: true });
    const headers = [{ h: "v" }];
    s.write({
      topic: "orders",
      partition: 3,
      value: Buffer.from("v"),
      key: "k",
      timestamp: 123,
      opaque: { id: 1 },
      headers,
    });
    await sleep(5);
    expect(p.produced).toEqual([
      {
        topic: "orders",
        partition: 3,
        value: Buffer.from("v"),
        key: "k",
        timestamp: 123,
        opaque: { id: 1 },
        headers,
      },
    ]);
    s.destroy();
  });

  test("_writev batches many chunks written while corked", async () => {
    const p = new FakeProducer();
    p.connected = true;
    const s = new ProducerStream(asProducer(p), { topic: "t" });
    s.cork();
    for (let i = 0; i < 20; i++) s.write(Buffer.from(String(i)));
    s.uncork();
    await sleep(10);
    expect(p.produced).toHaveLength(20);
    expect((p.produced[19]?.value as Buffer).toString()).toBe("19");
    s.destroy();
  });

  test("ERR__QUEUE_FULL: polls and retries with backoff until accepted", async () => {
    const p = new FakeProducer();
    p.connected = true;
    p.queueFullBudget = 3;
    const s = new ProducerStream(asProducer(p), { topic: "t" });
    const t0 = Date.now();
    let done = false;
    s.write(Buffer.from("x"), () => {
      done = true;
    });
    await sleep(5);
    expect(done).toBe(false);
    while (!done) await sleep(5);
    // 3 retries: 5 + 10 + 20 ms of backoff, bounded well below upstream's 500 ms.
    expect(Date.now() - t0).toBeGreaterThanOrEqual(30);
    expect(Date.now() - t0).toBeLessThan(500);
    expect(p.pollCalls).toBe(3);
    expect(p.produced).toHaveLength(1);
    s.destroy();
  });

  test("other produce errors fail the write and (autoClose) disconnect the producer", async () => {
    const p = new FakeProducer();
    p.connected = true;
    const s = new ProducerStream(asProducer(p), { topic: "t" });
    p.nextError = new LibrdKafkaError("boom", { code: ERROR_CODES.ERR_MSG_SIZE_TOO_LARGE });
    // (events.once(s, "close") would reject on the error — listen by hand.)
    const closed = new Promise<void>((resolve) => s.on("close", resolve));
    const errored = once(s, "error");
    s.write(Buffer.from("x"));
    const [err] = await errored;
    expect((err as LibrdKafkaError).code).toBe(ERROR_CODES.ERR_MSG_SIZE_TOO_LARGE);
    await closed;
    expect(p.disconnectCalls).toBe(1);
  });

  test("finish + autoClose: flushes, disconnects, emits close", async () => {
    const p = new FakeProducer();
    p.connected = true;
    const s = new ProducerStream(asProducer(p), { topic: "t" });
    const closed = once(s, "close");
    s.write(Buffer.from("a"));
    s.end(Buffer.from("b"));
    await closed;
    expect(p.produced).toHaveLength(2);
    expect(p.flushCalls).toBe(1);
    expect(p.disconnectCalls).toBe(1);
    expect(s.destroyed).toBe(true);
  });

  test("autoClose: false keeps the producer connected until close()", async () => {
    const p = new FakeProducer();
    p.connected = true;
    const s = new ProducerStream(asProducer(p), { topic: "t", autoClose: false });
    s.end(Buffer.from("a"));
    await once(s, "finish");
    await sleep(5);
    expect(p.disconnectCalls).toBe(0);
    expect(s.destroyed).toBe(false);
    const closed = new Promise<void>((resolve) => s.close(resolve));
    await closed;
    expect(p.disconnectCalls).toBe(1);
  });

  test("close() while the stream is still connecting disconnects the producer", async () => {
    const p = new FakeProducer();
    p.manualReady = true;
    const s = new ProducerStream(asProducer(p), { topic: "t" });
    const closed = once(s, "close");
    s.close();
    await closed;
    expect(p.disconnectCalls).toBe(1);
  });

  test("a connect failure destroys the stream with the error", async () => {
    const p = new FakeProducer();
    p.connectError = new LibrdKafkaError("no brokers", { code: ERROR_CODES.ERR__TRANSPORT });
    const s = new ProducerStream(asProducer(p), { topic: "t" });
    const [err] = await once(s, "error");
    expect((err as LibrdKafkaError).code).toBe(ERROR_CODES.ERR__TRANSPORT);
    expect(s.destroyed).toBe(true);
  });

  test("Readable.pipe() into the stream produces every chunk", async () => {
    const p = new FakeProducer();
    const s = new ProducerStream(asProducer(p), { topic: "t" });
    const source = Readable.from(["a", "b", "c"].map((x) => Buffer.from(x)));
    const closed = once(s, "close");
    source.pipe(s);
    await closed;
    expect(p.produced.map((r) => (r.value as Buffer).toString())).toEqual(["a", "b", "c"]);
  });
});

/* ========================================================================== */
/* ConsumerStream                                                              */
/* ========================================================================== */

describe("ConsumerStream", () => {
  test("option validation + defaults", () => {
    const c = new FakeConsumer();
    expect(() => new ConsumerStream(asConsumer(c), { topics: 5 as never })).toThrow(TypeError);
    expect(() => new ConsumerStream(asConsumer(c), "x" as never)).toThrow(TypeError);
    const s = new ConsumerStream(asConsumer(c), { topics: "t", fetchSize: 4 });
    expect(s.consumer).toBe(asConsumer(c));
    expect(s.waitInterval).toBe(1000);
    expect(s.fetchSize).toBe(4);
    expect(s.readableObjectMode).toBe(true);
    expect(s.readableHighWaterMark).toBe(6);
    expect(s.autoClose).toBe(true);
    expect(s.streamAsBatch).toBe(false);
    s.destroy();
    const n = new ConsumerStream(asConsumer(new FakeConsumer()), 25);
    expect(n.waitInterval).toBe(25);
    n.destroy();
    expect(KafkaConsumerStream).toBe(ConsumerStream);
  });

  test("normalizeTopics: strings, ^-anchored RegExp, arrays", () => {
    expect(normalizeTopics("a")).toEqual(["a"]);
    expect(normalizeTopics(["a", /^b.*/])).toEqual(["a", "^b.*"]);
    expect(() => normalizeTopics(/b/)).toThrow(/\^/);
    expect(() => normalizeTopics(/^b/i)).toThrow(/flags/);
  });

  test("connects, subscribes, and pushes messages in order (objectMode)", async () => {
    const c = new FakeConsumer();
    c.queue = [msg(0), msg(1), msg(2)];
    const s = new ConsumerStream(asConsumer(c), { topics: ["t"], waitInterval: 5 });
    const got = await collect<Message>(s, 3);
    expect(c.connectCalls).toBe(1);
    expect(c.subscribed).toEqual(["t"]);
    expect(got.map((m) => m.offset)).toEqual([0, 1, 2]);
    expect(got[0]?.value?.toString()).toBe("m0");
    s.destroy();
  });

  test("uses an already connected consumer without reconnecting", async () => {
    const c = new FakeConsumer();
    c.connected = true;
    c.queue = [msg(0)];
    const s = new ConsumerStream(asConsumer(c), { topics: "t", waitInterval: 5 });
    const got = await collect<Message>(s, 1);
    expect(c.connectCalls).toBe(0);
    expect(c.subscribed).toEqual(["t"]);
    expect(got).toHaveLength(1);
    s.destroy();
  });

  test("respects fetchSize: consume(n) is never asked for more", async () => {
    const c = new FakeConsumer();
    c.queue = Array.from({ length: 10 }, (_, i) => msg(i));
    const s = new ConsumerStream(asConsumer(c), { topics: "t", fetchSize: 3, waitInterval: 5 });
    const got = await collect<Message>(s, 10);
    expect(got).toHaveLength(10);
    expect(Math.max(...c.consumeRequests)).toBe(3);
    s.destroy();
  });

  test("streamAsBatch pushes the whole fetched array", async () => {
    const c = new FakeConsumer();
    c.queue = Array.from({ length: 6 }, (_, i) => msg(i));
    const s = new ConsumerStream(asConsumer(c), {
      topics: "t",
      fetchSize: 3,
      streamAsBatch: true,
      waitInterval: 5,
    });
    const batches = await collect<Message[]>(s, 2);
    expect(batches[0]?.map((m) => m.offset)).toEqual([0, 1, 2]);
    expect(batches[1]?.map((m) => m.offset)).toEqual([3, 4, 5]);
    s.destroy();
  });

  test("objectMode: false pushes message values (skipping tombstones)", async () => {
    const c = new FakeConsumer();
    const tomb = { ...msg(9), value: null } as Message;
    c.queue = [msg(0), tomb, msg(1)];
    const s = new ConsumerStream(asConsumer(c), {
      topics: "t",
      objectMode: false,
      fetchSize: 3,
      waitInterval: 5,
    });
    expect(s.readableObjectMode).toBe(false);
    // Byte-mode Readables may coalesce buffered chunks on read(), so assert on
    // the concatenated bytes rather than on chunk boundaries.
    let received = "";
    for await (const chunk of s) {
      received += (chunk as Buffer).toString();
      if (received.length >= "m0m1".length) break;
    }
    expect(received).toBe("m0m1");
    expect(c.disconnectCalls).toBe(1);
  });

  test("topics as a function receives the connect metadata", async () => {
    const c = new FakeConsumer();
    c.queue = [msg(0)];
    let seen: unknown;
    const s = new ConsumerStream(asConsumer(c), {
      topics: (metadata) => {
        seen = metadata;
        return (metadata as { topics: { name: string }[] }).topics.map((t) => t.name);
      },
      waitInterval: 5,
    });
    await collect<Message>(s, 1);
    expect(seen).toBe(c.metadata);
    expect(c.subscribed).toEqual(["alpha", "beta"]);
    s.destroy();
  });

  test("topics function on an already connected consumer fetches metadata", async () => {
    const c = new FakeConsumer();
    c.connected = true;
    c.queue = [msg(0)];
    const s = new ConsumerStream(asConsumer(c), {
      topics: (metadata) => (metadata as { topics: { name: string }[] }).topics[0]?.name ?? "x",
      waitInterval: 5,
    });
    await collect<Message>(s, 1);
    expect(c.metadataCalls).toBe(1);
    expect(c.subscribed).toEqual(["alpha"]);
    s.destroy();
  });

  test("waitInterval: retries after an empty consume; waitInterval 0 retries immediately", async () => {
    const c = new FakeConsumer();
    const s = new ConsumerStream(asConsumer(c), { topics: "t", waitInterval: 20 });
    const p = collect<Message>(s, 1);
    await sleep(15);
    const before = c.consumeRequests.length;
    expect(before).toBeGreaterThanOrEqual(1);
    c.queue.push(msg(0));
    const got = await p;
    expect(got).toHaveLength(1);
    expect(c.consumeRequests.length).toBeGreaterThan(before);
    s.destroy();

    const c0 = new FakeConsumer();
    const s0 = new ConsumerStream(asConsumer(c0), { topics: "t", waitInterval: 0 });
    s0.resume();
    await sleep(15);
    expect(c0.consumeRequests.length).toBeGreaterThan(5);
    s0.destroy();
  });

  test("consume errors are emitted and consumption continues", async () => {
    const c = new FakeConsumer();
    c.nextError = new LibrdKafkaError("transient", { code: ERROR_CODES.ERR__TIMED_OUT });
    c.queue = [msg(0)];
    const s = new ConsumerStream(asConsumer(c), { topics: "t", waitInterval: 5 });
    const errors: unknown[] = [];
    s.on("error", (e) => errors.push(e));
    const got = await new Promise<Message[]>((resolve) => {
      const out: Message[] = [];
      s.on("data", (m: Message) => {
        out.push(m);
        resolve(out);
      });
    });
    expect(errors).toHaveLength(1);
    expect(got).toHaveLength(1);
    s.destroy();
  });

  test("consumer 'unsubscribed' ends the stream; autoClose disconnects", async () => {
    const c = new FakeConsumer();
    const s = new ConsumerStream(asConsumer(c), { topics: "t", waitInterval: 5 });
    s.resume();
    await once(c, "subscribed");
    const ended = once(s, "end");
    const closed = once(s, "close");
    c.unsubscribe();
    await ended;
    await closed;
    expect(s.destroyed).toBe(true);
    // Stream teardown: one unsubscribe by the test + one by _destroy is NOT
    // expected — the consumer was already unsubscribed when close ran, but the
    // fake still counts the call. What matters is a single disconnect.
    expect(c.disconnectCalls).toBe(1);
  });

  test("close(cb) unsubscribes + disconnects, then emits close", async () => {
    const c = new FakeConsumer();
    const s = new ConsumerStream(asConsumer(c), { topics: "t", waitInterval: 5 });
    s.resume();
    await once(c, "subscribed");
    await new Promise<void>((resolve) => s.close(resolve));
    expect(c.unsubscribeCalls).toBe(1);
    expect(c.disconnectCalls).toBe(1);
    expect(s.destroyed).toBe(true);
    // No further consume() after close.
    const n = c.consumeRequests.length;
    await sleep(15);
    expect(c.consumeRequests.length).toBe(n);
  });

  test("destroy() before the connect completes still releases the consumer", async () => {
    const c = new FakeConsumer();
    const s = new ConsumerStream(asConsumer(c), { topics: "t" });
    const closed = once(s, "close");
    s.destroy();
    await closed;
    expect(c.disconnectCalls).toBe(1);
    expect(c.subscribed).toBeUndefined();
  });

  test("a connect failure destroys the stream with the error", async () => {
    const c = new FakeConsumer();
    c.connectError = new LibrdKafkaError("no brokers", { code: ERROR_CODES.ERR__TRANSPORT });
    const s = new ConsumerStream(asConsumer(c), { topics: "t" });
    const [err] = await once(s, "error");
    expect((err as LibrdKafkaError).code).toBe(ERROR_CODES.ERR__TRANSPORT);
    expect(s.destroyed).toBe(true);
  });

  test("async iteration (for await) works and closes the consumer on break", async () => {
    const c = new FakeConsumer();
    c.queue = Array.from({ length: 5 }, (_, i) => msg(i));
    const s = new ConsumerStream(asConsumer(c), { topics: "t", fetchSize: 2, waitInterval: 5 });
    const offsets: number[] = [];
    for await (const m of s) {
      offsets.push((m as Message).offset);
      if (offsets.length === 5) break;
    }
    expect(offsets).toEqual([0, 1, 2, 3, 4]);
    expect(s.destroyed).toBe(true);
    expect(c.disconnectCalls).toBe(1);
  });

  test("backpressure: stops asking the consumer once highWaterMark is reached", async () => {
    const c = new FakeConsumer();
    c.queue = Array.from({ length: 50 }, (_, i) => msg(i));
    const s = new ConsumerStream(asConsumer(c), {
      topics: "t",
      fetchSize: 2,
      highWaterMark: 4,
      waitInterval: 5,
    });
    // Nobody reads: the stream should fill up to ~highWaterMark and stop.
    await sleep(30);
    expect(c.queue.length).toBeGreaterThan(40);
    const sink = new Writable({
      objectMode: true,
      write(_chunk, _enc, cb) {
        setTimeout(cb, 1);
      },
    });
    s.pipe(sink);
    while (c.queue.length > 0) await sleep(5);
    s.destroy();
  });
});
