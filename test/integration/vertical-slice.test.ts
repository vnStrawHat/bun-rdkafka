/**
 * test/integration/vertical-slice.test.ts — M1's DoD (docs/03-implementation-plan.md).
 *
 * Proves on a **real Kafka broker** that every architectural assumption holds:
 *
 *  a. produce → consume roundtrip: key/value/headers/timestamp intact
 *     (per-message checksums), all delivery reports through the
 *     `DeliveryLedger`, offsets increasing.
 *  b. consumer group: `subscribe` → REBALANCE (assign) event → JS answers via
 *     `brk_assign` → messages arrive → `unsubscribe` (REVOKE) → clean destroy.
 *  c. NFR-2: while producing+consuming 100k messages, a `setInterval(10ms)`
 *     still ticks with an average drift < 5ms ⇒ the poll model never blocks
 *     the event loop.
 *  d. clean destroy: 2 consecutive rounds in one process without hangs/leaks.
 *
 * Self-skips when the shim is unbuilt or docker is absent (see `docker-kafka.ts`).
 */

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import {
  createTopic,
  integrationAvailable,
  nativeLibPath,
  startKafka,
  stopKafka,
} from "./docker-kafka.ts";
import { SliceConsumer, SliceProducer, waitFor } from "./slice-harness.ts";
import type {
  DecodedMessage,
  SliceProduceInput,
} from "./slice-harness.ts";
import { ERROR_CODES } from "../../packages/bun-rdkafka/src/core/errors.ts";

/* ========================================================================== */
/* Utilities                                                                   */
/* ========================================================================== */

const AVAILABLE = await integrationAvailable();
const RUN_ID = `${Date.now().toString(36)}`;
const DECODER = new TextDecoder();
const ENCODER = new TextEncoder();

let brokers = "localhost:9092";

/** FNV-1a 32-bit — a cheap checksum proving payloads travel without a single wrong byte. */
function fnv1a(bytes: Uint8Array): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < bytes.length; i++) {
    hash ^= bytes[i] as number;
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash >>> 0;
}

/** A 100-byte payload whose content depends on the index ⇒ catches shifts/dups/losses. */
function makeValue(index: number): Uint8Array {
  const head = `msg-${index}-`;
  const out = new Uint8Array(100);
  out.set(ENCODER.encode(head));
  for (let i = head.length; i < out.length; i++) {
    out[i] = (index * 31 + i * 17) & 0xff;
  }
  return out;
}

/** The index embedded in the payload (`msg-<i>-…`). */
function indexOfValue(value: Uint8Array): number {
  const text = DECODER.decode(value.subarray(0, 20));
  const match = /^msg-(\d+)-/.exec(text);
  if (!match) throw new Error(`malformed payload: ${JSON.stringify(text)}`);
  return Number(match[1]);
}

/** The process's thread count — used to catch native handle leaks. */
function threadCount(): number {
  if (process.platform !== "linux") return -1;
  const status = readFileSync("/proc/self/status", "utf8");
  return Number(/Threads:\s*(\d+)/.exec(status)?.[1] ?? -1);
}

const BASE_TS = 1_700_000_000_000;

function producerConfig(extra: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    "bootstrap.servers": brokers,
    "client.id": `vs-producer-${RUN_ID}`,
    "linger.ms": 5,
    acks: "1",
    "message.timeout.ms": 60_000,
    ...extra,
  };
}

function consumerConfig(
  groupId: string,
  extra: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    "bootstrap.servers": brokers,
    "group.id": groupId,
    "client.id": `vs-consumer-${RUN_ID}`,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": false,
    "enable.partition.eof": false,
    "fetch.wait.max.ms": 10,
    "session.timeout.ms": 10_000,
    ...extra,
  };
}

/* ========================================================================== */

describe.skipIf(!AVAILABLE)("M1 vertical slice — real broker", () => {
  beforeAll(async () => {
    brokers = (await startKafka()).brokers;
  }, 600_000);

  afterAll(async () => {
    await stopKafka();
  }, 180_000);

  test("the native library resolves from the local build", () => {
    expect(nativeLibPath()).toContain("libbunrdkafka");
  });

  /* ---------------------------------------------------------------- (a) */

  test(
    "a. a 1000-message roundtrip: content, delivery reports, increasing offsets",
    async () => {
      const topic = `vs-roundtrip-${RUN_ID}`;
      const total = 1000;
      await createTopic(topic, 1);

      // ---- produce -------------------------------------------------------
      const checksums = new Map<number, number>();
      const drOffsets: number[] = [];
      const drTags: number[] = [];

      const producer = new SliceProducer({
        config: producerConfig(),
        label: "roundtrip-producer",
        onDelivery: (result) => {
          drOffsets.push(result.offset);
          drTags.push(result.record);
        },
      });
      producer.connect();

      const records: SliceProduceInput[] = [];
      for (let i = 0; i < total; i++) {
        const value = makeValue(i);
        checksums.set(i, fnv1a(value));
        records.push({
          topic,
          partition: 0,
          timestamp: BASE_TS + i,
          key: `key-${i}`,
          value,
          headers: [
            { key: "seq", value: String(i) },
            { key: "null-header", value: null },
          ],
        });
      }

      const queued = producer.produce(records, [...checksums.keys()]);
      expect(queued).toBe(total);

      await producer.waitIdle(120_000);
      producer.throwPollErrors();
      expect(producer.delivered).toBe(total);
      expect(producer.deliveryFailures).toBe(0);
      expect(producer.rejected).toBe(0);
      expect(producer.ledger.pending).toBe(0);
      // The ledger must match DRs to the exact records it tracked.
      expect([...drTags].sort((x, y) => x - y)).toEqual([...checksums.keys()]);
      // One partition, sequential produce ⇒ contiguous offsets 0..999.
      expect([...drOffsets].sort((x, y) => x - y)).toEqual(
        Array.from({ length: total }, (_, i) => i),
      );

      producer.disconnect();
      expect(producer.client.state).toBe("CLOSED");

      // ---- consume -------------------------------------------------------
      const seen: DecodedMessage[] = [];
      const consumer = new SliceConsumer({
        config: consumerConfig(`vs-roundtrip-grp-${RUN_ID}`),
        label: "roundtrip-consumer",
        onMessage: (message) => {
          seen.push(message);
        },
      });
      consumer.connect();
      consumer.subscribe([topic]);
      await consumer.waitForMessages(total, 120_000);
      consumer.throwPollErrors();

      expect(seen.length).toBe(total);

      let lastOffset = -1;
      for (const message of seen) {
        expect(message.topic).toBe(topic);
        expect(message.partition).toBe(0);
        expect(message.err).toBe(0);
        expect(message.offset).toBeGreaterThan(lastOffset); // offsets increase
        lastOffset = message.offset;

        const index = indexOfValue(message.value as Uint8Array);
        expect(message.offset).toBe(index); // 1 partition ⇒ offset == index
        expect(fnv1a(message.value as Uint8Array)).toBe(checksums.get(index) as number);
        expect((message.value as Uint8Array).length).toBe(100);
        expect(DECODER.decode(message.key as Uint8Array)).toBe(`key-${index}`);
        expect(message.timestamp).toBe(BASE_TS + index);
        expect(message.timestampType).toBe(1); // CreateTime

        expect(message.headers.length).toBe(2);
        expect(message.headers[0]?.key).toBe("seq");
        expect(DECODER.decode(message.headers[0]?.value as Uint8Array)).toBe(String(index));
        expect(message.headers[1]?.key).toBe("null-header");
        expect(message.headers[1]?.value).toBeNull();
      }

      consumer.disconnect();
      expect(consumer.client.state).toBe("CLOSED");
    },
    180_000,
  );

  /* ---------------------------------------------------------------- (b) */

  test(
    "b. consumer group: REBALANCE → JS assign → messages → clean unsubscribe",
    async () => {
      const topic = `vs-group-${RUN_ID}`;
      const partitions = 2;
      const perPartition = 25;
      await createTopic(topic, partitions);

      const producer = new SliceProducer({
        config: producerConfig(),
        label: "group-producer",
      });
      producer.connect();
      const records: SliceProduceInput[] = [];
      for (let p = 0; p < partitions; p++) {
        for (let i = 0; i < perPartition; i++) {
          records.push({ topic, partition: p, value: makeValue(p * 1000 + i) });
        }
      }
      expect(producer.produce(records)).toBe(records.length);
      await producer.waitIdle(120_000);
      expect(producer.delivered).toBe(records.length);
      producer.disconnect();

      // A "user"-provided rebalance handler: the shim never assigns, JS must answer.
      const rebalanceCodes: number[] = [];
      const consumer = new SliceConsumer({
        config: consumerConfig(`vs-group-grp-${RUN_ID}`),
        label: "group-consumer",
        onRebalance: (event, self) => {
          rebalanceCodes.push(event.code);
          self.applyRebalance(event); // → brk_assign
        },
      });
      consumer.connect();
      expect(consumer.client.subscription()).toEqual([]);

      consumer.subscribe([topic]);
      expect(consumer.client.subscription()).toEqual([topic]);

      await consumer.waitForAssignment(60_000);
      consumer.throwPollErrors();

      expect(rebalanceCodes[0]).toBe(ERROR_CODES.ERR__ASSIGN_PARTITIONS);
      expect(consumer.assignReplies).toBeGreaterThanOrEqual(1);
      expect(consumer.assigned.length).toBe(partitions);
      expect(consumer.assigned.map((p) => p.partition).sort()).toEqual([0, 1]);
      // the real native-side assignment matches what JS assigned
      expect(consumer.client.assignment().map((p) => p.partition).sort()).toEqual([0, 1]);
      expect(consumer.client.assignment().every((p) => p.topic === topic)).toBe(true);

      await consumer.waitForMessages(partitions * perPartition, 120_000);
      expect(consumer.consumed).toBe(partitions * perPartition);
      const byPartition = new Map<number, number>();
      for (const message of consumer.messages) {
        byPartition.set(message.partition, (byPartition.get(message.partition) ?? 0) + 1);
      }
      expect([...byPartition.entries()].sort()).toEqual([
        [0, perPartition],
        [1, perPartition],
      ]);

      // ---- unsubscribe ⇒ REVOKE reaches JS, the assignment empties ---------
      consumer.unsubscribe();
      await waitFor(
        () => rebalanceCodes.includes(ERROR_CODES.ERR__REVOKE_PARTITIONS),
        60_000,
        () => `no REVOKE yet, only ${JSON.stringify(rebalanceCodes)}`,
      );
      expect(consumer.assigned.length).toBe(0);
      expect(consumer.client.subscription()).toEqual([]);
      consumer.throwPollErrors();

      consumer.disconnect();
      expect(consumer.client.state).toBe("CLOSED");
      // A second disconnect() must be a no-op (double brk_client_destroy = UB).
      consumer.disconnect();
      expect(consumer.client.state).toBe("CLOSED");
    },
    240_000,
  );

  /* ---------------------------------------------------------------- (c) */

  test(
    "c. NFR-2: 100k messages produced+consumed, setInterval(10ms) average drift < 5ms",
    async () => {
      const topic = `vs-loop-${RUN_ID}`;
      const total = 100_000;
      const chunk = 500;
      await createTopic(topic, 1);

      // ---- the event-loop responsiveness clock ----------------------------
      // Started BEFORE connect ⇒ the window covers group join/rebalance and
      // produce + consume.
      const TICK_MS = 10;
      const drifts: number[] = [];
      let previous = performance.now();
      const timer = setInterval(() => {
        const now = performance.now();
        drifts.push(Math.abs(now - previous - TICK_MS));
        previous = now;
      }, TICK_MS);
      const started = performance.now();

      let consumedChecksumMismatch = 0;
      const consumer = new SliceConsumer({
        config: consumerConfig(`vs-loop-grp-${RUN_ID}`, {
          "fetch.message.max.bytes": 1_048_576,
        }),
        label: "loop-consumer",
        maxMessages: 500,
        onMessage: (message) => {
          const value = message.value as Uint8Array;
          if (value.length !== 100) consumedChecksumMismatch++;
        },
      });
      consumer.connect();
      consumer.subscribe([topic]);
      await consumer.waitForAssignment(120_000);

      const producer = new SliceProducer({
        config: producerConfig({ "queue.buffering.max.messages": 200_000 }),
        label: "loop-producer",
        maxPending: 200_000,
      });
      producer.connect();

      for (let base = 0; base < total; base += chunk) {
        await producer.waitBelow(50_000, 120_000);
        const records: SliceProduceInput[] = new Array(chunk);
        for (let i = 0; i < chunk; i++) {
          records[i] = { topic, partition: 0, value: makeValue(base + i) };
        }
        expect(producer.produce(records)).toBe(chunk);
        // Yield a macrotask between batches — how a real app produces (a pure
        // JS `for` loop without awaits blocks the event loop by itself,
        // regardless of the poll model).
        await Bun.sleep(0);
      }
      await producer.waitIdle(180_000);
      const produceMs = performance.now() - started;

      await consumer.waitForMessages(total, 180_000);
      const totalMs = performance.now() - started;
      clearInterval(timer);

      producer.throwPollErrors();
      consumer.throwPollErrors();

      expect(producer.delivered).toBe(total);
      expect(producer.deliveryFailures).toBe(0);
      expect(consumer.consumed).toBe(total);
      expect(consumedChecksumMismatch).toBe(0);

      // ---- responsiveness evaluation --------------------------------------
      // The whole 100k produce+consume loop takes a few hundred ms on this
      // machine, so the sample count is modest; enough to catch blocking (one
      // 100ms block fails it).
      expect(drifts.length).toBeGreaterThanOrEqual(20); // the timer actually ran
      const avgDrift = drifts.reduce((a, b) => a + b, 0) / drifts.length;
      const sorted = [...drifts].sort((a, b) => a - b);
      const p99 = sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * 0.99))] as number;
      const maxDrift = sorted[sorted.length - 1] as number;
      console.log(
        `[NFR-2] ticks=${drifts.length} avgDrift=${avgDrift.toFixed(2)}ms ` +
          `p99=${p99.toFixed(2)}ms max=${maxDrift.toFixed(2)}ms | ` +
          `produce ${(total / (produceMs / 1000)).toFixed(0)} msg/s, ` +
          `roundtrip ${(total / (totalMs / 1000)).toFixed(0)} msg/s`,
      );
      expect(avgDrift).toBeLessThan(5);

      producer.disconnect();
      consumer.disconnect();
    },
    600_000,
  );

  /* ---------------------------------------------------------------- (d) */

  test(
    "d. clean destroy: 2 rounds of connect→produce→consume→destroy in one process",
    async () => {
      const threadsBefore = threadCount();
      const roundSizes: number[] = [];

      for (let round = 0; round < 2; round++) {
        const topic = `vs-lifecycle-${RUN_ID}-${round}`;
        const total = 200;
        await createTopic(topic, 1);

        const producer = new SliceProducer({
          config: producerConfig(),
          label: `lifecycle-producer-${round}`,
        });
        producer.connect();
        const records: SliceProduceInput[] = Array.from({ length: total }, (_, i) => ({
          topic,
          partition: 0,
          key: `k-${i}`,
          value: makeValue(i),
        }));
        expect(producer.produce(records)).toBe(total);
        await producer.waitIdle(120_000);
        expect(producer.delivered).toBe(total);

        const consumer = new SliceConsumer({
          config: consumerConfig(`vs-lifecycle-grp-${RUN_ID}-${round}`),
          label: `lifecycle-consumer-${round}`,
        });
        consumer.connect();
        consumer.subscribe([topic]);
        await consumer.waitForMessages(total, 120_000);
        roundSizes.push(consumer.consumed);

        producer.throwPollErrors();
        consumer.throwPollErrors();

        consumer.disconnect();
        producer.disconnect();
        expect(consumer.client.state).toBe("CLOSED");
        expect(producer.client.state).toBe("CLOSED");
        // The handle is destroyed: every API must be blocked by the state machine, no FFI touched.
        expect(() => consumer.client.consumeBatch(1)).toThrow();
        expect(() => producer.client.produceBatch([])).toThrow();
        expect(consumer.scheduler.running).toBe(false);
        expect(producer.scheduler.running).toBe(false);
      }

      expect(roundSizes).toEqual([200, 200]);

      // librdkafka's threads must all be joined by the time brk_client_destroy returns.
      if (threadsBefore > 0) {
        await Bun.sleep(500);
        const threadsAfter = threadCount();
        console.log(`[lifecycle] threads ${threadsBefore} → ${threadsAfter}`);
        expect(threadsAfter).toBeLessThanOrEqual(threadsBefore + 2);
      }
    },
    600_000,
  );
});
