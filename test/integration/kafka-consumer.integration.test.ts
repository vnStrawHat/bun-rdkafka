/**
 * test/integration/kafka-consumer.integration.test.ts — the M3 KafkaConsumer
 * against a real broker (DoD in docs/03-implementation-plan.md M3).
 *
 * Covers: a flowing roundtrip (content intact), non-flowing consume(n),
 * commit/committed/position, seek rereads, pause/resume, partition.eof, and a
 * 2-consumer same-group rebalance — EAGER + COOPERATIVE, looping 20
 * consecutive join/leave cycles (10 per protocol) to prove no flakiness.
 *
 * ⚠ The docker broker is SHARED with other agents/tests running in parallel:
 * no stopKafka() in afterAll; topics/groups all use the "m3-" prefix.
 */

import { beforeAll, describe, expect, test } from "bun:test";
import { Buffer } from "node:buffer";
import {
  createTopic,
  integrationAvailable,
  startKafka,
} from "./docker-kafka.ts";
import { SliceProducer, waitFor } from "./slice-harness.ts";
import {
  KafkaConsumer,
  type Message,
} from "../../packages/bun-rdkafka/src/callback/kafka-consumer.ts";

const AVAILABLE = await integrationAvailable();
const RUN_ID = Date.now().toString(36);

let brokers = "localhost:9092";

function consumerConfig(groupId: string, extra: Record<string, unknown> = {}) {
  return {
    "bootstrap.servers": brokers,
    "group.id": groupId,
    "client.id": `m3-consumer-${RUN_ID}`,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": false,
    "fetch.wait.max.ms": 10,
    "fetch.queue.backoff.ms": 10,
    "session.timeout.ms": 6000,
    "heartbeat.interval.ms": 1500,
    ...extra,
  };
}

/** The M1 producer harness — independent of the M2 code (written in parallel). */
async function produce(
  topic: string,
  count: number,
  opts: { partition?: number; withMeta?: boolean } = {},
): Promise<void> {
  const producer = new SliceProducer({
    config: {
      "bootstrap.servers": brokers,
      "linger.ms": 5,
      acks: "all",
    },
  });
  producer.connect();
  try {
    const records = Array.from({ length: count }, (_, i) => ({
      topic,
      partition: opts.partition ?? 0,
      value: `m3-value-${i}`,
      ...(opts.withMeta
        ? {
            key: `m3-key-${i}`,
            timestamp: 1_700_000_000_000 + i,
            headers: [{ key: "h-idx", value: String(i) }],
          }
        : {}),
    }));
    producer.produce(records);
    await producer.waitIdle(60_000);
    expect(producer.delivered).toBe(count);
  } finally {
    producer.disconnect();
  }
}

function connectConsumer(consumer: KafkaConsumer): Promise<void> {
  return new Promise((resolve, reject) => {
    consumer.connect({}, (err) => (err ? reject(err) : resolve()));
  });
}

/** Polls `assignments()` until the predicate holds. */
async function waitAssignments(
  consumer: KafkaConsumer,
  predicate: (parts: { topic: string; partition: number }[]) => boolean,
  what: string,
  timeoutMs = 60_000,
): Promise<void> {
  await waitFor(
    () => predicate(consumer.assignments() as { topic: string; partition: number }[]),
    timeoutMs,
    () => `${what}; currently: ${JSON.stringify(consumer.assignments())}`,
    25,
  );
}

/* ========================================================================== */

describe.skipIf(!AVAILABLE)("M3 KafkaConsumer — real broker", () => {
  beforeAll(async () => {
    brokers = (await startKafka()).brokers;
  }, 600_000);

  // NO stopKafka() — the broker is shared with other tests/agents (see the top of this file).

  test(
    "flowing: a 50-message roundtrip with content + order intact",
    async () => {
      const topic = `m3-flow-${RUN_ID}`;
      await createTopic(topic, 1);
      await produce(topic, 50, { withMeta: true });

      const consumer = new KafkaConsumer(consumerConfig(`m3-g-flow-${RUN_ID}`));
      await connectConsumer(consumer);
      const got: Message[] = [];
      consumer.on("data", (m: Message) => got.push(m));
      consumer.subscribe([topic]);
      consumer.consume();
      await waitFor(() => got.length >= 50, 60_000, () => `only ${got.length}/50 received`);

      for (let i = 0; i < 50; i++) {
        const m = got[i] as Message;
        expect(m.topic).toBe(topic);
        expect(m.partition).toBe(0);
        expect(m.offset).toBe(i);
        expect((m.value as Buffer).toString()).toBe(`m3-value-${i}`);
        expect((m.key as Buffer).toString()).toBe(`m3-key-${i}`);
        expect(m.timestamp).toBe(1_700_000_000_000 + i);
        const header = (m.headers as Record<string, Buffer>[])[0] as Record<string, Buffer>;
        expect(header["h-idx"]!.toString()).toBe(String(i));
      }
      consumer.disconnect();
    },
    120_000,
  );

  test(
    "flowing + js.consume.prefetch (experiment): same roundtrip through the shim prefetch thread, rebalance events still surface, clean disconnect",
    async () => {
      const topic = `m3-flow-pf-${RUN_ID}`;
      await createTopic(topic, 1);
      await produce(topic, 2000, { withMeta: true });

      const consumer = new KafkaConsumer(
        consumerConfig(`m3-g-flow-pf-${RUN_ID}`, {
          "js.consume.prefetch": true,
          "js.consume.prefetch.frames": 2,
        }),
      );
      await connectConsumer(consumer);
      const got: Message[] = [];
      let assigned = 0;
      consumer.on("rebalance", (err) => {
        if (err.code === -175 /* ERR__ASSIGN_PARTITIONS */) assigned++;
      });
      consumer.on("data", (m: Message) => got.push(m));
      consumer.subscribe([topic]);
      consumer.consume();
      await waitFor(() => got.length >= 2000, 60_000, () => `only ${got.length}/2000 received`);
      expect(assigned).toBe(1);

      for (let i = 0; i < 2000; i++) {
        const m = got[i] as Message;
        expect(m.offset).toBe(i);
        expect((m.value as Buffer).toString()).toBe(`m3-value-${i}`);
        expect((m.key as Buffer).toString()).toBe(`m3-key-${i}`);
      }
      await new Promise<void>((resolve) => consumer.disconnect(() => resolve()));
    },
    120_000,
  );

  test(
    "non-flowing consume(n): 3 × consume(10) collects all 30, in order",
    async () => {
      const topic = `m3-num-${RUN_ID}`;
      await createTopic(topic, 1);
      await produce(topic, 30);

      const consumer = new KafkaConsumer(consumerConfig(`m3-g-num-${RUN_ID}`));
      await connectConsumer(consumer);
      consumer.subscribe([topic]);
      consumer.setDefaultConsumeTimeout(30_000);

      const all: Message[] = [];
      for (let round = 0; round < 3; round++) {
        const batch = await new Promise<Message[]>((resolve, reject) =>
          consumer.consume(10, (err, ms) => (err ? reject(err) : resolve(ms))),
        );
        all.push(...batch);
      }
      expect(all.map((m) => m.offset)).toEqual(Array.from({ length: 30 }, (_, i) => i));
      consumer.disconnect();
    },
    120_000,
  );

  test(
    "commit/committed/position + a seek rereading from the start",
    async () => {
      const topic = `m3-off-${RUN_ID}`;
      await createTopic(topic, 1);
      await produce(topic, 20);
      const group = `m3-g-off-${RUN_ID}`;

      const consumer = new KafkaConsumer(consumerConfig(group));
      await connectConsumer(consumer);
      const got: Message[] = [];
      consumer.on("data", (m: Message) => got.push(m));
      consumer.subscribe([topic]);
      consumer.consume();
      await waitFor(() => got.length >= 20, 60_000, () => `only ${got.length}/20 received`);

      // commitMessageSync message cuối → committed = offset + 1.
      consumer.commitMessageSync(got[19] as Message);
      const committed = await new Promise<{ offset: number }[]>((resolve, reject) =>
        consumer.committed([{ topic, partition: 0 }], 10_000, (err, tps) =>
          err ? reject(err) : resolve(tps as { offset: number }[]),
        ),
      );
      expect(committed[0]?.offset).toBe(20);

      // position: all 20 messages read.
      const position = consumer.position([{ topic, partition: 0 }]);
      expect(position[0]?.offset).toBe(20);

      // seek to the start → message 0 rereads.
      got.length = 0;
      await new Promise<void>((resolve, reject) =>
        consumer.seek({ topic, partition: 0, offset: 0 }, 5000, (err) =>
          err ? reject(err) : resolve(),
        ),
      );
      await waitFor(() => got.length >= 20, 60_000, () => `only ${got.length}/20 received after the seek`);
      expect(got[0]?.offset).toBe(0);
      expect((got[0]?.value as Buffer).toString()).toBe("m3-value-0");
      consumer.disconnect();
    },
    120_000,
  );

  test(
    "pause stops the data, resume lets it flow again",
    async () => {
      const topic = `m3-pause-${RUN_ID}`;
      await createTopic(topic, 1);
      await produce(topic, 5);

      const consumer = new KafkaConsumer(consumerConfig(`m3-g-pause-${RUN_ID}`));
      await connectConsumer(consumer);
      const got: Message[] = [];
      consumer.on("data", (m: Message) => got.push(m));
      consumer.subscribe([topic]);
      consumer.consume();
      await waitFor(() => got.length >= 5, 60_000, () => `only ${got.length}/5 received`);

      consumer.pause([{ topic, partition: 0 }]);
      // Let what is already fetched in the C queue drain before measuring "silence".
      await Bun.sleep(500);
      const baseline = got.length;
      await produce(topic, 5);
      await Bun.sleep(1_200);
      expect(got.length).toBe(baseline); // the pause held — no new messages

      consumer.resume([{ topic, partition: 0 }]);
      await waitFor(
        () => got.length >= baseline + 5,
        60_000,
        () => `only ${got.length - baseline}/5 received after resume`,
      );
      consumer.disconnect();
    },
    120_000,
  );

  test(
    "partition.eof: 5 data records then EOF at offset 5",
    async () => {
      const topic = `m3-eof-${RUN_ID}`;
      await createTopic(topic, 1);
      await produce(topic, 5);

      const consumer = new KafkaConsumer(
        consumerConfig(`m3-g-eof-${RUN_ID}`, { "enable.partition.eof": true }),
      );
      await connectConsumer(consumer);
      const got: Message[] = [];
      const eofs: { topic: string; partition: number; offset: number }[] = [];
      consumer.on("data", (m: Message) => got.push(m));
      consumer.on("partition.eof", (e: (typeof eofs)[0]) => eofs.push(e));
      consumer.subscribe([topic]);
      consumer.consume();
      await waitFor(
        () => got.length >= 5 && eofs.length >= 1,
        60_000,
        () => `data ${got.length}/5, eof ${eofs.length}/1`,
      );
      expect(eofs[0]).toEqual({ topic, partition: 0, offset: 5 });
      expect(got).toHaveLength(5);
      consumer.disconnect();
    },
    120_000,
  );

  /* ------------------------------------------------------------ rebalance */

  /**
   * One cycle: c2 joins → both split all `partitions` without overlap →
   * c2 leaves → c1 reclaims everything. Returns once stable.
   */
  async function rebalanceCycle(
    c1: KafkaConsumer,
    makeC2: () => KafkaConsumer,
    topic: string,
    partitions: number,
    label: string,
  ): Promise<void> {
    const c2 = makeC2();
    await connectConsumer(c2);
    c2.subscribe([topic]);
    try {
      await waitFor(
        () => {
          const a1 = c1.assignments();
          const a2 = c2.assignments();
          if (a1.length === 0 || a2.length === 0) return false;
          if (a1.length + a2.length !== partitions) return false;
          const seen = new Set(a1.map((p) => p.partition));
          return a2.every((p) => !seen.has(p.partition)); // no overlap
        },
        60_000,
        () =>
          `${label}: not evenly split yet — c1=${JSON.stringify(c1.assignments())} c2=${JSON.stringify(
            c2.assignments(),
          )}`,
        25,
      );
    } finally {
      c2.disconnect();
    }
    await waitAssignments(
      c1,
      (parts) => parts.length === partitions,
      `${label}: c1 has not reclaimed all ${partitions} partitions after c2 left`,
    );
  }

  for (const strategy of ["range,roundrobin", "cooperative-sticky"] as const) {
    const proto = strategy === "cooperative-sticky" ? "COOPERATIVE" : "EAGER";
    test(
      `rebalance ${proto}: 10 consecutive join/leave cycles without flaking`,
      async () => {
        const topic = `m3-rb-${proto.toLowerCase()}-${RUN_ID}`;
        const partitions = 4;
        await createTopic(topic, partitions);
        const group = `m3-g-rb-${proto.toLowerCase()}-${RUN_ID}`;
        const config = () =>
          consumerConfig(group, { "partition.assignment.strategy": strategy });

        const c1 = new KafkaConsumer(config());
        await connectConsumer(c1);
        c1.subscribe([topic]);
        await waitAssignments(
          c1,
          (parts) => parts.length === partitions,
          "c1 has not received its initial assignment",
        );
        expect(c1.rebalanceProtocol()).toBe(proto);

        let cycles = 0;
        for (let i = 0; i < 10; i++) {
          await rebalanceCycle(
            c1,
            () => new KafkaConsumer(config()),
            topic,
            partitions,
            `${proto} cycle ${i + 1}`,
          );
          cycles++;
        }
        expect(cycles).toBe(10);
        c1.disconnect();
      },
      600_000,
    );
  }
});
