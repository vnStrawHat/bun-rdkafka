/**
 * bench/m1-baseline.ts — the M1 vertical slice's first performance baseline.
 *
 * NOT a test: a manually run script printing a numbers table to copy into
 * `bench/RESULTS.md`.
 *
 * Scenario (docs/03-implementation-plan.md, M1):
 *   1. produce N 100-byte messages → wait for the last delivery report
 *   2. consume all N messages with a fresh consumer group
 *      (auto.offset.reset=earliest)
 *   3. measure msgs/s both ways + RSS before/during/after to verify memory does
 *      not grow linearly with the message count.
 *
 * Usage:
 * ```sh
 * bun run bench/m1-baseline.ts                 # 1.000.000 message
 * TOTAL=500000 bun run bench/m1-baseline.ts    # lighter load for low-RAM machines
 * KAFKA_BROKERS=host:9092 bun run bench/m1-baseline.ts   # an existing broker
 * ```
 * The script brings the broker up via `test/integration/docker-kafka.ts`
 * (idempotent) and does **not** remove the container when done — clean up with
 * `docker rm -f bun-rdkafka-test-kafka`.
 */

import { createTopic, startKafka } from "../test/integration/docker-kafka.ts";
import { SliceConsumer, SliceProducer, type SliceProduceInput } from "../test/integration/slice-harness.ts";

/* ========================================================================== */
/* Tham số                                                                     */
/* ========================================================================== */

const TOTAL = Number(process.env["TOTAL"] ?? 1_000_000);
/** Value size (bytes). */
const SIZE = Number(process.env["SIZE"] ?? 100);
/** Records per `brk_produce_batch` call. */
const CHUNK = Number(process.env["CHUNK"] ?? 1_000);
/** Cap on records awaiting DRs (JS-side backpressure). */
const MAX_INFLIGHT = Number(process.env["MAX_INFLIGHT"] ?? 100_000);
/** Max messages per `brk_consume_batch`. */
const CONSUME_BATCH = Number(process.env["CONSUME_BATCH"] ?? 1_000);
const PARTITIONS = Number(process.env["PARTITIONS"] ?? 1);

/** Samples RSS ~20 times per phase. */
const SAMPLE_EVERY = Math.max(1, Math.floor(TOTAL / 20));

const MB = 1024 * 1024;
const rssMb = (): number => process.memoryUsage.rss() / MB;

/* ========================================================================== */
/* Utilities                                                                   */
/* ========================================================================== */

const HEAD = new TextEncoder().encode("bench-");

function makeValue(index: number): Uint8Array {
  const out = new Uint8Array(SIZE);
  out.set(HEAD);
  const tag = new TextEncoder().encode(String(index));
  out.set(tag.subarray(0, Math.max(0, SIZE - HEAD.length)), HEAD.length);
  return out;
}

interface RssTrack {
  start: number;
  peak: number;
  samples: { at: number; rss: number }[];
}

function newTrack(): RssTrack {
  const start = rssMb();
  return { start, peak: start, samples: [] };
}

function sample(track: RssTrack, at: number): void {
  const rss = rssMb();
  if (rss > track.peak) track.peak = rss;
  track.samples.push({ at, rss });
}

function fmt(n: number, digits = 1): string {
  return n.toLocaleString("en-US", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function table(rows: [string, string][]): string {
  const width = Math.max(...rows.map(([k]) => k.length));
  return rows.map(([k, v]) => `  ${k.padEnd(width)}  ${v}`).join("\n");
}

/* ========================================================================== */
/* Run                                                                         */
/* ========================================================================== */

console.log(`bun-rdkafka — M1 baseline (${TOTAL.toLocaleString("en-US")} message × ${SIZE} B)`);
console.log("starting the broker…");
const { brokers, image, reused } = await startKafka();
const topic = `bench-m1-${Date.now().toString(36)}`;
await createTopic(topic, PARTITIONS);
console.log(`broker: ${brokers} (image ${image ?? "?"}, ${reused ? "reused" : "created"}), topic: ${topic}\n`);

const rssBaseline = rssMb();

/* ------------------------------------------------------------------ produce */

const produceTrack = newTrack();
const producer = new SliceProducer({
  config: {
    "bootstrap.servers": brokers,
    "client.id": "bench-m1-producer",
    "linger.ms": 5,
    acks: "1",
    "message.timeout.ms": 300_000,
    "queue.buffering.max.messages": String(MAX_INFLIGHT * 2),
    "queue.buffering.max.kbytes": String(1024 * 1024),
  },
  label: "bench-producer",
  maxPending: MAX_INFLIGHT * 2,
});
producer.connect();

console.log("produce…");
const produceCpu0 = process.cpuUsage();
const produceStart = performance.now();
for (let base = 0; base < TOTAL; base += CHUNK) {
  await producer.waitBelow(MAX_INFLIGHT, 300_000);
  const n = Math.min(CHUNK, TOTAL - base);
  const records: SliceProduceInput[] = new Array(n);
  for (let i = 0; i < n; i++) {
    records[i] = { topic, partition: PARTITIONS === 1 ? 0 : -1, value: makeValue(base + i) };
  }
  producer.produce(records);
  if (base % SAMPLE_EVERY < CHUNK) sample(produceTrack, base);
  await Bun.sleep(0); // yield a macrotask between batches (see NFR-2)
}
await producer.waitIdle(600_000);
const produceMs = performance.now() - produceStart;
const produceCpu = process.cpuUsage(produceCpu0);
const produceCpuMs = (produceCpu.user + produceCpu.system) / 1000;
sample(produceTrack, TOTAL);
producer.throwPollErrors();

const delivered = producer.delivered;
const deliveryFailures = producer.deliveryFailures;
const drFrames = producer.drFrames;
const producePolls = producer.scheduler.pollCount;
producer.disconnect();

/* ------------------------------------------------------------------ consume */

interface ConsumeRun {
  label: string;
  ms: number;
  polls: number;
  consumed: number;
  bytes: number;
  cpuMs: number;
  track: RssTrack;
}

/**
 * One pass consuming all `TOTAL` messages with a fresh consumer group.
 *
 * Two variants separate the JS layer's cost from librdkafka's prefetch config —
 * the default `fetch.queue.backoff.ms` of 1000 ms lets the local queue drain
 * and then wait a whole second, tanking the consume numbers even with an idle
 * poll loop.
 */
async function runConsume(
  label: string,
  extra: Record<string, unknown>,
): Promise<ConsumeRun> {
  const track = newTrack();
  let consumed = 0;
  let bytes = 0;
  const consumer = new SliceConsumer({
    config: {
      "bootstrap.servers": brokers,
      "group.id": `bench-m1-${label}-${Date.now().toString(36)}`,
      "client.id": "bench-m1-consumer",
      "auto.offset.reset": "earliest",
      "enable.auto.commit": false,
      "enable.partition.eof": false,
      "fetch.wait.max.ms": 10,
      "fetch.message.max.bytes": String(4 * MB),
      ...extra,
    },
    label: `bench-consumer-${label}`,
    maxMessages: CONSUME_BATCH,
    onMessage: (message) => {
      consumed++;
      bytes += message.value?.length ?? 0;
      if (consumed % SAMPLE_EVERY === 0) sample(track, consumed);
    },
  });
  consumer.connect();
  consumer.subscribe([topic]);
  await consumer.waitForAssignment(120_000);

  console.log(`consume (${label})…`);
  const cpu0 = process.cpuUsage();
  const start = performance.now();
  await consumer.waitForMessages(TOTAL, 600_000);
  const ms = performance.now() - start;
  const cpu = process.cpuUsage(cpu0);
  sample(track, consumed);
  consumer.throwPollErrors();
  const polls = consumer.scheduler.pollCount;
  consumer.disconnect();
  return { label, ms, polls, consumed, bytes, cpuMs: (cpu.user + cpu.system) / 1000, track };
}

const runs: ConsumeRun[] = [
  await runConsume("librdkafka defaults", {}),
  await runConsume("prefetch tuned", { "fetch.queue.backoff.ms": 10 }),
];
const baseRun = runs[0] as ConsumeRun;
const bestRun = runs.reduce((a, b) => (b.ms < a.ms ? b : a));
const consumeTrack = bestRun.track;
const consumed = baseRun.consumed;
const bytes = baseRun.bytes;

/* -------------------------------------------------------------------- report */

// Give the GC a beat so the "after" RSS reflects a steady state.
Bun.gc(true);
await Bun.sleep(500);
const rssAfter = rssMb();

const produceRate = TOTAL / (produceMs / 1000);
const mib = (TOTAL * SIZE) / MB;

console.log(`\n=== M1 baseline — ${new Date().toISOString()} ===`);
console.log(
  table([
    ["message", `${TOTAL.toLocaleString("en-US")} × ${SIZE} B (${fmt(mib)} MiB payload)`],
    ["partition", String(PARTITIONS)],
    [
      "produce",
      `${fmt(produceMs)} ms → ${fmt(produceRate, 0)} msg/s (${fmt(mib / (produceMs / 1000))} MiB/s), ` +
        `CPU ${fmt(produceCpuMs, 0)} ms (${fmt((produceCpuMs / produceMs) * 100, 0)}% of one core)`,
    ],
    ...runs.map(
      (r): [string, string] => [
        `consume [${r.label}]`,
        `${fmt(r.ms)} ms → ${fmt(TOTAL / (r.ms / 1000), 0)} msg/s ` +
          `(${fmt(mib / (r.ms / 1000))} MiB/s), CPU ${fmt(r.cpuMs, 0)} ms ` +
          `(${fmt((r.cpuMs / r.ms) * 100, 0)}% of one core), ${r.polls.toLocaleString("en-US")} poll rounds ` +
          `→ ${fmt(TOTAL / Math.max(1, r.polls), 0)} msg/round`,
      ],
    ),
    ["delivery reports", `${delivered.toLocaleString("en-US")} ok / ${deliveryFailures} failed, ${drFrames} DR frames`],
    ["messages received", `${consumed.toLocaleString("en-US")} (${fmt(bytes / MB)} MiB)`],
    ["produce poll rounds", `${producePolls.toLocaleString("en-US")}`],
    ["msg / FFI call", `produce ${fmt(CHUNK, 0)} (batch), consume ≤ ${fmt(CONSUME_BATCH, 0)}`],
    ["RSS at start", `${fmt(rssBaseline)} MiB`],
    ["RSS peak (produce)", `${fmt(produceTrack.peak)} MiB`],
    ["RSS peak (consume)", `${fmt(Math.max(...runs.map((r) => r.track.peak)))} MiB`],
    ["RSS at end", `${fmt(rssAfter)} MiB (Δ ${fmt(rssAfter - rssBaseline)} MiB)`],
  ]),
);

function progression(track: RssTrack): string {
  return [0.25, 0.5, 0.75, 1]
    .map((p) => {
      const target = Math.floor(TOTAL * p);
      const found = track.samples.reduce<{ at: number; rss: number } | undefined>(
        (best, s) => (s.at <= target && (!best || s.at > best.at) ? s : best),
        undefined,
      );
      return `${Math.round(p * 100)}%: ${found ? fmt(found.rss) : "–"} MiB`;
    })
    .join("  |  ");
}
console.log(`\nRSS across produce progress — ${progression(produceTrack)}`);
console.log(`RSS across consume progress — ${progression(consumeTrack)}`);
console.log(
  "\n(the broker keeps running; clean up with: docker rm -f bun-rdkafka-test-kafka)",
);

process.exit(0);
