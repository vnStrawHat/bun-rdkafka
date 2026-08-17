/*
 * Orchestrator: runs the bun-rdkafka vs confluent-kafka-javascript comparison
 * benches. Each measurement runs RUNS times, taking the median. Results land
 * in bench/compare/results.json.
 *
 * Run: bun bench/compare/run.ts [--quick]   (--quick: 1 run, small N — smoke)
 */
import { spawnSync } from "node:child_process";

const ROOT = new URL("../..", import.meta.url).pathname.replace(/\/$/, "");
const OURS = `${ROOT}/packages/bun-rdkafka/src/index.ts`;
const UPSTREAM = `${ROOT}/bench/upstream/node_modules/@confluentinc/kafka-javascript`;
const NODE = "/root/.nvm/versions/node/v24.15.0/bin/node";
const BROKERS = "localhost:9092";
const QUICK = process.argv.includes("--quick");
// --producer-only: rerun just the 4 producer cases and merge them into the
// existing results.json (keeps consumer/latency numbers from the prior session).
const PRODUCER_ONLY = process.argv.includes("--producer-only");
const RUNS = QUICK ? 1 : 3;
const N = QUICK ? "20000" : "500000";
const WARMUP = QUICK ? "5000" : "100000";

interface Row {
  name: string;
  cmd: string[];
  lib: string;
  available: boolean;
  /** extra env for the bench scripts (e.g. CONSUMER_EXTRA) */
  env?: Record<string, string>;
  /** consumer-side variant only: skip the producer cases (identical code path) */
  consumerOnly?: boolean;
}

function probe(cmd: string[], lib: string): boolean {
  const r = spawnSync(cmd[0]!, [...cmd.slice(1), "-e", "require(process.env.LIB); console.log('ok')"], {
    env: { ...process.env, LIB: lib },
    timeout: 30000,
  });
  return r.stdout?.toString().includes("ok") ?? false;
}

// Compared against upstream on Node only (user decision, 2026-08-14):
// confluent-kafka-javascript does not run on Bun 1.4 — prebuilts stop at
// NODE_MODULE_VERSION 137 (Node 24) while Bun 1.4 requires 147.
// --prefetch: add a third row running our consumer with the experimental
// shim-side prefetch thread (docs/notes/consumer-prefetch-thread.md).
const WITH_PREFETCH = process.argv.includes("--prefetch");
const rows: Row[] = [
  { name: "bun-rdkafka / Bun", cmd: ["bun"], lib: OURS, available: true },
  ...(WITH_PREFETCH
    ? [{
        name: "bun-rdkafka / Bun (js.consume.prefetch)",
        cmd: ["bun"],
        lib: OURS,
        available: true,
        env: { CONSUMER_EXTRA: JSON.stringify({ "js.consume.prefetch": true }) },
        consumerOnly: true,
      } satisfies Row]
    : []),
  { name: "confluent-kafka-js / Node 24", cmd: [NODE], lib: UPSTREAM, available: probe([NODE], UPSTREAM) },
];

function runOnce(script: string, row: Row, extraEnv: Record<string, string>): Record<string, number> | null {
  const r = spawnSync(row.cmd[0]!, [...row.cmd.slice(1), `${ROOT}/bench/compare/${script}`], {
    env: { ...process.env, LIB: row.lib, BROKERS, N, WARMUP, ...(row.env ?? {}), ...extraEnv },
    timeout: 240000,
  });
  const lines = (r.stdout?.toString() ?? "").trim().split("\n");
  for (let i = lines.length - 1; i >= 0; i--) {
    const line = lines[i]!.trim();
    if (line.startsWith("{")) {
      try {
        return JSON.parse(line);
      } catch {}
    }
  }
  console.error(`  FAIL ${row.name}: ${r.stderr?.toString().split("\n").slice(-3).join(" | ")}`);
  return null;
}

/**
 * Best-effort topic cleanup between cases so a full run does not fill the
 * broker's disk (a 1KB producer case writes ~600 MB per run and the test
 * broker has no size-based retention). No-op when the docker test container
 * is not there (external broker).
 */
function delTopic(topic: string): void {
  spawnSync("docker", [
    "exec", "bun-rdkafka-test-kafka", "/opt/kafka/bin/kafka-topics.sh",
    "--bootstrap-server", "localhost:9092", "--delete", "--topic", topic,
  ], { timeout: 60000 });
}

function median(xs: number[]): number {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.floor(s.length / 2)]!;
}

function bench(script: string, row: Row, extraEnv: Record<string, string>, key: string): Record<string, number> | null {
  const results: Record<string, number>[] = [];
  for (let i = 0; i < RUNS; i++) {
    const r = runOnce(script, row, extraEnv);
    if (r) results.push(r);
  }
  if (results.length === 0) return null;
  const out: Record<string, number> = {};
  for (const k of Object.keys(results[0]!)) out[k] = median(results.map((r) => r[k]!));
  console.log(`  ${row.name}: ${key}=${out[key]}`);
  return out;
}

// ---- scenarios ----
const summary: Record<string, Record<string, Record<string, number> | null>> = {};

// Producer: {size}×{acks}
for (const size of ["100", "1024"]) {
  for (const acks of ["1", "all"]) {
    const caseName = `producer ${size}B acks=${acks}`;
    console.log(`\n== ${caseName} ==`);
    summary[caseName] = {};
    for (const row of rows) {
      if (!row.available || row.consumerOnly) {
        summary[caseName][row.name] = null;
        continue;
      }
      const topic = `bench-p-${row.cmd[0] === "bun" ? "b" : "n"}-${row.lib === OURS ? "ours" : "up"}-${size}-${acks}`;
      summary[caseName][row.name] = bench(
        "producer-bench.cjs",
        row,
        { TOPIC: topic, SIZE: size, ACKS: acks },
        "msgs_per_s",
      );
      delTopic(topic);
    }
  }
}

// Consumer: fill the topic once with bun-rdkafka; every row reads the same data
for (const size of PRODUCER_ONLY ? [] : ["100", "1024"]) {
  const topic = `bench-c-${size}`;
  const caseName = `consumer ${size}B`;
  console.log(`\n== fill ${topic} ==`);
  runOnce("producer-bench.cjs", rows[0]!, { TOPIC: topic, SIZE: size, ACKS: "1" });
  console.log(`== ${caseName} ==`);
  summary[caseName] = {};
  for (const row of rows) {
    summary[caseName][row.name] = row.available
      ? bench("consumer-bench.cjs", row, { TOPIC: topic }, "msgs_per_s")
      : null;
  }
  delTopic(topic);
}

// Latency: 100B acks=1, 10k msg/s
if (!PRODUCER_ONLY) {
  const caseName = "e2e latency 10k msg/s";
  console.log(`\n== ${caseName} ==`);
  summary[caseName] = {};
  for (const row of rows) {
    const topic = `bench-l-${row.lib === OURS ? (row.consumerOnly ? "ours-pf" : "ours") : row.cmd[0] === "bun" ? "upb" : "upn"}`;
    summary[caseName][row.name] = row.available
      ? bench("latency-bench.cjs", row, { TOPIC: topic, DURATION_S: QUICK ? "5" : "20" }, "p99_ms")
      : null;
  }
}

let finalSummary = summary;
if (PRODUCER_ONLY) {
  // Merge over the previous full run so consumer/latency medians are kept.
  try {
    const prev = JSON.parse(await Bun.file(`${ROOT}/bench/compare/results.json`).text());
    finalSummary = { ...prev.summary, ...summary };
  } catch {}
}
await Bun.write(`${ROOT}/bench/compare/results.json`, JSON.stringify({ runs: RUNS, n: N, rows: rows.map((r) => ({ name: r.name, available: r.available })), summary: finalSummary }, null, 2));
console.log("\nWrote bench/compare/results.json");
