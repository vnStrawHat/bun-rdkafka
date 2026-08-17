# bun-rdkafka

A native Apache Kafka client for [Bun](https://bun.sh): [librdkafka](https://github.com/confluentinc/librdkafka) bound through `bun:ffi`, with an API compatible with [confluent-kafka-javascript](https://github.com/confluentinc/confluent-kafka-javascript).

> **Status: early release (0.x).** Published to npm as [`@vnstrawhat/bun-rdkafka`](https://www.npmjs.com/package/@vnstrawhat/bun-rdkafka) with CI-built prebuilt binaries for linux-x64 and win32-x64; other platforms build from source automatically (see [Platform support](#platform-support)). The API surface is complete and conformance-tested against upstream's type definitions, but expect 0.x-style iteration before 1.0.

## Why

- **confluent-kafka-javascript does not run on Bun.** Its prebuilt N-API binaries stop at `NODE_MODULE_VERSION` 137 (Node ≤ 24), while Bun 1.4 requires 147. We verified this experimentally — see `bench/RESULTS.md` §M6.
- **Even where the N-API path works, it is not the fast path for Bun.** bun-rdkafka is designed around `bun:ffi` from the ground up: a thin C shim statically links librdkafka into a single shared library, converts every librdkafka callback into pollable event queues (no cross-thread C→JS calls), and moves data across the FFI boundary in packed binary batches — one FFI call carries thousands of messages, decoded with `DataView`.
- **Same API, drop-in migration.** All three API styles of confluent-kafka-javascript are provided: the promisified `KafkaJS` namespace (`Kafka`, `producer()`, `consumer()`, `admin()`), the classic callback API (`Producer`, `KafkaConsumer`, `AdminClient`), and the Stream API (`Producer.createWriteStream`, `KafkaConsumer.createReadStream`). The official upstream examples run verbatim with only the import line changed (`examples/upstream-*.js`). See [MIGRATION.md](./MIGRATION.md).

## Install

Requires Bun ≥ 1.2.

```sh
bun add @vnstrawhat/bun-rdkafka
```

Bun blocks dependency lifecycle scripts by default, and bun-rdkafka uses a `postinstall`
script to fetch its prebuilt native binary. Pick one of the two options:

**Option 1 — trust the package (recommended).** Add it to `trustedDependencies` in your
`package.json`, then run `bun install` again:

```jsonc
// package.json
{
  "trustedDependencies": ["@vnstrawhat/bun-rdkafka"]
}
```

**Option 2 — run the installer manually** (once, after `bun add`):

```sh
bunx bun-rdkafka-install
```

What the installer does: downloads `libbunrdkafka-<platform>.tar.gz` from the
[GitHub Release](https://github.com/vnStrawHat/bun-rdkafka/releases) matching the
installed package version, verifies its SHA-256 against the release's `SHA256SUMS`, and
unpacks it into the package's `prebuilds/` directory. **If no prebuilt exists for your
platform (or the download fails), it automatically falls back to building from source**
— the npm tarball ships the C shim sources; you need `cmake` and a C compiler
(librdkafka is fetched and statically linked during the build).

Environment variables:

| Variable | Effect |
|---|---|
| `BUN_RDKAFKA_LIB_PATH` | use an existing `libbunrdkafka` library; skips all lookup |
| `BUN_RDKAFKA_BINARY_MIRROR` | base URL replacing the GitHub release base (air-gapped mirrors); must serve `<mirror>/v<version>/<asset>` |
| `BUN_RDKAFKA_SKIP_DOWNLOAD=1` | postinstall does nothing |
| `BUN_RDKAFKA_FORCE_BUILD=1` | never download, always build from source |
| `BUN_RDKAFKA_INSTALL_STRICT=1` | fail the install on error instead of deferring the error to runtime |

## Quick start

Requires a Kafka broker. Each of the three API styles below is shown with a producer and a consumer.

### KafkaJS-style API (promisified)

```ts
import { KafkaJS } from "@vnstrawhat/bun-rdkafka";

const kafka = new KafkaJS.Kafka({
  kafkaJS: { brokers: ["localhost:9092"] },
});

const producer = kafka.producer();
await producer.connect();

const [report] = await producer.send({
  topic: "quickstart",
  messages: [{ key: "user-1", value: JSON.stringify({ hello: "bun" }) }],
});
console.log(`delivered to partition ${report.partition} @ offset ${report.baseOffset}`);

await producer.disconnect();
```

```ts
import { KafkaJS } from "@vnstrawhat/bun-rdkafka";

const kafka = new KafkaJS.Kafka({
  kafkaJS: { brokers: ["localhost:9092"] },
});

const consumer = kafka.consumer({
  kafkaJS: { groupId: "quickstart-group", fromBeginning: true },
});
await consumer.connect();
await consumer.subscribe({ topics: ["quickstart"] });

await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    console.log(`${topic}[${partition}]@${message.offset}: ${message.value}`);
  },
});
```

### Callback API (node-rdkafka style)

```ts
import { Producer } from "@vnstrawhat/bun-rdkafka";

const producer = new Producer({
  "metadata.broker.list": "localhost:9092",
  dr_cb: true,
});

producer.on("ready", () => {
  producer.produce("quickstart", -1, Buffer.from("hello from Bun"), "key-1");
});

producer.on("delivery-report", (err, report) => {
  if (err) throw err;
  console.log(`delivered: ${report.topic}[${report.partition}]@${report.offset}`);
  producer.disconnect();
});

producer.connect();
```

```ts
import { KafkaConsumer } from "@vnstrawhat/bun-rdkafka";

const consumer = new KafkaConsumer(
  {
    "metadata.broker.list": "localhost:9092",
    "group.id": "quickstart-cb-group",
  },
  { "auto.offset.reset": "earliest" },
);

consumer.on("ready", () => {
  consumer.subscribe(["quickstart"]);
  consumer.consume(); // flowing mode: emits one 'data' event per message
});

consumer.on("data", (message) => {
  console.log(`${message.topic}[${message.partition}]@${message.offset}: ${message.value}`);
});

consumer.connect();
```

### Stream API (Node streams over the callback API)

```ts
import { Producer } from "@vnstrawhat/bun-rdkafka";

// A Writable stream to one topic; the Producer is created and connected for you.
const stream = Producer.createWriteStream(
  { "metadata.broker.list": "localhost:9092" },
  {},
  { topic: "quickstart", autoClose: true }, // autoClose: flush + disconnect on end()
);

stream.on("error", (err) => console.error("producer stream error", err));

for (let i = 0; i < 10; i++) {
  // write() only tells you the message was queued; its return value is the backpressure signal
  stream.write(Buffer.from(`hello from Bun #${i}`));
}
stream.end(() => console.log("10 messages written"));
```

```ts
import { KafkaConsumer } from "@vnstrawhat/bun-rdkafka";

// A Readable stream in objectMode: one Message per chunk (also an async iterable).
const stream = KafkaConsumer.createReadStream(
  {
    "metadata.broker.list": "localhost:9092",
    "group.id": "quickstart-stream-group",
  },
  { "auto.offset.reset": "earliest" },
  { topics: ["quickstart"], fetchSize: 100 },
);

stream.on("error", (err) => console.error("consumer stream error", err));

for await (const message of stream) {
  console.log(`${message.topic}[${message.partition}]@${message.offset}: ${message.value}`);
}
```

Use `{ objectMode: true }` on the write stream to pass `{ topic, partition, key, value, headers }` objects, `streamAsBatch: true` on the read stream to receive arrays, and `stream.consumer` / `stream.producer` to reach the underlying client (e.g. `stream.consumer.commit()`).

All six snippets above were verified against a real broker before being committed to this README. Configuration is librdkafka's own property set, passed through unmodified — see the [librdkafka configuration reference](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).

## Performance

Measured against `@confluentinc/kafka-javascript` 1.10.0 on Node 24 (same machine, same broker, identical librdkafka 2.15.0 configuration; all seven cases from one benchmarking session on the current code, 3-run medians — full method and raw data in [`bench/RESULTS.md`](./bench/RESULTS.md), section "Final consolidated run"):

| Case | bun-rdkafka / Bun | upstream / Node 24 | Ratio |
|---|---:|---:|---:|
| producer, 100 B, acks=1 | 981,760 msg/s | 668,537 | **1.47×** |
| producer, 100 B, acks=all | 914,716 msg/s | 664,739 | **1.38×** |
| producer, 1 KB, acks=1 | 408,700 msg/s | 646,153 | 0.63× |
| producer, 1 KB, acks=all | 517,344 msg/s | 638,564 | 0.81× |
| consumer, 100 B | 1,010,779 msg/s | 408,069 | **2.48×** |
| consumer, 1 KB | 671,952 msg/s | 270,955 | **2.48×** |
| e2e latency p50/p99 @10k msg/s | 4 / 6 ms | 2 / 3 ms | — |

Honest caveats:

- Benchmarked on a 4-vCPU / 3 GB box with the broker co-located, so absolute numbers are compressed; ratios are the meaningful signal.
- The 1 KB producer case is an *unbounded-burst microbenchmark* where bun-rdkafka currently loses (0.63–0.81× across sessions, with high run-to-run variance): enqueueing the whole 600 MB burst near-instantly defeats pipeline overlap on shared CPUs. With a bounded queue (`queue.buffering.max.messages=65536`, closer to production configs) the same case reaches parity or better. Analysis and follow-ups in `bench/RESULTS.md` §M6d.
- Latency p99 is single-digit ms but roughly 2× upstream — the inherent cost of the poll-based event model. Tune `js.poll.idle.max.ms` down if latency matters more than idle CPU.

## Platform support

| Target | Prebuilt binary | Tested |
|---|---|---|
| linux-x64 (glibc ≥ 2.28) | ✅ CI release asset (`linux-x64-gnu`, static OpenSSL/zlib/zstd/lz4 — depends only on glibc) | ✅ full unit + integration suite and benchmarks against a real broker; CI `verify-native` gate (dlopen + `ldd` + `builtin.features`) |
| win32-x64 (MSVC, vcpkg) | ✅ CI release asset (`win32-x64`) | ⚠️ builds green in CI; not yet exercised against a broker on Windows |
| linux-arm64 (glibc) | ❌ source build (postinstall fallback) | ⏳ not tested |
| darwin-arm64 / darwin-x64 (macOS ≥ 12) | ❌ source build (postinstall fallback) | ⏳ not tested |
| linux-x64 musl (Alpine) | ❌ source build (postinstall fallback) | ⏳ not tested |

Prebuilt binaries for the two Tier-1 targets are built by CI and attached to every [GitHub Release](https://github.com/vnStrawHat/bun-rdkafka/releases); the postinstall script picks the matching one (see [Install](#install)). On every other platform the installer falls back to the source build automatically — you need `cmake` and a C compiler; librdkafka is fetched and statically linked during the build. Contributions of prebuilt matrix entries (arm64/darwin) are welcome once we have CI runners to test them.

## Configuration: `js.*` options

Configuration is librdkafka's own property set, passed through unmodified (see the
[librdkafka configuration reference](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)).
On top of that, bun-rdkafka accepts a small set of `js.*` keys — passed in the same
config object — that tune the TypeScript layer (poll scheduling, buffers, backpressure).
They are never forwarded to librdkafka; unknown `js.*` keys are rejected at construction.

All of it is typed: `new Producer(conf, topicConf?)` takes `ProducerConfig` / `ProducerTopicConfig`,
`new KafkaConsumer(...)` takes `KafkaConsumerConfig` / `ConsumerTopicConfig`, and
`AdminClient.create(...)` takes `AdminClientConfig` — every librdkafka property (generated from
librdkafka's `CONFIGURATION.md`, with its description, default and range as hover docs), the `js.*`
keys below, and the callback properties (`dr_cb`, `rebalance_cb`, …), so editors complete keys and
flag typos or out-of-range enum values. The KafkaJS namespace is typed the same way — `new
Kafka(config)` / `kafka.producer(config)` / `kafka.consumer(config)` / `kafka.admin(config)`
take `CommonConstructorConfig` / `ProducerConstructorConfig` / `ConsumerConstructorConfig` /
`AdminConstructorConfig` (upstream's names): the librdkafka pass-through keys plus a typed
`kafkaJS` block (`KafkaConfig` / `ProducerConfig` / `ConsumerConfig` / `AdminConfig`). Event names
are typed too: `client.on("…")` completes the events of that client and types the listener's
parameters.

| Key | Default | What it does | Recommended |
|---|---|---|---|
| `js.poll.idle.max.ms` | `50` | Ceiling of the adaptive poll backoff when a client is idle. When there is data the client polls continuously; when data runs out it backs off 1 → 2 → 4 … up to this value. It is therefore the **worst-case added latency** for a message arriving on an idle consumer, and for rebalance / offset-commit / delivery-report delivery. | Keep `50` for throughput-oriented workloads. Set `5`–`10` for latency-sensitive consumers (costs a little idle CPU). |
| `js.poll.interval.ms` | `500` | Poll interval while a client is *cold* (no subscription/assignment and no in-flight produce): only picks up log/stats/error events. The timer is `unref`'d, so an idle client never keeps the process alive. | Keep the default. Lower only if you need faster `event.stats`/`event.log` while idle. |
| `js.consume.buffer.bytes` | `4194304` (4 MiB) | Size of the reusable buffer one `brk_consume_batch` FFI call fills (up to 500 messages per call). It grows automatically if a single message does not fit, so this is a performance knob, not a limit. | Keep the default. Raise (e.g. `16777216`) if your messages are ≥ 1 MiB so batches stay full; lower (e.g. `262144`) to save memory on many small consumers. |
| `js.event.buffer.bytes` | `262144` (256 KiB) | Size of the reusable buffer for the event drain (`brk_events_poll`): delivery reports, rebalance/commit events, stats JSON. Grows automatically when a frame does not fit. | Keep the default. Raise if you enable `statistics.interval.ms` with many topics/partitions (the stats JSON can reach several hundred KiB). |
| `js.producer.max.pending` | = `queue.buffering.max.messages` (librdkafka default `100000`) | Backpressure threshold of the producer's delivery ledger — the number of produced messages still waiting for a delivery report. Beyond it `produce()` throws `ERR__QUEUE_FULL` synchronously (same as upstream), so you can `poll()` and retry. | Leave it tied to `queue.buffering.max.messages`. Lower both (e.g. `65536`) for large payloads to bound memory and keep the pipeline flowing. |
| `js.consumer.max.batch.size` | `32` | KafkaJS API only: the maximum number of messages handed to one `eachBatch` call. `eachMessage` is unaffected. | `32` is a good default; raise (`100`–`500`) when your batch handler amortizes work (bulk writes), lower for tighter per-message latency. |
| `js.consume.prefetch` | `false` | **Experimental.** Serializes consume batches on a shim-owned thread so the JS thread only decodes and emits. Measured +26–33 % consumer throughput on a machine with an idle core, at +33–40 % CPU (see `bench/RESULTS.md`, "Consumer prefetch thread"). Prefetched frames are still delivered after `seek`/`pause`/revoke — see [docs/notes/consumer-prefetch-thread.md](./docs/notes/consumer-prefetch-thread.md) before enabling. | Off by default. Try `true` for high-throughput flowing consumers on ≥ 2 cores when you commit from your handler (`enable.auto.commit=false`). |
| `js.consume.prefetch.frames` | `4` | Ring depth of the prefetch thread: how many `js.consume.buffer.bytes` frames may be filled ahead of the JS thread. | Keep `4`. More frames only add prefetch depth (memory + at-least-once exposure), not throughput. |
| `js.consumer.zero.copy` | `false` | Reserved for returning message `value`/`key` as views into the consume buffer instead of copies. **Not yet effective** — messages are always copied today. | Leave unset. |
| `js.poll.worker` | `false` | Reserved for a Worker-based blocking poll mode (design §5.2). **Not yet effective.** The `js.consume.prefetch` experiment above is the shim-side realisation of the same idea. | Leave unset. |

Two librdkafka properties are worth calling out because bun-rdkafka's fast drain
changes their impact (both keep librdkafka's defaults to preserve upstream semantics):

- **`fetch.queue.backoff.ms`** (librdkafka default `1000`): how long librdkafka waits before
  refetching after its local queue was full. bun-rdkafka drains that queue much faster than
  N-API clients, so the default can cost >10× consumer throughput. **Recommended: `10`.**
- **`queue.buffering.max.messages`** (librdkafka default `100000`): also drives
  `js.producer.max.pending` (above).

## Tuning notes

- **Consumers: set `fetch.queue.backoff.ms=10`** — the single most important knob (see the note under [Configuration](#configuration-js-options); librdkafka's default of 1000 ms can cost >10× throughput with bun-rdkafka's fast drain).
- **Latency-sensitive consumers: lower `js.poll.idle.max.ms`** (e.g. `5`–`10`) — it bounds the added latency on an idle consumer at a small idle-CPU cost.
- **High-throughput flowing consumers on ≥ 2 cores:** try the experimental `js.consume.prefetch=true`.

## Architecture

A C shim (`libbunrdkafka`) statically links librdkafka and all its dependencies into exactly one shared library, flattens the API into ~40 FFI-friendly `brk_*` functions, and turns every librdkafka callback into pollable event queues. The TypeScript side dlopens this library via `bun:ffi` and actively polls/drains in batches using a packed binary protocol (one FFI call ↔ thousands of messages), with an adaptive scheduler that backs off when idle. On top of that core sit the two API layers, matching confluent-kafka-javascript. Full design docs are in [`docs/`](./docs/README.md).

## Documentation

- [MIGRATION.md](./MIGRATION.md) — migrating from confluent-kafka-javascript: what changes (almost nothing) and the honest list of behavioral differences.
- [CONTRIBUTING.md](./CONTRIBUTING.md) — building the native shim, running tests and benchmarks.
- [docs/](./docs/README.md) — specification, detailed design, implementation plan.
- API reference: `bun run docs:api` generates the TypeDoc site into `docs/api/`.

## License

MIT. Not affiliated with or endorsed by Confluent Inc. `confluent-kafka-javascript` and `librdkafka` are projects of Confluent Inc., used here under their MIT/BSD licenses.
