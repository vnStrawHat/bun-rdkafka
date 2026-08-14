# bun-rdkafka

A native Apache Kafka client for [Bun](https://bun.sh): [librdkafka](https://github.com/confluentinc/librdkafka) bound through `bun:ffi`, with an API compatible with [confluent-kafka-javascript](https://github.com/confluentinc/confluent-kafka-javascript).

> **Status: pre-release.** Not yet published to npm. Built and fully tested on linux-x64; the multi-platform CI and prebuilt-binary pipeline are configured but not yet exercised (see [Platform support](#platform-support)).

## Why

- **confluent-kafka-javascript does not run on Bun 1.4.** Its prebuilt N-API binaries stop at `NODE_MODULE_VERSION` 137 (Node ≤ 24), while Bun 1.4 requires 147. We verified this experimentally — see `bench/RESULTS.md` §M6.
- **Even where the N-API path works, it is not the fast path for Bun.** bun-rdkafka is designed around `bun:ffi` from the ground up: a thin C shim statically links librdkafka into a single shared library, converts every librdkafka callback into pollable event queues (no cross-thread C→JS calls), and moves data across the FFI boundary in packed binary batches — one FFI call carries thousands of messages, decoded with `DataView`.
- **Same API, drop-in migration.** Both API styles of confluent-kafka-javascript are provided: the classic callback API (`Producer`, `KafkaConsumer`, `AdminClient`) and the promisified `KafkaJS` namespace (`Kafka`, `producer()`, `consumer()`, `admin()`). The official upstream examples run verbatim with only the import line changed (`examples/upstream-*.js`). See [MIGRATION.md](./MIGRATION.md).

## Install

Requires Bun ≥ 1.2. (Until the first npm release, build from source — see [CONTRIBUTING.md](./CONTRIBUTING.md).)

```sh
bun add @vnstrawhat/bun-rdkafka
```

> **Bun blocks dependency lifecycle scripts by default**, and bun-rdkafka uses a
> `postinstall` script to fetch its prebuilt native binary. Add the package to
> `trustedDependencies` in your `package.json`, then install:
>
> ```jsonc
> { "trustedDependencies": ["@vnstrawhat/bun-rdkafka"] }
> ```
>
> Alternatively run the installer once manually: `bunx bun-rdkafka-install`.

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

Requires a Kafka broker.

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

All four snippets above were verified against a real broker before being committed to this README. Configuration is librdkafka's own property set, passed through unmodified — see the [librdkafka configuration reference](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).

## Performance

Measured against `@confluentinc/kafka-javascript` 1.10.0 on Node 24 (same machine, same broker, identical librdkafka 2.15.0 configuration, 3-run medians; full method and raw data in [`bench/RESULTS.md`](./bench/RESULTS.md)):

| Case | bun-rdkafka / Bun | upstream / Node 24 | Ratio |
|---|---:|---:|---:|
| producer, 100 B, acks=1 | 1,041,241 msg/s | 722,873 | **1.44×** |
| producer, 100 B, acks=all | 977,427 msg/s | 676,209 | **1.45×** |
| producer, 1 KB, acks=1 | 439,613 msg/s | 593,242 | 0.74× |
| producer, 1 KB, acks=all | 450,779 msg/s | 579,320 | 0.78× |
| consumer, 100 B | 1,009,890 msg/s | 411,854 | **2.45×** |
| consumer, 1 KB | 668,644 msg/s | 281,888 | **2.37×** |
| e2e latency p50/p99 @10k msg/s | 4 / 6 ms | 2 / 3 ms | — |

Honest caveats:

- Benchmarked on a 4-vCPU / 3 GB box with the broker co-located, so absolute numbers are compressed; ratios are the meaningful signal.
- The 1 KB producer case is an *unbounded-burst microbenchmark* where bun-rdkafka currently loses: enqueueing the whole 600 MB burst near-instantly defeats pipeline overlap on shared CPUs. With a bounded queue (`queue.buffering.max.messages=65536`, closer to production configs) the same case reaches parity or better. Analysis and follow-ups in `bench/RESULTS.md` §M6d.
- Latency p99 is single-digit ms but roughly 2× upstream — the inherent cost of the poll-based event model. Tune `js.poll.idle.max.ms` down if latency matters more than idle CPU.

## Platform support

| Target | Build config | Tested |
|---|---|---|
| linux-x64 (glibc ≥ 2.28) | ✅ | ✅ full test suite + benchmarks, real broker |
| linux-arm64 (glibc ≥ 2.28) | ✅ CI matrix ready | ⏳ pending first CI run |
| darwin-arm64 (macOS ≥ 12) | ✅ CI matrix ready | ⏳ pending first CI run |
| darwin-x64 (macOS ≥ 12) | ✅ CI matrix ready | ⏳ pending first CI run |
| win32-x64 (MSVC, vcpkg) | ✅ CI matrix ready | ⏳ pending first CI run |

Prebuilt binaries for all five targets are built by CI and attached to every [GitHub Release](https://github.com/vnStrawHat/bun-rdkafka/releases); the postinstall script picks the right one (see [Install](#install)). Platforms without a prebuilt (e.g. musl/Alpine) fall back to the source build automatically.

## Tuning notes

- **Consumers: set `fetch.queue.backoff.ms` to a small value (e.g. `10`).** librdkafka's default of 1000 ms throttles refetching when the local queue was recently full; because bun-rdkafka drains very fast, the default can cost >10× consumer throughput. We keep librdkafka's default to preserve upstream semantics, so this is opt-in.
- `js.poll.idle.max.ms` (default 50) caps the poll backoff when idle — it is also the worst-case latency for a message arriving on an idle consumer, and for rebalance/offset-commit delivery. Lower it for latency-sensitive consumers at a small idle-CPU cost.
- Additional `js.*` options (staging, buffer sizes, batch sizes) are documented in the API docs (`bun run docs:api`).

## Architecture

A C shim (`libbunrdkafka`) statically links librdkafka and all its dependencies into exactly one shared library, flattens the API into ~40 FFI-friendly `brk_*` functions, and turns every librdkafka callback into pollable event queues. The TypeScript side dlopens this library via `bun:ffi` and actively polls/drains in batches using a packed binary protocol (one FFI call ↔ thousands of messages), with an adaptive scheduler that backs off when idle. On top of that core sit the two API layers, matching confluent-kafka-javascript. Full design docs are in [`docs/`](./docs/README.md).

## Documentation

- [MIGRATION.md](./MIGRATION.md) — migrating from confluent-kafka-javascript: what changes (almost nothing) and the honest list of behavioral differences.
- [CONTRIBUTING.md](./CONTRIBUTING.md) — building the native shim, running tests and benchmarks.
- [docs/](./docs/README.md) — specification, detailed design, implementation plan.
- API reference: `bun run docs:api` generates the TypeDoc site into `docs/api/`.

## License

MIT. Not affiliated with or endorsed by Confluent Inc. `confluent-kafka-javascript` and `librdkafka` are projects of Confluent Inc., used here under their MIT/BSD licenses.
