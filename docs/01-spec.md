# bun-rdkafka — Requirements Specification

> A high-performance Kafka client, native to Bun, built on librdkafka via `bun:ffi`,
> providing an API compatible with [confluent-kafka-javascript](https://github.com/confluentinc/confluent-kafka-javascript).

- **Status:** Draft v1
- **Date:** 2026-08-14
- **Related documents:** [02-detail-design.md](./02-detail-design.md), [03-implementation-plan.md](./03-implementation-plan.md)

---

## 1. Background & Motivation

`confluent-kafka-javascript` is Confluent's official client for Node.js: an N-API wrapper
(node-gyp) around **librdkafka**, offering two API styles:

1. **Callback API** — inherited from node-rdkafka (`Producer`, `KafkaConsumer`, `AdminClient`).
2. **Promisified API** — the `KafkaJS` namespace, modeled after the KafkaJS API (`Kafka`, `producer()`, `consumer()`, `admin()`).

That client can run on Bun through Bun's Node-API compatibility layer, but it takes no
advantage of anything Bun-specific while carrying the full cost of N-API and node-gyp. The
goal of **bun-rdkafka** is to build an API-equivalent client that:

- Uses **`bun:ffi`** (dlopen) instead of N-API — eliminating the node-gyp/node-addon-api layer.
- Designs the data path specifically for Bun: batches every FFI boundary crossing and uses a
  binary protocol instead of per-field object marshalling.
- Targets Bun only, carrying no Node.js/Deno compatibility cost.

## 2. Goals

| # | Goal | Measure |
|---|----------|----------|
| G1 | API compatible with confluent-kafka-javascript (both API styles) | The official confluent-kafka-javascript examples run by changing only the imported package name |
| G2 | Bun-native, using `bun:ffi` | No N-API, no node-gyp, no runtime dependency other than `bun` |
| G3 | High performance | Produce/consume throughput ≥ 1.5× confluent-kafka-javascript running on Bun, ≥ 1.2× running on Node (same machine, same broker, standard benchmark in the repo) |
| G4 | Prebuilt binaries — `bun add` and it just works, no C toolchain required | A clean install on a machine without a compiler still works |
| G5 | Multi-platform CI/CD | GitHub Actions builds + releases binaries for the full platform matrix in §6 |
| G6 | Modern architecture, easy to maintain/extend | Clear layering (native shim / ffi / core / api), strict TypeScript, tests + benchmarks in the repo |
| G7 | As few dependencies as possible | Runtime dependencies = 0 (other than the project's own prebuilt packages); minimal devDependencies |

## 3. Non-goals

- **No** Node.js, Deno, or browser compatibility. No polyfills, no other-runtime checks.
- **No** copying of confluent-kafka-javascript's internal architecture. Compatibility is only committed at the API surface and observable semantics; internals are designed independently, optimized for Bun.
- **No** pure-TypeScript implementation of the Kafka protocol (that is KafkaJS's approach; we use librdkafka).
- **No** Schema Registry client (same as upstream — they split it into `@confluentinc/schemaregistry`). Possibly a satellite project later.
- **No** mock/in-memory broker support.
- The first version does **not** support musl/Alpine or 32-bit Linux x86 (may be added later, see §6).

## 4. Functional Requirements

### FR-1. Callback API (node-rdkafka-style compatible)

Classes and behavior must match confluent-kafka-javascript:

- **`Producer`** — `connect()`, `disconnect()`, `produce(topic, partition, message, key, timestamp, opaque, headers)`, `flush()`, `poll()`, `setPollInterval()`; transaction API: `initTransactions()`, `beginTransaction()`, `commitTransaction()`, `abortTransaction()`, `sendOffsetsToTransaction()`.
- **`HighLevelProducer`** — `produce(..., callback)` with a per-message delivery-report callback.
- **`KafkaConsumer`** — `connect()`, `subscribe()`, `unsubscribe()`, `consume()` (flowing & non-flowing), `commit()`, `commitSync()`, `committed()`, `seek()`, `assign()`/`unassign()`, `incrementalAssign()`/`incrementalUnassign()`, `assignments()`, `position()`, `pause()`/`resume()`, `offsetsStore()`, `getWatermarkOffsets()`, `queryWatermarkOffsets()`.
- **`AdminClient.create()`** — `createTopic()`, `deleteTopic()`, `createPartitions()`, `listTopics()`, `listGroups()`, `describeGroups()`, `deleteGroups()`, `fetchOffsets()`, `deleteRecords()`, `describeTopics()`, `listConsumerGroupOffsets()`.
- **Events** (EventEmitter): `ready`, `data`, `delivery-report`, `disconnected`, `event.error`, `event.log`, `event.stats`, `event.throttle`, `rebalance`, `offset.commit`, `subscribed`, `unsubscribed`.
- **Configuration**: accepts librdkafka properties verbatim (`metadata.broker.list`, `linger.ms`, …) in global config and topic config, passed through to librdkafka; callback properties (`rebalance_cb`, `offset_commit_cb`) accept functions like upstream.
- **`CODES.ERRORS`**, the `LibrdKafkaError` class with `code`, `origin`, `message`.

### FR-2. Promisified API (`KafkaJS` namespace, compatible with Confluent's KafkaJS style)

- **`Kafka`** — constructor accepts `{ kafkaJS: CommonConstructorConfig }` and/or librdkafka properties; `producer()`, `consumer()`, `admin()`.
- **`Producer`** — `connect()`, `disconnect()`, `send({topic, messages})`, `sendBatch()`, `flush()`, `transaction()` (returns a transactional context with `send`/`sendOffsets`/`commit`/`abort`).
- **`Consumer`** — `connect()`, `disconnect()`, `subscribe({topics, replace?})`, `run({eachMessage?, eachBatch?, partitionsConsumedConcurrently?})`, `commitOffsets()`, `seek()`, `pause()`/`resume()`, `assignment()`, `stop()`.
- **`Admin`** — the upstream command set: `createTopics`, `deleteTopics`, `listTopics`, `listGroups`, `describeGroups`, `deleteGroups`, `fetchOffsets`, `fetchTopicMetadata`, `fetchTopicOffsets`, `fetchTopicOffsetsByTimestamp`, `deleteTopicRecords`.
- Preserve **the differences from original KafkaJS** that Confluent has settled on (so their migration guide applies unchanged): `acks/compression/timeout` configured at producer creation rather than per-send; `fromBeginning` at consumer creation; `autoCommit` at consumer creation; errors distinguished by `error.code`; `eachBatch` size controlled by `js.consumer.max.batch.size`; a transactional producer must use `transaction()`.
- `logLevel`, custom logger, `CompressionTypes` (GZIP/SNAPPY/LZ4/ZSTD), `PartitionAssigners`.

### FR-3. Required Kafka features (via librdkafka)

- Idempotent & transactional producer (EOS).
- Consumer groups: cooperative-sticky + eager rebalancing, incremental assign.
- SSL/TLS, SASL PLAIN / SCRAM-SHA-256/512 / GSSAPI (build-dependent) / **OAUTHBEARER** (including OIDC).
- Compression: gzip, snappy, lz4, zstd.
- Headers, timestamps, murmur2 partitioner (default compatible with the Java client, like upstream).
- Statistics (`statistics.interval.ms` → `event.stats`), log forwarding, throttle events.

### FR-4. Distribution

*(Revised 2026-08-14 by user decision — replaces the earlier per-platform-packages /
no-postinstall model; see design ADR-8.)*

- A **single npm package `@vnstrawhat/bun-rdkafka`** (repo: <https://github.com/vnStrawHat/bun-rdkafka>).
  The tarball contains the TypeScript source, the installer scripts, and the native shim
  sources (`native/` + `librdkafka.version`) for the source-build fallback — no binaries.
- CI builds prebuilt binaries for the 5 Tier-1 targets and attaches them to the
  **GitHub Release** of every tag (`libbunrdkafka-<target>.tar.gz` + `SHA256SUMS`).
- A **postinstall script** downloads the asset matching the current platform from the
  release of the installed version, verifies its SHA-256, and unpacks it into
  `<package>/prebuilds/<target>/`. If no matching asset exists (or the download/checksum
  fails), it **falls back to building from source** (requires cmake + a C compiler).
- Escape hatches: `BUN_RDKAFKA_LIB_PATH` (use an existing library),
  `BUN_RDKAFKA_BINARY_MIRROR` (air-gapped mirror), `BUN_RDKAFKA_SKIP_DOWNLOAD`,
  `BUN_RDKAFKA_FORCE_BUILD`, and a manual `bunx bun-rdkafka-install` bin (needed on Bun
  when the package is not in `trustedDependencies`, since Bun blocks dependency
  lifecycle scripts by default).

## 5. Non-functional Requirements

| ID | Requirement |
|----|---------|
| NFR-1 | **Performance**: meet G3; the number of FFI boundary crossings must be O(batch), not O(message), in both the produce and consume directions |
| NFR-2 | **Never block the event loop**: every FFI call on the main thread must be non-blocking (timeout 0); waiting uses adaptive scheduling |
| NFR-3 | **Memory safety**: no double-free/use-after-free; handles guarded by a state machine + `FinalizationRegistry` as the last safety net; the process exits cleanly without `disconnect()` (with a warning) |
| NFR-4 | **Strict TypeScript** (`strict: true`), fully typed API, published with `.d.ts` |
| NFR-5 | **Runtime dependencies = 0**; minimal devDeps (typescript + tests use the built-in `bun test`) |
| NFR-6 | **Version stability**: pin one specific librdkafka version per release (recorded in `librdkafka.version`); expose `librdkafkaVersion` |
| NFR-7 | **Size**: npm tarball < 1 MB (source + shim sources only); each release asset contains exactly 1 shared library (~5–15 MB due to statically linked OpenSSL/zstd) |
| NFR-8 | **Observability**: librdkafka logs forwarded to a JS logger; stats JSON exposed verbatim |

## 6. Supported platforms

| Target | Tier | Notes |
|--------|------|---------|
| linux-x64 (glibc ≥ 2.28) | 1 | built on a manylinux-equivalent (AlmaLinux 8 container) |
| linux-arm64 (glibc ≥ 2.28) | 1 | GitHub `ubuntu-24.04-arm` runner |
| darwin-arm64 (macOS ≥ 12) | 1 | `macos-14` runner |
| darwin-x64 (macOS ≥ 12) | 1 | `macos-13` runner |
| win32-x64 | 1 | `windows-2022` runner, MSVC |
| linux-{x64,arm64}-musl | 2 (later) | Alpine; needs a separate build since Bun ships a musl build |
| win32-arm64 | 2 (later) | when GitHub runners are available |

Minimum Bun: **1.2** (stable `bun:ffi`, `dlopen`, `FinalizationRegistry`).

## 7. Key technical constraints (shaping the design)

1. **`bun:ffi` has no safe mechanism for native threads to call back into JS.**
   Bun's `JSCallback` is not reliably threadsafe, while librdkafka fires callbacks
   (delivery reports, rebalance, log, stats) from its internal threads.
   ⇒ Mandatory architecture: **no C→JS callbacks**. Every event is turned by the C shim into
   an event on an `rd_kafka_queue_t`, and JS actively **polls + drains in batches** (details in
   [02-detail-design.md](./02-detail-design.md) §4, §6).
2. **FFI calls have a fixed cost** (~ns–µs per call) ⇒ the hot path must batch: one FFI call
   returns thousands of messages in one binary buffer, decoded with `DataView`.
3. The librdkafka C API is a stable ABI, but many structs (admin API, metadata) are hard to use
   directly through plain FFI ⇒ a **C shim** is needed to flatten the API (flat functions, POD
   buffers) and statically link librdkafka + deps (OpenSSL, zlib, zstd, lz4) into **one** single
   shared library.

## 8. v1.0 acceptance criteria

- [ ] All of FR-1..FR-4 complete; API surface verified by a conformance test cross-checked against confluent-kafka-javascript's `.d.ts`.
- [ ] Integration tests (producer/consumer/admin/transactions/oauthbearer) green on CI against a real broker (Apache Kafka KRaft container) for linux-x64.
- [ ] In-repo benchmark demonstrating G3, results published in `bench/RESULTS.md`.
- [ ] `bun add @vnstrawhat/bun-rdkafka` on a clean machine for all 5 Tier-1 targets → the producer + consumer examples run.
- [ ] Release pipeline: pushing tag `vX.Y.Z` → CI builds 5 binaries, creates a GitHub Release, publishes 6 npm packages, automated and idempotent.
- [ ] Documentation: README, API docs (typedoc), MIGRATION.md from confluent-kafka-javascript (~"change the import and you're done" + a table of differences if any).

## 9. Key risks

| Risk | Impact | Mitigation |
|--------|-----------|------------|
| Polling model increases latency when idle or burns CPU when busy-polling | Performance/UX | Adaptive poll scheduler (design §6); optional blocking-poll on a Worker thread in a later phase |
| Building static librdkafka + OpenSSL on Windows/MSVC is complex | Schedule slip | Use vcpkg for Windows; librdkafka has an official vcpkg port |
| confluent-kafka-javascript's API surface is wide; behavioral details are hard to match 100% | Compatibility | Prioritize by real-world usage; conformance test generated from upstream `.d.ts`; document differences explicitly |
| Bun changes `bun:ffi` | Stability | Pin a minimum Bun version; CI runs on both Bun stable and canary |
| Buffer reuse on the consume path causes aliasing if the user keeps references | Correctness | Safe copy-per-message by default; opt-in zero-copy mode with documented warnings (design §7.3) |
