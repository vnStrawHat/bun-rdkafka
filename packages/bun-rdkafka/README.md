# bun-rdkafka

A native Apache Kafka client for [Bun](https://bun.sh): [librdkafka](https://github.com/confluentinc/librdkafka) bound through `bun:ffi`, API-compatible with [confluent-kafka-javascript](https://github.com/confluentinc/confluent-kafka-javascript).

- **Drop-in**: both upstream API styles — the callback API (`Producer`, `KafkaConsumer`, `AdminClient`) and the promisified `KafkaJS` namespace. Official upstream examples run with only the import changed.
- **Bun-native fast path**: no N-API. A C shim statically links librdkafka into one shared library; data crosses FFI in packed binary batches (one call ↔ thousands of messages). Measured 2.4× upstream consumer throughput and 1.4× producer throughput at small payloads.
- **Zero runtime dependencies**: a postinstall script fetches the prebuilt binary for your platform from the GitHub Release (SHA-256 verified) and falls back to building from the bundled C sources when no prebuilt matches.

## Install

```sh
bun add @vnstrawhat/bun-rdkafka
```

Bun blocks dependency lifecycle scripts by default — add `"@vnstrawhat/bun-rdkafka"` to
`trustedDependencies` in your `package.json` (then reinstall), or run
`bunx bun-rdkafka-install` once manually. See the repository README for details
and environment variables (`BUN_RDKAFKA_LIB_PATH`, `BUN_RDKAFKA_BINARY_MIRROR`, …).

```ts
import { KafkaJS } from "@vnstrawhat/bun-rdkafka";

const kafka = new KafkaJS.Kafka({ kafkaJS: { brokers: ["localhost:9092"] } });

const producer = kafka.producer();
await producer.connect();
await producer.send({ topic: "quickstart", messages: [{ value: "hello from Bun" }] });
await producer.disconnect();
```

Full documentation, benchmarks, and the migration guide: [project repository](https://github.com/vnStrawHat/bun-rdkafka).

License: MIT. Not affiliated with Confluent Inc.
