# bun-rdkafka

A native Apache Kafka client for [Bun](https://bun.sh): [librdkafka](https://github.com/confluentinc/librdkafka) bound through `bun:ffi`, API-compatible with [confluent-kafka-javascript](https://github.com/confluentinc/confluent-kafka-javascript).

- **Drop-in**: all three upstream API styles — the promisified `KafkaJS` namespace, the callback API (`Producer`, `KafkaConsumer`, `AdminClient`), and the Stream API (`createWriteStream` / `createReadStream`). Official upstream examples run with only the import changed.
- **Bun-native fast path**: no N-API. A C shim statically links librdkafka into one shared library; data crosses FFI in packed binary batches (one call ↔ thousands of messages). Measured 2.4× upstream consumer throughput and 1.4× producer throughput at small payloads.
- **Zero runtime dependencies**: a postinstall script fetches the prebuilt binary for your platform from the GitHub Release (SHA-256 verified) and falls back to building from the bundled C sources when no prebuilt matches.

## Install

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

If no prebuilt binary matches your platform, the installer falls back to building from
the bundled C sources (needs `cmake` and a C compiler). See the repository README for
details and environment variables (`BUN_RDKAFKA_LIB_PATH`, `BUN_RDKAFKA_BINARY_MIRROR`, …).

## Quick start

Requires a Kafka broker. Each API style is shown with a producer and a consumer.

### KafkaJS-style API (promisified)

```ts
import { KafkaJS } from "@vnstrawhat/bun-rdkafka";

const kafka = new KafkaJS.Kafka({ kafkaJS: { brokers: ["localhost:9092"] } });

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

const kafka = new KafkaJS.Kafka({ kafkaJS: { brokers: ["localhost:9092"] } });

const consumer = kafka.consumer({ kafkaJS: { groupId: "quickstart-group", fromBeginning: true } });
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

const producer = new Producer({ "metadata.broker.list": "localhost:9092", dr_cb: true });

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
  { "metadata.broker.list": "localhost:9092", "group.id": "quickstart-cb-group" },
  { "auto.offset.reset": "earliest" },
);

consumer.on("ready", () => {
  consumer.subscribe(["quickstart"]);
  consumer.consume(); // flowing mode: one 'data' event per message
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
  stream.write(Buffer.from(`hello from Bun #${i}`)); // return value = backpressure signal
}
stream.end(() => console.log("10 messages written"));
```

```ts
import { KafkaConsumer } from "@vnstrawhat/bun-rdkafka";

// A Readable stream in objectMode: one Message per chunk (also an async iterable).
const stream = KafkaConsumer.createReadStream(
  { "metadata.broker.list": "localhost:9092", "group.id": "quickstart-stream-group" },
  { "auto.offset.reset": "earliest" },
  { topics: ["quickstart"], fetchSize: 100 },
);
stream.on("error", (err) => console.error("consumer stream error", err));

for await (const message of stream) {
  console.log(`${message.topic}[${message.partition}]@${message.offset}: ${message.value}`);
}
```

Configuration is librdkafka's own property set, passed through unmodified — see the [librdkafka configuration reference](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).

Full documentation, benchmarks, and the migration guide: [project repository](https://github.com/vnStrawHat/bun-rdkafka).

License: MIT. Not affiliated with Confluent Inc.
