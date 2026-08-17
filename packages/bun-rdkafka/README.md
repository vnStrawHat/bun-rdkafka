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
| `js.poll.idle.max.ms` | `50` | Ceiling of the adaptive poll backoff when a client is idle. When there is data the client polls continuously; when data runs out it keeps polling every 1 ms for 100 ms (steady traffic stays at timer granularity), then backs off 1 → 2 → 4 … up to this value. It is therefore the **worst-case added latency** for a message arriving on an idle consumer, and for rebalance / offset-commit / delivery-report delivery. | Keep `50` for throughput-oriented workloads. Set `5`–`10` for latency-sensitive consumers (costs a little idle CPU). |
| `js.poll.interval.ms` | `500` | Poll interval while a client is *cold* (no subscription/assignment and no in-flight produce): only picks up log/stats/error events. The timer is `unref`'d, so an idle client never keeps the process alive. | Keep the default. Lower only if you need faster `event.stats`/`event.log` while idle. |
| `js.consume.buffer.bytes` | `262144` (256 KiB) | Cap of the buffer one `brk_consume_batch` FFI call fills (up to 500 messages per call). Every batch gets a fresh buffer sized to the traffic (up to this cap) and message key/value are views into it — no per-message copy — so retaining a message keeps at most one such buffer alive. It grows automatically if a single message does not fit, so this is a performance knob, not a limit. | Keep the default. Raise (e.g. `1048576`) if your messages are ≥ 64 KiB so batches stay full; values above ~512 KiB are slower on most allocators (fresh pages per batch). |
| `js.event.buffer.bytes` | `262144` (256 KiB) | Size of the reusable buffer for the event drain (`brk_events_poll`): delivery reports, rebalance/commit events, stats JSON. Grows automatically when a frame does not fit. | Keep the default. Raise if you enable `statistics.interval.ms` with many topics/partitions (the stats JSON can reach several hundred KiB). |
| `js.producer.max.pending` | = `queue.buffering.max.messages` (librdkafka default `100000`) | Backpressure threshold of the producer's delivery ledger — the number of produced messages still waiting for a delivery report. Beyond it `produce()` throws `ERR__QUEUE_FULL` synchronously (same as upstream), so you can `poll()` and retry. | Leave it tied to `queue.buffering.max.messages`. Lower both (e.g. `65536`) for large payloads to bound memory and keep the pipeline flowing. |
| `js.consumer.max.batch.size` | `32` | KafkaJS API only: the maximum number of messages handed to one `eachBatch` call. `eachMessage` is unaffected. | `32` is a good default; raise (`100`–`500`) when your batch handler amortizes work (bulk writes), lower for tighter per-message latency. |
| `js.consume.prefetch` | `false` | **Experimental.** Serializes consume batches on a shim-owned thread so the JS thread only decodes and emits. Measured +26–33 % consumer throughput on a machine with an idle core, at +33–40 % CPU (see the repository's `bench/RESULTS.md`). Prefetched frames are still delivered after `seek`/`pause`/revoke — see [docs/notes/consumer-prefetch-thread.md](https://github.com/vnStrawHat/bun-rdkafka/blob/main/docs/notes/consumer-prefetch-thread.md) before enabling. | Off by default. Try `true` for high-throughput flowing consumers on ≥ 2 cores when you commit from your handler (`enable.auto.commit=false`). |
| `js.consume.prefetch.frames` | `4` | Ring depth of the prefetch thread: how many `js.consume.buffer.bytes` frames may be filled ahead of the JS thread. | Keep `4`. More frames only add prefetch depth (memory + at-least-once exposure), not throughput. |
| `js.consumer.zero.copy` | `false` | Reserved for a stricter mode where message `value`/`key` are only valid inside the callback. **Not yet effective** — today messages are already views into a per-batch buffer that is never reused, so they are safe to keep. | Leave unset. |
| `js.poll.worker` | `false` | Reserved for a Worker-based blocking poll mode (design §5.2). **Not yet effective.** The `js.consume.prefetch` experiment above is the shim-side realisation of the same idea. | Leave unset. |

Two librdkafka properties are worth calling out because bun-rdkafka's fast drain
changes their impact (both keep librdkafka's defaults to preserve upstream semantics):

- **`fetch.queue.backoff.ms`** (librdkafka default `1000`): how long librdkafka waits before
  refetching after its local queue was full. bun-rdkafka drains that queue much faster than
  N-API clients, so the default can cost >10× consumer throughput. **Recommended: `10`.**
- **`queue.buffering.max.messages`** (librdkafka default `100000`): also drives
  `js.producer.max.pending` (above).


Full documentation, benchmarks, and the migration guide: [project repository](https://github.com/vnStrawHat/bun-rdkafka).

License: MIT. Not affiliated with Confluent Inc.
