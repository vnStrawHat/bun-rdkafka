# Migrating from confluent-kafka-javascript

bun-rdkafka is API-compatible with [confluent-kafka-javascript](https://github.com/confluentinc/confluent-kafka-javascript) at the surface and observable-semantics level. For most applications the migration is one line:

```diff
- const Kafka = require("@confluentinc/kafka-javascript");
+ const Kafka = require("@vnstrawhat/bun-rdkafka");
```

Both API styles are provided: the callback API (`Producer`, `HighLevelProducer`, `KafkaConsumer`, `AdminClient`, `CODES`) and the promisified `KafkaJS` namespace. The four official upstream examples (callback producer/consumer, KafkaJS producer/consumer) run verbatim in this repo with only the import changed — see `examples/upstream-*.js`.

Compatibility is enforced mechanically: `test/conformance/` parses upstream's published `.d.ts` files and fails the build if any API is missing without a documented reason. The full machine-checked exclusion list (with reasons) lives in `test/conformance/exclusions.ts`; this document is its human-readable summary.

If you are migrating from **KafkaJS** (the original library) rather than from confluent-kafka-javascript: bun-rdkafka deliberately preserves the differences that Confluent documented in their [migration guide](https://github.com/confluentinc/confluent-kafka-javascript/blob/master/MIGRATION.md) (e.g. `acks`/`compression`/`timeout` are producer-level not per-send, `fromBeginning`/`autoCommit` are consumer-level, errors are distinguished by `error.code`), so their guide applies to bun-rdkafka unchanged.

---

## 1. No change needed

- All librdkafka configuration properties pass through unmodified (both global and topic config).
- The `kafkaJS` config block and its key set (13 common / 10 producer / 19 consumer keys) behave as upstream, including the rule that defaults only apply when the block is non-empty and that top-level librdkafka properties override translated ones.
- Message shapes, delivery-report shapes, `RecordMetadata`, header handling (including array-valued headers), rebalance flows (eager and cooperative-sticky, with `rebalance_cb` override), `partition.eof`, offset commit/seek/pause/resume semantics, transactions/EOS, SASL PLAIN / SCRAM, OAUTHBEARER token refresh callbacks.
- Errors: `LibrdKafkaError` with `code`/`origin`/`isFatal`/`isRetriable`/`isTxnRequiresAbort`; `CODES.ERRORS` matches upstream 175/175 (including legacy aliases). `KafkaJSError` distinguished by `error.code`, as upstream.

## 2. Behavioral differences (intentional, verified by tests)

| Area | Difference | Why |
|---|---|---|
| `librdkafkaVersion`, `features` | Functions (`librdkafkaVersion()`), not string/array constants. `features()` lands with the next ABI addition. | Constants would force the native library to load at import time; bun-rdkafka loads it lazily so the package imports (and unit-tests) without a compiled binary. |
| Client `name` | `"producer#1"` format instead of `"rdkafka#producer-1"`. | Simplification; change is cosmetic. |
| Producer per-record errors | An error for record *N* surfaces on the *next* `produce()` call (or at `flush()`), not on the call that staged it. Queue-full from the message-count cap (`js.producer.max.pending`, default follows `queue.buffering.max.messages`) still throws synchronously. Byte-based caps (`queue.buffering.max.kbytes`) surface via delivery-report errors. | `produce()` stages records and crosses FFI once per microtask — the main producer performance win. |
| Delivery-report `key` | Keeps the type you passed in (a string stays a string); upstream always converts to Buffer. | Avoids information loss; revisit on demand. |
| Transactions | `initTransactions`/`commitTransaction`/`abortTransaction`/`sendOffsetsToTransaction` do **not** block the event loop (internally retried in ≤100 ms steps). Observable behavior — err-first callbacks, `isRetriable`, `isTxnRequiresAbort` — is unchanged. | Upstream blocks a worker thread; Bun's event loop must stay responsive (NFR-2). |
| `setPollInterval()` | A compatibility no-op safety net: the internal scheduler already polls adaptively. Calling it is harmless. | Polling is owned by the runtime, not the app. |
| `connect()` | Performs a blocking metadata request (up to `timeout`, default 30 s, measured ~200 ms against a live broker). During a failed connect, errors arrive at the connect callback; intermediate `event.error` emissions are not observable mid-connect. | The single deliberately-blocking cold path. |
| `committed()` (callback API) | Blocks the event loop up to `timeout` (cold path, same contract as `connect()`). | Same as above. |
| `consume(cb)` (flowing) | Calls `cb(null, message)` per message — this matches upstream's *runtime* behavior; their `.d.ts` incorrectly declares an array. | We follow the implementation, not the typings bug. |
| Header values | `null` header values decode as empty Buffers (upstream types don't allow null). | Type-compat. |
| Latency profile | Poll-based event model: e2e p99 ≈ 6 ms vs ≈ 3 ms upstream at 10k msg/s. Tune `js.poll.idle.max.ms` (default 50) down for latency-sensitive consumers. | Inherent pull-model trade-off; see `bench/RESULTS.md`. |
| `KafkaJS.Consumer.stop()` | Actually implemented (stops `run()`, keeps the connection, `run()` again works). Upstream throws “not implemented”. | Superset — code written for upstream is unaffected. |
| `KafkaJS` `rebalance_cb` with `assignmentFns` | Same contract as upstream: `cb(err, assignment, {assign, unassign, assignmentLost})` is awaited; `assignmentFns.assign(x)` (or a truthy return value) replaces the default assignment, a throwing callback is logged and the default continues. Two details differ: the internal client's `rebalance` event fires right after your callback returns (before the asynchronous assign lands, upstream fires it after), and the final revoke of `disconnect()` is answered natively and does not reach the callback. Pending `seek()`s are folded into the assign call as start offsets. | The rebalance reaches JS as a polled event, not a librdkafka thread callback; the shim answers the close-time revoke itself. |
| `KafkaJS.Consumer.storeOffsets()` | Provided (upstream declares it in `.d.ts` only). A user-stored offset becomes the partition's next-unprocessed reference for `pause()`/rewind until the run() scheduler stores again after the next processed message. | Interplay with the per-partition scheduler; documented in `consumer.ts`. |
| `KafkaJS.Consumer.committed()` | Blocks the event loop up to `timeout` (default -1 = infinite, as upstream) — the same cold-path contract as the callback API's `committed()`. | Same as above. |
| OAUTHBEARER | You must provide an `oauthbearer_token_refresh_cb` (or the KafkaJS `sasl.oauthBearerProvider`); librdkafka's built-in unsecured-JWT fallback does not run because the shim always uses the event API. A clear `ERR__AUTHENTICATION` explains this if the callback is missing. | Event-queue architecture. |
| `AdminClient.create()` | Does not block; every method internally awaits readiness. Observable behavior is the same (you can call methods immediately after `create()`). | Non-blocking policy. |
| `OffsetSpec` | A constant object (`OffsetSpec.EARLIEST/LATEST/MAX_TIMESTAMP`), not a class; pass timestamps as plain numbers to `listOffsets()`. Constant usage is upstream-identical. | No wrapper objects on the FFI path. |
| `setSaslCredentialProvider` (KafkaJS) | Not provided — this name is a bug in upstream's `.d.ts`; their implementation is `setSaslCredentials()`, which bun-rdkafka provides on all three API layers (verified with live re-authentication tests). | We follow upstream's implementation, not its typings. |
| Extra `js.*` config namespace | bun-rdkafka-specific options: `js.poll.idle.max.ms`, `js.consume.buffer.bytes`, `js.producer.max.pending`, `js.consumer.max.batch.size`, etc. Unknown `js.*` keys throw (typo protection). | Runtime tuning knobs that have no librdkafka equivalent. |

| Stream API (`createReadStream` / `createWriteStream`) | Same options, events, `stream.producer` / `stream.consumer` fields and `close(cb)`. Internals follow the modern `node:stream` lifecycle: `autoClose` maps to `autoDestroy`, so `stream.destroy()`, `pipeline()` and breaking out of `for await` also release the client; `ProducerStream` flushes before disconnecting; `ERR__QUEUE_FULL` retries with a 5→500 ms backoff instead of a fixed 500 ms; a byte-mode `ConsumerStream` skips tombstones (upstream pushes `null`, which ends the stream); `topics` may be omitted to read the consumer's existing subscription/assignment. | Built on Bun's `node:stream` rather than ported 1:1. |

Also note the tuning recommendation that applies to any librdkafka client but bites harder here: set `fetch.queue.backoff.ms=10` on fast consumers (see README “Tuning notes”).

### Stream API usage

```ts
import { Producer, KafkaConsumer } from "@vnstrawhat/bun-rdkafka";

const writeStream = Producer.createWriteStream({ "bootstrap.servers": "localhost:9092" }, {}, { topic: "topic-name" });
writeStream.on("error", (err) => console.error(err));
writeStream.write(Buffer.from("Awesome message"));
writeStream.end(); // autoClose: flush + disconnect

const readStream = KafkaConsumer.createReadStream(
  { "bootstrap.servers": "localhost:9092", "group.id": "g1" }, {}, { topics: ["topic-name"] },
);
readStream.on("data", (message) => console.log(message.value.toString()));
readStream.consumer.commit(); // the underlying KafkaConsumer is exposed
```

See `examples/streams.ts` for a runnable version.

## 3. Not yet implemented

Each entry is tracked in `test/conformance/exclusions.ts` with a milestone; the conformance suite fails if an entry ships but stays on this list.

| API | Notes |
|---|---|
| `KafkaConsumer.offsetsForTimes` | Needs a new async shim entry point (must not block the event loop). |
| `Client.getLastError` | Use the `event.error` event, as upstream itself recommends. |
| `HighLevelProducer.setTopicKeySerializer` / `setTopicValueSerializer` | Global `setKeySerializer`/`setValueSerializer` are available. |
| `features` (as a function) | Pending one shim symbol. |
| SASL OAUTHBEARER OIDC (`sasl.oauthbearer.method=oidc`) | Requires rebuilding the shim with `WITH_CURL=ON`. Custom token-refresh callbacks work today. |
| `createTopics` `validateOnly`/`waitForLeaders`/`replicaAssignment`, `fetchOffsets` `resolveOffsets` | Throw “not implemented” — **exactly as upstream does**. |

## 4. Not applicable (will not be implemented)

| API | Why |
|---|---|
| `Client.getClient()` | Upstream returns the raw N-API binding object; no such object exists here, and exposing the native handle would break memory-safety guarantees. |
| `Producer.setPollInBackground()` | Polling is owned by the adaptive scheduler; there is no N-API background thread to move it to. Nearest equivalent: `setPollInterval()` (itself a safety net). |
| `KafkaConsumer.setDefaultConsumeLoopTimeoutDelay()` | Tunes node-rdkafka's consume-loop thread, which does not exist here. `setDefaultConsumeTimeout()` is available. |
