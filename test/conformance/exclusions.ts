/**
 * exclusions.ts — the list of confluent-kafka-javascript APIs that bun-rdkafka
 * **deliberately** does not (yet) provide.
 *
 * Mechanism (see `api-surface.test.ts`):
 *  - An API missing **without** an entry here  → test FAILS (unintended gap).
 *  - An entry here whose API is **no longer missing** → test FAILS (forcing
 *    list cleanup).
 *
 * `path` uses the `<group>.<member>` format the conformance report prints, e.g.:
 *  - `rdkafka:module.features`
 *  - `rdkafka:Producer.setPollInBackground`
 *  - `kafkajs:Consumer.storeOffsets`
 *  - `rdkafka:module.librdkafkaVersion#kind` — the name exists but its *kind* differs from upstream
 *    (const ↔ function).
 *  - `rdkafka:Producer.produce#arity` — the name exists but the arity conflicts with upstream.
 *
 * `reason` must be honest and say *why*; `milestone` is filled in when the
 * remaining work is substantial and has a slot in docs/03-implementation-plan.md.
 */

export interface Exclusion {
  /** `<group>.<member>` as the conformance report prints it. */
  path: string;
  /** Why it is not (yet) done — written for report readers, not the compiler. */
  reason: string;
  /** Planned milestone; empty means "not applicable, will not be done". */
  milestone?: "M6" | "M7" | "M8";
}

export const EXCLUSIONS: readonly Exclusion[] = [
  /* ---------------------------------------------------------------------- *
   * 1. Not applicable to the bun:ffi architecture — will NOT be done        *
   * ---------------------------------------------------------------------- */
  {
    path: "rdkafka:Client.getClient",
    reason:
      "Not applicable: upstream returns the raw N-API binding object. bun-rdkafka " +
      "has no such object — the native handle is a pointer managed by NativeClient, " +
      "and exposing it to JS would break the NFR-3 memory-safety guarantee.",
  },
  {
    path: "rdkafka:Producer.setPollInBackground",
    reason:
      "Not applicable: upstream moves polling onto an N-API background thread. " +
      "bun-rdkafka polls via the PollScheduler (HOT/WARM/COLD, design §6), " +
      "self-managed on the event loop — spec §7.1 forbids C→JS callbacks from " +
      "native threads. The closest equivalent already exists: setPollInterval().",
  },
  {
    path: "rdkafka:KafkaConsumer.setDefaultConsumeLoopTimeoutDelay",
    reason:
      "Not applicable: a node-rdkafka consume-loop tuning knob. bun-rdkafka's " +
      "consume cadence is decided by the PollScheduler based on load; " +
      "setDefaultConsumeTimeout() still exists to tune the per-drain timeout.",
  },

  /* ---------------------------------------------------------------------- *
   * 2. Deliberate deviations preserving native lazy-load (index.ts must     *
   *    import before the .so is built — a precondition for all unit tests)   *
   * ---------------------------------------------------------------------- */
  {
    path: "rdkafka:module.librdkafkaVersion#kind",
    reason:
      "Deliberate deviation: upstream has `const librdkafkaVersion: string`, " +
      "bun-rdkafka has `librdkafkaVersion(): string`. A string constant forces a " +
      "dlopen at package import time; loader.ts is intentionally lazy (see the " +
      "ffi/loader.ts doc comment) so importing never pulls in native. To be noted " +
      "in MIGRATION.md at M8.",
    milestone: "M8",
  },
  {
    path: "rdkafka:module.features#kind",
    reason:
      "Deliberate deviation for the same reason as librdkafkaVersion: upstream has " +
      "`const features: string[]` (read from librdkafka at import time), bun-rdkafka " +
      "has `features(): string[]` (lazy — no dlopen at import). The default export " +
      "(`import Kafka from …; Kafka.features`) exposes it as an upstream-style lazy " +
      "getter. To be noted in MIGRATION.md at M8.",
    milestone: "M8",
  },

  {
    path: "rdkafka:module.OffsetSpec#kind",
    reason:
      "Deliberate deviation: upstream has `class OffsetSpec` with " +
      "`new OffsetSpec(timestamp)`; bun-rdkafka has a constant object " +
      "{ EARLIEST, LATEST, MAX_TIMESTAMP } with the exact `rd_kafka_OffsetSpec_t` " +
      "values, and timestamps are passed to listOffsets as plain ms numbers — no " +
      "wrapper object needed on the FFI path. `OffsetSpec.EARLIEST/LATEST/" +
      "MAX_TIMESTAMP` works exactly like upstream.",
  },

  /* ---------------------------------------------------------------------- *
   * 3. Outside the FR-1/FR-2 commitments — the Stream API                   *
   * ---------------------------------------------------------------------- */
  {
    path: "rdkafka:module.createReadStream",
    reason:
      "Not committed: spec FR-1 §4 does not list the Stream API " +
      "(ConsumerStream/ProducerStream). Needs node:stream Readable/Writable layers " +
      "+ dedicated backpressure. To be considered at M8.",
    milestone: "M8",
  },
  {
    path: "rdkafka:module.createWriteStream",
    reason: "Not committed — see rdkafka:module.createReadStream.",
    milestone: "M8",
  },
  {
    path: "rdkafka:KafkaConsumer.static.createReadStream",
    reason: "Not committed — see rdkafka:module.createReadStream.",
    milestone: "M8",
  },
  {
    path: "rdkafka:Producer.static.createWriteStream",
    reason: "Not committed — see rdkafka:module.createReadStream.",
    milestone: "M8",
  },

  /* ---------------------------------------------------------------------- *
   * 3b. The upstream `.d.ts` diverging from upstream's own implementation   *
   * ---------------------------------------------------------------------- */
  {
    path: "kafkajs:Producer.setSaslCredentialProvider",
    reason:
      "A bug in upstream's types: `types/kafkajs.d.ts` declares " +
      "`setSaslCredentialProvider(authInfo)` but their own implementation " +
      "(`lib/kafkajs/_producer.js`) defines `setSaslCredentials(args)`. " +
      "bun-rdkafka theo implementation — `Producer.setSaslCredentials({username, " +
      "password})` already exists and matches the behavior. No alias for the wrong name.",
  },
  {
    path: "kafkajs:Consumer.setSaslCredentialProvider",
    reason:
      "The same types bug as kafkajs:Producer.setSaslCredentialProvider, and worse: " +
      "upstream's `lib/kafkajs/_consumer.js` has no SASL-credential-changing method " +
      "at all — the `.d.ts` declaration is a nonexistent API. We will follow upstream " +
      "if they actually add it (the Callback API's Client.setSaslCredentials is ready).",
  },

  /* ---------------------------------------------------------------------- *
   * 4. Requiring substantial C-shim work (new ABI) — recorded in the plan   *
   * ---------------------------------------------------------------------- */
  {
    path: "kafkajs:Consumer.logger",
    reason:
      "KafkaJS.Consumer does not hold its own Logger yet (Producer/Admin do). Needs " +
      "the logger built from the config in the constructor, then wired to the " +
      "underlying KafkaConsumer's 'event.log' — state + wiring, not an alias. " +
      "FR-2 §4 only commits to logLevel + a custom logger at the config level " +
      "(already present).",
    milestone: "M6",
  },
  {
    path: "kafkajs:Consumer.dependentAdmin",
    reason:
      "Depends on kafkajs:Consumer.logger: Admin(existingClient) requires the parent " +
      "client to have both _getInternalClient() and logger(). To be done together " +
      "with logger().",
    milestone: "M6",
  },
  {
    path: "kafkajs:Consumer.storeOffsets",
    reason:
      "The promisified Consumer manages offset stores inside the ADR-4b scheduler " +
      "(per-partition queue + epoch). Letting users store arbitrary offsets must " +
      "reconcile with the running epochs — real work, not an alias. Not listed in " +
      "FR-2 §4.",
    milestone: "M6",
  },
  {
    path: "kafkajs:Consumer.committed",
    reason:
      "Needs KafkaConsumer.committed() (blocking on librdkafka's side) wrapped in " +
      "the async pattern with KafkaJS's string offset format. Not listed in FR-2 §4.",
    milestone: "M6",
  },
];
