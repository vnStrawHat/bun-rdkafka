# bun-rdkafka — Implementation Plan

> Requirements: [01-spec.md](./01-spec.md) · Design: [02-detail-design.md](./02-detail-design.md)

The plan is split into 8 milestones. Principle: **the biggest technical risk goes first**
(FFI + shim + poll model proven in M1 by a vertical slice of produce→consume running for
real), CI stands up from day one, and every milestone ends in a runnable/demoable state.

Estimates are in engineer-weeks (1 person full-time, reasonably familiar with librdkafka &
Bun). Total: **~14–18 weeks** to v1.0.

---

## M0 — Scaffold & CI skeleton (1 week)

> **Status 2026-08-14: COMPLETE locally** — scaffold, ABI header v1, CMake +
> shim building clean (librdkafka v2.15.0 static), loader + version test green, both
> workflows written. Remaining: verify CI on the 5 real targets (requires pushing to GitHub).

**Goal:** the repo laid out per design §2, with an empty shim building on all 5 targets.

- [ ] Initialize the bun workspace: `packages/bun-rdkafka`, `native/`, `test/`, `bench/`, strict tsconfig.
- [ ] `native/CMakeLists.txt`: FetchContent librdkafka at `librdkafka.version`, static build + shim skeleton (`brk_abi_version`, `brk_librdkafka_version`).
- [ ] Minimal `ffi/loader.ts` + `symbols.ts`; a "load the library, read the version" test.
- [ ] `.github/workflows/ci.yml`: 5-target build matrix (almalinux8 container, arm runner, macos-13/14, windows vcpkg), artifact upload, a unit job running the version test against the artifact.
- **Definition of Done (DoD):** CI green on all 5 targets; `bun test` prints the correct librdkafka version on all 5.
- **Risk to tackle early:** vcpkg/MSVC on Windows — if stuck, isolate it right at M0 rather than leaving it for M7.

## M1 — Vertical slice: produce → consume one message (2–3 weeks) ⚠ riskiest milestone

> **Status 2026-08-14: COMPLETE on linux-x64** (real broker, 8 integration +
> 179 unit tests green). Baseline (4 vCPU/3GB): produce 838k msg/s, consume 1.37M msg/s
> (hitting the broker ceiling), average event-loop drift < 2ms — see bench/RESULTS.md.
> **Decision point settled: adaptive polling PASSES; no Worker blocking-poll needed.**
> Operational note: librdkafka's default `fetch.queue.backoff.ms` of 1000ms is the main
> consume bottleneck — recommend a small value (10ms) in the user docs. Still missing
> versus the DoD: a run on darwin-arm64 + the ASan job (moved to CI, M6).

**Goal:** prove all architectural assumptions: the shim event-queue model, the packed
binary protocol, the PollScheduler, no event-loop blocking, no crashes.

- [ ] Shim: `brk_conf_*`, `brk_client_new/destroy`, `brk_events_poll` (DR + ERROR + LOG), `brk_produce_batch` (first version), `brk_consume_batch`, `brk_subscribe`.
- [ ] Core: `NativeClient` state machine, `PollScheduler` (HOT/WARM/COLD), `EventDecoder`, `BatchDecoder` (copy mode), `DeliveryLedger`, `ConfigBuilder` pass-through.
- [ ] Demo script: produce 1M messages of 100B → consume all 1M, checksums match, RSS stable.
- [ ] Measure a first performance baseline and record it (G3 not required yet).
- [ ] C↔TS fixture tests for the binary protocol (C harness generates fixtures, TS decodes).
- **DoD:** the demo runs on linux-x64 + darwin-arm64; the event loop stays responsive (concurrent timer test); ASan clean.
- **Decision point:** if adaptive polling can't reach acceptable latency/CPU → activate the Worker blocking-poll fallback early (design §5.2) before moving to M2.

## M2 — Callback API: complete Producer (2 weeks)

> **Status 2026-08-14: COMPLETE.** The upstream producer example runs verbatim (1
> require line changed). All 4 codecs + idempotent + EOS transactions green against a real
> broker. Transactions don't block the event loop (retriable loop of ≤100ms steps — an
> improvement over upstream's AsyncWorker). Intentional deviations pending M5 conformance:
> per-record errors surface on the next produce(); DR keys keep the user-supplied type.

- [ ] `Producer`/`HighLevelProducer` complete per FR-1: events, `flush`, `poll`, `setPollInterval`, headers, timestamps, opaque, partitioner config.
- [ ] Staging-buffer produce (batching FFI calls per microtask) + QUEUE_FULL backpressure.
- [ ] Transactions: `brk_*_transaction` + the `initTransactions`…`sendOffsetsToTransaction` API.
- [ ] `LibrdKafkaError` + `CODES.ERRORS` generated from the header.
- [ ] Producer integration tests (docker Kafka on CI): codecs, acks, idempotent, txn.
- **DoD:** the confluent-kafka-javascript producer example runs verbatim (import changed).

## M3 — Callback API: complete Consumer (2–3 weeks)

> **Status 2026-08-14: COMPLETE** (1 item blocked by a C bug, see below). Rebalance
> 20/20 cycles eager + cooperative without flakes; the upstream consumer example runs
> verbatim. Bug fixed: an empty incremental assign treated as NULL hung cooperative groups
> permanently. ~~Waiting on C: partition.eof must take the message-record path~~ (fixed in
> M4, test un-skipped). ~~ABI v2 debt: assignment_lost flag in the REBALANCE payload,
> metadata in the commit tpl~~ (paid in M6: `assignmentLost()` returns the real value;
> commit metadata round-trips through the extended TPL format, verified against a real
> broker).

- [ ] `KafkaConsumer` complete per FR-1: flowing + non-flowing, commit/committed/seek/position, assign/incremental, pause/resume, offsetsStore, watermarks.
- [ ] Rebalance via events up to JS (eager + cooperative-sticky), user override via `rebalance_cb`.
- [ ] `offset.commit` event, auto-commit semantics matching librdkafka.
- [ ] Integration: 2-consumer rebalance on both protocols, seek/pause/resume, EOF handling.
- **DoD:** the upstream consumer example runs verbatim; the rebalance test is stably green (20 consecutive runs without flakes).

## M4 — Admin + metadata + security (1.5 weeks)

> **Status 2026-08-14: COMPLETE** (except OIDC — needs a shim rebuild with
> `WITH_CURL=ON`, TODO recorded in CMakeLists). C side: `brk_metadata`,
> `brk_admin_request` with 9 ops + ADMIN_RESULT by correlation_id, OAUTHBEARER
> set_token; partition.eof fixed to take the message-record path per the ABI. TS side:
> `AdminClient.create/createFrom` with the full command set (fan-out for
> listConsumerGroupOffsets/deleteRecords due to librdkafka limits), OAUTHBEARER refresh
> wiring in the base Client (err-first callback or Promise). Integration: a dedicated SASL
> broker (second container, PLAIN + SCRAM-SHA-256) — roundtrip on both mechanisms,
> auth failure yields a clear error event, OAUTHBEARER refresh→set_token flow green.
> `listOffsets` (not in FR-1) not yet done. Known caveat: connect() blocks in metadata, so
> error events during a failed connect are not observable through the Callback API (the
> test uses the slice harness).

- [ ] `brk_admin_request`/`ADMIN_RESULT` JSON path; `AdminClient` with the full FR-1 command set.
- [ ] `brk_metadata`, `getMetadata`, `queryWatermarkOffsets` on the Client base.
- [ ] SSL/SASL config tests (SASL_PLAINTEXT + SCRAM on compose), OAUTHBEARER refresh event + `brk_oauthbearer_set_token`, mock OIDC test.
- **DoD:** admin + security integration green on CI.

## M5 — KafkaJS namespace (2–3 weeks)

> **Status 2026-08-14: COMPLETE** (except `.d.ts` conformance — moved to M6).
> config-mapper cross-checked against upstream source: all 13/13 common + 10/10 producer +
> 19/19 consumer keys; promisified Producer/Admin/Consumer; `run()` with the ADR-4b
> scheduler (per-partition queue + epoch + worker pool) — per-partition ordering verified
> against a real broker; handler-throw/eachBatchAutoResolve/pause-from-payload match
> upstream semantics. The upstream kafkajs producer + consumer examples run for real (the
> consumer requires commenting out their own '<fill>' ssl/sasl placeholders). Total: 306
> unit + 56 integration green.
> ~~Gaps recorded for M6: BRK_ADMIN_LIST_OFFSETS (fetchTopicOffsets read-committed +
> ByTimestamp), rd_kafka_sasl_set_credentials, protocolType in listGroups~~ —
> closed in M6: the LIST_OFFSETS op (EARLIEST/LATEST/MAX_TIMESTAMP/timestamp +
> isolation level), `brk_sasl_set_credentials` (all 3 API layers, verified by a
> wrong-password→correct re-auth against a real SASL broker), `listGroups` with per-group
> `type` + `matchConsumerGroupTypes` filter. As for `protocolType`: librdkafka does not
> expose it on ConsumerGroupListing — recorded as a permanent limitation unless upstream
> adds it.

- [ ] `config-mapper.ts` with the full translation table + tests ported from upstream MIGRATION.md.
- [ ] `Kafka`, `Producer` (`send/sendBatch/transaction`), `Consumer` (`run` with eachMessage/eachBatch, `partitionsConsumedConcurrently`, mid-stream seek/pause/resume, graceful stop), `Admin`.
- [ ] A dedicated scheduler for `run()` (design §7, ADR-4b): per-partition queue + partition epoch + worker pool; behavior tests cross-checking semantics with upstream (ordering, mid-stream pause/seek, autoCommit).
- [ ] `KafkaJSError` mapping by `error.code`; logger + logLevel.
- [ ] Conformance test comparing `.d.ts` with upstream for the KafkaJS namespace.
- [ ] Port upstream's KafkaJS-style example set into `examples/` as smoke tests.
- **DoD:** upstream's `kafkajs/*` examples run verbatim; the conformance report has no missing items outside the intentional exclusion list.

## M6 — Performance & robustness (2 weeks)

> **Status 2026-08-14 (updated after M6d): SUBSTANTIALLY COMPLETE.**
> Done: (a) all M3–M5 shim/ABI gaps closed (LIST_OFFSETS, sasl_set_credentials,
> assignment_lost, commit metadata); (b) `.d.ts` conformance vs upstream: FR-1/FR-2
> 100%, callback 94.5% / KafkaJS 97.9%, zero unintended gaps, disciplined two-way
> exclusion list (`test/conformance/`); (c) head-to-head bench vs upstream@1.10.0
> on Node 24 (bench/RESULTS.md §M6, §M6d) — **upstream does NOT run on Bun 1.4**
> (prebuilt tops out at NODE_MODULE_VERSION 137 < 147), so the on-Node ratio is the
> comparison, per user decision; (d) **M6d produce 1-copy**: PRODUCE BATCH now
> pointer-based for key/value (single F_COPY in C; 310 unit + 61 integration green).
> **Final G3 verdict** (consolidated single-session run on HEAD, see RESULTS.md
> "Final consolidated run"): consumer **2.48×**, producer 100B **1.38–1.47×** — MET
> (4/6 cases ≥1.2×); producer 1KB unbounded-burst microbench **0.63–0.81× across
> sessions — NOT MET on this
> 4 vCPU shared-broker box**; the cause is NOT client CPU (ours is lower) but
> burst-enqueue vs paced-enqueue pipeline overlap — with a bounded queue
> (max.messages 65k) the 1KB case reaches parity or better (§M6d analysis).
> Latency p99 6ms vs 3ms (inherent pull-model cost, single-digit ms).
> Remaining (moved to CI-era work): paced staging flush for large payloads;
> re-bench 1KB on non-shared hardware; soak 30'/ASan/fuzz → CI nightly; zero-copy
> opt-in; bench-smoke CI; conformance group (c): offsetsForTimes, getLastError,
> per-topic HLP serializers, KafkaJS Consumer logger/dependentAdmin/storeOffsets/committed.

- [ ] The full bench suite (design §12) + confluent-kafka-javascript baseline (Node & Bun).
- [ ] Profile-driven optimization: batch/buffer sizes, reduced allocation in the decoder (reuse header objects, lazy key/value), scheduler tuning; consider enabling Worker-poll mode if needed for G3.
- [ ] Opt-in zero-copy mode + documentation.
- [ ] Nightly 30' soak test, ASan/UBSan job, decoder fuzzing with mutated fixtures.
- [ ] bench-smoke regression guard on CI.
- **DoD:** G3 met and recorded in `bench/RESULTS.md`; soak shows no leaks.

## M7 — Prebuilt release pipeline (1.5 weeks)

> **Status 2026-08-14: distribution model REVISED by user decision (ADR-8), local part
> implemented** — repo: <https://github.com/vnStrawHat/bun-rdkafka>, single npm package
> `@vnstrawhat/bun-rdkafka` v0.1.0. Implemented: `scripts/install.ts` (postinstall +
> `bun-rdkafka-install` bin: release-asset download with SHA-256 verify → source-build
> fallback; env escape hatches; dev-repo no-op), `prepack`/`postpack` (bundle `native/`
> + `librdkafka.version` into the tarball), loader resolves `prebuilds/<target>/`, CI
> matrix targets renamed to platform keys, `release.yml` with two entry points:
> **workflow_dispatch choosing a bump type (patch/minor/major) → version + changelog
> preview (no commit) → build → tag & bump ONLY after green builds** (`finalize` commits
> `chore(release)` + tags + pushes with a one-retry race guard, so a failed build never
> burns a version; reworked 2026-08-14) → GitHub Release (changelog as notes) →
> npm publish (gated on NPM_TOKEN, idempotent,
> release-before-publish ordering); manual `v*` tag pushes also work. Remaining: push
> to GitHub, verify CI on the prebuilt targets (matrix reduced to 2 on 2026-08-14:
> linux-x64-gnu, win32-x64 — darwin and linux-arm64 dropped; those platforms fall
> back to the installer's source build), trial release end-to-end, clean-machine
> install checks.
> First-CI fixes (2026-08-14): AlmaLinux container was missing `libzstd-devel`/`lz4-devel`
> (PowerTools repo now enabled); vcpkg manifest requested a nonexistent `lz4` feature
> (librdkafka bundles lz4). **TODO before the first release:** (a) Windows vcpkg
> version skew — the runner's vcpkg snapshot ships an older librdkafka port (2.14.2 seen)
> than the pin in `librdkafka.version`; unify Windows onto FetchContent or pin a vcpkg
> `builtin-baseline`, and decide. (b) Prebuilt portability: non-Windows binaries currently
> link system libssl/libzstd/liblz4 dynamically — sonames differ across distros (e.g.
> EL8 OpenSSL 1.1 vs Ubuntu 22+ OpenSSL 3), so the linux prebuilds are NOT portable until
> `BRK_STATIC_DEPS` (static deps) is implemented; cyrus-sasl intentionally left out of CI
> builds for the same reason (builtin PLAIN/SCRAM/OAUTHBEARER unaffected, GSSAPI absent).
> Feature gate: `test/unit/native-features.test.ts` asserts every prebuilt ships with
> gzip/snappy/lz4/zstd/ssl/sasl_scram/sasl_oauthbearer (runs in the `unit` job on the
> linux-x64 artifact). TODO: add a Windows `unit` job so the win32-x64
> artifact gets the same gate — currently only build-verified.

- [x] Distribution scripts (`install.ts`/`install-plan.ts`/`prepack.ts`/`postpack.ts`) + loader `prebuilds/` resolution + unit/integration tests (local mirror).
- [x] `release.yml`: dispatch bump (patch/minor/major) → changelog preview → build → tag/bump only on green builds → package assets + SHA256SUMS → GitHub Release → npm publish `@vnstrawhat/bun-rdkafka` with provenance, secret-gated, idempotent.
- [ ] Trial release (e.g. `v0.1.0`) end-to-end; clean-machine installs on the 2 prebuilt targets (simulated CI job), including the Bun `trustedDependencies` path, plus one source-build-fallback check (macOS or linux-arm64).
- **DoD:** pushing one tag completes the entire release chain with no manual intervention.

## M8 — Documentation & v1.0 (1 week)

- [ ] **Convert everything to English** (requirement settled 2026-08-14): docs/ (spec,
      design, plan), `bunrdkafka.h` + C shim comments, TypeScript doc comments, test
      comments, bench/RESULTS.md. New code/docs from M6d onward are written in English
      directly.
- [ ] README (quick start, comparison with upstream — including the fact that upstream does not run on Bun 1.4 and the M6 bench table), typedoc API site, MIGRATION.md from confluent-kafka-javascript, CONTRIBUTING (local native build, running integration).
- [x] Stream API landed (2026-08-17): `Producer.createWriteStream` / `KafkaConsumer.createReadStream` + module-level `createReadStream`/`createWriteStream` (`ProducerStream` / `ConsumerStream` over `node:stream`), conformance exclusions removed, unit + integration tests, `examples/streams.ts`.
- [ ] Re-walk the v1.0 acceptance checklist in spec §8; close remaining differences or record them publicly.
- [ ] Tag `v1.0.0`.

---

## Sequencing & dependencies

```
M0 ─ M1 ─┬─ M2 ─┬─ M4 ─┐
         └─ M3 ─┴─ M5 ─┴─ M6 ─ M7 ─ M8
```

M2 and M3 can run in parallel with 2 people (shared M1 foundation). M4 needs M2 (client
base), M5 needs M2+M3. M7 technically only needs M0 but is placed after M6 so the first
release ships with numbers.

## Working conventions

- **Shared definition of done** for every task: has tests, CI green on 5 targets, no bench-smoke regression, new APIs have `.d.ts` + doc comments.
- Every shim ABI change: bump `BRK_ABI_VERSION`, update the header + `symbols.ts` + fixtures simultaneously.
- librdkafka version bump: a dedicated PR changing only `librdkafka.version` + a full integration run.
- Track progress with GitHub Projects; each milestone is a Project column + tag.

## Schedule risks (complementing spec §9)

| When | Risk | Plan B |
|---|---|---|
| M0 | Windows static build gets stuck | Temporarily demote Windows to Tier-2 for v1, add in v1.1 (allowed by spec if re-approved) |
| M1 | Poll model misses latency targets | Worker blocking-poll (pre-designed, §5.2) |
| M5 | Too many KafkaJS-compat behavioral details | Cut by conformance report: prioritize APIs present in upstream's examples/migration guide, record the rest as "known gaps" |
| M6 | G3 (1.5×) not met | Minimum acceptable threshold: ≥1.0× upstream-on-Node and ≥1.2× upstream-on-Bun, with analysis and a follow-up optimization plan |
