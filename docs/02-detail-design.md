# bun-rdkafka — Detail Design

> This document describes the architecture and design decisions. Requirements are in
> [01-spec.md](./01-spec.md), the implementation plan in [03-implementation-plan.md](./03-implementation-plan.md).

---

## 1. Architecture overview

The system is split into 4 layers with one-directional top-down dependencies:

```
┌─────────────────────────────────────────────────────────────┐
│  API Layer (TypeScript)                                     │
│  ┌───────────────────────┐  ┌─────────────────────────────┐ │
│  │ Callback API          │  │ KafkaJS namespace           │ │
│  │ Producer,             │  │ Kafka, Producer, Consumer,  │ │
│  │ KafkaConsumer,        │◄─┤ Admin (promisified; built   │ │
│  │ AdminClient           │  │ ON TOP OF the Callback API) │ │
│  └───────────┬───────────┘  └─────────────────────────────┘ │
├──────────────┼──────────────────────────────────────────────┤
│  Core Layer (TypeScript)                                    │
│  NativeClient (handle + state machine), PollScheduler,      │
│  EventDecoder/BatchDecoder (binary protocol), ConfigBuilder,│
│  DeliveryLedger, LibrdKafkaError, TopicNameTable            │
├──────────────┬──────────────────────────────────────────────┤
│  FFI Layer (TypeScript, bun:ffi)                            │
│  loader.ts (locate & dlopen the library), symbols.ts        │
│  (function signature declarations), no business logic       │
├──────────────┬──────────────────────────────────────────────┤
│  Native Shim (C) — libbunrdkafka.{so,dylib,dll}             │
│  Flattens the librdkafka C API into an FFI-friendly ABI;    │
│  turns EVERY callback into an event queue; serializes       │
│  messages/events into packed binary buffers.                │
│  Static-link: librdkafka + OpenSSL + zlib + zstd + lz4      │
└─────────────────────────────────────────────────────────────┘
```

**Guiding principles:**

1. **No C→JS callbacks.** librdkafka's native threads never touch JS.
   Everything goes through `rd_kafka_queue_t`; JS actively drains.
2. **One FFI call ↔ one batch.** The hot path (produce, consume, event drain) always works
   on a buffer holding N records, never 1 call / 1 message.
3. **JS owns the buffers, C only writes into them.** Avoids JS having to free C-allocated
   memory on the hot path (except a few cold APIs with `brk_mem_free`).
4. **The KafkaJS API is a thin layer on top of the Callback API** — matching
   confluent-kafka-javascript's layering, which makes behavior cross-checking convenient.
5. **Compatibility at the API surface, not at the internals.** confluent-kafka-javascript is
   a *behavioral contract* (API + observable semantics, verified by conformance/behavior
   tests); every internal structure is designed independently, optimized for Bun and the
   pull-model — no 1:1 porting of upstream code or internal architecture.

## 2. Repository structure

```
bun-rdkafka/
├── package.json                  # workspace root (bun workspaces)
├── bunfig.toml
├── tsconfig.json                 # strict, moduleResolution bundler
├── librdkafka.version            # e.g. v2.11.1 — single source of truth
├── native/
│   ├── CMakeLists.txt            # build shim + FetchContent librdkafka at pinned version
│   ├── include/bunrdkafka.h      # the shim's public ABI (versioned)
│   └── src/
│       ├── shim_common.c         # conf, error, mem, version
│       ├── shim_events.c         # event queue drain + serialize
│       ├── shim_producer.c
│       ├── shim_consumer.c
│       └── shim_admin.c
├── packages/
│   ├── bun-rdkafka/              # the single published package
│   │   ├── package.json          # postinstall → scripts/install.ts (ADR-8)
│   │   ├── scripts/              # install.ts, install-plan.ts, prepack.ts, postpack.ts
│   │   └── src/
│   │       ├── index.ts          # exports Callback API + KafkaJS + CODES
│   │       ├── ffi/              # loader.ts, symbols.ts, types.ts
│   │       ├── core/             # native-client.ts, poll-scheduler.ts,
│   │       │                     # batch-decoder.ts, config.ts, errors.ts,
│   │       │                     # delivery-ledger.ts
│   │       ├── callback/         # producer.ts, high-level-producer.ts,
│   │       │                     # kafka-consumer.ts, admin.ts, client.ts
│   │       └── kafkajs/          # kafka.ts, producer.ts, consumer.ts,
│   │                             # admin.ts, config-mapper.ts, errors.ts
│   └── (prebuilds/ is created inside the package at install time — gitignored)
├── test/
│   ├── unit/                     # bun test, no broker needed (decoders, config, ledger…)
│   ├── conformance/              # cross-check API surface against upstream .d.ts
│   └── integration/              # needs a broker (docker compose: Kafka KRaft)
├── bench/                        # producer.bench.ts, consumer.bench.ts, RESULTS.md
├── examples/
├── scripts/                      # repo-level tooling (distribution scripts live in the package)
└── .github/workflows/            # ci.yml, release.yml
```

Runtime dependencies of `bun-rdkafka`: **0**. DevDeps: `typescript`, `@types/bun`.

## 3. Native Shim (C) — `libbunrdkafka`

### 3.1 Why a shim instead of dlopen-ing librdkafka directly?

| Problem with direct binding | How the shim solves it |
|---|---|
| librdkafka fires callbacks from internal threads → cannot safely point them at a `JSCallback` | The shim routes all callbacks to `rd_kafka_queue_t`; JS polls |
| Many APIs use complex structs (`rd_kafka_message_t`, topic_partition_list, admin results) — reading structs through FFI is fragile and layout-dependent | The shim serializes into a versioned packed binary format; JS decodes with `DataView` |
| 1 FFI call / message is too expensive | The shim provides batch APIs: 1 call ↔ N messages |
| Would have to ship librdkafka + OpenSSL + zlib + zstd… as multiple files | The shim statically links everything into **1 shared library**, exporting only ~60 `brk_*` symbols |
| The librdkafka ABI changes between versions | The shim's ABI is under our control, with `BRK_ABI_VERSION` |

### 3.2 ABI conventions

- Every function is prefixed `brk_` (bun-rdkafka), `extern "C"`, using only POD types:
  `void*` (handle), `int32/64`, `const char*`, `uint8_t* buf + size`.
- Every exchanged buffer is **JS-allocated, C-written** (except cold APIs returning JSON
  strings with `brk_mem_free`). Buffer-writing functions return the number of bytes written,
  or a negative number = error code; `BRK_ERR_BUFFER_TOO_SMALL` comes with
  `brk_last_required_size(handle)` so JS can grow the buffer and retry.
- Integers in packed formats: **little-endian** (all Tier-1 targets are LE).
- `brk_abi_version(void) -> int32` — the loader verifies it matches the TS constant before use.

### 3.3 Main symbol table (abridged)

```c
// ---- common ----
int32_t  brk_abi_version(void);
const char* brk_librdkafka_version(void);
void*    brk_conf_new(void);
int32_t  brk_conf_set(void* conf, const char* k, const char* v,
                      char* errstr, int32_t errstr_size);
void     brk_conf_destroy(void* conf);            // only if not consumed by *_new
void     brk_mem_free(void* p);

// ---- lifecycle ----
// type: 0=producer, 1=consumer. Consumes conf. Registers log/stats/error/dr/rebalance/
// offset-commit onto background-compatible queues; returns NULL + errstr on failure.
void*    brk_client_new(int32_t type, void* conf, char* errstr, int32_t errstr_size);
void     brk_client_destroy(void* h);              // rd_kafka_destroy + join threads
int32_t  brk_client_outq_len(void* h);

// ---- event drain (all clients) ----
// Drains the main event queue: delivery reports, error, log, stats, throttle,
// rebalance, offset-commit. Writes packed events into buf. timeout is always 0 from
// the main thread.
int32_t  brk_events_poll(void* h, uint8_t* buf, int32_t buf_cap,
                         int32_t max_events, int32_t timeout_ms);

// ---- producer ----
// Batch produce: reads packed records from in_buf, calls rd_kafka_producev per record
// (in C — cheap), writes per-record err codes into err_out (int16 * n). Returns the
// number of records accepted.
int32_t  brk_produce_batch(void* h, const uint8_t* in_buf, int32_t in_len,
                           int16_t* err_out, int32_t max_records);
int32_t  brk_flush(void* h, int32_t timeout_ms);
// transactions
int32_t  brk_init_transactions(void* h, int32_t timeout_ms, uint8_t* errbuf, int32_t cap);
int32_t  brk_begin_transaction(void* h, uint8_t* errbuf, int32_t cap);
int32_t  brk_commit_transaction(void* h, int32_t timeout_ms, uint8_t* errbuf, int32_t cap);
int32_t  brk_abort_transaction(void* h, int32_t timeout_ms, uint8_t* errbuf, int32_t cap);
int32_t  brk_send_offsets_to_transaction(void* h, const uint8_t* tpl_buf, int32_t len,
                                         void* consumer_h, int32_t timeout_ms,
                                         uint8_t* errbuf, int32_t cap);

// ---- consumer ----
int32_t  brk_subscribe(void* h, const uint8_t* topics_buf, int32_t len); // packed strings
int32_t  brk_unsubscribe(void* h);
// The heart of the consume path: 1 call returns up to max_msgs messages, packed into buf.
int32_t  brk_consume_batch(void* h, uint8_t* buf, int32_t buf_cap,
                           int32_t max_msgs, int32_t timeout_ms);
int32_t  brk_commit(void* h, const uint8_t* tpl_buf, int32_t len, int32_t async);
int32_t  brk_committed(void* h, const uint8_t* tpl_buf, int32_t tpl_len,
                       uint8_t* out_buf, int32_t out_cap, int32_t timeout_ms);
                       // tpl_len == 0 → current assignment
int32_t  brk_seek(void* h, const char* topic, int32_t partition, int64_t offset,
                  int32_t timeout_ms);  // by topic name (cold path)
int32_t  brk_assign(void* h, const uint8_t* tpl_buf, int32_t len, int32_t mode);
                  // mode: 0=assign 1=incremental_assign 2=incremental_unassign 3=unassign
int32_t  brk_assignment(void* h, uint8_t* buf, int32_t cap);
int32_t  brk_position(void* h, uint8_t* buf, int32_t cap);
int32_t  brk_pause_resume(void* h, const uint8_t* tpl_buf, int32_t len, int32_t resume);
int32_t  brk_offsets_store(void* h, const uint8_t* tpl_buf, int32_t len);
int32_t  brk_query_watermark(void* h, const char* topic, int32_t partition,
                             int64_t* lo, int64_t* hi, int32_t timeout_ms);

// ---- topic name interning ----
int32_t  brk_topic_name(void* h, int32_t topic_id, uint8_t* buf, int32_t cap);

// ---- metadata & admin (cold path, JSON) ----
// Returns a C-allocated JSON string (*out); JS reads it via CString then brk_mem_free.
int32_t  brk_metadata(void* h, const char* topic_or_null, int32_t timeout_ms, char** out);
// Admin: op_id enum (CREATE_TOPICS, DELETE_TOPICS, ...), req is JSON.
// Results come back asynchronously via brk_events_poll (ADMIN_RESULT event, JSON payload,
// correlation id assigned by JS).
int32_t  brk_admin_request(void* h, int32_t op_id, uint64_t correlation_id,
                           const char* req_json);
```

~55–65 functions in total. The `bunrdkafka.h` header is the single contract between C and TS.

### 3.4 Callback handling inside the shim

In `brk_client_new`, the shim configures librdkafka so that **every** event channel lands
on a pollable queue:

- `rd_kafka_conf_set_events(conf, RD_KAFKA_EVENT_DR | LOG | STATS | ERROR | REBALANCE | OFFSET_COMMIT | OAUTHBEARER_TOKEN_REFRESH | ...)` — using librdkafka's **event API** instead of the callback API wherever possible.
- Consumer: `rd_kafka_poll_set_consumer(rk)` to merge queues; producer: main queue.
- Rebalance: the shim does **not** call assign itself. The `REBALANCE` event is serialized
  (protocol, partitions) and pushed up to JS; JS decides (by default assign/incremental-assign,
  or invokes the user's `rebalance_cb`) and then calls back into `brk_assign(...)`. This is
  exactly node-rdkafka's model (`rebalance_cb` runs on the JS thread).
- OAUTHBEARER token refresh: event goes up to JS, user/SDK resolves the token, JS calls
  `brk_oauthbearer_set_token(...)`.

### 3.5 Packed binary formats (versioned by `BRK_ABI_VERSION`)

**Event frame** (from `brk_events_poll`):

```
u8  event_type      # 1=DR, 2=ERROR, 3=LOG, 4=STATS, 5=REBALANCE,
                    # 6=OFFSET_COMMIT, 7=THROTTLE, 8=OAUTH_REFRESH, 9=ADMIN_RESULT
u32 payload_len     # payload length, NOT including the frame's 5-byte header
payload             # depends on event_type
```

Example `DR` (delivery report) payload — many reports packed into 1 frame:

```
u32 count
count × {
  u64 opaque_id     # id assigned by JS at produce time → looked up in DeliveryLedger
  i16 err           # rd_kafka_resp_err_t
  i32 partition
  i64 offset
  i64 timestamp
}
```

**Message frame** (from `brk_consume_batch`), per message:

```
i32 topic_id        # interned; JS looks up TopicNameTable, on miss calls brk_topic_name
i32 partition
i64 offset
i64 timestamp  u8 timestamp_type
i16 err             # error messages (e.g. _PARTITION_EOF) also take this path
i32 key_len   (-1 = null)   bytes...
i32 value_len (-1 = null)   bytes...
u16 header_count × { u16 klen, bytes, i32 vlen, bytes }
i32 leader_epoch    (-1 if absent)
```

**Topic-partition list** (`tpl_buf`, bidirectional JS↔C): `u32 count × { i32 topic_id | -1, u16 name_len, name, i32 partition, i64 offset, i32 leader_epoch, i16 metadata_len (-1 = absent), metadata }` — the metadata field carries commit metadata (added in M6).

**Produce batch** (JS→C, `brk_produce_batch`): since M6d, key/value travel as
`(u64 ptr, i32 len)` pointers into the caller's live Buffers instead of inline bytes
(headers stay inline). This is safe because the call is synchronous and C copies
immediately (`F_COPY`) — it removes the JS-side staging memcpy, leaving exactly one copy
on the produce path.

> The byte-exact specification of every format lives in `native/include/bunrdkafka.h`
> (the single source of truth); this section is an overview and may lag behind it.

Why binary rather than JSON for the hot path: avoids per-message string
serialization/parsing; `DataView`/`Buffer` decoding over an ArrayBuffer is Bun's fastest
path. JSON is used only for the cold path (metadata, admin, stats — stats is already JSON
from librdkafka).

## 4. FFI Layer

```ts
// ffi/loader.ts
import { dlopen, FFIType, suffix } from "bun:ffi";

function resolveLibPath(): string {
  if (process.env.BUN_RDKAFKA_LIB_PATH) return process.env.BUN_RDKAFKA_LIB_PATH;
  const key = `${process.platform}-${process.arch}${process.platform === "linux" ? "-gnu" : ""}`;
  // prebuilds/<key>/ is populated by scripts/install.ts (download or source build)
  const prebuilt = join(pkgRoot, "prebuilds", key, `libbunrdkafka.${suffix}`);
  if (existsSync(prebuilt)) return prebuilt;
  throw new Error(unsupportedPlatformMessage(key));
}

export const native = dlopen(resolveLibPath(), symbols); // symbols.ts: brk_* signatures
if (native.symbols.brk_abi_version() !== BRK_ABI_VERSION) {
  throw new Error("bun-rdkafka: native ABI mismatch — reinstall dependencies");
}
```

- `symbols.ts` declares the exact signatures (`FFIType.ptr`, `i32`, `u64`, `cstring`…),
  partly generated from `bunrdkafka.h` by a script to avoid manual drift (a simple
  header-parsing script, run in CI).
- This layer has no logic; the core layer is where buffers/handles are managed.

## 5. Core Layer

### 5.1 `NativeClient` — handle & state machine

```
INIT → CONNECTING → READY → DISCONNECTING → CLOSED
```

- Wraps the `void* handle` in a class with `state`; every method checks state before an FFI
  call (calling into a destroyed handle = process crash, so this guard is mandatory).
- `connect()` in librdkafka is really "create the handle + wait for the first metadata":
  create the handle (`brk_client_new`), issue a metadata request, and when metadata comes
  back OK → emit `ready` (mirroring upstream's `connect()` semantics).
- A `FinalizationRegistry` holds the `handle` (number) — if the user forgets `disconnect()`,
  the finalizer calls `brk_client_destroy` and prints a warning. Registers
  `process.on("beforeExit")` to flush a producer with pending messages (matching upstream
  behavior on a best-effort basis).

### 5.2 `PollScheduler` — the client's heartbeat (replacing uv_async/N-API threads)

Since there are no callbacks from C, JS must actively call `brk_events_poll` /
`brk_consume_batch`. The scheduler runs **adaptively**:

```
HOT state :  the previous poll had data
   → poll again in the next microtask (setImmediate-style: Promise.resolve().then
     interleaved with setTimeout(0) so I/O is not starved)
WARM state:  data just ran out
   → setTimeout 1ms; after N empty rounds back off 1 → 2 → 4 … up to idleMax (default
     50ms, configurable via 'js.poll.idle.max.ms')
COLD state:  no consumer running, producer outq_len == 0
   → poll only at the producer's pollInterval (default 500ms) to pick up log/stats
```

- Producer: after each `produce()` batch, the scheduler is "kicked" back to HOT (delivery
  reports are about to arrive).
- Consumer flowing mode: the HOT loop is the `brk_consume_batch` read loop itself.
- Every call uses `timeout_ms = 0` → **the event loop is never blocked** (NFR-2).
- Timers use regular `Bun.sleep`/`setTimeout` + `timer.unref()` when COLD, so the process
  isn't kept alive unintentionally.
- **Later-phase extension (not required for v1):** a `js.poll.worker=true` mode — a dedicated
  `Worker` calls `brk_consume_batch(timeout=100ms)` blocking and `postMessage`-transfers the
  ArrayBuffer to the main thread. Eliminates idle polling entirely; the shim design already
  allows it (the handle is usable from another thread for consuming, guaranteed thread-safe
  by librdkafka).

### 5.3 `BatchDecoder` / `EventDecoder`

- Keeps a reusable `ArrayBuffer` (default 4 MB, configurable via `js.consume.buffer.bytes`,
  auto-grows on `BRK_ERR_BUFFER_TOO_SMALL`).
- Decodes with a `DataView` + running offset; creates `Buffer`s for key/value:
  - **Default (safe) mode:** `Buffer.from(view.slice(...))` — a copy; the message lives
    independently of the reusable buffer.
  - **Zero-copy mode (opt-in `js.consumer.zero.copy=true`):** returns a `Buffer` that is a
    view; only valid inside the `eachMessage/eachBatch` callback, clearly documented with
    warnings. For parse-then-discard pipelines.
- `TopicNameTable`: `topic_id → string` map; on miss calls `brk_topic_name` once and caches —
  topic names are never repeatedly copied on the hot path.

### 5.4 `DeliveryLedger`

- `Map<bigint, PendingRecord>`; monotonically increasing u64 `opaque_id` assigned at produce time.
- When a DR event frame arrives → resolves the corresponding callback/promise, with
  `{topic, partition, offset, timestamp}`.
- Backpressure: if the ledger exceeds `js.producer.max.pending` (default =
  `queue.buffering.max.messages`), `produce()` returns a `QUEUE_FULL` error, matching
  librdkafka semantics.

### 5.5 `ConfigBuilder` & `errors.ts`

- Accepts a node-rdkafka-style config object (`{'metadata.broker.list': ..., 'linger.ms': ...}`),
  splits it into: (a) librdkafka properties → `brk_conf_set` per pair; (b) `js.*` properties →
  JS-layer configuration; (c) function properties (`rebalance_cb`, `offset_commit_cb`,
  `oauthbearer_token_refresh_cb`) → registered as JS handlers.
- `LibrdKafkaError` mirrors upstream: `{ message, code, errno, origin, isFatal, isRetriable, isTxnRequiresAbort }`; `CODES.ERRORS` generated from the `rd_kafka_resp_err_t` table
  (script generates it from the librdkafka header at build time, committed into the repo).

## 6. Callback API layer

Behavior-compatible with confluent-kafka-javascript (upstream's `lib/*.js` is a semantics
reference for writing cross-check tests, not code to port):

- `client.ts` — base `Client extends EventEmitter`: connect/disconnect flow, wiring
  `event.error/log/stats/throttle` from the EventDecoder to the emitter.
- `producer.ts` — `produce()` does **not** make an FFI call per message: it pushes the record
  into a TS staging buffer; staging flushes down to `brk_produce_batch` when (a) the
  microtask ends, (b) staging is full, or (c) the user calls `flush()/poll()`. For a
  sequential caller like `for { produce() }`, the whole loop becomes 1 FFI call — this is
  the main source of G3. Per-record errors (e.g. QUEUE_FULL) surface as a synchronous
  exception on the next produce call, matching upstream semantics at the observable level
  (throws when produce fails).
- `kafka-consumer.ts` — two modes like upstream: flowing (`consume()` + `data` event)
  and non-flowing (`consume(n, cb)`); rebalance defaults to assign/unassign, user overrides
  via `rebalance_cb`.
- `admin.ts` — per command: assign a `correlation_id`, call `brk_admin_request`, wait for the
  matching `ADMIN_RESULT` event (JSON), map to upstream's result shape.

## 7. KafkaJS namespace

- `config-mapper.ts` is the "hard in the details" part: translates the `kafkaJS` block →
  librdkafka properties, copying upstream's translation table exactly
  (`brokers`→`bootstrap.servers`, `ssl`→`security.protocol`+certs, `sasl`→`sasl.*`,
  `retry.*`→`retry.backoff.ms`…, `fromBeginning`→`auto.offset.reset`,
  `autoCommit*`→`enable.auto.commit`/`auto.commit.interval.ms`,
  `idempotent`→`enable.idempotence`, `acks`/`compression` → topic conf…).
  Ported together with test cases from upstream's MIGRATION.md.
- `consumer.run()`: a read loop over the Callback-API consumer in batch mode; dispatches
  `eachMessage` sequentially per partition, in parallel across partitions per
  `partitionsConsumedConcurrently`; `eachBatch` receives batches up to
  `js.consumer.max.batch.size`. Manages: per-partition pause/resume, mid-stream seek,
  autoCommit on an interval, graceful `stop()`/`disconnect()` (waits for in-flight handlers,
  commits one last time).
  **A scheduler designed specifically for Bun** (not a port of upstream's
  `_consumer_cache.js`): the BatchDecoder feeds messages straight into **per-partition
  queues**; N workers (N = min(`partitionsConsumedConcurrently`, number of active
  partitions)) wait on a shared notifier, each worker holding exclusive ownership of one
  partition at a time (preserving per-partition ordering). Pause/seek/rebalance use an
  **epoch counter per partition**: the operation bumps the epoch → already-decoded messages
  carrying the old epoch are dropped on the spot, with no global cache-stale flag or
  pending-operation queue like upstream. In zero-copy mode the queue holds only indexes into
  the packed buffer instead of decoded objects. Upstream
  (`_consumer.js`/`_consumer_cache.js`) is only a **semantics reference** (ordering,
  mid-stream pause/seek behavior, autoCommit, worker respawn when the partition count
  changes) for writing cross-check behavior tests — note that Confluent themselves also
  fetch batches and schedule in JS for `run()`, confirming our pull-model gives up nothing
  architecturally versus upstream.
- `producer.send()`: maps `{topic, messages[]}` → staging produce + groups promises via the
  DeliveryLedger; resolves when every message in the call has a DR. The transaction context
  is a thin wrapper over `brk_*_transaction`.
- Error mapping: `KafkaJSError` with `code` (prioritizing `error.code` as upstream settled on).

## 8. Memory & lifecycle management — rules summary

| Resource | Allocated by | Freed by |
|---|---|---|
| conf handle | C (`brk_conf_new`) | consumed by `brk_client_new`, or `brk_conf_destroy` on early failure |
| client handle | C | `brk_client_destroy` on `disconnect()`; FinalizationRegistry as the last net |
| Hot-path buffers (consume/events/produce staging) | JS (reusable `ArrayBuffer`) | JS GC |
| Cold-path JSON results | C (`malloc`) | JS calls `brk_mem_free` right after copying into a string |
| Strings passed into C | JS (`Buffer` + NUL) | JS; C keeps no pointer after return (the shim copies if it needs to retain) |

Golden rules: **C never keeps a pointer into a JS buffer after the function returns**, and
**JS never reads a buffer after making the next FFI call that overwrites it** (except in
zero-copy mode with its explicit contract).

## 9. Packaging & prebuilt binaries

### 9.1 Distribution: postinstall download + source-build fallback (ADR-8)

*(Rewritten 2026-08-14 — supersedes the per-platform-packages model of ADR-5.)*

A **single npm package `@vnstrawhat/bun-rdkafka`** ships TypeScript source (Bun runs TS natively),
the installer scripts, and — copied in at pack time by `scripts/prepack.ts` — the native
shim sources (`native/`, minus `build/`, plus `librdkafka.version`, ~100 KB; librdkafka
itself is fetched by CMake at build time, never shipped).

Install flow (`packages/bun-rdkafka/scripts/install.ts`, wired as `postinstall` and as
the `bun-rdkafka-install` bin; pure decision logic in `install-plan.ts`):

1. Detect the platform key (`linux-x64-gnu`, `darwin-arm64`, … — same names as the
   loader and the CI matrix). musl is detected and routed to the source build.
2. Download `libbunrdkafka-<target>.tar.gz` + `SHA256SUMS` from
   `https://github.com/vnStrawHat/bun-rdkafka/releases/download/v<version>/`
   (`BUN_RDKAFKA_BINARY_MIRROR` overrides the base URL), verify the SHA-256, and unpack
   into `<package>/prebuilds/<target>/` — the loader's second lookup location.
3. **Fallback**: no matching asset, download failure, or checksum mismatch → build from
   the shipped sources (`cmake` + a C compiler required; clear guidance printed when the
   toolchain is missing).
4. A failed install prints a prominent warning but exits 0 (`BUN_RDKAFKA_INSTALL_STRICT=1`
   opts into hard failure) — the loader raises the actionable error at runtime instead of
   `bun install` failing mysteriously.
5. Inside the development monorepo the hook is a no-op (devs run `bun run build:native`).

**Bun caveat:** Bun blocks dependency lifecycle scripts by default, so `postinstall`
only runs when users add `"@vnstrawhat/bun-rdkafka"` to `trustedDependencies`. The README documents
this prominently; `bunx bun-rdkafka-install` is the manual escape hatch, and the loader's
error message explains both.

### 9.2 Native build

- **CMake** unified across all platforms; librdkafka fetched via `FetchContent` at
  `librdkafka.version`, built with `RDKAFKA_BUILD_STATIC=ON`.
- Static dependencies: OpenSSL, zlib, zstd, lz4 — Linux/macOS build from source via CMake
  or use a cache; Windows uses **vcpkg** (librdkafka has an official port, triplet
  `x64-windows-static-md`).
- Linux: built in an AlmaLinux 8 container (glibc 2.28) so the binary runs widely; symbol
  visibility hidden, only `brk_*` exported; `-O2 -flto`.
- macOS: `MACOSX_DEPLOYMENT_TARGET=12.0`; ad-hoc codesign.
- Sanitizer builds (ASan/UBSan) are a separate CMake configuration used in CI tests.

## 10. CI/CD (GitHub Actions)

### 10.1 `ci.yml` — every PR/push

| Job | Runner | Contents |
|---|---|---|
| lint-type | ubuntu | `tsc --noEmit`, format check |
| build-native | 5-target matrix | build the shim, upload artifact |
| unit | ubuntu (+ macos) | `bun test test/unit test/conformance` with the artifact binary |
| integration | ubuntu | docker compose Kafka (KRaft, `apache/kafka` image), runs `test/integration`; a second job runs with the ASan binary |
| bench-smoke | ubuntu | runs an abridged bench, compares against a regression threshold (fails on >20% drop) |

Build matrix: `ubuntu-24.04` (almalinux:8 container) / `ubuntu-24.04-arm` (similar
container) / `macos-13` / `macos-14` / `windows-2022`. Cache: vcpkg + CMake FetchContent.

### 10.2 `release.yml` — dispatch with a bump type, or pushing a `v*` tag

The primary entry point is **workflow_dispatch with a version-bump choice**
(`patch` / `minor` / `major`): a `prepare` job bumps the package version
accordingly, generates the CHANGELOG.md section from **Conventional Commits**
since the last `v*` tag (`scripts/changelog.ts` — grouped into Breaking
Changes / Features / Bug Fixes / Performance / Documentation / Maintenance,
with a compare link), commits `chore(release): v<next>`, tags, and pushes; the
same run then continues into the steps below. Pushing a `v*` tag manually still
works (the tagged commit must already carry the matching package version).
The generated section body doubles as the GitHub Release notes.

0. *(dispatch only)* `prepare`: bump → changelog → commit → tag `v<next>` → push.
1. Reuse the build matrix (targets named by platform key) → 5 binary artifacts.
2. Package each artifact as `libbunrdkafka-<target>.tar.gz` + a combined `SHA256SUMS`.
3. Create the **GitHub Release** with those assets — this must precede the npm publish,
   because postinstall of the published version downloads from this release.
4. Verify `package.json` version == tag, then `npm publish --provenance` the single
   `@vnstrawhat/bun-rdkafka` package (prepack bundles `native/` into the tarball). The
   step skips gracefully when the `NPM_TOKEN` secret is absent.
5. Idempotent: re-running a tag skips versions already on the registry.

## 11. Test strategy

- **Unit (no broker):** BatchDecoder/EventDecoder with binary fixtures generated by a C test
  harness (guaranteeing the C encoder and TS decoder match byte-for-byte); ConfigBuilder;
  KafkaJS config-mapper (tests ported from upstream MIGRATION.md); DeliveryLedger;
  PollScheduler (fake timers).
- **Conformance:** a script loads confluent-kafka-javascript's `.d.ts` (devDep, test-only)
  and cross-checks the method/property surface of both APIs — reporting what's missing.
- **Integration (real broker):** produce/consume roundtrip (all compression codecs, headers,
  keys, timestamps), consumer group rebalance (eager + cooperative, 2 consumers),
  commit/seek/pause/resume, end-to-end EOS transactions, the full admin command set,
  SASL/SSL (compose with a SASL_PLAINTEXT listener), OAUTHBEARER with a mock OIDC,
  crash-safety: kill a consumer mid-flight, check for leaks (ASan job).
- **Stress:** a 30-minute soak test of continuous produce+consume, monitoring RSS (to catch
  leaks on the ledger/decoder side), run nightly.

## 12. Benchmarks

`bench/` uses a local broker (docker, same machine) — measuring:

1. Producer throughput: 100B / 1KB / 10KB payloads, acks=1 and acks=all, msgs/s + MB/s.
2. Consumer throughput: batch mode + eachMessage mode.
3. End-to-end p50/p99 latency at moderate load.
4. Reference baseline: confluent-kafka-javascript on Node 22 and on Bun (same script).

Results + methodology recorded in `bench/RESULTS.md`; bench-smoke on CI guards against
regressions.

## 13. Settled design decisions (ADR summary)

| # | Decision | Rationale | Rejected alternative |
|---|-----------|-------|-------------------|
| ADR-1 | C shim + static-link, no direct dlopen of librdkafka | Callbacks unsafe through FFI; complex structs; ship 1 file | dlopen librdkafka directly |
| ADR-2 | Poll-based event model, adaptive scheduler | `JSCallback` not reliably threadsafe in Bun | Threadsafe JSCallback; pipe fd + socket watch (not portable to Windows) |
| ADR-3 | Packed binary protocol for the hot path, JSON for the cold path | FFI and parsing cost | JSON everywhere; reading C structs directly by offset |
| ADR-4 | KafkaJS API built on top of the Callback API | Matches upstream architecture, easy behavior cross-checking | Two parallel APIs both calling core |
| ADR-4b | `consumer.run()`: batch-fetch + a JS scheduler designed for Bun (per-partition queue + partition epoch); upstream is only a semantics reference | Compatible at API/semantics, internals optimized for the pull-model + BatchDecoder | 1:1 port of `_consumer_cache.js`; pushing each message through a `data` event |
| ADR-5 | ~~Per-platform npm packages via optionalDependencies, no postinstall~~ **Superseded by ADR-8** (2026-08-14) | Modern standard (esbuild/napi-rs), supply-chain safe | postinstall download (blocked in many environments) |
| ADR-6 | Copy-per-message by default, zero-copy opt-in | Correctness first, speed second | Zero-copy by default (easily causes aliasing bugs for users) |
| ADR-7 | Ship TS source directly (Bun runs TS), d.ts generated separately | Bun-native, drops the JS build step | Bundling to JS |
| ADR-8 | Single package + postinstall download from GitHub Release, source-build fallback when no asset matches (§9.1) | User decision 2026-08-14: one package, prebuilts on the release, `build:native` only as fallback. Trade-off accepted: Bun users must add the package to `trustedDependencies` (documented + `bunx bun-rdkafka-install` escape hatch) | ADR-5's per-platform packages (no postinstall, but 6 packages to maintain and a registry-mirror burden) |
