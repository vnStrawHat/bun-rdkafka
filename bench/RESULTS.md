# bench/RESULTS.md — bun-rdkafka performance numbers

Each section is one measurement session with its date, hardware, and repro command
recorded. Numbers are **actually measured**, never interpolated.

---

## M1 baseline — 2026-08-14, 4 CPU / 3 GB RAM machine, no optimization yet

First measurement of the M1 vertical slice (`docs/03-implementation-plan.md` §M1:
"measure a first performance baseline and record it — G3 not required yet"). **No
optimization of any kind yet**: the decoder copies every key/value, no producer staging
buffer, no zero-copy, no Worker-poll.

### Environment

| | |
|---|---|
| Machine | 4 vCPU, 3.8 GB RAM (≈2 GB free), Linux 5.15 x86-64 |
| Runtime | Bun 1.4.0-canary.1 |
| librdkafka | 2.15.0, statically linked into `libbunrdkafka.so` (Release) |
| Broker | `apache/kafka:3.9.0` single-node KRaft in docker, `KAFKA_HEAP_OPTS=-Xmx512m -Xms256m`, same machine (localhost:9092) |
| Load | 1,000,000 messages × 100 B (95.4 MiB payload), 1 partition, `acks=1`, `linger.ms=5` |
| Batching | produce 1,000 records / `brk_produce_batch` call; consume ≤ 1,000 messages / `brk_consume_batch` call |

### Results

| Phase | Time | Throughput | CPU (JS process) | Poll rounds |
|---|---:|---:|---:|---:|
| **produce** (to the last DR) | 1,192.6 ms | **838,532 msg/s** (80.0 MiB/s) | 1,544 ms ≈ 129 % of one core | 1,050 |
| **consume** — librdkafka defaults | 9,393.3 ms | 106,459 msg/s (10.2 MiB/s) | 1,163 ms ≈ 12 % of one core | 1,237 (808 msg/round) |
| **consume** — `fetch.queue.backoff.ms=10` | 732.3 ms | **1,365,607 msg/s** (130.2 MiB/s) | 700 ms ≈ 96 % of one core | 1,015 (985 msg/round) |

- Delivery reports: 1,000,000 ok / 0 errors, packed into 199 DR event frames.
- Messages received: 1,000,000 (95.4 MiB) — checksums match in
  `test/integration/vertical-slice.test.ts`.

Broker-side reference (bypassing bun-rdkafka), same machine, same topic:

```
kafka-consumer-perf-test.sh --messages 1000000
→ 1,336,898 msg/s (127.5 MB/s)
```

⇒ With the prefetch tuned, bun-rdkafka's consume path **reaches the broker's own
ceiling** (1.37 M msg/s vs 1.34 M msg/s for the Java tool).

### Why the two consume numbers differ 13×

librdkafka's `fetch.queue.backoff.ms` defaults to **1000 ms**: when the local fetch
queue is full (`queued.min.messages` = 100,000), librdkafka stops fetching and only
retries after 1 s. Our consumer drains much faster ⇒ the queue keeps running dry, and
most of the time is spent **waiting for data, not waiting for CPU**:

```
gaps > 5ms between two poll rounds: 196 occurrences, total 8,720 ms = 93 % of wall time
time actually spent inside brk_consume_batch (FFI + decode): 418 ms = 4.5 % of wall time
```

This is a **librdkafka configuration**, not a limit of the poll model. Verified by
sweeping `js.poll.idle.max.ms` (the PollScheduler's WARM backoff ceiling):

| `js.poll.idle.max.ms` | throughput | CPU | note |
|---:|---:|---:|---|
| 50 (default) | 107,106 msg/s | 12 % | 196 stalls of ~50 ms |
| 10 | 118,409 msg/s | 15 % | |
| 5 | 133,120 msg/s | 17 % | |
| 1 | 134,535 msg/s | 17 % | no more stalls (2 % of wall) but still throttled by prefetch |
| 50 **+ `fetch.queue.backoff.ms=10`** | **1,267,490 msg/s** | 97 % | poll loop CPU-saturated |

Lowering `js.poll.idle.max.ms` only recovers ~25 %; fixing the prefetch recovers 12×.
The conclusion feeds into the "decision point" section below.

### Memory (RSS)

| | |
|---|---|
| RSS at start | 38.9 MiB |
| RSS peak during produce | 126.1 MiB |
| RSS peak during consume | 172.5 MiB |
| Final RSS (after `Bun.gc(true)`) | 151.9 MiB |

RSS does **not grow linearly** with the message count — sampled at progress marks:

```
produce  25%: 125.6 MiB | 50%: 117.3 MiB | 75%: 119.3 MiB | 100%: 115.9 MiB
consume  25%: 164.8 MiB | 50%: 167.9 MiB | 75%: 170.4 MiB | 100%: 172.5 MiB
```

Produce is flat (even dropping after GC clears staging); consume grows 7.7 MiB over 1
million messages ≈ **8 bytes/message** — two orders of magnitude below the 100-byte
payload, i.e. the reusable buffers + `DeliveryLedger` behave correctly and retain no
messages. The residual RSS at the end (~113 MiB over the start) is the fetch queues of
the **two** consumers created in the same process (`queued.max.messages.kbytes`
defaults to 64 MB per client), not JS objects.

### Event-loop responsiveness (NFR-2)

Measured in `test/integration/vertical-slice.test.ts` case (c): produce + consume
100,000 messages while a `setInterval(10 ms)` runs concurrently.

```
[NFR-2] ticks=25 avgDrift=0.66ms p99=2.83ms max=2.83ms
        | produce 551,449 msg/s, roundtrip 373,551 msg/s
```

Average drift **0.66 ms** (5 ms acceptance threshold), worst case 2.83 ms.

### The M1 decision point — does adaptive polling pass?

**It passes. No need to activate the Worker blocking-poll fallback early (design §5.2).**

Grounds:

1. **Throughput is not limited by the poll model.** When the local queue has data, the
   HOT loop runs at 0.3–0.5 ms/round, pulling 1,000 messages per round ⇒ 1.37 M msg/s,
   at the broker ceiling. The cost sits in the decoder (key/value copies), not in poll
   scheduling.
2. **Idle CPU is near zero.** WARM backoff 1→2→4…→50 ms plus COLD 500 ms (`unref`)
   means an idle producer/consumer costs only a few dozen polls per second.
3. **Latency has a clear, tunable ceiling.** The worst-case delay with an empty queue
   equals `js.poll.idle.max.ms` (default 50 ms) — which is also the ceiling for
   REBALANCE/OFFSET_COMMIT delay. Lowering it to 5–10 ms costs only ~5 % extra CPU.
4. **The event loop is not monopolized** (0.66 ms average drift at 100k msg load).

Work for M3/M6 (not blocking M2):

- Consider adding a smaller `fetch.queue.backoff.ms` to the **recommended consumer
  configuration** in the docs (without changing the default, to preserve librdkafka
  semantics like confluent-kafka-javascript/node-rdkafka).
- Consider a history-adaptive WARM backoff (keep the ceiling low right after traffic,
  only stretch to 50 ms when truly idle for a while) — cheaper than enabling the Worker.
- Reduce allocation in `BatchDecoder` (this is the real CPU cost of consume,
  ~0.7 µs/message).

### Repro commands

```sh
# 0. requires: docker daemon running + shim built (bun run build:native)
#    image apache/kafka:3.9.0 (the script pulls it if missing)

# 1. full baseline (spins up the broker itself, keeps the container afterwards)
bun run bench/m1-baseline.ts

# variants:
TOTAL=500000 bun run bench/m1-baseline.ts        # lighter load for low-RAM machines
CHUNK=500 CONSUME_BATCH=500 bun run bench/m1-baseline.ts
KAFKA_BROKERS=host:9092 bun run bench/m1-baseline.ts   # use an existing broker

# 2. the NFR-2 numbers (event-loop drift) live in the integration test
bun test test/integration/vertical-slice.test.ts

# 3. broker-side reference
docker exec bun-rdkafka-test-kafka /opt/kafka/bin/kafka-consumer-perf-test.sh \
  --bootstrap-server localhost:9092 --topic <topic> --messages 1000000 --group perf1

# 4. clean up
docker rm -f bun-rdkafka-test-kafka
```

### Limitations of this session

- Ran on **linux-x64** only; the M1 DoD also requires darwin-arm64 (no machine available).
- Broker on the same machine ⇒ numbers are a "no-network" ceiling, not a real cluster.
- No `confluent-kafka-javascript` baseline yet (M6 work, goal G3).
- 1 partition, 1 consumer; rebalance/multi-partition under load not yet measured.

---

## M6 — bun-rdkafka vs confluent-kafka-javascript — 2026-08-14

G3 verification (spec §2). **Compared only against upstream running on Node** (decision
of 2026-08-14): confluent-kafka-javascript v1.10.0 **does not run on Bun 1.4** —
prebuilts only go up to NODE_MODULE_VERSION 137 (Node ≤24) while Bun 1.4 requires 147;
a source build for ABI 147 was not feasible within the scope of the bench.

### Environment & methodology

| | |
|---|---|
| Machine | 4 vCPU, 3.8 GB RAM, Linux 5.15 x86-64 — broker on the SAME machine (absolute numbers are compressed, ratios remain meaningful) |
| Broker | apache/kafka:3.9.0 (KRaft, 1 node, 512m heap), restarted clean before the session |
| bun-rdkafka | Bun 1.4.0-canary.1, librdkafka 2.15.0 static, callback API |
| upstream | @confluentinc/kafka-javascript **1.10.0** on **Node v24.15.0**, librdkafka 2.15.0 (bundled prebuilt), callback API |
| Shared config | `queue.buffering.max.ms=5`, `batch.num.messages=10000`, `queue.buffering.max.messages=1e6`, consumer `fetch.queue.backoff.ms=10`, no compression |
| Method | each case run 3 times, **median** taken; producer: 100k warmup + 500k measured (to the last DR); consumer: flowing `'data'`, same data topic for both sides; latency: steady 10k msg/s, 5s warmup + 20s measured |
| Repro | `bun bench/compare/run.ts` (install the contender: `cd bench/upstream && bun add @confluentinc/kafka-javascript`) |

### Results (median of 3)

| Case | bun-rdkafka / Bun | upstream / Node 24 | Ratio | G3 ≥1.2× |
|---|---:|---:|---:|:--:|
| producer 100B acks=1 | **999,821 msg/s** (95.4 MiB/s) | 708,736 (67.6) | **1.41×** | MET |
| producer 100B acks=all | **998,638** (95.2) | 726,585 (69.3) | **1.37×** | MET |
| producer 1KB acks=1 | 446,108 (435.7) | **625,550** (610.9) | **0.71×** | **NOT MET** |
| producer 1KB acks=all | 495,982 (484.4) | **665,616** (650.0) | **0.75×** | **NOT MET** |
| consumer 100B | **1,009,890 msg/s** | 411,854 | **2.45×** | MET |
| consumer 1KB | **668,644** | 281,888 | **2.37×** | MET |
| e2e latency p50/p99 @10k msg/s | 4 / 6 ms | **2 / 3 ms** | — | (not part of G3) |

### Analysis

- **Consumer wins decisively (2.4×)**: as designed — 1 FFI call ↔ N messages +
  `DataView` decoding, versus upstream's per-message V8 object marshalling.
- **Producer 100B wins 1.4×**: staging batches amortize the boundary-crossing cost.
- **Producer 1KB LOSES (0.71–0.75×) — a surprise, cause understood**: the staging
  path currently copies the value into the JS staging buffer (copy 1) and then C's
  `producev(F_COPY)` copies again (copy 2). Upstream copies only once (V8 Buffer →
  `RK_MSG_COPY`). At 100B the FFI cost dominates → we win; at 1KB the memcpy cost
  dominates → the double copy loses. **Fix direction identified** (TODO M6-perf):
  staging writes the original Buffer's `(ptr, len)` instead of bytes (keeping a ref to
  the Buffer until flush; valid because `brk_produce_batch` is synchronous and `F_COPY`
  copies within the call — C keeps no pointer after return, per the design §8 rule) →
  1 copy like upstream, while keeping the batched-FFI advantage.
- **Latency p99 6ms vs 3ms**: the inherent cost of the pull-model (poll cadence +
  batching) versus push-style uv_async. Still single-digit ms; reducible with a smaller
  `js.poll.idle.max.ms` if the user needs latency, trading idle CPU.

### G3 conclusion

| Criterion | Result |
|---|---|
| ≥1.2× upstream-on-Node (spec G3) | **MET in 4/6 cases** (producer 100B ×2, consumer ×2); **NOT MET in 2/6** (producer 1KB ×2) |
| ≥1.5× upstream-on-Bun | Not measurable — upstream does not run on Bun 1.4 (see above) |
| Plan M6 minimum threshold (≥1.0× on-Node) | MET 4/6, NOT MET 2/6 — the double-copy must be fixed before closing M6 |

---

## M6d — produce 1-copy (pointer-based PRODUCE BATCH) — 2026-08-14

Follow-up to the M6 finding above: PRODUCE BATCH (format 3) now carries
`(u64 ptr, i32 len)` for key/value instead of inline bytes — the JS staging
layer no longer memcpys payloads; the single remaining copy is librdkafka's
`F_COPY` inside the synchronous `brk_produce_batch` call. Headers stay inline.
Safety contract documented in `bunrdkafka.h` §format 3 (JS keeps buffers
referenced until the FFI call returns; verified `ptr()` handles view
`byteOffset` correctly, unit-tested with subarrays).

Producer cases re-measured (3-run median, same method as M6: broker recreated
clean before the session, both sides identical librdkafka config). Consumer and
latency numbers are NOT re-measured — produce path change does not affect them;
see the M6 section above.

| Case | bun-rdkafka / Bun | upstream / Node 24 | Ratio |
|---|---:|---:|---:|
| producer 100B acks=1 | 1,041,241 msg/s | 722,873 | **1.44×** |
| producer 100B acks=all | 977,427 msg/s | 676,209 | **1.45×** |
| producer 1KB acks=1 | 439,613 msg/s | 593,242 | **0.74×** |
| producer 1KB acks=all | 450,779 msg/s | 579,320 | **0.78×** |

**100B improved** (999.8k → 1.04M msg/s) and the copy is gone, but **1KB did
not close the gap** — the double-copy was not the dominant cost. Root-cause
analysis of the remaining 1KB gap:

- Client CPU is NOT the bottleneck: at 1KB our process uses *less* total CPU
  than upstream (1.79s vs 2.03s user+sys for the same workload) and the
  FFI+encode segment is only ~17% of wall time.
- The measured difference sits in drain-side pipeline overlap on this
  4 vCPU box (broker shares the CPUs). Our staging enqueues the whole
  600 MB burst near-instantly (peak RSS 787 MB vs upstream 549 MB), while
  upstream's per-message N-API call paces its enqueue and overlaps
  transmission with production.
- Supporting evidence: with a bounded queue (`queue.buffering.max.messages`
  = 65,536 — closer to production configs), the 1KB result flips to parity
  or better: ours 442–515k vs upstream 369–488k msg/s (2 runs each).
- Interleaved A/B re-runs of the unbounded case show 0.75–0.90× with high
  variance; the machine (4 vCPU / 3 GB, broker co-located, disk near-full
  during some sessions) contributes significant noise at this payload size.

### Final G3 verdict

| Criterion | Result |
|---|---|
| ≥1.2× upstream-on-Node (spec G3) | **MET in 4/6 cases** (producer 100B ×2 at 1.44×, consumer ×2 at 2.4×); **NOT MET in 2/6** (producer 1KB unbounded-burst microbench: 0.74–0.78×) |
| ≥1.5× upstream-on-Bun | Not measurable — upstream does not run on Bun 1.4 (no prebuilt for NODE_MODULE_VERSION 147) |
| Plan M6 minimum (≥1.0× on-Node) | MET in 4/6; the 1KB unbounded case misses it on this hardware, but reaches ≥1.0× with a bounded queue (see analysis) |

Follow-ups recorded for later milestones: paced staging flush for large
payloads (write-combining / flush threshold by bytes, not only record count);
re-run the 1KB case on non-shared hardware in CI (M7+); sync-throw semantics
for byte-based queue caps (`queue.buffering.max.kbytes`) — currently only the
message-count cap (`js.producer.max.pending`) throws synchronously from
`produce()`, byte-cap rejections surface via delivery-report errors.

Repro: `bun bench/compare/run.ts --producer-only` (merges into
`bench/compare/results.json`, keeps consumer/latency medians).

---

## Final consolidated run — 2026-08-14, current HEAD (post 1-copy)

One full session, all seven cases, both sides, on the exact code of the initial
commit (`40f0d84`, which includes the pointer-based PRODUCE BATCH and the
TPL-metadata format): shim rebuilt from HEAD, broker recreated clean, the SASL
side container stopped, 3-run medians, same methodology and shared config as
§M6. **This table supersedes the earlier mixed table** (producer numbers from
§M6d combined with consumer/latency numbers from §M6, which were measured
against different binaries) and is the set quoted in the README.

| Case | bun-rdkafka / Bun | upstream / Node 24 | Ratio | G3 ≥1.2× |
|---|---:|---:|---:|:--:|
| producer 100B acks=1 | **981,760 msg/s** (93.6 MiB/s) | 668,537 (63.8) | **1.47×** | MET |
| producer 100B acks=all | **914,716** (87.2) | 664,739 (63.4) | **1.38×** | MET |
| producer 1KB acks=1 | 408,700 (399.1) | **646,153** (631.0) | **0.63×** | NOT MET |
| producer 1KB acks=all | 517,344 (505.2) | **638,564** (623.6) | **0.81×** | NOT MET |
| consumer 100B | **1,010,779 msg/s** | 408,069 | **2.48×** | MET |
| consumer 1KB | **671,952** | 270,955 | **2.48×** | MET |
| e2e latency p50/p99 @10k msg/s | 4 / 6 ms | **2 / 3 ms** | — | (not part of G3) |

Notes versus the earlier sessions:

- All ratios are consistent with §M6/§M6d: producer 100B 1.38–1.47× (previously
  1.37–1.45×), consumer ~2.5× (previously 2.37–2.45×), latency identical.
- The 1KB producer cases again show the high run-to-run variance documented in
  §M6d: 0.63× and 0.81× here versus 0.74–0.78× there — the two acks variants
  even land on opposite sides of their previous values. The bounded-queue
  analysis and the follow-ups in §M6d remain the current understanding; the G3
  verdict is unchanged (MET in 4/6 cases).

Repro: `bun bench/compare/run.ts` (full run, ~25 minutes on this box).

---

## Consumer prefetch thread (experiment, `js.consume.prefetch`) — 2026-08-17

Measures the shim-side prefetch thread described in
[docs/notes/consumer-prefetch-thread.md](../docs/notes/consumer-prefetch-thread.md):
a thread inside `libbunrdkafka` pre-serializes MESSAGE BATCH frames (≤ 500
messages, ≤ `js.consume.buffer.bytes` each) into a ring of 4 frames, and
`brk_consume_batch` from JS becomes a memcpy of one ready frame. Opt-in, off by
default; producer path unchanged.

### Environment & methodology

Same as §M6 / "Final consolidated run" (4 vCPU / 3.8 GB, broker co-located,
apache/kafka:3.9.0 recreated clean earlier the same day, Bun 1.4.0-canary.1,
librdkafka 2.15.0 static; upstream @confluentinc/kafka-javascript 1.10.0 on
Node v24.15.0; shared librdkafka config incl. consumer
`fetch.queue.backoff.ms=10`; 3-run medians, 100k warmup + 500k measured;
latency 10k msg/s, 5 s warmup + 20 s measured). One harness change: `run.ts`
now deletes each bench topic after its case (`delTopic`) so a full run no
longer fills the broker's disk. Repro: `bun bench/compare/run.ts --prefetch`
(raw numbers in `bench/compare/results.json`).

### Results (median of 3)

| Case | bun-rdkafka / Bun (default) | bun-rdkafka / Bun **+ prefetch thread** | upstream / Node 24 | prefetch ÷ default | prefetch ÷ upstream |
|---|---:|---:|---:|---:|---:|
| consumer 100B | 997,499 msg/s | **1,256,128 msg/s** | 411,091 | **1.26×** | **3.06×** |
| consumer 1KB | 677,664 | **863,998** | 278,685 | **1.28×** | **3.10×** |
| e2e latency p50/p99 @10k msg/s | 4 / 6 ms | 4 / 6 ms | 2 / 3 ms | = | — |
| producer 100B acks=1 | 1,084,103 | (n/a — producer path unchanged) | 743,556 | — | 1.46× (default) |
| producer 100B acks=all | 1,028,990 | | 727,555 | — | 1.41× |
| producer 1KB acks=1 | 592,906 | | 590,276 | — | 1.00× |
| producer 1KB acks=all | 648,041 | | 660,349 | — | 0.98× |

CPU of the consuming process (single run each, 600k messages, offsets checked
0…599,999 monotonic, no duplicates or gaps in both modes):

| payload | default | + prefetch thread |
|---|---:|---:|
| 100 B | 1,029,049 msg/s @ **102 %** of a core | 1,362,147 msg/s @ **135 %** |
| 1 KiB | 685,199 msg/s @ 123 % | 909,536 msg/s @ 162 % |

### Analysis

- **+26–33 % consumer throughput for +33–40 % CPU** — the parallelism buys
  almost exactly what the profile in the note predicted (FFI share of the wall
  clock was 25–30 %, so the ceiling was ~1.3–1.4×). Per core it is a wash; it
  is a win when there is an idle core, which is the common deployment.
- **Latency unchanged** (p50 4 ms / p99 6 ms): the frame is handed over as
  soon as it is filled (the thread polls with a 100 ms timeout but returns on
  the first message), and the JS side still polls on the same PollScheduler
  cadence — the pull-model timer, not the thread, sets the floor.
- **The remaining consume cost is JS-side** (`BatchDecoder` + per-message
  routing/`data` emit ≈ 70 % of wall at 100 B). That is where the next step
  is, independent of threading.
- **Producer 1KB at parity with upstream this session** (1.00× / 0.98×, versus
  0.63–0.81× in the earlier sessions). Nothing changed in the producer path
  today; what changed is the environment: broker recreated clean and topics
  deleted between cases, i.e. a broker with a near-empty log dir and a
  non-full disk. This supports the §M6d reading that the earlier 1KB deficit
  was environmental noise on this box rather than a client-side cost.

### Verdict

Keep as an **opt-in experiment**. Correctness holds for the flowing path
(2000-message integration test with rebalance events + clean disconnect;
600k-message offset checks). Before it can become a default: drop/re-seek
prefetched frames on seek/pause/revoke (see the note's "Semantics" section),
and a Windows CI build of the thread primitives.

---

## Consume path optimizations — 2026-08-17 (later the same day)

Profile-driven pass over the consume path after the prefetch experiment had
shown that ~70 % of the JS thread's wall time was JS-side. Findings and fixes,
in order of impact (all measured on this box with the micro-benchmarks in the
session's scratchpad and confirmed by the full run below):

1. **`setTimeout(fn, 0)` in the HOT poll loop costs ~1.14 ms per hop in Bun**
   (`setImmediate`: 3.7 µs, still yields to I/O). The scheduler used it every
   8th HOT round, capping the hot loop at ~500 messages per millisecond and
   punishing small batches badly. → `PollScheduler` uses `setImmediate` for the
   macrotask break (`SchedulerTimers.setImmediate`, falls back to a 0 ms timer).
2. **`Buffer.from(ab, off, len)` on a `slice()`d Uint8Array is ~5× slower than
   `Buffer.copyBytesFrom`** in Bun (creating a second view materializes the
   ArrayBuffer): 4.6 M/s vs 21–23 M/s at 100 B. `toMessage()` did exactly that
   for every key/value.
3. **Fresh buffer per consume batch, messages as views** (ADR-6 updated): the
   C fill writes into a `Buffer.allocUnsafeSlow` sized to the traffic (2× the
   previous batch, 4 KiB … `js.consume.buffer.bytes`), and key/value/headers are
   `subarray` views — no per-message copy at all, and the buffer is never passed
   to C again so the messages stay valid indefinitely. Trade-off: a retained
   message pins its (≤ 256 KiB) batch buffer. Sweep of the cap at 1 KiB:
   64 KiB 1.0 M/s, 256 KiB 1.05 M/s, 512 KiB 1.0 M/s, 1 MiB 0.85 M/s, 4 MiB
   0.85 M/s — blocks above ~512 KiB are fresh mmaps (page faults per batch),
   so the default `js.consume.buffer.bytes` moved from 4 MiB to **256 KiB**.
4. Decoder: hand-rolled hot loop (one bounds check per fixed header, i64 as two
   u32 reads instead of BigInt, per-batch topic-id cache) — 10.9 → 13.0 M
   decoded msg/s at 100 B in isolation.
5. Consumer buffer: `Array.shift()`/`splice(0, n)` replaced by an O(1) FIFO
   (`core/fifo.ts`).
6. C: `serialize_msg` writes straight into the out buffer when the record fits
   (was: scratch + memcpy per message), and the topic intern lookup (mutex +
   strcmp per message) is cached per fill by `rd_kafka_topic_t*`.
7. Latency: after data runs out the scheduler now stays at the 1 ms cadence for
   100 ms before backing off exponentially (`idleHoldMs`), so steady traffic is
   never met by a 2–8 ms timer.

8. KafkaJS `eachMessage`: the per-message `offsetsStore` went through a tpl
   encode (JS) → tpl decode + list alloc (C) → `rd_kafka_offsets_store` and
   showed up as 13 % of the JS thread. New `brk_offset_store_single(topic_id,
   partition, offset, leader_epoch)` reuses one cached list on the C side;
   `NativeClient.offsetsStore` takes it for a single already-interned
   partition. `eachMessage` 960k → 1.28–1.32 M msg/s at 100 B (`eachBatch`
   1.33 M; callback flowing mode 1.59 M).

JS-side cost per message (micro-benchmark, decode + convert + queue + emit):
100 B 370 ns → 135 ns; 1 KiB 370 ns → ~250 ns.

### Environment & methodology

Same as the prefetch section (4 vCPU / 3.8 GB, broker co-located, Bun
1.4.0-canary.1, librdkafka 2.15.0 static, upstream 1.10.0 on Node v24.15.0,
3-run medians, 100k warmup + 500k measured, latency 20 s @ 10k msg/s). Repro:
`bun bench/compare/run.ts --prefetch`; raw numbers in `bench/compare/results.json`.

### Results (median of 3)

| Case | bun-rdkafka / Bun (default) | + `js.consume.prefetch` | upstream / Node 24 | default ÷ upstream | default ÷ previous session |
|---|---:|---:|---:|---:|---:|
| consumer 100B | **1,588,927 msg/s** | 1,689,034 | 396,047 | **4.01×** | 1.59× (997k) |
| consumer 1KB | **1,045,989** | 850,449 | 223,684 | **4.68×** | 1.54× (678k) |
| e2e latency p50/p99 @10k msg/s | **2 / 4 ms** | 2 / 4 ms | 2 / 3 ms | — | was 4 / 6 ms |
| producer 100B acks=1 | 906,242 | (unchanged path) | 713,628 | 1.27× | — |
| producer 100B acks=all | 934,675 | | 703,847 | 1.33× | — |
| producer 1KB acks=1 | 468,174 | | 578,442 | 0.81× | — |
| producer 1KB acks=all | 595,660 | | 572,919 | 1.04× | — |

CPU of the consuming process (single run each): 100 B 1.59 M msg/s @ 114 %
(prefetch: 1.55 M @ 134 %); 1 KiB 0.79–1.05 M msg/s @ 108 % (prefetch:
0.81 M @ 123 %). Both consumer cases vary ±15 % run to run at 1 KiB on this box
(the measured window is now < 0.5 s; the topic holds 600k messages).

### Analysis

- **Consumer 1.55–1.6× faster than this morning, 4–4.7× upstream.** The JS
  thread's profile is now ~66 % inside `brk_consume_batch` (librdkafka fetch +
  serialization) and ~15 % decode/emit; the process is at ~110 % of a core with
  librdkafka's threads included, i.e. the JS thread is no longer saturated.
- **The prefetch thread stopped paying off** (+6 % at 100 B, −19 % at 1 KiB
  in this run, +20 % CPU): its whole premise was a saturated JS thread. It stays
  opt-in and documented as such; a case where the JS thread is busy with other
  work (HTTP server + consumer in one process) may still benefit.
- **Where the ceiling is now:** the fetch side — librdkafka's broker thread
  parsing the fetch responses and the co-located broker on a 4-vCPU box.
  Nothing on the JS side would move it much further here.
- **Latency p50 = upstream** (2 ms); p99 4 ms vs 3 ms is Bun's ~1 ms timer
  granularity on the idle poll (`setTimeout(1)` measures 1.14 ms).
- Producer numbers moved with the environment again (100 B 1.27–1.33× vs
  1.41–1.46× this morning; 1 KB 0.81× / 1.04× vs 1.00× / 0.98×) — the producer
  path is untouched; see §M6d for the variance discussion.
