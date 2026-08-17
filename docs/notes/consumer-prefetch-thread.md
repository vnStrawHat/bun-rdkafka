# Technical note — C-side consumer prefetch thread

Status: **experiment** (opt-in `js.consume.prefetch=true`). Date: 2026-08-17.

## Idea

Today the whole consume path runs on the JS thread: `PollScheduler` calls
`brk_consume_batch` (which does `rd_kafka_queue_poll` + serializes up to N
messages into the JS-owned buffer, format 4), then `BatchDecoder` decodes and
the Callback layer routes/emits. librdkafka's own I/O threads run in parallel,
but the *serialization* of the fetch queue into our batch format is
serialized with JS.

The alternative is to move that step onto a dedicated thread inside the shim:
the thread pulls from `consumer_q`, serializes into a small ring of
pre-allocated frames (each ≤ `js.consume.buffer.bytes`, ≤ 500 messages), and
`brk_consume_batch` from JS becomes a `memcpy` of one ready frame. JS then only
decodes and emits. On a machine with an idle core this overlaps librdkafka →
frame serialization with JS decode/emit.

This is the shim-side realisation of the "blocking poll on a second thread"
extension already sketched in design §5.2 (`js.poll.worker`), without a JS
Worker and without `postMessage`.

## Why it might pay off — measured split of the current path

Profile of the current single-thread path (flowing mode, 500k measured
messages, this box: 4 vCPU, broker co-located; see `bench/RESULTS.md` for the
environment):

| payload | msgs/s | wall | inside `brk_consume_batch` (FFI) | `BatchDecoder` | route/emit/loop |
|---|---:|---:|---:|---:|---:|
| 100 B | 1.02 M | 489 ms | 146 ms (30 %) | 79 ms (16 %) | 264 ms (54 %) |
| 1 KiB | 620 k | 807 ms | 203 ms (25 %) | 233 ms (29 %) | 371 ms (46 %) |

So the ceiling of a prefetch thread on this workload is roughly
1 / (1 − 0.30) ≈ **1.4×** at 100 B and ≈ 1.3× at 1 KiB, at the cost of a second
core. Anything beyond that has to come from the JS side (decode/emit).

Two constraints for the design, both consequences of the profile above:
the parallelism only helps at small payloads (the FFI share shrinks as payload
grows), and at large payloads the per-message copy discipline matters more —
so the thread must keep the current one-copy, zero-alloc frame serialization
(serialize straight into the frame; never malloc/copy per message).

## Semantics to keep in mind

- **Prefetch depth = ring frames × frame size.** Messages already serialized
  into a frame have been *consumed* from librdkafka's point of view: the
  position has advanced and `enable.auto.offset.store` has stored them. With
  auto-commit that can commit offsets JS has not seen yet — same class of
  hazard as any client-side prefetch; users needing at-least-once must
  commit from their handler (`enable.auto.commit=false` / `commitMessage`),
  as with the JS-side buffer today (`MAX_BUFFERED_MESSAGES`).
- **Rebalance/seek/pause:** frames already prefetched for a revoked, sought or
  paused partition are still delivered. Documented limitation of the
  experiment; a production version would drop frames for those partitions
  and `seek` back to the first dropped offset (or flush the ring on
  assign/seek/pause and re-seek).
- **Non-fetch events** on `consumer_q` (REBALANCE, OFFSET_COMMIT) are stashed
  by the thread exactly as the JS-thread path does, so `brk_events_poll`
  keeps returning them; the JS cadence rule "consumeBatch then pollEvents every
  round" is unchanged.
- **Thread safety:** the handle's `scratch` is only touched by whichever
  thread serializes; the events path (`brk_events_poll`, JS thread) and the
  consume path (prefetch thread) get separate scratch buffers; the stash and
  the topic intern table are already mutex-protected.
- **Shutdown:** `brk_client_destroy` stops the thread (flag +
  `rd_kafka_queue_yield` + join) *before* `rd_kafka_consumer_close_queue`.

## Implementation (experiment)

- Shim: `brk_consume_prefetch_start(h, frame_cap, max_msgs, nframes)` /
  `brk_consume_prefetch_stop(h)` / `brk_consume_prefetch_stats(h)`
  (`native/src/shim_consumer.c`). The former body of `brk_consume_batch` is
  now `consume_fill()`; the thread calls it into ring frames, JS's
  `brk_consume_batch` memcpys one ready frame (SPSC ring, mutex + condvar,
  portable thread helpers in `shim_common.c`). The consume path serializes
  into `h->cscratch`, the events path into `h->scratch` — no shared scratch.
  `brk_client_destroy` stops the thread before `consumer_close`.
- JS: `js.consume.prefetch` (boolean, default false) and
  `js.consume.prefetch.frames` (default 4); `NativeClient.connect()` starts
  the thread for consumers right after `brk_client_new`.
- Bench: `bun bench/compare/run.ts --prefetch` adds the row;
  `CONSUMER_EXTRA` (JSON) on `consumer-bench.cjs` / `latency-bench.cjs`.
- Test: `kafka-consumer.integration.test.ts` "flowing + js.consume.prefetch".

## Results (2026-08-17)

Full table and CPU numbers: `bench/RESULTS.md`, section "Consumer prefetch
thread". Summary (3-run medians, same box/broker as the M6 sessions):

| Case | default | + prefetch thread | upstream / Node 24 |
|---|---:|---:|---:|
| consumer 100B | 997,499 msg/s @ ~102 % CPU | **1,256,128** (1.26×) @ ~135 % | 411,091 |
| consumer 1KB | 677,664 @ ~123 % | **863,998** (1.28×) @ ~162 % | 278,685 |
| e2e latency p50/p99 | 4 / 6 ms | 4 / 6 ms | 2 / 3 ms |

Matches the predicted ceiling (~1.3–1.4×). Kept opt-in; see the verdict in
RESULTS.md for what is missing before it could become the default.
