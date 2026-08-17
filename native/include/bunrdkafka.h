/*
 * bunrdkafka.h — The single ABI contract between the native shim (C) and the
 * TypeScript layer (bun:ffi).
 *
 * Source of truth for: the symbol table, error codes, enums, and the packed
 * binary formats. Any compatibility-breaking change MUST bump BRK_ABI_VERSION
 * and update packages/bun-rdkafka/src/ffi/{symbols,types}.ts at the same time
 * (docs/02-detail-design.md §3).
 *
 * General conventions:
 *  - POD types only: void* (handle), int16/32/64, uint8_t* buf + size, const char*.
 *  - Strings passed into C are UTF-8, NUL-terminated except when carried inside
 *    a packed buffer (there they are length-prefixed, NO NUL).
 *  - Hot-path buffers are allocated by JS; C only writes into them. C NEVER
 *    keeps a pointer into JS memory after the function returns.
 *  - Integers in packed formats: little-endian, no padding/alignment.
 *  - int32 return values: >= 0 means success (usually a count or byte count);
 *    < 0 is an error per the BRK_ERR_* / BRK_KAFKA_ERR convention below.
 */

#ifndef BUNRDKAFKA_H
#define BUNRDKAFKA_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#if defined(_WIN32)
#define BRK_EXPORT __declspec(dllexport)
#else
#define BRK_EXPORT __attribute__((visibility("default")))
#endif

/* ========================================================================== */
/* Version                                                                     */
/* ========================================================================== */

/* ABI history:
 *   1 — initial release (0.1.x)
 *   2 — added brk_features, brk_offsets_for_times (no format changes) */
#define BRK_ABI_VERSION 2

/* ========================================================================== */
/* Error codes                                                                 */
/* ==========================================================================
 * Two ranges, both negative, non-overlapping:
 *
 *  1) Shim-layer errors:  -1 .. -99   (BRK_ERR_*)
 *  2) librdkafka errors:  ret = BRK_ERR_KAFKA_OFFSET + rd_kafka_resp_err_t
 *     with BRK_ERR_KAFKA_OFFSET = -1000.
 *     rd_kafka_resp_err_t lies in [-200, ~+120], so ret lies in
 *     [-1200, -880] — always negative, never touching the shim range.
 *     JS-side decoding: kafkaErr = ret - BRK_ERR_KAFKA_OFFSET (i.e. ret + 1000).
 *
 * (Note: we do not use a "-(err)" convention because rd_kafka internal errors
 *  are already negative; negating them would make them positive and collide
 *  with the success range.)
 *
 * Human-readable detail for the most recent error on a handle:
 * brk_last_error_string(). For BRK_ERR_BUFFER_TOO_SMALL, the required size is
 * available via brk_last_required_size().
 */

#define BRK_OK 0
#define BRK_ERR_BUFFER_TOO_SMALL (-1) /* out-buffer cannot fit even one record */
#define BRK_ERR_INVALID_HANDLE (-2)   /* handle is NULL or already destroyed */
#define BRK_ERR_INVALID_STATE (-3)    /* wrong client type / wrong state */
#define BRK_ERR_BAD_ARGS (-4)         /* invalid argument / packed buffer */
#define BRK_ERR_DECODE (-5)           /* packed buffer from JS failed to parse */
#define BRK_ERR_UNKNOWN_TOPIC_ID (-6) /* topic_id not present in intern table */
#define BRK_ERR_NOMEM (-7)
#define BRK_ERR_UNSUPPORTED (-8)

#define BRK_ERR_KAFKA_OFFSET (-1000)
#define BRK_KAFKA_ERR(e) (BRK_ERR_KAFKA_OFFSET + (int32_t)(e))
#define BRK_IS_KAFKA_ERR(ret) ((ret) <= -800 && (ret) >= -1300)
#define BRK_KAFKA_ERR_OF(ret) ((int32_t)(ret)-BRK_ERR_KAFKA_OFFSET)

/* ========================================================================== */
/* Enums                                                                       */
/* ========================================================================== */

/* Client type (brk_client_new) */
#define BRK_CLIENT_PRODUCER 0
#define BRK_CLIENT_CONSUMER 1

/* Event types in the event frame (brk_events_poll) */
#define BRK_EVENT_DR 1            /* delivery reports (many DRs per frame)     */
#define BRK_EVENT_ERROR 2         /* client-level error (including fatal)      */
#define BRK_EVENT_LOG 3
#define BRK_EVENT_STATS 4         /* raw statistics JSON from librdkafka       */
#define BRK_EVENT_REBALANCE 5
#define BRK_EVENT_OFFSET_COMMIT 6
#define BRK_EVENT_THROTTLE 7
#define BRK_EVENT_OAUTH_REFRESH 8 /* new token needed: JS calls brk_oauthbearer_set_token */
#define BRK_EVENT_ADMIN_RESULT 9

/* Modes for brk_assign */
#define BRK_ASSIGN 0
#define BRK_ASSIGN_INCREMENTAL 1
#define BRK_UNASSIGN_INCREMENTAL 2
#define BRK_UNASSIGN 3

/* Protocol in the REBALANCE payload */
#define BRK_REBALANCE_PROTOCOL_EAGER 0
#define BRK_REBALANCE_PROTOCOL_COOPERATIVE 1

/* op_id for brk_admin_request (request/response payloads are JSON, see §Admin) */
#define BRK_ADMIN_CREATE_TOPICS 1
#define BRK_ADMIN_DELETE_TOPICS 2
#define BRK_ADMIN_CREATE_PARTITIONS 3
#define BRK_ADMIN_LIST_GROUPS 4
#define BRK_ADMIN_DESCRIBE_GROUPS 5
#define BRK_ADMIN_DELETE_GROUPS 6
#define BRK_ADMIN_LIST_CONSUMER_GROUP_OFFSETS 7
#define BRK_ADMIN_DELETE_RECORDS 8
#define BRK_ADMIN_DESCRIBE_TOPICS 9
#define BRK_ADMIN_LIST_OFFSETS 10 /* offset_spec: -2 EARLIEST, -1 LATEST,
                                   * -3 MAX_TIMESTAMP, >=0 = timestamp ms
                                   * (rd_kafka_OffsetSpec_t) */

/* ========================================================================== */
/* PACKED BINARY FORMATS (little-endian, no padding)                           */
/* ==========================================================================
 *
 * Notation: u8/u16/u32/u64/i16/i32/i64 = LE integers; "bytes" = raw bytes.
 * Strings/bytes are length-prefixed; length = -1 (for signed types) means NULL
 * (distinct from an empty string of length 0).
 *
 * ---- 1. STRING LIST (JS→C, e.g. brk_subscribe) ------------------------------
 *   u32 count
 *   count × { u16 len, bytes }                        // UTF-8, no NUL
 *
 * ---- 2. TOPIC-PARTITION LIST "tpl" (both directions JS↔C) -------------------
 *   u32 count
 *   count × {
 *     i32 topic_id      // interned id; -1 when using name (mandatory for JS→C
 *                       // when the topic has never been seen; C→JS always uses
 *                       // the id when available)
 *     u16 name_len      // 0 when topic_id is used
 *     bytes name
 *     i32 partition
 *     i64 offset        // may be RD_KAFKA_OFFSET_INVALID (-1001) etc.
 *     i32 leader_epoch  // -1 if absent
 *     i16 metadata_len  // -1 = absent; ≥0 = commit metadata (UTF-8, no NUL)
 *     bytes metadata    // broker-limited by offset.metadata.max.bytes (~4KB)
 *   }
 *
 * ---- 3. PRODUCE BATCH (JS→C, brk_produce_batch in_buf) ----------------------
 *   u32 count
 *   count × {
 *     u16 topic_len, bytes topic
 *     i32 partition                 // -1 = RD_KAFKA_PARTITION_UA (partitioner)
 *     i64 timestamp_ms              // 0 = let librdkafka use now
 *     u64 opaque_id                 // assigned by JS, returned verbatim in DR
 *     u64 key_ptr,   i32 key_len    // key/value are NOT inline: pointers into
 *     u64 value_ptr, i32 value_len  //   JS memory (1 copy — see note below)
 *     u16 header_count
 *     header_count × { u16 klen, bytes k, i32 vlen, bytes v }  // headers stay
 *   }                               //   inline (small); vlen -1 = null
 *
 *   key/value ptr semantics: len == -1 → NULL (ptr ignored, written as 0 by
 *   convention); len == 0 → EMPTY value (distinct from NULL; ptr may be 0, C
 *   substitutes an internal dummy pointer so librdkafka does not treat it as
 *   NULL); len > 0 → ptr must be non-zero and point to len valid bytes.
 *
 *   MEMORY SAFETY: this is valid BECAUSE brk_produce_batch is synchronous and
 *   produce uses RD_KAFKA_MSG_F_COPY — C finishes copying within the call and
 *   keeps no pointer after returning (per the general convention at the top of
 *   this file). JS MUST keep the buffers alive (hold references) until the FFI
 *   call returns.
 *
 * ---- 4. MESSAGE BATCH (C→JS, brk_consume_batch out buf) ---------------------
 *   Buffer = count records back to back (count is the function's return value):
 *   {
 *     i32 topic_id        // look up in intern table; on miss → brk_topic_name()
 *     i32 partition
 *     i64 offset
 *     i64 timestamp_ms    // -1 if absent
 *     u8  timestamp_type  // rd_kafka_timestamp_type_t
 *     i16 err             // 0 = normal message; non-zero (e.g. _PARTITION_EOF)
 *                         //   means key/value/headers are empty and JS treats
 *                         //   the record as an event
 *     i32 key_len,   bytes key      // -1 = null
 *     i32 value_len, bytes value    // -1 = null
 *     u16 header_count
 *     header_count × { u16 klen, bytes k, i32 vlen, bytes v }
 *     i32 leader_epoch    // -1 if absent
 *   }
 *
 * ---- 5. EVENT FRAME (C→JS, brk_events_poll out buf) -------------------------
 *   Buffer = count frames back to back (count is the return value):
 *   { u8 event_type; u32 payload_len; bytes payload }
 *
 *   Payload by event_type:
 *   DR:            u32 n; n × { u64 opaque_id, i16 err, i32 partition,
 *                               i64 offset, i64 timestamp_ms }
 *   ERROR:         i32 kafka_err, u8 is_fatal, u16 reason_len, bytes reason
 *   LOG:           i32 level, u16 fac_len, bytes fac, u32 msg_len, bytes msg
 *   STATS:         u32 json_len, bytes json
 *   REBALANCE:     i32 kafka_err   // __ASSIGN_PARTITIONS | __REVOKE_PARTITIONS
 *                  u8 protocol     // BRK_REBALANCE_PROTOCOL_*
 *                  u8 assignment_lost // rd_kafka_assignment_lost at event time
 *                  tpl             // format 2
 *                  // The shim does NOT assign on its own; JS must respond
 *                  // via brk_assign().
 *   OFFSET_COMMIT: i32 kafka_err, tpl
 *   THROTTLE:      i32 broker_id, i32 throttle_ms, u16 name_len, bytes broker_name
 *   OAUTH_REFRESH: u16 cfg_len, bytes oauthbearer_config
 *   ADMIN_RESULT:  u64 correlation_id, i32 kafka_err, u32 json_len, bytes json
 */

/* ========================================================================== */
/* Common                                                                      */
/* ========================================================================== */

/* Returns the library's BRK_ABI_VERSION — the loader checks this BEFORE any
 * other call. */
BRK_EXPORT int32_t brk_abi_version(void);

/* Version string of the statically linked librdkafka, e.g. "2.15.0". Static
 * pointer, do not free. */
BRK_EXPORT const char *brk_librdkafka_version(void);

/* Comma-separated `builtin.features` of the statically linked librdkafka,
 * e.g. "gzip,snappy,ssl,sasl,regex,lz4,sasl_gssapi,sasl_plain,sasl_scram,
 * plugins,zstd,sasl_oauthbearer,http,oidc". Computed once (via a temporary
 * rd_kafka_conf_t) and cached in a static buffer — do not free. Empty string
 * if the property could not be read (never NULL). ABI 2. */
BRK_EXPORT const char *brk_features(void);

/* Frees memory allocated by the shim (only used by cold-path APIs returning
 * char**). */
BRK_EXPORT void brk_mem_free(void *p);

/* ========================================================================== */
/* Config                                                                      */
/* ========================================================================== */

/* Creates a conf handle. Consumed ("swallowed") by a successful brk_client_new;
 * if abandoned, it must be freed with brk_conf_destroy. */
BRK_EXPORT void *brk_conf_new(void);
BRK_EXPORT void brk_conf_destroy(void *conf);

/* Sets one librdkafka property (both global and default topic-level; the shim
 * routes automatically). On error: BRK_KAFKA_ERR(...) with a description
 * written into errstr. */
BRK_EXPORT int32_t brk_conf_set(void *conf, const char *name, const char *value,
                                char *errstr, int32_t errstr_size);

/* ========================================================================== */
/* Client lifecycle                                                            */
/* ========================================================================== */

/* type: BRK_CLIENT_*. Consumes conf on success. On failure: NULL + errstr.
 * The shim automatically: enables the event API for DR/LOG/STATS/ERROR/
 * REBALANCE/OFFSET_COMMIT/OAUTH_REFRESH; there is NO C→JS callback. */
BRK_EXPORT void *brk_client_new(int32_t type, void *conf, char *errstr,
                                int32_t errstr_size);

/* Destroys the handle (blocks until internal threads join). Idempotent-hostile:
 * calling twice = undefined; the TS layer guards with its state machine. */
BRK_EXPORT void brk_client_destroy(void *h);

/* Number of messages/requests still waiting in the out queue
 * (rd_kafka_outq_len). */
BRK_EXPORT int32_t brk_client_outq_len(void *h);

/* Most recent kafka error on the handle + description (for negative returns
 * that carry no errstr). */
BRK_EXPORT int32_t brk_last_error(void *h);
BRK_EXPORT int32_t brk_last_error_string(void *h, char *buf, int32_t cap);

/* After BRK_ERR_BUFFER_TOO_SMALL: minimum byte count needed for the next
 * record. */
BRK_EXPORT int32_t brk_last_required_size(void *h);

/* ========================================================================== */
/* Event drain (all clients)                                                   */
/* ========================================================================== */

/* Drains the main event queue into EVENT FRAMEs (format 5).
 * Returns: number of frames written (0 = nothing available); negative = error.
 * Frames that fit are kept; if the first frame does not fit → BUFFER_TOO_SMALL.
 * timeout_ms: from the main thread ALWAYS pass 0 (non-blocking); values >0 are
 * reserved for the Worker blocking-poll mode. */
BRK_EXPORT int32_t brk_events_poll(void *h, uint8_t *buf, int32_t buf_cap,
                                   int32_t max_events, int32_t timeout_ms);

/* ========================================================================== */
/* Producer                                                                    */
/* ========================================================================== */

/* Batch produce per PRODUCE BATCH (format 3).
 * err_out: int16 array, element i = rd_kafka_resp_err_t of record i
 * (0 = enqueued; QUEUE_FULL etc. are per-record and do NOT fail the batch).
 * Returns: number of records processed; negative = parse/handle error (no
 * record was enqueued). */
BRK_EXPORT int32_t brk_produce_batch(void *h, const uint8_t *in_buf,
                                     int32_t in_len, int16_t *err_out,
                                     int32_t max_records);

/* rd_kafka_flush. NOTE: blocks up to timeout — only call with small repeated
 * timeouts from the main thread, or a large timeout from a Worker. Returns 0
 * or BRK_KAFKA_ERR(__TIMED_OUT). */
BRK_EXPORT int32_t brk_flush(void *h, int32_t timeout_ms);

/* --- Transactions (block up to timeout; same usage guidance as brk_flush) --- */
BRK_EXPORT int32_t brk_init_transactions(void *h, int32_t timeout_ms,
                                         char *errstr, int32_t errstr_size);
BRK_EXPORT int32_t brk_begin_transaction(void *h, char *errstr,
                                         int32_t errstr_size);
BRK_EXPORT int32_t brk_commit_transaction(void *h, int32_t timeout_ms,
                                          char *errstr, int32_t errstr_size);
BRK_EXPORT int32_t brk_abort_transaction(void *h, int32_t timeout_ms,
                                         char *errstr, int32_t errstr_size);
/* tpl_buf: format 2. consumer_h: handle of the consumer providing the offsets
 * (used to fetch its group metadata). */
BRK_EXPORT int32_t brk_send_offsets_to_transaction(void *h,
                                                   const uint8_t *tpl_buf,
                                                   int32_t tpl_len,
                                                   void *consumer_h,
                                                   int32_t timeout_ms,
                                                   char *errstr,
                                                   int32_t errstr_size);

/* ========================================================================== */
/* Consumer                                                                    */
/* ========================================================================== */

/* topics_buf: STRING LIST (format 1). */
BRK_EXPORT int32_t brk_subscribe(void *h, const uint8_t *topics_buf,
                                 int32_t len);
BRK_EXPORT int32_t brk_unsubscribe(void *h);
/* Writes the current subscription as a STRING LIST into buf; returns the topic
 * count. */
BRK_EXPORT int32_t brk_subscription(void *h, uint8_t *buf, int32_t cap);

/* Heart of the consume path: drains up to max_msgs messages into a MESSAGE
 * BATCH (format 4). Returns: number of messages written; 0 = drained;
 * negative = error.
 * Error messages (e.g. _PARTITION_EOF) are also records with err != 0.
 * timeout_ms: 0 from the main thread; >0 only for Worker blocking-poll. */
BRK_EXPORT int32_t brk_consume_batch(void *h, uint8_t *buf, int32_t buf_cap,
                                     int32_t max_msgs, int32_t timeout_ms);

/* EXPERIMENT (docs/notes/consumer-prefetch-thread.md): starts a shim-owned
 * thread that pre-serializes MESSAGE BATCH frames off the JS thread into a
 * ring of `nframes` frames of `frame_cap` bytes / `max_msgs` messages each.
 * While active, brk_consume_batch copies one ready frame into `buf` (its
 * max_msgs/timeout_ms are ignored; BUFFER_TOO_SMALL + brk_last_required_size
 * apply). Must be called before subscribe/assign; stop is implicit in
 * brk_client_destroy. Returns BRK_OK or a negative error. */
BRK_EXPORT int32_t brk_consume_prefetch_start(void *h, int32_t frame_cap,
                                              int32_t max_msgs, int32_t nframes);
BRK_EXPORT int32_t brk_consume_prefetch_stop(void *h);
/* Frames filled by the prefetch thread so far; -1 when not active. */
BRK_EXPORT int64_t brk_consume_prefetch_stats(void *h);

/* tpl_buf NULL / len 0 = commit all current positions.
 * async != 0: returns immediately, result arrives via the OFFSET_COMMIT event.
 * async == 0: blocks until the commit completes (use from a Worker or accept
 * the block). */
BRK_EXPORT int32_t brk_commit(void *h, const uint8_t *tpl_buf, int32_t len,
                              int32_t async);

/* Writes committed offsets (tpl, format 2) into out_buf. tpl_buf/tpl_len select
 * the partitions to query; tpl_len == 0 → use the current assignment. Blocks up
 * to timeout_ms (broker round-trip). Returns the element count. */
BRK_EXPORT int32_t brk_committed(void *h, const uint8_t *tpl_buf,
                                 int32_t tpl_len, uint8_t *out_buf,
                                 int32_t out_cap, int32_t timeout_ms);

/* rd_kafka_offsets_for_times: for every entry of tpl_buf (format 2, the
 * `offset` field carries the TIMESTAMP in ms) looks up the earliest offset
 * whose timestamp is >= that timestamp and writes the result as a tpl (format
 * 2, `offset` = the found offset, or RD_KAFKA_OFFSET_END/-1 when no such
 * message exists) into out_buf. Blocks up to timeout_ms (broker round-trip).
 * Works on any client type. Returns the element count; a per-partition error
 * (e.g. UNKNOWN_PARTITION) fails the whole call with BRK_KAFKA_ERR of the first
 * failing partition. On BRK_ERR_BUFFER_TOO_SMALL the caller grows out_buf and
 * retries (same convention as brk_committed). ABI 2. */
BRK_EXPORT int32_t brk_offsets_for_times(void *h, const uint8_t *tpl_buf,
                                         int32_t tpl_len, uint8_t *out_buf,
                                         int32_t out_cap, int32_t timeout_ms);

/* Seek by topic name (cold path — no interned id needed; always works right
 * after assign). */
BRK_EXPORT int32_t brk_seek(void *h, const char *topic, int32_t partition,
                            int64_t offset, int32_t timeout_ms);

/* mode: BRK_ASSIGN / BRK_ASSIGN_INCREMENTAL / BRK_UNASSIGN_INCREMENTAL /
 * BRK_UNASSIGN. tpl_buf is ignored for BRK_UNASSIGN. Used by JS to answer a
 * REBALANCE event or to assign manually. */
BRK_EXPORT int32_t brk_assign(void *h, const uint8_t *tpl_buf, int32_t len,
                              int32_t mode);

/* Writes the current assignment/position (tpl) into buf; returns the element
 * count. */
BRK_EXPORT int32_t brk_assignment(void *h, uint8_t *buf, int32_t cap);
BRK_EXPORT int32_t brk_position(void *h, uint8_t *buf, int32_t cap);

/* resume != 0 → resume, otherwise pause. tpl_buf: format 2 (offset ignored). */
BRK_EXPORT int32_t brk_pause_resume(void *h, const uint8_t *tpl_buf,
                                    int32_t len, int32_t resume);

/* Stores offsets to be auto-committed (enable.auto.offset.store=false
 * workflow). */
BRK_EXPORT int32_t brk_offsets_store(void *h, const uint8_t *tpl_buf,
                                     int32_t len);

/* Watermarks: query = ask the broker (blocks up to timeout); get = local
 * cache. */
BRK_EXPORT int32_t brk_query_watermark(void *h, const char *topic,
                                       int32_t partition, int64_t *lo,
                                       int64_t *hi, int32_t timeout_ms);
BRK_EXPORT int32_t brk_get_watermark(void *h, const char *topic,
                                     int32_t partition, int64_t *lo,
                                     int64_t *hi);

/* ========================================================================== */
/* Topic name interning                                                        */
/* ========================================================================== */

/* Writes the topic name (UTF-8, no NUL) for topic_id into buf; returns the
 * byte count. topic_id is stable for the lifetime of a handle. */
BRK_EXPORT int32_t brk_topic_name(void *h, int32_t topic_id, uint8_t *buf,
                                  int32_t cap);

/* ========================================================================== */
/* OAUTHBEARER                                                                 */
/* ========================================================================== */

/* Answers an OAUTH_REFRESH event. extensions_buf: STRING LIST (format 1) laid
 * out as [k1, v1, k2, v2, ...]; NULL/0 when absent. */
BRK_EXPORT int32_t brk_oauthbearer_set_token(void *h, const char *token,
                                             int64_t lifetime_ms,
                                             const char *principal,
                                             const uint8_t *extensions_buf,
                                             int32_t extensions_len,
                                             char *errstr, int32_t errstr_size);
BRK_EXPORT int32_t brk_oauthbearer_set_token_failure(void *h,
                                                     const char *errstr);

/* ========================================================================== */
/* SASL                                                                        */
/* ========================================================================== */

/* Changes the SASL PLAIN/SCRAM credentials after the client has been created.
 * Existing connections are not torn down; the new credentials are used for the
 * next authentication (rd_kafka_sasl_set_credentials). */
BRK_EXPORT int32_t brk_sasl_set_credentials(void *h, const char *username,
                                            const char *password);

/* ========================================================================== */
/* Metadata & Admin (cold path — JSON)                                         */
/* ========================================================================== */

/* Cluster metadata (topic_or_null = NULL → all locally known topics).
 * *out = JSON malloc'd by the shim; JS copies it then calls brk_mem_free(*out).
 * Blocks up to timeout_ms. */
BRK_EXPORT int32_t brk_metadata(void *h, const char *topic_or_null,
                                int32_t timeout_ms, char **out);

/* Sends an admin request (op_id: BRK_ADMIN_*, req_json: per-op parameters —
 * schema documented at the top of shim_admin.c, M4). Does not block; the result
 * arrives via an ADMIN_RESULT event carrying the same correlation_id. */
BRK_EXPORT int32_t brk_admin_request(void *h, int32_t op_id,
                                     uint64_t correlation_id,
                                     const char *req_json);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* BUNRDKAFKA_H */
