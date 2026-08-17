/**
 * ffi/symbols.ts — the FFI signature table for every `brk_*` function.
 *
 * SOURCE OF TRUTH: `native/include/bunrdkafka.h`. Ordering and grouping here
 * follow the header's declaration order for easy visual cross-checking. Any
 * header change must update this file at the same time and bump
 * `BRK_ABI_VERSION` (design §3.2).
 *
 * Type mapping conventions:
 *  - `void*` (handle, `char**` out-param)      → "ptr"
 *  - `const char*` (NUL-terminated string in)  → "cstring" (as an arg ≡ pointer;
 *                                                 JS passes a Uint8Array with a trailing NUL)
 *  - `char*` / `uint8_t*` / `int16_t*` (JS-allocated buffer) → "ptr" (pass a TypedArray)
 *  - `int32_t` → "i32", `int64_t` → "i64", `uint64_t` → "u64"
 *  - returned `const char*` → "cstring" (Bun wraps it as CString)
 *
 * This layer contains NO logic; buffer/handle management lives in the core layer.
 */

import type { FFIFunction } from "bun:ffi";

export const brkSymbols = {
  /* --------------------------------------------------------------- common */

  /** `int32_t brk_abi_version(void)` */
  brk_abi_version: { args: [], returns: "i32" },
  /** `const char *brk_librdkafka_version(void)` */
  brk_librdkafka_version: { args: [], returns: "cstring" },
  /** `const char *brk_features(void)` (ABI 2) */
  brk_features: { args: [], returns: "cstring" },
  /** `void brk_mem_free(void *p)` */
  brk_mem_free: { args: ["ptr"], returns: "void" },

  /* --------------------------------------------------------------- config */

  /** `void *brk_conf_new(void)` */
  brk_conf_new: { args: [], returns: "ptr" },
  /** `void brk_conf_destroy(void *conf)` */
  brk_conf_destroy: { args: ["ptr"], returns: "void" },
  /** `int32_t brk_conf_set(void *conf, const char *name, const char *value, char *errstr, int32_t errstr_size)` */
  brk_conf_set: { args: ["ptr", "cstring", "cstring", "ptr", "i32"], returns: "i32" },

  /* ------------------------------------------------------ client lifecycle */

  /** `void *brk_client_new(int32_t type, void *conf, char *errstr, int32_t errstr_size)` */
  brk_client_new: { args: ["i32", "ptr", "ptr", "i32"], returns: "ptr" },
  /** `void brk_client_destroy(void *h)` */
  brk_client_destroy: { args: ["ptr"], returns: "void" },
  /** `int32_t brk_client_outq_len(void *h)` */
  brk_client_outq_len: { args: ["ptr"], returns: "i32" },
  /** `int32_t brk_last_error(void *h)` */
  brk_last_error: { args: ["ptr"], returns: "i32" },
  /** `int32_t brk_last_error_string(void *h, char *buf, int32_t cap)` */
  brk_last_error_string: { args: ["ptr", "ptr", "i32"], returns: "i32" },
  /** `int32_t brk_last_required_size(void *h)` */
  brk_last_required_size: { args: ["ptr"], returns: "i32" },

  /* ----------------------------------------------------------- event drain */

  /** `int32_t brk_events_poll(void *h, uint8_t *buf, int32_t buf_cap, int32_t max_events, int32_t timeout_ms)` */
  brk_events_poll: { args: ["ptr", "ptr", "i32", "i32", "i32"], returns: "i32" },

  /* -------------------------------------------------------------- producer */

  /** `int32_t brk_produce_batch(void *h, const uint8_t *in_buf, int32_t in_len, int16_t *err_out, int32_t max_records)` */
  brk_produce_batch: { args: ["ptr", "ptr", "i32", "ptr", "i32"], returns: "i32" },
  /** `int32_t brk_flush(void *h, int32_t timeout_ms)` */
  brk_flush: { args: ["ptr", "i32"], returns: "i32" },

  /** `int32_t brk_init_transactions(void *h, int32_t timeout_ms, char *errstr, int32_t errstr_size)` */
  brk_init_transactions: { args: ["ptr", "i32", "ptr", "i32"], returns: "i32" },
  /** `int32_t brk_begin_transaction(void *h, char *errstr, int32_t errstr_size)` */
  brk_begin_transaction: { args: ["ptr", "ptr", "i32"], returns: "i32" },
  /** `int32_t brk_commit_transaction(void *h, int32_t timeout_ms, char *errstr, int32_t errstr_size)` */
  brk_commit_transaction: { args: ["ptr", "i32", "ptr", "i32"], returns: "i32" },
  /** `int32_t brk_abort_transaction(void *h, int32_t timeout_ms, char *errstr, int32_t errstr_size)` */
  brk_abort_transaction: { args: ["ptr", "i32", "ptr", "i32"], returns: "i32" },
  /**
   * `int32_t brk_send_offsets_to_transaction(void *h, const uint8_t *tpl_buf, int32_t tpl_len,
   *                                          void *consumer_h, int32_t timeout_ms,
   *                                          char *errstr, int32_t errstr_size)`
   */
  brk_send_offsets_to_transaction: {
    args: ["ptr", "ptr", "i32", "ptr", "i32", "ptr", "i32"],
    returns: "i32",
  },

  /* -------------------------------------------------------------- consumer */

  /** `int32_t brk_subscribe(void *h, const uint8_t *topics_buf, int32_t len)` */
  brk_subscribe: { args: ["ptr", "ptr", "i32"], returns: "i32" },
  /** `int32_t brk_unsubscribe(void *h)` */
  brk_unsubscribe: { args: ["ptr"], returns: "i32" },
  /** `int32_t brk_subscription(void *h, uint8_t *buf, int32_t cap)` */
  brk_subscription: { args: ["ptr", "ptr", "i32"], returns: "i32" },
  /** `int32_t brk_consume_batch(void *h, uint8_t *buf, int32_t buf_cap, int32_t max_msgs, int32_t timeout_ms)` */
  brk_consume_batch: { args: ["ptr", "ptr", "i32", "i32", "i32"], returns: "i32" },
  /** `int32_t brk_consume_prefetch_start(void *h, int32_t frame_cap, int32_t max_msgs, int32_t nframes)` (experiment) */
  brk_consume_prefetch_start: { args: ["ptr", "i32", "i32", "i32"], returns: "i32" },
  /** `int32_t brk_consume_prefetch_stop(void *h)` */
  brk_consume_prefetch_stop: { args: ["ptr"], returns: "i32" },
  /** `int64_t brk_consume_prefetch_stats(void *h)` — frames filled, -1 when inactive */
  brk_consume_prefetch_stats: { args: ["ptr"], returns: "i64" },
  /** `int32_t brk_commit(void *h, const uint8_t *tpl_buf, int32_t len, int32_t async)` */
  brk_commit: { args: ["ptr", "ptr", "i32", "i32"], returns: "i32" },
  /**
   * `int32_t brk_committed(void *h, const uint8_t *tpl_buf, int32_t tpl_len,
   *                        uint8_t *out_buf, int32_t out_cap, int32_t timeout_ms)`
   */
  brk_committed: { args: ["ptr", "ptr", "i32", "ptr", "i32", "i32"], returns: "i32" },
  /**
   * `int32_t brk_offsets_for_times(void *h, const uint8_t *tpl_buf, int32_t tpl_len,
   *                                uint8_t *out_buf, int32_t out_cap, int32_t timeout_ms)` (ABI 2)
   */
  brk_offsets_for_times: { args: ["ptr", "ptr", "i32", "ptr", "i32", "i32"], returns: "i32" },
  /** `int32_t brk_seek(void *h, const char *topic, int32_t partition, int64_t offset, int32_t timeout_ms)` */
  brk_seek: { args: ["ptr", "cstring", "i32", "i64", "i32"], returns: "i32" },
  /** `int32_t brk_assign(void *h, const uint8_t *tpl_buf, int32_t len, int32_t mode)` */
  brk_assign: { args: ["ptr", "ptr", "i32", "i32"], returns: "i32" },
  /** `int32_t brk_assignment(void *h, uint8_t *buf, int32_t cap)` */
  brk_assignment: { args: ["ptr", "ptr", "i32"], returns: "i32" },
  /** `int32_t brk_position(void *h, uint8_t *buf, int32_t cap)` */
  brk_position: { args: ["ptr", "ptr", "i32"], returns: "i32" },
  /** `int32_t brk_pause_resume(void *h, const uint8_t *tpl_buf, int32_t len, int32_t resume)` */
  brk_pause_resume: { args: ["ptr", "ptr", "i32", "i32"], returns: "i32" },
  /** `int32_t brk_offsets_store(void *h, const uint8_t *tpl_buf, int32_t len)` */
  brk_offsets_store: { args: ["ptr", "ptr", "i32"], returns: "i32" },
  /** `int32_t brk_offset_store_single(void *h, int32_t topic_id, int32_t partition, int64_t offset, int32_t leader_epoch)` */
  brk_offset_store_single: { args: ["ptr", "i32", "i32", "i64", "i32"], returns: "i32" },
  /**
   * `int32_t brk_query_watermark(void *h, const char *topic, int32_t partition,
   *                              int64_t *lo, int64_t *hi, int32_t timeout_ms)`
   */
  brk_query_watermark: {
    args: ["ptr", "cstring", "i32", "ptr", "ptr", "i32"],
    returns: "i32",
  },
  /** `int32_t brk_get_watermark(void *h, const char *topic, int32_t partition, int64_t *lo, int64_t *hi)` */
  brk_get_watermark: { args: ["ptr", "cstring", "i32", "ptr", "ptr"], returns: "i32" },

  /* ------------------------------------------------- topic name interning */

  /** `int32_t brk_topic_name(void *h, int32_t topic_id, uint8_t *buf, int32_t cap)` */
  brk_topic_name: { args: ["ptr", "i32", "ptr", "i32"], returns: "i32" },

  /* ---------------------------------------------------------- oauthbearer */

  /**
   * `int32_t brk_oauthbearer_set_token(void *h, const char *token, int64_t lifetime_ms,
   *                                    const char *principal, const uint8_t *extensions_buf,
   *                                    int32_t extensions_len, char *errstr, int32_t errstr_size)`
   */
  brk_oauthbearer_set_token: {
    args: ["ptr", "cstring", "i64", "cstring", "ptr", "i32", "ptr", "i32"],
    returns: "i32",
  },
  /** `int32_t brk_oauthbearer_set_token_failure(void *h, const char *errstr)` */
  brk_oauthbearer_set_token_failure: { args: ["ptr", "cstring"], returns: "i32" },

  /* ------------------------------------------------------------------ sasl */

  /** `int32_t brk_sasl_set_credentials(void *h, const char *username, const char *password)` */
  brk_sasl_set_credentials: { args: ["ptr", "cstring", "cstring"], returns: "i32" },

  /* ------------------------------------------------------ metadata & admin */

  /** `int32_t brk_metadata(void *h, const char *topic_or_null, int32_t timeout_ms, char **out)` */
  brk_metadata: { args: ["ptr", "cstring", "i32", "ptr"], returns: "i32" },
  /** `int32_t brk_admin_request(void *h, int32_t op_id, uint64_t correlation_id, const char *req_json)` */
  brk_admin_request: { args: ["ptr", "i32", "u64", "cstring"], returns: "i32" },
} as const satisfies Record<string, FFIFunction>;

export type BrkSymbols = typeof brkSymbols;

/** Names of all symbols the native library must export. */
export const BRK_SYMBOL_NAMES = Object.keys(brkSymbols) as (keyof BrkSymbols)[];
