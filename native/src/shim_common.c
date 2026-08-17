/* Conf, client lifecycle, error state, intern table, stash — see shim_internal.h */
#include <stdarg.h>

#include "shim_internal.h"

/* ========================================================================== */
/* Helpers                                                                     */
/* ========================================================================== */

brk_handle *brk_check(void *h) {
  brk_handle *hd = (brk_handle *)h;
  if (hd == NULL || hd->magic != BRK_HANDLE_MAGIC) return NULL;
  return hd;
}

void brk_set_err(brk_handle *h, int32_t code, const char *fmt, ...) {
  if (h == NULL) return;
  brk_mutex_lock(&h->mu);
  h->last_err = code;
  if (fmt != NULL) {
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(h->last_err_str, sizeof(h->last_err_str), fmt, ap);
    va_end(ap);
  } else {
    h->last_err_str[0] = '\0';
  }
  brk_mutex_unlock(&h->mu);
}

int32_t brk_intern_topic(brk_handle *h, const char *name) {
  brk_mutex_lock(&h->mu);
  for (int32_t i = 0; i < h->topics.count; i++) {
    if (strcmp(h->topics.names[i], name) == 0) {
      brk_mutex_unlock(&h->mu);
      return i;
    }
  }
  if (h->topics.count == h->topics.cap) {
    int32_t ncap = h->topics.cap == 0 ? 8 : h->topics.cap * 2;
    char **nn = realloc(h->topics.names, (size_t)ncap * sizeof(char *));
    if (nn == NULL) {
      brk_mutex_unlock(&h->mu);
      return BRK_ERR_NOMEM;
    }
    h->topics.names = nn;
    h->topics.cap = ncap;
  }
  char *copy = strdup(name);
  if (copy == NULL) {
    brk_mutex_unlock(&h->mu);
    return BRK_ERR_NOMEM;
  }
  int32_t id = h->topics.count;
  h->topics.names[id] = copy;
  h->topics.count++;
  brk_mutex_unlock(&h->mu);
  return id;
}

void brk_stash_push(brk_handle *h, const uint8_t *frame, int32_t len) {
  brk_frame *f = malloc(sizeof(brk_frame) + (size_t)len);
  if (f == NULL) return; /* event lost under OOM — acceptable, err recorded */
  f->next = NULL;
  f->len = len;
  memcpy(f->data, frame, (size_t)len);
  brk_mutex_lock(&h->mu);
  if (h->stash_tail != NULL)
    h->stash_tail->next = f;
  else
    h->stash_head = f;
  h->stash_tail = f;
  brk_mutex_unlock(&h->mu);
}

brk_frame *brk_stash_pop(brk_handle *h) {
  brk_mutex_lock(&h->mu);
  brk_frame *f = h->stash_head;
  if (f != NULL) {
    h->stash_head = f->next;
    if (h->stash_head == NULL) h->stash_tail = NULL;
  }
  brk_mutex_unlock(&h->mu);
  return f;
}

uint8_t *brk_buf_reserve(uint8_t **buf, int32_t *cap, int32_t need) {
  if (need <= *cap) return *buf;
  int32_t ncap = *cap == 0 ? 65536 : *cap;
  while (ncap < need) ncap *= 2;
  uint8_t *ns = realloc(*buf, (size_t)ncap);
  if (ns == NULL) return NULL;
  *buf = ns;
  *cap = ncap;
  return ns;
}

uint8_t *brk_scratch_reserve(brk_handle *h, int32_t need) {
  return brk_buf_reserve(&h->scratch, &h->scratch_cap, need);
}

/* ---- threads ------------------------------------------------------------- */
#if defined(_WIN32)
typedef struct { void (*fn)(void *); void *arg; } brk_thread_boot;
static DWORD WINAPI brk_thread_tramp(LPVOID p) {
  brk_thread_boot b = *(brk_thread_boot *)p;
  free(p);
  b.fn(b.arg);
  return 0;
}
int brk_thread_start(brk_thread_t *t, void (*fn)(void *), void *arg) {
  brk_thread_boot *b = malloc(sizeof(*b));
  if (b == NULL) return -1;
  b->fn = fn;
  b->arg = arg;
  *t = CreateThread(NULL, 0, brk_thread_tramp, b, 0, NULL);
  if (*t == NULL) {
    free(b);
    return -1;
  }
  return 0;
}
void brk_thread_join(brk_thread_t *t) {
  WaitForSingleObject(*t, INFINITE);
  CloseHandle(*t);
}
#else
typedef struct { void (*fn)(void *); void *arg; } brk_thread_boot;
static void *brk_thread_tramp(void *p) {
  brk_thread_boot b = *(brk_thread_boot *)p;
  free(p);
  b.fn(b.arg);
  return NULL;
}
int brk_thread_start(brk_thread_t *t, void (*fn)(void *), void *arg) {
  brk_thread_boot *b = malloc(sizeof(*b));
  if (b == NULL) return -1;
  b->fn = fn;
  b->arg = arg;
  if (pthread_create(t, NULL, brk_thread_tramp, b) != 0) {
    free(b);
    return -1;
  }
  return 0;
}
void brk_thread_join(brk_thread_t *t) { pthread_join(*t, NULL); }
#endif

/* ========================================================================== */
/* Version & mem                                                               */
/* ========================================================================== */

BRK_EXPORT int32_t brk_abi_version(void) { return BRK_ABI_VERSION; }

BRK_EXPORT const char *brk_librdkafka_version(void) {
  return rd_kafka_version_str();
}

/* builtin.features, read once through a throwaway conf. Only ever called from
 * the JS thread (bun:ffi is single-threaded), so a plain static is enough. */
BRK_EXPORT const char *brk_features(void) {
  static char features[1024];
  static int loaded = 0;
  if (!loaded) {
    features[0] = '\0';
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (conf != NULL) {
      size_t size = sizeof(features);
      if (rd_kafka_conf_get(conf, "builtin.features", features, &size) !=
          RD_KAFKA_CONF_OK)
        features[0] = '\0';
      rd_kafka_conf_destroy(conf);
    }
    features[sizeof(features) - 1] = '\0';
    loaded = 1;
  }
  return features;
}

BRK_EXPORT void brk_mem_free(void *p) { free(p); }

/* ========================================================================== */
/* Config                                                                      */
/* ========================================================================== */

BRK_EXPORT void *brk_conf_new(void) { return rd_kafka_conf_new(); }

BRK_EXPORT void brk_conf_destroy(void *conf) {
  if (conf != NULL) rd_kafka_conf_destroy((rd_kafka_conf_t *)conf);
}

BRK_EXPORT int32_t brk_conf_set(void *conf, const char *name, const char *value,
                                char *errstr, int32_t errstr_size) {
  if (conf == NULL || name == NULL || value == NULL) return BRK_ERR_BAD_ARGS;
  /* rd_kafka_conf_set also accepts topic-level properties (auto-routed into
   * default_topic_conf since librdkafka >= 1.0). */
  rd_kafka_conf_res_t res = rd_kafka_conf_set(
      (rd_kafka_conf_t *)conf, name, value, errstr, (size_t)errstr_size);
  switch (res) {
    case RD_KAFKA_CONF_OK:
      return BRK_OK;
    case RD_KAFKA_CONF_UNKNOWN:
    case RD_KAFKA_CONF_INVALID:
    default:
      /* unknown vs invalid is distinguished via errstr — librdkafka has no
       * dedicated code */
      return BRK_KAFKA_ERR(RD_KAFKA_RESP_ERR__INVALID_ARG);
  }
}

/* ========================================================================== */
/* Client lifecycle                                                            */
/* ========================================================================== */

/* Throttle has no event type in librdkafka — we use throttle_cb: librdkafka
 * ONLY invokes this callback inside our own rd_kafka_queue_poll/rd_kafka_poll
 * (i.e. on the JS thread, within a brk_* call), never from an internal thread
 * ⇒ safe; stash it and hand it back via brk_events_poll. */
static void brk_throttle_cb(rd_kafka_t *rk, const char *broker_name,
                            int32_t broker_id, int throttle_time_ms,
                            void *opaque) {
  (void)rk;
  brk_handle *h = (brk_handle *)opaque;
  size_t nlen = strlen(broker_name);
  if (nlen > 65535) nlen = 65535;
  int32_t payload = 4 + 4 + 2 + (int32_t)nlen;
  int32_t total = 1 + 4 + payload;
  uint8_t *buf = malloc((size_t)total);
  if (buf == NULL) return;
  brk_wbuf w = {buf, total, 0};
  wb_u8(&w, BRK_EVENT_THROTTLE);
  wb_u32(&w, (uint32_t)payload);
  wb_i32(&w, broker_id);
  wb_i32(&w, throttle_time_ms);
  wb_u16(&w, (uint16_t)nlen);
  wb_raw(&w, broker_name, (int32_t)nlen);
  brk_stash_push(h, buf, total);
  free(buf);
}

BRK_EXPORT void *brk_client_new(int32_t type, void *conf, char *errstr,
                                int32_t errstr_size) {
  if (conf == NULL ||
      (type != BRK_CLIENT_PRODUCER && type != BRK_CLIENT_CONSUMER)) {
    snprintf(errstr, (size_t)errstr_size, "bad client type or NULL conf");
    return NULL;
  }
  brk_handle *h = calloc(1, sizeof(brk_handle));
  if (h == NULL) {
    snprintf(errstr, (size_t)errstr_size, "out of memory");
    return NULL;
  }
  rd_kafka_conf_t *rkc = (rd_kafka_conf_t *)conf;

  /* Event API for every channel — absolutely NO C→JS callbacks. REBALANCE/
   * OFFSET_COMMIT only matter for consumers, but setting them globally is
   * harmless. */
  rd_kafka_conf_set_events(
      rkc, RD_KAFKA_EVENT_DR | RD_KAFKA_EVENT_LOG | RD_KAFKA_EVENT_STATS |
               RD_KAFKA_EVENT_ERROR | RD_KAFKA_EVENT_REBALANCE |
               RD_KAFKA_EVENT_OFFSET_COMMIT |
               RD_KAFKA_EVENT_OAUTHBEARER_TOKEN_REFRESH);
  rd_kafka_conf_set_throttle_cb(rkc, brk_throttle_cb);
  rd_kafka_conf_set_opaque(rkc, h);
  /* Mandatory: without this line librdkafka still prints logs straight to
   * stderr instead of pushing them onto the queue, even with EVENT_LOG set. */
  rd_kafka_conf_set(rkc, "log.queue", "true", NULL, 0);

  rd_kafka_t *rk = rd_kafka_new(
      type == BRK_CLIENT_PRODUCER ? RD_KAFKA_PRODUCER : RD_KAFKA_CONSUMER, rkc,
      errstr, (size_t)errstr_size);
  if (rk == NULL) {
    /* rd_kafka_new does NOT consume the conf on failure — the caller keeps
     * ownership and may destroy it. */
    free(h);
    return NULL;
  }

  h->magic = BRK_HANDLE_MAGIC;
  h->type = type;
  h->rk = rk;
  h->main_q = rd_kafka_queue_get_main(rk);
  /* `log.queue=true` parks logs on librdkafka's private log queue, which is
   * NOT served by anything until it is forwarded: without this call every log
   * (including `debug=` output) is silently dropped. Forward it to main_q so
   * RD_KAFKA_EVENT_LOG surfaces through brk_events_poll. */
  rd_kafka_set_log_queue(rk, h->main_q);
  brk_mutex_init(&h->mu);
  if (type == BRK_CLIENT_CONSUMER) {
    /* NOTE (diverges from a header comment, not from the ABI): we do not use
     * rd_kafka_poll_set_consumer because it forwards the main queue into the
     * consumer queue, mixing log/stats with fetch. We keep TWO separate queues:
     *   main_q      → brk_events_poll  (log/stats/error/oauth)
     *   consumer_q  → brk_consume_batch (fetch + rebalance + offset_commit;
     *                 the latter two get stashed for brk_events_poll)
     * Consequence: rebalance only surfaces when brk_consume_batch is called —
     * the TS layer always polls consume for consumers, so this holds. */
    h->consumer_q = rd_kafka_queue_get_consumer(rk);
  }
  return h;
}

BRK_EXPORT void brk_client_destroy(void *hv) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return;

  if (h->type == BRK_CLIENT_CONSUMER) {
    /* The prefetch thread (if any) polls consumer_q — stop it before close. */
    brk_consume_prefetch_teardown(h);
    /* Close via a dedicated queue: answer the rebalance (revoke) that arises
     * during close right here with assign(NULL)/incremental_unassign. */
    rd_kafka_queue_t *cq = rd_kafka_queue_new(h->rk);
    rd_kafka_error_t *ce = rd_kafka_consumer_close_queue(h->rk, cq);
    if (ce != NULL) {
      /* close could not start (e.g. never subscribed) — ignore; destroy
       * handles it */
      rd_kafka_error_destroy(ce);
      rd_kafka_queue_destroy(cq);
      cq = NULL;
    }
    if (cq != NULL) {
      while (!rd_kafka_consumer_closed(h->rk)) {
        rd_kafka_event_t *ev = rd_kafka_queue_poll(cq, 100);
        if (ev == NULL) continue;
        if (rd_kafka_event_type(ev) == RD_KAFKA_EVENT_REBALANCE) {
          if (rd_kafka_event_error(ev) ==
                  RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS &&
              strcmp(rd_kafka_rebalance_protocol(h->rk), "COOPERATIVE") == 0) {
            rd_kafka_incremental_unassign(
                h->rk, rd_kafka_event_topic_partition_list(ev));
          } else {
            rd_kafka_assign(h->rk, NULL);
          }
        }
        rd_kafka_event_destroy(ev);
      }
      rd_kafka_queue_destroy(cq);
    }
  } else {
    /* The TS layer flushes before disconnect; the purge here is only a
     * backstop so destroy cannot hang on messages still in the queue. */
    rd_kafka_purge(h->rk,
                   RD_KAFKA_PURGE_F_QUEUE | RD_KAFKA_PURGE_F_INFLIGHT);
  }

  if (h->cur_fetch != NULL) rd_kafka_event_destroy(h->cur_fetch);
  if (h->consumer_q != NULL) rd_kafka_queue_destroy(h->consumer_q);
  if (h->main_q != NULL) rd_kafka_queue_destroy(h->main_q);
  rd_kafka_destroy(h->rk);

  brk_frame *f = h->stash_head;
  while (f != NULL) {
    brk_frame *n = f->next;
    free(f);
    f = n;
  }
  for (int32_t i = 0; i < h->topics.count; i++) free(h->topics.names[i]);
  free(h->topics.names);
  free(h->scratch);
  free(h->cscratch);
  h->magic = 0;
  brk_mutex_destroy(&h->mu);
  free(h);
}

BRK_EXPORT int32_t brk_client_outq_len(void *hv) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  return rd_kafka_outq_len(h->rk);
}

/* ========================================================================== */
/* Error state                                                                 */
/* ========================================================================== */

BRK_EXPORT int32_t brk_last_error(void *hv) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  brk_mutex_lock(&h->mu);
  int32_t e = h->last_err;
  brk_mutex_unlock(&h->mu);
  return e;
}

BRK_EXPORT int32_t brk_last_error_string(void *hv, char *buf, int32_t cap) {
  brk_handle *h = brk_check(hv);
  if (h == NULL || buf == NULL || cap <= 0) return BRK_ERR_BAD_ARGS;
  brk_mutex_lock(&h->mu);
  int32_t n = (int32_t)strlen(h->last_err_str);
  if (n >= cap) n = cap - 1;
  memcpy(buf, h->last_err_str, (size_t)n);
  buf[n] = '\0';
  brk_mutex_unlock(&h->mu);
  return n;
}

BRK_EXPORT int32_t brk_last_required_size(void *hv) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  brk_mutex_lock(&h->mu);
  int32_t r = h->last_required;
  brk_mutex_unlock(&h->mu);
  return r;
}

/* ========================================================================== */
/* Topic name interning                                                        */
/* ========================================================================== */

BRK_EXPORT int32_t brk_topic_name(void *hv, int32_t topic_id, uint8_t *buf,
                                  int32_t cap) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (buf == NULL || cap < 0) return BRK_ERR_BAD_ARGS;
  brk_mutex_lock(&h->mu);
  if (topic_id < 0 || topic_id >= h->topics.count) {
    brk_mutex_unlock(&h->mu);
    return BRK_ERR_UNKNOWN_TOPIC_ID;
  }
  const char *name = h->topics.names[topic_id];
  int32_t n = (int32_t)strlen(name);
  if (n > cap) {
    h->last_required = n;
    brk_mutex_unlock(&h->mu);
    return BRK_ERR_BUFFER_TOO_SMALL;
  }
  memcpy(buf, name, (size_t)n);
  brk_mutex_unlock(&h->mu);
  return n;
}
