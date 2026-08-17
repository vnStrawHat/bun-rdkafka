/* Consumer: subscribe, consume batch (format 4), commit/seek/assign/position,
 * pause/resume, offsets_store, watermark + the shared tpl codec (format 2). */
#include "shim_internal.h"

/* ========================================================================== */
/* TOPIC-PARTITION LIST codec (format 2)                                       */
/* ========================================================================== */

rd_kafka_topic_partition_list_t *brk_tpl_decode(brk_handle *h,
                                                const uint8_t *buf,
                                                int32_t len) {
  if (buf == NULL || len < 4) return NULL;
  brk_rbuf r = {buf, len, 0};
  uint32_t count;
  if (!rb_u32(&r, &count) || count > 1000000) return NULL;
  rd_kafka_topic_partition_list_t *tpl =
      rd_kafka_topic_partition_list_new((int)count);
  char name[512];
  for (uint32_t i = 0; i < count; i++) {
    int32_t topic_id, partition, leader_epoch;
    uint16_t name_len;
    int64_t offset;
    if (!rb_i32(&r, &topic_id) || !rb_u16(&r, &name_len)) goto fail;
    const uint8_t *np = rb_bytes(&r, name_len);
    if (name_len > 0 && np == NULL) goto fail;
    if (!rb_i32(&r, &partition) || !rb_i64(&r, &offset) ||
        !rb_i32(&r, &leader_epoch))
      goto fail;
    int16_t metadata_len;
    if (!rb_raw(&r, &metadata_len, 2)) goto fail;
    const uint8_t *mp = NULL;
    if (metadata_len > 0 && (mp = rb_bytes(&r, metadata_len)) == NULL)
      goto fail;

    const char *topic;
    if (name_len > 0) {
      if (name_len >= sizeof(name)) goto fail;
      memcpy(name, np, name_len);
      name[name_len] = '\0';
      topic = name;
      brk_intern_topic(h, name); /* new topic from JS → intern right away */
    } else {
      brk_mutex_lock(&h->mu);
      if (topic_id < 0 || topic_id >= h->topics.count) {
        brk_mutex_unlock(&h->mu);
        goto fail;
      }
      topic = h->topics.names[topic_id];
      brk_mutex_unlock(&h->mu);
    }
    rd_kafka_topic_partition_t *p =
        rd_kafka_topic_partition_list_add(tpl, topic, partition);
    p->offset = offset;
    if (leader_epoch >= 0)
      rd_kafka_topic_partition_set_leader_epoch(p, leader_epoch);
    if (metadata_len >= 0) {
      /* metadata is malloc'd — rd_kafka_topic_partition_list_destroy rd_free()s
       * it (same libc allocator since we static-link into one binary). len 0 is
       * valid: empty metadata differs from "absent". */
      p->metadata = malloc(metadata_len > 0 ? (size_t)metadata_len : 1);
      if (p->metadata == NULL) goto fail;
      if (metadata_len > 0) memcpy(p->metadata, mp, (size_t)metadata_len);
      p->metadata_size = (size_t)metadata_len;
    }
  }
  return tpl;
fail:
  rd_kafka_topic_partition_list_destroy(tpl);
  return NULL;
}

int32_t brk_tpl_encode(brk_handle *h,
                       const rd_kafka_topic_partition_list_t *tpl,
                       brk_wbuf *w) {
  uint32_t count = tpl != NULL ? (uint32_t)tpl->cnt : 0;
  if (!wb_has(w, 4)) return BRK_ERR_BUFFER_TOO_SMALL;
  wb_u32(w, count);
  for (uint32_t i = 0; i < count; i++) {
    const rd_kafka_topic_partition_t *p = &tpl->elems[i];
    int32_t id = brk_intern_topic(h, p->topic);
    int32_t name_len = id >= 0 ? 0 : (int32_t)strlen(p->topic);
    int32_t md_len =
        (p->metadata != NULL && p->metadata_size <= INT16_MAX)
            ? (int32_t)p->metadata_size
            : -1;
    if (!wb_has(w, 4 + 2 + name_len + 4 + 8 + 4 + 2 + (md_len > 0 ? md_len : 0)))
      return BRK_ERR_BUFFER_TOO_SMALL;
    wb_i32(w, id >= 0 ? id : -1);
    wb_u16(w, (uint16_t)name_len);
    wb_raw(w, p->topic, name_len);
    wb_i32(w, p->partition);
    wb_i64(w, p->offset);
    wb_i32(w, rd_kafka_topic_partition_get_leader_epoch(p));
    wb_i16(w, (int16_t)md_len);
    if (md_len > 0) wb_raw(w, p->metadata, md_len);
  }
  return w->off;
}

/* ========================================================================== */
/* Subscribe                                                                   */
/* ========================================================================== */

BRK_EXPORT int32_t brk_subscribe(void *hv, const uint8_t *topics_buf,
                                 int32_t len) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_CONSUMER) return BRK_ERR_INVALID_STATE;
  if (topics_buf == NULL || len < 4) return BRK_ERR_BAD_ARGS;

  brk_rbuf r = {topics_buf, len, 0};
  uint32_t count;
  if (!rb_u32(&r, &count) || count == 0 || count > 100000)
    return BRK_ERR_DECODE;
  rd_kafka_topic_partition_list_t *tpl =
      rd_kafka_topic_partition_list_new((int)count);
  char name[512];
  for (uint32_t i = 0; i < count; i++) {
    uint16_t nlen;
    const uint8_t *np;
    if (!rb_u16(&r, &nlen) || (np = rb_bytes(&r, nlen)) == NULL ||
        nlen == 0 || nlen >= sizeof(name)) {
      rd_kafka_topic_partition_list_destroy(tpl);
      return BRK_ERR_DECODE;
    }
    memcpy(name, np, nlen);
    name[nlen] = '\0';
    brk_intern_topic(h, name);
    rd_kafka_topic_partition_list_add(tpl, name, RD_KAFKA_PARTITION_UA);
  }
  rd_kafka_resp_err_t err = rd_kafka_subscribe(h->rk, tpl);
  rd_kafka_topic_partition_list_destroy(tpl);
  return err == RD_KAFKA_RESP_ERR_NO_ERROR ? BRK_OK : BRK_KAFKA_ERR(err);
}

BRK_EXPORT int32_t brk_unsubscribe(void *hv) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_CONSUMER) return BRK_ERR_INVALID_STATE;
  rd_kafka_resp_err_t err = rd_kafka_unsubscribe(h->rk);
  return err == RD_KAFKA_RESP_ERR_NO_ERROR ? BRK_OK : BRK_KAFKA_ERR(err);
}

BRK_EXPORT int32_t brk_subscription(void *hv, uint8_t *buf, int32_t cap) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_CONSUMER) return BRK_ERR_INVALID_STATE;
  if (buf == NULL || cap < 4) return BRK_ERR_BAD_ARGS;
  rd_kafka_topic_partition_list_t *tpl = NULL;
  rd_kafka_resp_err_t err = rd_kafka_subscription(h->rk, &tpl);
  if (err != RD_KAFKA_RESP_ERR_NO_ERROR) return BRK_KAFKA_ERR(err);
  brk_wbuf w = {buf, cap, 0};
  wb_u32(&w, (uint32_t)tpl->cnt);
  for (int i = 0; i < tpl->cnt; i++) {
    int32_t nlen = (int32_t)strlen(tpl->elems[i].topic);
    if (!wb_has(&w, 2 + nlen)) {
      rd_kafka_topic_partition_list_destroy(tpl);
      return BRK_ERR_BUFFER_TOO_SMALL;
    }
    wb_u16(&w, (uint16_t)nlen);
    wb_raw(&w, tpl->elems[i].topic, nlen);
  }
  int32_t n = tpl->cnt;
  rd_kafka_topic_partition_list_destroy(tpl);
  return n;
}

/* ========================================================================== */
/* Consume batch                                                               */
/* ========================================================================== */

/* Serializes 1 message (format 4) into scratch; returns bytes or negative. */
/* Serializes `m` straight into `*out` when the record fits (advances out->off,
 * *direct = true) — otherwise into cscratch (*direct = false) so the caller
 * can hold it over to the next call. Either way returns the record length. */
/* Per-fill cache of the last topic interned: consecutive messages of a fetch
 * share the same rd_kafka_topic_t, so a pointer compare (plus a strcmp of the
 * name, in case the topic object was recycled mid-fill) replaces the mutexed
 * intern-table scan per message. Reset for every consume_fill call. */
typedef struct {
  const rd_kafka_topic_t *rkt;
  int32_t tid;
  char name[256]; /* topic names are <= 249 bytes */
} brk_topic_cache;

static int32_t serialize_msg(brk_handle *h, const rd_kafka_message_t *m,
                             brk_wbuf *out, bool *direct, brk_topic_cache *tc) {
  int32_t tid;
  const char *tname = m->rkt != NULL ? rd_kafka_topic_name(m->rkt) : "";
  if (m->rkt != NULL && m->rkt == tc->rkt && strcmp(tname, tc->name) == 0) {
    tid = tc->tid;
  } else {
    tid = brk_intern_topic(h, tname);
    if (tid < 0) return tid;
    if (m->rkt != NULL && strlen(tname) < sizeof(tc->name)) {
      tc->rkt = m->rkt;
      tc->tid = tid;
      strcpy(tc->name, tname);
    }
  }

  rd_kafka_headers_t *hdrs = NULL;
  size_t hdr_cnt = 0;
  if (m->err == RD_KAFKA_RESP_ERR_NO_ERROR &&
      rd_kafka_message_headers(m, &hdrs) == RD_KAFKA_RESP_ERR_NO_ERROR &&
      hdrs != NULL)
    hdr_cnt = rd_kafka_header_cnt(hdrs);
  else
    hdrs = NULL;

  bool is_err = m->err != RD_KAFKA_RESP_ERR_NO_ERROR;
  int32_t key_len = is_err ? -1 : (m->key != NULL ? (int32_t)m->key_len : -1);
  int32_t val_len =
      is_err ? -1 : (m->payload != NULL ? (int32_t)m->len : -1);

  int32_t bound = 4 + 4 + 8 + 8 + 1 + 2 + 4 + (key_len > 0 ? key_len : 0) + 4 +
                  (val_len > 0 ? val_len : 0) + 2 + 4;
  for (size_t i = 0; i < hdr_cnt; i++) {
    const char *hname;
    const void *hval;
    size_t hsize;
    rd_kafka_header_get_all(hdrs, i, &hname, &hval, &hsize);
    bound += 2 + (int32_t)strlen(hname) + 4 + (int32_t)hsize;
  }

  brk_wbuf w;
  if (bound <= out->cap - out->off) {
    /* Fast path: write in place, no scratch round-trip (saves a memcpy of
     * the payload per message). */
    w.p = out->p + out->off;
    w.cap = bound;
    w.off = 0;
    *direct = true;
  } else {
    uint8_t *s = brk_buf_reserve(&h->cscratch, &h->cscratch_cap, bound);
    if (s == NULL) return BRK_ERR_NOMEM;
    w.p = s;
    w.cap = bound;
    w.off = 0;
    *direct = false;
  }

  rd_kafka_timestamp_type_t tstype = RD_KAFKA_TIMESTAMP_NOT_AVAILABLE;
  int64_t ts = rd_kafka_message_timestamp(m, &tstype);

  wb_i32(&w, tid);
  wb_i32(&w, m->partition);
  wb_i64(&w, m->offset);
  wb_i64(&w, ts);
  wb_u8(&w, (uint8_t)tstype);
  wb_i16(&w, (int16_t)m->err);
  wb_i32(&w, key_len);
  if (key_len > 0) wb_raw(&w, m->key, key_len);
  wb_i32(&w, val_len);
  if (val_len > 0) wb_raw(&w, m->payload, val_len);
  wb_u16(&w, (uint16_t)hdr_cnt);
  for (size_t i = 0; i < hdr_cnt; i++) {
    const char *hname;
    const void *hval;
    size_t hsize;
    rd_kafka_header_get_all(hdrs, i, &hname, &hval, &hsize);
    int32_t nl = (int32_t)strlen(hname);
    wb_u16(&w, (uint16_t)nl);
    wb_raw(&w, hname, nl);
    if (hval == NULL) {
      wb_i32(&w, -1);
    } else {
      wb_i32(&w, (int32_t)hsize);
      wb_raw(&w, hval, (int32_t)hsize);
    }
  }
  wb_i32(&w, rd_kafka_message_leader_epoch(m));
  if (*direct) out->off += w.off;
  return w.off;
}

/* EOF is a format-4 record (err = _PARTITION_EOF) per the contract in
 * bunrdkafka.h — but with the event API librdkafka delivers EOF via
 * RD_KAFKA_EVENT_ERROR (with a topic_partition), so we reconstruct the record
 * here (the same way confluent-kafka-go handles it). */
static int32_t serialize_eof(brk_handle *h,
                             const rd_kafka_topic_partition_t *tp) {
  int32_t tid = brk_intern_topic(h, tp->topic);
  if (tid < 0) return tid;
  int32_t bound = 4 + 4 + 8 + 8 + 1 + 2 + 4 + 4 + 2 + 4;
  uint8_t *s = brk_buf_reserve(&h->cscratch, &h->cscratch_cap, bound);
  if (s == NULL) return BRK_ERR_NOMEM;
  brk_wbuf w = {s, bound, 0};
  wb_i32(&w, tid);
  wb_i32(&w, tp->partition);
  wb_i64(&w, tp->offset); /* EOF position (high watermark) */
  wb_i64(&w, -1);
  wb_u8(&w, 0); /* RD_KAFKA_TIMESTAMP_NOT_AVAILABLE */
  wb_i16(&w, (int16_t)RD_KAFKA_RESP_ERR__PARTITION_EOF);
  wb_i32(&w, -1); /* key null */
  wb_i32(&w, -1); /* value null */
  wb_u16(&w, 0);  /* headers */
  wb_i32(&w, rd_kafka_topic_partition_get_leader_epoch(tp));
  return w.off;
}

/* Writes the record serialized in scratch (n bytes) into the out-buf; if it
 * does not fit, keeps it pending for the next call. Returns 1 = written;
 * 0 = pending (caller stops the loop); BRK_ERR_BUFFER_TOO_SMALL when no record
 * has been written yet (msgs == 0). */
static int32_t emit_or_pend(brk_handle *h, brk_wbuf *w, int32_t n,
                            int32_t msgs) {
  if (n <= w->cap - w->off) {
    wb_raw(w, h->cscratch, n);
    return 1;
  }
  h->pending_msg = malloc((size_t)n);
  if (h->pending_msg != NULL) {
    memcpy(h->pending_msg, h->cscratch, (size_t)n);
    h->pending_msg_len = n;
  }
  if (msgs == 0) {
    brk_mutex_lock(&h->mu);
    h->last_required = n;
    brk_mutex_unlock(&h->mu);
    return BRK_ERR_BUFFER_TOO_SMALL;
  }
  return 0;
}

/* Fills `buf` with up to max_msgs serialized messages from consumer_q. Runs on
 * the JS thread (default) or on the prefetch thread (never both). Returns the
 * message count or a negative error. */
static int32_t consume_fill(brk_handle *h, uint8_t *buf, int32_t buf_cap,
                            int32_t max_msgs, int32_t timeout_ms,
                            int32_t *out_len) {
  brk_wbuf w = {buf, buf_cap, 0};
  *out_len = 0;
  int32_t msgs = 0;
  brk_topic_cache tc = {NULL, -1, {0}};

  /* 0) Message held over from the previous call (out-buf was full then). */
  if (h->pending_msg != NULL) {
    if (h->pending_msg_len > buf_cap) {
      brk_mutex_lock(&h->mu);
      h->last_required = h->pending_msg_len;
      brk_mutex_unlock(&h->mu);
      return BRK_ERR_BUFFER_TOO_SMALL;
    }
    wb_raw(&w, h->pending_msg, h->pending_msg_len);
    free(h->pending_msg);
    h->pending_msg = NULL;
    h->pending_msg_len = 0;
    msgs++;
  }

  while (msgs < max_msgs) {
    if (h->cur_fetch == NULL) {
      rd_kafka_event_t *ev =
          rd_kafka_queue_poll(h->consumer_q, msgs == 0 ? timeout_ms : 0);
      if (ev == NULL) break;
      if (rd_kafka_event_type(ev) == RD_KAFKA_EVENT_FETCH) {
        h->cur_fetch = ev;
      } else if (rd_kafka_event_type(ev) == RD_KAFKA_EVENT_ERROR &&
                 rd_kafka_event_error(ev) ==
                     RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        /* EOF: reconstruct as a message record (format 4) — see serialize_eof. */
        rd_kafka_topic_partition_t *tp = rd_kafka_event_topic_partition(ev);
        rd_kafka_event_destroy(ev);
        if (tp == NULL) continue;
        int32_t n = serialize_eof(h, tp);
        rd_kafka_topic_partition_destroy(tp);
        if (n < 0) {
          brk_set_err(h, n, "serialize eof failed");
          *out_len = w.off;
          *out_len = w.off;
      return msgs > 0 ? msgs : n;
        }
        int32_t e = emit_or_pend(h, &w, n, msgs);
        if (e < 0) return e;
        if (e == 0) break;
        msgs++;
        continue;
      } else {
        /* REBALANCE / OFFSET_COMMIT / etc. on the consumer queue → stash for
         * brk_events_poll (JS always calls events_poll in the same cadence as
         * consume). */
        int32_t n = brk_serialize_event_into(h, ev, &h->cscratch, &h->cscratch_cap);
        rd_kafka_event_destroy(ev);
        if (n > 0) brk_stash_push(h, h->cscratch, n);
        continue;
      }
    }

    const rd_kafka_message_t *m = rd_kafka_event_message_next(h->cur_fetch);
    if (m == NULL) {
      rd_kafka_event_destroy(h->cur_fetch);
      h->cur_fetch = NULL;
      continue;
    }
    bool direct = false;
    int32_t n = serialize_msg(h, m, &w, &direct, &tc);
    if (n < 0) {
      brk_set_err(h, n, "serialize message failed");
      *out_len = w.off;
      return msgs > 0 ? msgs : n;
    }
    if (direct) {
      msgs++;
      continue;
    }
    int32_t e = emit_or_pend(h, &w, n, msgs);
    if (e < 0) return e;
    if (e == 0) break;
    msgs++;
  }
  *out_len = w.off;
  return msgs;
}

/* ========================================================================== */
/* Prefetch thread (opt-in): fills a ring of frames off the JS thread          */
/* ========================================================================== */

static void prefetch_main(void *arg) {
  brk_handle *h = arg;
  brk_prefetch *pf = &h->pf;
  for (;;) {
    brk_mutex_lock(&pf->mu);
    while (pf->running && pf->ready == pf->nframes) brk_cond_wait(&pf->cv, &pf->mu);
    if (!pf->running) {
      brk_mutex_unlock(&pf->mu);
      return;
    }
    brk_pf_frame *f = &pf->frames[pf->tail];
    brk_mutex_unlock(&pf->mu);

    /* Fill outside the lock: JS never touches frames[tail] while ready < nframes. */
    int32_t flen = 0;
    int32_t n = consume_fill(h, f->buf, f->cap, pf->max_msgs, 100, &flen);
    if (n <= 0) {
      /* 0 = nothing within 100 ms; negative = error (recorded via brk_set_err
       * inside consume_fill; BUFFER_TOO_SMALL cannot happen for a fresh frame
       * unless one message exceeds the frame — surfaced to JS below). */
      if (n == BRK_ERR_BUFFER_TOO_SMALL) {
        /* Grow this frame to fit the pending message and retry next round. */
        brk_mutex_lock(&h->mu);
        int32_t need = h->last_required;
        brk_mutex_unlock(&h->mu);
        if (need > f->cap) {
          uint8_t *nb = realloc(f->buf, (size_t)need);
          if (nb != NULL) {
            f->buf = nb;
            f->cap = need;
          }
        }
      }
      continue;
    }
    f->count = n;
    f->len = flen;
    brk_mutex_lock(&pf->mu);
    pf->tail = (pf->tail + 1) % pf->nframes;
    pf->ready++;
    pf->frames_filled++;
    brk_cond_broadcast(&pf->cv);
    brk_mutex_unlock(&pf->mu);
  }
}

BRK_EXPORT int32_t brk_consume_prefetch_start(void *hv, int32_t frame_cap,
                                              int32_t max_msgs,
                                              int32_t nframes) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_CONSUMER) return BRK_ERR_INVALID_STATE;
  if (frame_cap < 4096 || max_msgs <= 0 || nframes < 1 || nframes > 64)
    return BRK_ERR_BAD_ARGS;
  if (h->pf.enabled) return BRK_ERR_INVALID_STATE;

  brk_prefetch *pf = &h->pf;
  pf->frames = calloc((size_t)nframes, sizeof(brk_pf_frame));
  if (pf->frames == NULL) return BRK_ERR_NOMEM;
  for (int32_t i = 0; i < nframes; i++) {
    pf->frames[i].buf = malloc((size_t)frame_cap);
    if (pf->frames[i].buf == NULL) {
      for (int32_t j = 0; j < i; j++) free(pf->frames[j].buf);
      free(pf->frames);
      pf->frames = NULL;
      return BRK_ERR_NOMEM;
    }
    pf->frames[i].cap = frame_cap;
  }
  pf->nframes = nframes;
  pf->max_msgs = max_msgs;
  pf->head = pf->tail = pf->ready = 0;
  pf->frames_filled = 0;
  brk_mutex_init(&pf->mu);
  brk_cond_init(&pf->cv);
  pf->running = true;
  pf->enabled = true;
  if (brk_thread_start(&pf->thread, prefetch_main, h) != 0) {
    pf->running = false;
    pf->enabled = false;
    brk_cond_destroy(&pf->cv);
    brk_mutex_destroy(&pf->mu);
    for (int32_t i = 0; i < nframes; i++) free(pf->frames[i].buf);
    free(pf->frames);
    pf->frames = NULL;
    return BRK_ERR_NOMEM;
  }
  return BRK_OK;
}

void brk_consume_prefetch_teardown(brk_handle *h) {
  brk_prefetch *pf = &h->pf;
  if (!pf->enabled) return;
  brk_mutex_lock(&pf->mu);
  pf->running = false;
  brk_cond_broadcast(&pf->cv);
  brk_mutex_unlock(&pf->mu);
  /* Wake the thread out of rd_kafka_queue_poll(consumer_q, 100). */
  if (h->consumer_q != NULL) rd_kafka_queue_yield(h->consumer_q);
  brk_thread_join(&pf->thread);
  for (int32_t i = 0; i < pf->nframes; i++) free(pf->frames[i].buf);
  free(pf->frames);
  pf->frames = NULL;
  brk_cond_destroy(&pf->cv);
  brk_mutex_destroy(&pf->mu);
  pf->enabled = false;
}

BRK_EXPORT int32_t brk_consume_prefetch_stop(void *hv) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  brk_consume_prefetch_teardown(h);
  return BRK_OK;
}

BRK_EXPORT int64_t brk_consume_prefetch_stats(void *hv) {
  brk_handle *h = brk_check(hv);
  if (h == NULL || !h->pf.enabled) return -1;
  brk_mutex_lock(&h->pf.mu);
  int64_t v = h->pf.frames_filled;
  brk_mutex_unlock(&h->pf.mu);
  return v;
}

/* Copies one ready frame into the JS buffer. 0 = none ready. */
static int32_t prefetch_take(brk_handle *h, uint8_t *buf, int32_t buf_cap) {
  brk_prefetch *pf = &h->pf;
  brk_mutex_lock(&pf->mu);
  if (pf->ready == 0) {
    brk_mutex_unlock(&pf->mu);
    return 0;
  }
  brk_pf_frame *f = &pf->frames[pf->head];
  if (f->len > buf_cap) {
    brk_mutex_unlock(&pf->mu);
    brk_mutex_lock(&h->mu);
    h->last_required = f->len;
    brk_mutex_unlock(&h->mu);
    return BRK_ERR_BUFFER_TOO_SMALL;
  }
  memcpy(buf, f->buf, (size_t)f->len);
  int32_t count = f->count;
  pf->head = (pf->head + 1) % pf->nframes;
  pf->ready--;
  brk_cond_broadcast(&pf->cv);
  brk_mutex_unlock(&pf->mu);
  return count;
}

BRK_EXPORT int32_t brk_consume_batch(void *hv, uint8_t *buf, int32_t buf_cap,
                                     int32_t max_msgs, int32_t timeout_ms) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_CONSUMER) return BRK_ERR_INVALID_STATE;
  if (buf == NULL || buf_cap <= 0 || max_msgs <= 0) return BRK_ERR_BAD_ARGS;
  if (h->pf.enabled) return prefetch_take(h, buf, buf_cap);
  int32_t len = 0;
  return consume_fill(h, buf, buf_cap, max_msgs, timeout_ms, &len);
}

/* ========================================================================== */
/* Commit / committed / seek / assign / position                               */
/* ========================================================================== */

BRK_EXPORT int32_t brk_commit(void *hv, const uint8_t *tpl_buf, int32_t len,
                              int32_t async) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_CONSUMER) return BRK_ERR_INVALID_STATE;
  rd_kafka_topic_partition_list_t *tpl = NULL;
  if (tpl_buf != NULL && len > 0) {
    tpl = brk_tpl_decode(h, tpl_buf, len);
    if (tpl == NULL) return BRK_ERR_DECODE;
  }
  rd_kafka_resp_err_t err = rd_kafka_commit(h->rk, tpl, async != 0);
  if (tpl != NULL) rd_kafka_topic_partition_list_destroy(tpl);
  return err == RD_KAFKA_RESP_ERR_NO_ERROR ? BRK_OK : BRK_KAFKA_ERR(err);
}

BRK_EXPORT int32_t brk_committed(void *hv, const uint8_t *tpl_buf,
                                 int32_t tpl_len, uint8_t *out_buf,
                                 int32_t out_cap, int32_t timeout_ms) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_CONSUMER) return BRK_ERR_INVALID_STATE;
  if (out_buf == NULL || out_cap < 4) return BRK_ERR_BAD_ARGS;

  rd_kafka_topic_partition_list_t *tpl = NULL;
  if (tpl_buf != NULL && tpl_len > 0) {
    tpl = brk_tpl_decode(h, tpl_buf, tpl_len);
    if (tpl == NULL) return BRK_ERR_DECODE;
  } else {
    rd_kafka_resp_err_t aerr = rd_kafka_assignment(h->rk, &tpl);
    if (aerr != RD_KAFKA_RESP_ERR_NO_ERROR) return BRK_KAFKA_ERR(aerr);
  }
  rd_kafka_resp_err_t err = rd_kafka_committed(h->rk, tpl, timeout_ms);
  if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
    rd_kafka_topic_partition_list_destroy(tpl);
    return BRK_KAFKA_ERR(err);
  }
  brk_wbuf w = {out_buf, out_cap, 0};
  int32_t r = brk_tpl_encode(h, tpl, &w);
  int32_t cnt = tpl->cnt;
  rd_kafka_topic_partition_list_destroy(tpl);
  return r < 0 ? r : cnt;
}

BRK_EXPORT int32_t brk_offsets_for_times(void *hv, const uint8_t *tpl_buf,
                                         int32_t tpl_len, uint8_t *out_buf,
                                         int32_t out_cap, int32_t timeout_ms) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (tpl_buf == NULL || tpl_len < 4 || out_buf == NULL || out_cap < 4)
    return BRK_ERR_BAD_ARGS;

  rd_kafka_topic_partition_list_t *tpl = brk_tpl_decode(h, tpl_buf, tpl_len);
  if (tpl == NULL) return BRK_ERR_DECODE;
  rd_kafka_resp_err_t err = rd_kafka_offsets_for_times(h->rk, tpl, timeout_ms);
  if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
    /* Per-partition errors: format 2 carries no err field, so surface the
     * first one as the call's error (same policy as brk_seek). */
    for (int i = 0; i < tpl->cnt; i++) {
      if (tpl->elems[i].err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        err = tpl->elems[i].err;
        brk_set_err(h, BRK_KAFKA_ERR(err), "offsets_for_times: %s [%d]: %s",
                    tpl->elems[i].topic, tpl->elems[i].partition,
                    rd_kafka_err2str(err));
        break;
      }
    }
  }
  if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
    rd_kafka_topic_partition_list_destroy(tpl);
    return BRK_KAFKA_ERR(err);
  }
  brk_wbuf w = {out_buf, out_cap, 0};
  int32_t r = brk_tpl_encode(h, tpl, &w);
  int32_t cnt = tpl->cnt;
  rd_kafka_topic_partition_list_destroy(tpl);
  return r < 0 ? r : cnt;
}

BRK_EXPORT int32_t brk_seek(void *hv, const char *topic, int32_t partition,
                            int64_t offset, int32_t timeout_ms) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_CONSUMER) return BRK_ERR_INVALID_STATE;
  if (topic == NULL) return BRK_ERR_BAD_ARGS;
  rd_kafka_topic_partition_list_t *tpl =
      rd_kafka_topic_partition_list_new(1);
  rd_kafka_topic_partition_t *p =
      rd_kafka_topic_partition_list_add(tpl, topic, partition);
  p->offset = offset;
  rd_kafka_error_t *e = rd_kafka_seek_partitions(h->rk, tpl, timeout_ms);
  int32_t ret = BRK_OK;
  if (e != NULL) {
    ret = BRK_KAFKA_ERR(rd_kafka_error_code(e));
    brk_set_err(h, ret, "%s", rd_kafka_error_string(e));
    rd_kafka_error_destroy(e);
  } else if (tpl->elems[0].err != RD_KAFKA_RESP_ERR_NO_ERROR) {
    ret = BRK_KAFKA_ERR(tpl->elems[0].err);
  }
  rd_kafka_topic_partition_list_destroy(tpl);
  return ret;
}

BRK_EXPORT int32_t brk_assign(void *hv, const uint8_t *tpl_buf, int32_t len,
                              int32_t mode) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_CONSUMER) return BRK_ERR_INVALID_STATE;

  rd_kafka_topic_partition_list_t *tpl = NULL;
  if (mode != BRK_UNASSIGN) {
    tpl = brk_tpl_decode(h, tpl_buf, len);
    if (tpl == NULL) return BRK_ERR_DECODE;
  }
  int32_t ret;
  if (mode == BRK_ASSIGN) {
    rd_kafka_resp_err_t err = rd_kafka_assign(h->rk, tpl);
    ret = err == RD_KAFKA_RESP_ERR_NO_ERROR ? BRK_OK : BRK_KAFKA_ERR(err);
  } else if (mode == BRK_UNASSIGN) {
    rd_kafka_resp_err_t err = rd_kafka_assign(h->rk, NULL);
    ret = err == RD_KAFKA_RESP_ERR_NO_ERROR ? BRK_OK : BRK_KAFKA_ERR(err);
  } else if (mode == BRK_ASSIGN_INCREMENTAL ||
             mode == BRK_UNASSIGN_INCREMENTAL) {
    rd_kafka_error_t *e = mode == BRK_ASSIGN_INCREMENTAL
                              ? rd_kafka_incremental_assign(h->rk, tpl)
                              : rd_kafka_incremental_unassign(h->rk, tpl);
    if (e != NULL) {
      ret = BRK_KAFKA_ERR(rd_kafka_error_code(e));
      brk_set_err(h, ret, "%s", rd_kafka_error_string(e));
      rd_kafka_error_destroy(e);
    } else {
      ret = BRK_OK;
    }
  } else {
    ret = BRK_ERR_BAD_ARGS;
  }
  if (tpl != NULL) rd_kafka_topic_partition_list_destroy(tpl);
  return ret;
}

static int32_t encode_tpl_result(brk_handle *h,
                                 rd_kafka_topic_partition_list_t *tpl,
                                 uint8_t *buf, int32_t cap) {
  brk_wbuf w = {buf, cap, 0};
  int32_t r = brk_tpl_encode(h, tpl, &w);
  int32_t cnt = tpl != NULL ? tpl->cnt : 0;
  if (tpl != NULL) rd_kafka_topic_partition_list_destroy(tpl);
  return r < 0 ? r : cnt;
}

BRK_EXPORT int32_t brk_assignment(void *hv, uint8_t *buf, int32_t cap) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_CONSUMER) return BRK_ERR_INVALID_STATE;
  if (buf == NULL || cap < 4) return BRK_ERR_BAD_ARGS;
  rd_kafka_topic_partition_list_t *tpl = NULL;
  rd_kafka_resp_err_t err = rd_kafka_assignment(h->rk, &tpl);
  if (err != RD_KAFKA_RESP_ERR_NO_ERROR) return BRK_KAFKA_ERR(err);
  return encode_tpl_result(h, tpl, buf, cap);
}

BRK_EXPORT int32_t brk_position(void *hv, uint8_t *buf, int32_t cap) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_CONSUMER) return BRK_ERR_INVALID_STATE;
  if (buf == NULL || cap < 4) return BRK_ERR_BAD_ARGS;
  rd_kafka_topic_partition_list_t *tpl = NULL;
  rd_kafka_resp_err_t err = rd_kafka_assignment(h->rk, &tpl);
  if (err != RD_KAFKA_RESP_ERR_NO_ERROR) return BRK_KAFKA_ERR(err);
  err = rd_kafka_position(h->rk, tpl);
  if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
    rd_kafka_topic_partition_list_destroy(tpl);
    return BRK_KAFKA_ERR(err);
  }
  return encode_tpl_result(h, tpl, buf, cap);
}

BRK_EXPORT int32_t brk_pause_resume(void *hv, const uint8_t *tpl_buf,
                                    int32_t len, int32_t resume) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_CONSUMER) return BRK_ERR_INVALID_STATE;
  rd_kafka_topic_partition_list_t *tpl = brk_tpl_decode(h, tpl_buf, len);
  if (tpl == NULL) return BRK_ERR_DECODE;
  rd_kafka_resp_err_t err = resume != 0
                                ? rd_kafka_resume_partitions(h->rk, tpl)
                                : rd_kafka_pause_partitions(h->rk, tpl);
  int32_t ret = err == RD_KAFKA_RESP_ERR_NO_ERROR ? BRK_OK : BRK_KAFKA_ERR(err);
  if (ret == BRK_OK) {
    for (int i = 0; i < tpl->cnt; i++) {
      if (tpl->elems[i].err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        ret = BRK_KAFKA_ERR(tpl->elems[i].err);
        break;
      }
    }
  }
  rd_kafka_topic_partition_list_destroy(tpl);
  return ret;
}

BRK_EXPORT int32_t brk_offsets_store(void *hv, const uint8_t *tpl_buf,
                                     int32_t len) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_CONSUMER) return BRK_ERR_INVALID_STATE;
  rd_kafka_topic_partition_list_t *tpl = brk_tpl_decode(h, tpl_buf, len);
  if (tpl == NULL) return BRK_ERR_DECODE;
  rd_kafka_resp_err_t err = rd_kafka_offsets_store(h->rk, tpl);
  int32_t ret = err == RD_KAFKA_RESP_ERR_NO_ERROR ? BRK_OK : BRK_KAFKA_ERR(err);
  if (ret == BRK_OK) {
    for (int i = 0; i < tpl->cnt; i++) {
      if (tpl->elems[i].err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        ret = BRK_KAFKA_ERR(tpl->elems[i].err);
        break;
      }
    }
  }
  rd_kafka_topic_partition_list_destroy(tpl);
  return ret;
}

BRK_EXPORT int32_t brk_offset_store_single(void *hv, int32_t topic_id,
                                           int32_t partition, int64_t offset,
                                           int32_t leader_epoch) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_CONSUMER) return BRK_ERR_INVALID_STATE;
  brk_mutex_lock(&h->mu);
  if (topic_id < 0 || topic_id >= h->topics.count) {
    brk_mutex_unlock(&h->mu);
    return BRK_ERR_UNKNOWN_TOPIC_ID;
  }
  /* Interned names are strdup'ed once and live until destroy: the pointer
   * stays valid after the unlock. */
  const char *name = h->topics.names[topic_id];
  brk_mutex_unlock(&h->mu);

  /* The cached list is reused only for the SAME (topic, partition):
   * rd_kafka_offsets_store attaches the toppar to the element (_private) and
   * later calls trust it, so an element must never be re-pointed at another
   * partition. Consecutive stores are almost always the same partition. */
  rd_kafka_topic_partition_list_t *tpl = h->store_tpl;
  if (tpl == NULL || tpl->cnt != 1 || tpl->elems[0].partition != partition ||
      strcmp(tpl->elems[0].topic, name) != 0) {
    if (tpl != NULL) rd_kafka_topic_partition_list_destroy(tpl);
    tpl = rd_kafka_topic_partition_list_new(1);
    if (tpl == NULL) return BRK_ERR_NOMEM;
    rd_kafka_topic_partition_list_add(tpl, name, partition);
    h->store_tpl = tpl;
  }
  rd_kafka_topic_partition_t *e = &tpl->elems[0];
  e->offset = offset;
  e->err = RD_KAFKA_RESP_ERR_NO_ERROR;
  rd_kafka_topic_partition_set_leader_epoch(e, leader_epoch);
  rd_kafka_resp_err_t err = rd_kafka_offsets_store(h->rk, tpl);
  if (err != RD_KAFKA_RESP_ERR_NO_ERROR) return BRK_KAFKA_ERR(err);
  if (e->err != RD_KAFKA_RESP_ERR_NO_ERROR) return BRK_KAFKA_ERR(e->err);
  return BRK_OK;
}

BRK_EXPORT int32_t brk_query_watermark(void *hv, const char *topic,
                                       int32_t partition, int64_t *lo,
                                       int64_t *hi, int32_t timeout_ms) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (topic == NULL || lo == NULL || hi == NULL) return BRK_ERR_BAD_ARGS;
  rd_kafka_resp_err_t err = rd_kafka_query_watermark_offsets(
      h->rk, topic, partition, lo, hi, timeout_ms);
  return err == RD_KAFKA_RESP_ERR_NO_ERROR ? BRK_OK : BRK_KAFKA_ERR(err);
}

BRK_EXPORT int32_t brk_get_watermark(void *hv, const char *topic,
                                     int32_t partition, int64_t *lo,
                                     int64_t *hi) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (topic == NULL || lo == NULL || hi == NULL) return BRK_ERR_BAD_ARGS;
  rd_kafka_resp_err_t err =
      rd_kafka_get_watermark_offsets(h->rk, topic, partition, lo, hi);
  return err == RD_KAFKA_RESP_ERR_NO_ERROR ? BRK_OK : BRK_KAFKA_ERR(err);
}
