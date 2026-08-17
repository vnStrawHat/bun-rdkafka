/* brk_events_poll + event serialization → EVENT FRAME (format 5, see bunrdkafka.h) */
#include "shim_internal.h"

/* Upper bound on the bytes needed to encode a tpl (format 2) — C→JS prefers
 * topic_id (name_len 0) but budgets for having to write the name when
 * interning fails. */
static int32_t tpl_encode_bound(const rd_kafka_topic_partition_list_t *tpl) {
  int32_t sz = 4;
  if (tpl == NULL) return sz;
  for (int i = 0; i < tpl->cnt; i++)
    sz += 4 + 2 + (int32_t)strlen(tpl->elems[i].topic) + 4 + 8 + 4 + 2 +
          (int32_t)tpl->elems[i].metadata_size;
  return sz;
}

/* Serializes 1 event into scratch. Returns total frame bytes; 0 = skip;
 * negative = error. */
int32_t brk_serialize_event(brk_handle *h, rd_kafka_event_t *ev) {
  return brk_serialize_event_into(h, ev, &h->scratch, &h->scratch_cap);
}

int32_t brk_serialize_event_into(brk_handle *h, rd_kafka_event_t *ev,
                                 uint8_t **sbuf, int32_t *scap) {
  rd_kafka_event_type_t t = rd_kafka_event_type(ev);
  uint8_t type;
  int32_t bound; /* upper bound payload */

  /* Admin result: build the JSON first (its length is only known after
   * building) then frame it in place — bypasses the bound/patch structure
   * below. */
  if (brk_event_is_admin_result((int32_t)t)) {
    char *json = NULL;
    int32_t jlen = brk_admin_result_json(h, ev, &json);
    if (jlen < 0) return jlen;
    int32_t payload = 8 + 4 + 4 + jlen;
    uint8_t *s = brk_buf_reserve(sbuf, scap, 5 + payload);
    if (s == NULL) {
      free(json);
      return BRK_ERR_NOMEM;
    }
    brk_wbuf w = {s, 5 + payload, 0};
    wb_u8(&w, BRK_EVENT_ADMIN_RESULT);
    wb_u32(&w, (uint32_t)payload);
    wb_u64(&w, (uint64_t)(uintptr_t)rd_kafka_event_opaque(ev));
    wb_i32(&w, (int32_t)rd_kafka_event_error(ev));
    wb_u32(&w, (uint32_t)jlen);
    wb_raw(&w, json, jlen);
    free(json);
    return w.off;
  }

  switch (t) {
    case RD_KAFKA_EVENT_DR: {
      type = BRK_EVENT_DR;
      bound = 4 + (int32_t)rd_kafka_event_message_count(ev) * 30;
      break;
    }
    case RD_KAFKA_EVENT_ERROR: {
      type = BRK_EVENT_ERROR;
      const char *s = rd_kafka_event_error_string(ev);
      bound = 4 + 1 + 2 + (s ? (int32_t)strlen(s) : 0);
      break;
    }
    case RD_KAFKA_EVENT_LOG: {
      type = BRK_EVENT_LOG;
      bound = 4 + 2 + 512 + 4 + 2048; /* fac/msg truncated to these limits */
      break;
    }
    case RD_KAFKA_EVENT_STATS: {
      type = BRK_EVENT_STATS;
      const char *json = rd_kafka_event_stats(ev);
      bound = 4 + (json ? (int32_t)strlen(json) : 0);
      break;
    }
    case RD_KAFKA_EVENT_REBALANCE: {
      type = BRK_EVENT_REBALANCE;
      bound =
          4 + 1 + 1 + tpl_encode_bound(rd_kafka_event_topic_partition_list(ev));
      break;
    }
    case RD_KAFKA_EVENT_OFFSET_COMMIT: {
      type = BRK_EVENT_OFFSET_COMMIT;
      bound = 4 + tpl_encode_bound(rd_kafka_event_topic_partition_list(ev));
      break;
    }
    case RD_KAFKA_EVENT_OAUTHBEARER_TOKEN_REFRESH: {
      type = BRK_EVENT_OAUTH_REFRESH;
      const char *cfg = rd_kafka_event_config_string(ev);
      bound = 2 + (cfg ? (int32_t)strlen(cfg) : 0);
      break;
    }
    default:
      return 0; /* non-forwarded type (e.g. a stray FETCH) — skip */
  }

  uint8_t *s = brk_buf_reserve(sbuf, scap, 5 + bound);
  if (s == NULL) return BRK_ERR_NOMEM;
  brk_wbuf w = {s, 5 + bound, 0};
  wb_u8(&w, type);
  wb_u32(&w, 0); /* payload_len — patched at the end of the function */

  switch (t) {
    case RD_KAFKA_EVENT_DR: {
      int32_t cnt_off = w.off;
      wb_u32(&w, 0);
      uint32_t n = 0;
      const rd_kafka_message_t *m;
      while ((m = rd_kafka_event_message_next(ev)) != NULL) {
        wb_u64(&w, (uint64_t)(uintptr_t)m->_private); /* opaque_id from produce */
        wb_i16(&w, (int16_t)m->err);
        wb_i32(&w, m->partition);
        wb_i64(&w, m->offset);
        rd_kafka_timestamp_type_t tst;
        wb_i64(&w, rd_kafka_message_timestamp(m, &tst));
        n++;
      }
      memcpy(s + cnt_off, &n, 4);
      break;
    }
    case RD_KAFKA_EVENT_ERROR: {
      const char *str = rd_kafka_event_error_string(ev);
      uint16_t len = str ? (uint16_t)strlen(str) : 0;
      wb_i32(&w, (int32_t)rd_kafka_event_error(ev));
      wb_u8(&w, rd_kafka_event_error_is_fatal(ev) ? 1 : 0);
      wb_u16(&w, len);
      wb_raw(&w, str, len);
      break;
    }
    case RD_KAFKA_EVENT_LOG: {
      const char *fac = NULL, *msg = NULL;
      int level = 0;
      rd_kafka_event_log(ev, &fac, &msg, &level);
      size_t flen = fac ? strlen(fac) : 0;
      size_t mlen = msg ? strlen(msg) : 0;
      if (flen > 512) flen = 512;
      if (mlen > 2048) mlen = 2048;
      wb_i32(&w, level);
      wb_u16(&w, (uint16_t)flen);
      wb_raw(&w, fac, (int32_t)flen);
      wb_u32(&w, (uint32_t)mlen);
      wb_raw(&w, msg, (int32_t)mlen);
      break;
    }
    case RD_KAFKA_EVENT_STATS: {
      const char *json = rd_kafka_event_stats(ev);
      uint32_t len = json ? (uint32_t)strlen(json) : 0;
      wb_u32(&w, len);
      wb_raw(&w, json, (int32_t)len);
      break;
    }
    case RD_KAFKA_EVENT_REBALANCE: {
      wb_i32(&w, (int32_t)rd_kafka_event_error(ev));
      wb_u8(&w, strcmp(rd_kafka_rebalance_protocol(h->rk), "COOPERATIVE") == 0
                    ? BRK_REBALANCE_PROTOCOL_COOPERATIVE
                    : BRK_REBALANCE_PROTOCOL_EAGER);
      wb_u8(&w, rd_kafka_assignment_lost(h->rk) ? 1 : 0);
      int32_t r = brk_tpl_encode(
          h, rd_kafka_event_topic_partition_list(ev), &w);
      if (r < 0) return r;
      break;
    }
    case RD_KAFKA_EVENT_OFFSET_COMMIT: {
      wb_i32(&w, (int32_t)rd_kafka_event_error(ev));
      int32_t r = brk_tpl_encode(
          h, rd_kafka_event_topic_partition_list(ev), &w);
      if (r < 0) return r;
      break;
    }
    case RD_KAFKA_EVENT_OAUTHBEARER_TOKEN_REFRESH: {
      const char *cfg = rd_kafka_event_config_string(ev);
      uint16_t len = cfg ? (uint16_t)strlen(cfg) : 0;
      wb_u16(&w, len);
      wb_raw(&w, cfg, len);
      break;
    }
    default:
      return 0;
  }

  uint32_t payload_len = (uint32_t)(w.off - 5);
  memcpy(s + 1, &payload_len, 4);
  return w.off;
}

BRK_EXPORT int32_t brk_events_poll(void *hv, uint8_t *buf, int32_t buf_cap,
                                   int32_t max_events, int32_t timeout_ms) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (buf == NULL || buf_cap <= 0 || max_events <= 0) return BRK_ERR_BAD_ARGS;

  brk_wbuf w = {buf, buf_cap, 0};
  int32_t frames = 0;

  /* 1) Stash first (events picked up on the consume path, throttle, earlier
   * overflow) — order is preserved because the stash is always older than
   * whatever remains in the queue. */
  while (frames < max_events) {
    brk_mutex_lock(&h->mu);
    brk_frame *f = h->stash_head;
    if (f == NULL) {
      brk_mutex_unlock(&h->mu);
      break;
    }
    if (f->len > buf_cap - w.off) {
      if (frames == 0) h->last_required = f->len;
      brk_mutex_unlock(&h->mu);
      if (frames == 0) return BRK_ERR_BUFFER_TOO_SMALL;
      return frames;
    }
    h->stash_head = f->next;
    if (h->stash_head == NULL) h->stash_tail = NULL;
    brk_mutex_unlock(&h->mu);
    wb_raw(&w, f->data, f->len);
    free(f);
    frames++;
  }

  /* 2) Drain the main queue. Only block (timeout_ms>0, Worker mode) while
   * nothing has been produced yet. */
  while (frames < max_events) {
    rd_kafka_event_t *ev =
        rd_kafka_queue_poll(h->main_q, frames == 0 ? timeout_ms : 0);
    if (ev == NULL) break;
    int32_t n = brk_serialize_event(h, ev);
    rd_kafka_event_destroy(ev);
    if (n == 0) continue;
    if (n < 0) {
      brk_set_err(h, n, "serialize event failed");
      return frames > 0 ? frames : n;
    }
    if (n <= buf_cap - w.off) {
      wb_raw(&w, h->scratch, n);
      frames++;
    } else {
      /* Does not fit: stash it for next time, the event is NOT lost. */
      brk_stash_push(h, h->scratch, n);
      if (frames == 0) {
        brk_mutex_lock(&h->mu);
        h->last_required = n;
        brk_mutex_unlock(&h->mu);
        return BRK_ERR_BUFFER_TOO_SMALL;
      }
      break;
    }
  }
  return frames;
}
