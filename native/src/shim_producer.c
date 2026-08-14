/* Produce batch (format 3), flush, transactions */
#include "shim_internal.h"

/* One record decoded from a PRODUCE BATCH. topic/headers point into in_buf;
 * key/value point DIRECTLY into JS memory via (ptr,len) in the packed buffer —
 * both are only valid for the duration of the call; producev uses MSG_F_COPY
 * so this is safe. */
typedef struct {
  const uint8_t *topic;
  uint16_t topic_len;
  int32_t partition;
  int64_t timestamp_ms;
  uint64_t opaque_id;
  const uint8_t *key;
  int32_t key_len; /* -1 = null */
  const uint8_t *value;
  int32_t value_len; /* -1 = null */
  uint16_t header_count;
  int32_t headers_off; /* offset of the headers region within in_buf */
} brk_record;

/* Anchor for EMPTY values (len 0): librdkafka distinguishes a NULL payload
 * (null value) from a non-NULL len-0 payload (empty value) — a non-NULL
 * pointer is required. */
static const uint8_t brk_empty_anchor[1] = {0};

/* Reads a (u64 ptr, i32 len) pair per format-3 semantics:
 * len -1 → NULL; len 0 → dummy pointer (empty ≠ null); len > 0 → ptr must be
 * non-zero. */
static bool rb_ptr_field(brk_rbuf *r, const uint8_t **out, int32_t *out_len) {
  uint64_t p;
  int32_t len;
  if (!rb_u64(r, &p) || !rb_i32(r, &len)) return false;
  if (len < -1) return false;
  if (len == -1) {
    *out = NULL;
  } else if (len == 0) {
    *out = brk_empty_anchor;
  } else {
    if (p == 0) return false;
    *out = (const uint8_t *)(uintptr_t)p;
  }
  *out_len = len;
  return true;
}

static bool decode_record(brk_rbuf *r, brk_record *rec) {
  if (!rb_u16(r, &rec->topic_len)) return false;
  rec->topic = rb_bytes(r, rec->topic_len);
  if (rec->topic == NULL || rec->topic_len == 0) return false;
  if (!rb_i32(r, &rec->partition)) return false;
  if (!rb_i64(r, &rec->timestamp_ms)) return false;
  if (!rb_u64(r, &rec->opaque_id)) return false;
  if (!rb_ptr_field(r, &rec->key, &rec->key_len)) return false;
  if (!rb_ptr_field(r, &rec->value, &rec->value_len)) return false;
  if (!rb_u16(r, &rec->header_count)) return false;
  rec->headers_off = r->off;
  for (uint16_t i = 0; i < rec->header_count; i++) {
    uint16_t klen;
    int32_t vlen;
    if (!rb_u16(r, &klen) || rb_bytes(r, klen) == NULL) return false;
    if (!rb_i32(r, &vlen)) return false;
    if (vlen > 0 && rb_bytes(r, vlen) == NULL) return false;
  }
  return true;
}

BRK_EXPORT int32_t brk_produce_batch(void *hv, const uint8_t *in_buf,
                                     int32_t in_len, int16_t *err_out,
                                     int32_t max_records) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_PRODUCER) return BRK_ERR_INVALID_STATE;
  if (in_buf == NULL || err_out == NULL || in_len < 4 || max_records <= 0)
    return BRK_ERR_BAD_ARGS;

  brk_rbuf r = {in_buf, in_len, 0};
  uint32_t count;
  if (!rb_u32(&r, &count)) return BRK_ERR_DECODE;
  if ((int32_t)count > max_records) return BRK_ERR_BAD_ARGS;

  /* Pass 1: validate everything FIRST — per the contract, a decode error
   * means no record gets enqueued. */
  brk_rbuf v = r;
  brk_record tmp;
  for (uint32_t i = 0; i < count; i++) {
    if (!decode_record(&v, &tmp)) {
      brk_set_err(h, BRK_ERR_DECODE, "produce batch: bad record %u", i);
      return BRK_ERR_DECODE;
    }
  }

  /* Pass 2: produce */
  char topic[512];
  for (uint32_t i = 0; i < count; i++) {
    brk_record rec = {0};
    decode_record(&r, &rec); /* already validated in pass 1 */
    if (rec.topic_len >= sizeof(topic)) {
      err_out[i] = (int16_t)RD_KAFKA_RESP_ERR__INVALID_ARG;
      continue;
    }
    memcpy(topic, rec.topic, rec.topic_len);
    topic[rec.topic_len] = '\0';

    rd_kafka_headers_t *hdrs = NULL;
    if (rec.header_count > 0) {
      hdrs = rd_kafka_headers_new(rec.header_count);
      brk_rbuf hr = {in_buf, in_len, rec.headers_off};
      char hkey[512];
      for (uint16_t k = 0; k < rec.header_count; k++) {
        uint16_t klen = 0;
        int32_t vlen = -1;
        rb_u16(&hr, &klen);
        const uint8_t *kp = rb_bytes(&hr, klen);
        rb_i32(&hr, &vlen);
        const uint8_t *vp = vlen >= 0 ? rb_bytes(&hr, vlen) : NULL;
        uint16_t kl = klen < sizeof(hkey) ? klen : (uint16_t)(sizeof(hkey) - 1);
        memcpy(hkey, kp, kl);
        hkey[kl] = '\0';
        rd_kafka_header_add(hdrs, hkey, kl, vp,
                            vlen >= 0 ? (ssize_t)vlen : -1);
      }
    }

    rd_kafka_resp_err_t err = rd_kafka_producev(
        h->rk, RD_KAFKA_V_TOPIC(topic), RD_KAFKA_V_PARTITION(rec.partition),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_TIMESTAMP(rec.timestamp_ms), /* 0 = now (librdkafka) */
        RD_KAFKA_V_KEY(rec.key, rec.key_len >= 0 ? (size_t)rec.key_len : 0),
        RD_KAFKA_V_VALUE((void *)rec.value,
                         rec.value_len >= 0 ? (size_t)rec.value_len : 0),
        RD_KAFKA_V_HEADERS(hdrs),
        RD_KAFKA_V_OPAQUE((void *)(uintptr_t)rec.opaque_id), RD_KAFKA_V_END);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR && hdrs != NULL)
      rd_kafka_headers_destroy(hdrs); /* librdkafka only takes ownership on OK */
    err_out[i] = (int16_t)err;
  }
  return (int32_t)count;
}

BRK_EXPORT int32_t brk_flush(void *hv, int32_t timeout_ms) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_PRODUCER) return BRK_ERR_INVALID_STATE;
  rd_kafka_resp_err_t err = rd_kafka_flush(h->rk, timeout_ms);
  return err == RD_KAFKA_RESP_ERR_NO_ERROR ? BRK_OK : BRK_KAFKA_ERR(err);
}

/* ---- Transactions ---------------------------------------------------------
 * rd_kafka_error_t → return code + errstr. TODO(ABI v2): expose is_retriable/
 * txn_requires_abort via brk_last_error_flags; for now TS derives them from
 * the errstr markers. */
static int32_t txn_result(brk_handle *h, rd_kafka_error_t *e, char *errstr,
                          int32_t errstr_size) {
  if (e == NULL) return BRK_OK;
  int32_t code = BRK_KAFKA_ERR(rd_kafka_error_code(e));
  if (errstr != NULL && errstr_size > 0)
    snprintf(errstr, (size_t)errstr_size, "%s%s%s", rd_kafka_error_string(e),
             rd_kafka_error_txn_requires_abort(e) ? " [txn-requires-abort]" : "",
             rd_kafka_error_is_retriable(e) ? " [retriable]" : "");
  brk_set_err(h, code, "%s", rd_kafka_error_string(e));
  rd_kafka_error_destroy(e);
  return code;
}

BRK_EXPORT int32_t brk_init_transactions(void *hv, int32_t timeout_ms,
                                         char *errstr, int32_t errstr_size) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_PRODUCER) return BRK_ERR_INVALID_STATE;
  return txn_result(h, rd_kafka_init_transactions(h->rk, timeout_ms), errstr,
                    errstr_size);
}

BRK_EXPORT int32_t brk_begin_transaction(void *hv, char *errstr,
                                         int32_t errstr_size) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_PRODUCER) return BRK_ERR_INVALID_STATE;
  return txn_result(h, rd_kafka_begin_transaction(h->rk), errstr, errstr_size);
}

BRK_EXPORT int32_t brk_commit_transaction(void *hv, int32_t timeout_ms,
                                          char *errstr, int32_t errstr_size) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_PRODUCER) return BRK_ERR_INVALID_STATE;
  return txn_result(h, rd_kafka_commit_transaction(h->rk, timeout_ms), errstr,
                    errstr_size);
}

BRK_EXPORT int32_t brk_abort_transaction(void *hv, int32_t timeout_ms,
                                         char *errstr, int32_t errstr_size) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_PRODUCER) return BRK_ERR_INVALID_STATE;
  return txn_result(h, rd_kafka_abort_transaction(h->rk, timeout_ms), errstr,
                    errstr_size);
}

BRK_EXPORT int32_t brk_send_offsets_to_transaction(
    void *hv, const uint8_t *tpl_buf, int32_t tpl_len, void *consumer_hv,
    int32_t timeout_ms, char *errstr, int32_t errstr_size) {
  brk_handle *h = brk_check(hv);
  brk_handle *c = brk_check(consumer_hv);
  if (h == NULL || c == NULL) return BRK_ERR_INVALID_HANDLE;
  if (h->type != BRK_CLIENT_PRODUCER || c->type != BRK_CLIENT_CONSUMER)
    return BRK_ERR_INVALID_STATE;
  rd_kafka_topic_partition_list_t *tpl = brk_tpl_decode(h, tpl_buf, tpl_len);
  if (tpl == NULL) return BRK_ERR_DECODE;
  rd_kafka_consumer_group_metadata_t *md =
      rd_kafka_consumer_group_metadata(c->rk);
  if (md == NULL) {
    rd_kafka_topic_partition_list_destroy(tpl);
    return BRK_ERR_INVALID_STATE;
  }
  rd_kafka_error_t *e =
      rd_kafka_send_offsets_to_transaction(h->rk, tpl, md, timeout_ms);
  rd_kafka_consumer_group_metadata_destroy(md);
  rd_kafka_topic_partition_list_destroy(tpl);
  return txn_result(h, e, errstr, errstr_size);
}
