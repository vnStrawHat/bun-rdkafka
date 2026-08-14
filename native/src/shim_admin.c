/* Metadata + Admin API + OAUTHBEARER.
 *
 * ==========================================================================
 * ADMIN REQUEST/RESPONSE JSON SCHEMAS — source of truth for the TS layer (M4)
 * ==========================================================================
 * Common to every request: "timeout_ms" (int, optional — request timeout),
 * "operation_timeout_ms" (int, optional — only CREATE_TOPICS / DELETE_TOPICS /
 * CREATE_PARTITIONS / DELETE_RECORDS). Results are delivered asynchronously via
 * the ADMIN_RESULT event (correlation_id, top-level kafka_err, json below).
 *
 * 1 CREATE_TOPICS
 *   req : {"topics":[{"name":s,"num_partitions":i?=-1,"replication_factor":i?=-1,
 *                     "config":{k:v}?}], "validate_only":b?}
 *   res : {"topics":[{"name":s,"error_code":i,"error_string":s}]}
 * 2 DELETE_TOPICS
 *   req : {"topics":[s]}
 *   res : same as CREATE_TOPICS
 * 3 CREATE_PARTITIONS
 *   req : {"topics":[{"name":s,"total_count":i}]}
 *   res : same as CREATE_TOPICS
 * 4 LIST_GROUPS
 *   req : {}
 *   res : {"groups":[{"group_id":s,"is_simple":b,"state":s,"type":s}],
 *          "errors":[{"error_code":i,"error_string":s}]}
 *         // type: rd_kafka_consumer_group_type_name ("Unknown"/"Consumer"/
 *         // "Classic"). protocolType is ABSENT — librdkafka does not expose it.
 * 5 DESCRIBE_GROUPS
 *   req : {"groups":[s], "include_authorized_operations":b?}
 *   res : {"groups":[{"group_id":s,"error_code":i,"error_string":s,
 *          "is_simple":b,"partition_assignor":s,"state":s,
 *          "coordinator":{"id":i,"host":s,"port":i}|null,
 *          "members":[{"member_id":s,"client_id":s,"group_instance_id":s|null,
 *                      "host":s,"assignment":[TP]}]}]}
 * 6 DELETE_GROUPS
 *   req : {"groups":[s]}
 *   res : {"groups":[{"group_id":s,"error_code":i,"error_string":s}]}
 * 7 LIST_CONSUMER_GROUP_OFFSETS
 *   req : {"group_id":s, "partitions":[{"topic":s,"partition":i}]|null,
 *          "require_stable":b?}       // partitions null = all partitions
 *   res : {"groups":[{"group_id":s,"error_code":i,"error_string":s,
 *                     "partitions":[TP]}]}
 * 8 DELETE_RECORDS
 *   req : {"partitions":[{"topic":s,"partition":i,"offset":i}]}
 *                                     // offset = delete records BEFORE this offset
 *   res : {"partitions":[TP]}         // TP.offset = low_watermark after deletion
 * 9 DESCRIBE_TOPICS
 *   req : {"topics":[s], "include_authorized_operations":b?}
 *   res : {"topics":[{"name":s,"error_code":i,"error_string":s,"is_internal":b,
 *          "partitions":[{"partition":i,"leader":NODE|null,
 *                         "isr":[NODE],"replicas":[NODE]}]}]}
 * 10 LIST_OFFSETS
 *   req : {"partitions":[{"topic":s,"partition":i,"offset_spec":i}],
 *          "isolation_level":i?}     // offset_spec: -2 EARLIEST, -1 LATEST,
 *                                    // -3 MAX_TIMESTAMP, >=0 timestamp ms;
 *                                    // isolation_level: 0 RU (default), 1 RC
 *   res : {"partitions":[{"topic":s,"partition":i,"offset":i,"timestamp":i,
 *                         "leader_epoch":i,"error_code":i,"error_string":s}]}
 *                                    // timestamp -1 if absent
 *
 * TP   = {"topic":s,"partition":i,"offset":i,"leader_epoch":i,
 *         "metadata":s|null,"error_code":i}
 * NODE = {"id":i,"host":s,"port":i}
 * ========================================================================== */
#include "shim_internal.h"

/* ---- malloc-grow string builder for cold-path JSON ------------------------ */
typedef struct {
  char *p;
  size_t len, cap;
  bool oom;
} brk_sb;

static bool sb_reserve(brk_sb *sb, size_t need) {
  if (sb->oom) return false;
  if (sb->len + need + 1 <= sb->cap) return true;
  size_t ncap = sb->cap ? sb->cap : 1024;
  while (sb->len + need + 1 > ncap) ncap *= 2;
  char *np = realloc(sb->p, ncap);
  if (np == NULL) {
    sb->oom = true;
    return false;
  }
  sb->p = np;
  sb->cap = ncap;
  return true;
}

static void sb_raw(brk_sb *sb, const char *s, size_t n) {
  if (!sb_reserve(sb, n)) return;
  memcpy(sb->p + sb->len, s, n);
  sb->len += n;
  sb->p[sb->len] = '\0';
}

static void sb_str(brk_sb *sb, const char *s) { sb_raw(sb, s, strlen(s)); }

static void sb_int(brk_sb *sb, int64_t v) {
  char tmp[24];
  int n = snprintf(tmp, sizeof(tmp), "%lld", (long long)v);
  sb_raw(sb, tmp, (size_t)n);
}

/* Standards-compliant escaped JSON string with an explicit length (a tpl's
 * metadata is not guaranteed to be NUL-terminated). */
static void sb_json_strn(brk_sb *sb, const char *s, size_t n) {
  sb_str(sb, "\"");
  for (size_t i = 0; i < n; i++) {
    unsigned char c = (unsigned char)s[i];
    switch (c) {
    case '"': sb_str(sb, "\\\""); break;
    case '\\': sb_str(sb, "\\\\"); break;
    case '\b': sb_str(sb, "\\b"); break;
    case '\f': sb_str(sb, "\\f"); break;
    case '\n': sb_str(sb, "\\n"); break;
    case '\r': sb_str(sb, "\\r"); break;
    case '\t': sb_str(sb, "\\t"); break;
    default:
      if (c < 0x20) {
        char tmp[8];
        snprintf(tmp, sizeof(tmp), "\\u%04x", c);
        sb_str(sb, tmp);
      } else {
        sb_raw(sb, (const char *)&s[i], 1);
      }
    }
  }
  sb_str(sb, "\"");
}

/* NULL is treated as an empty string. */
static void sb_json_str(brk_sb *sb, const char *s) {
  sb_json_strn(sb, s ? s : "", s ? strlen(s) : 0);
}

/* ==========================================================================
 * Mini JSON parser — just enough for the request schemas above (cold path).
 * Pointers always point into the NUL-terminated req_json string passed from JS.
 * ========================================================================== */

static const char *js_ws(const char *p) {
  while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
  return p;
}

/* p is at the opening '"'. Returns the pointer AFTER the closing '"', NULL on
 * error. */
static const char *js_skip_str(const char *p) {
  p++;
  while (*p && *p != '"') {
    if (*p == '\\' && p[1]) p++;
    p++;
  }
  return *p == '"' ? p + 1 : NULL;
}

static const char *js_skip_value(const char *p);

static const char *js_skip_container(const char *p) {
  char open = *p, close = open == '{' ? '}' : ']';
  p = js_ws(p + 1);
  if (*p == close) return p + 1;
  for (;;) {
    if (open == '{') {
      if (*p != '"') return NULL;
      p = js_skip_str(p);
      if (p == NULL) return NULL;
      p = js_ws(p);
      if (*p != ':') return NULL;
      p++;
    }
    p = js_skip_value(p);
    if (p == NULL) return NULL;
    p = js_ws(p);
    if (*p == ',') {
      p = js_ws(p + 1);
      continue;
    }
    if (*p == close) return p + 1;
    return NULL;
  }
}

static const char *js_skip_value(const char *p) {
  p = js_ws(p);
  switch (*p) {
  case '"': return js_skip_str(p);
  case '{':
  case '[': return js_skip_container(p);
  case 't': return strncmp(p, "true", 4) == 0 ? p + 4 : NULL;
  case 'f': return strncmp(p, "false", 5) == 0 ? p + 5 : NULL;
  case 'n': return strncmp(p, "null", 4) == 0 ? p + 4 : NULL;
  default: {
    const char *q = p;
    if (*q == '-') q++;
    if (!(*q >= '0' && *q <= '9')) return NULL;
    while ((*q >= '0' && *q <= '9') || *q == '.' || *q == 'e' || *q == 'E' ||
           *q == '+' || *q == '-')
      q++;
    return q;
  }
  }
}

/* obj is at '{'. Returns a pointer to the key's value (schema keys are plain
 * ASCII), NULL when absent. */
static const char *js_obj_find(const char *obj, const char *key) {
  if (obj == NULL) return NULL;
  const char *p = js_ws(obj);
  if (*p != '{') return NULL;
  p = js_ws(p + 1);
  if (*p == '}') return NULL;
  size_t klen = strlen(key);
  for (;;) {
    if (*p != '"') return NULL;
    const char *kstart = p + 1;
    const char *kend = js_skip_str(p);
    if (kend == NULL) return NULL;
    bool match = (size_t)(kend - 1 - kstart) == klen &&
                 memcmp(kstart, key, klen) == 0;
    p = js_ws(kend);
    if (*p != ':') return NULL;
    p = js_ws(p + 1);
    if (match) return p;
    p = js_skip_value(p);
    if (p == NULL) return NULL;
    p = js_ws(p);
    if (*p != ',') return NULL;
    p = js_ws(p + 1);
  }
}

/* Decodes the JSON string at the value position → malloc'd UTF-8 (including
 * \uXXXX + surrogate pairs). NULL on error / when not a string. */
static char *js_str_dup(const char *p) {
  if (p == NULL) return NULL;
  p = js_ws(p);
  if (*p != '"') return NULL;
  const char *end = js_skip_str(p);
  if (end == NULL) return NULL;
  char *out = malloc((size_t)(end - p) + 4);
  if (out == NULL) return NULL;
  size_t o = 0;
  const char *q = p + 1;
  while (q < end - 1) {
    if (*q != '\\') {
      out[o++] = *q++;
      continue;
    }
    q++;
    switch (*q) {
    case '"': out[o++] = '"'; q++; break;
    case '\\': out[o++] = '\\'; q++; break;
    case '/': out[o++] = '/'; q++; break;
    case 'b': out[o++] = '\b'; q++; break;
    case 'f': out[o++] = '\f'; q++; break;
    case 'n': out[o++] = '\n'; q++; break;
    case 'r': out[o++] = '\r'; q++; break;
    case 't': out[o++] = '\t'; q++; break;
    case 'u': {
      unsigned cp = 0;
      q++;
      for (int i = 0; i < 4; i++) {
        char c = *q++;
        cp <<= 4;
        if (c >= '0' && c <= '9') cp |= (unsigned)(c - '0');
        else if (c >= 'a' && c <= 'f') cp |= (unsigned)(c - 'a' + 10);
        else if (c >= 'A' && c <= 'F') cp |= (unsigned)(c - 'A' + 10);
        else { free(out); return NULL; }
      }
      if (cp >= 0xD800 && cp <= 0xDBFF && q[0] == '\\' && q[1] == 'u') {
        unsigned lo = 0;
        const char *q2 = q + 2;
        bool ok = true;
        for (int i = 0; i < 4; i++) {
          char c = *q2++;
          lo <<= 4;
          if (c >= '0' && c <= '9') lo |= (unsigned)(c - '0');
          else if (c >= 'a' && c <= 'f') lo |= (unsigned)(c - 'a' + 10);
          else if (c >= 'A' && c <= 'F') lo |= (unsigned)(c - 'A' + 10);
          else { ok = false; break; }
        }
        if (ok && lo >= 0xDC00 && lo <= 0xDFFF) {
          cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
          q = q2;
        }
      }
      if (cp < 0x80) out[o++] = (char)cp;
      else if (cp < 0x800) {
        out[o++] = (char)(0xC0 | (cp >> 6));
        out[o++] = (char)(0x80 | (cp & 0x3F));
      } else if (cp < 0x10000) {
        out[o++] = (char)(0xE0 | (cp >> 12));
        out[o++] = (char)(0x80 | ((cp >> 6) & 0x3F));
        out[o++] = (char)(0x80 | (cp & 0x3F));
      } else {
        out[o++] = (char)(0xF0 | (cp >> 18));
        out[o++] = (char)(0x80 | ((cp >> 12) & 0x3F));
        out[o++] = (char)(0x80 | ((cp >> 6) & 0x3F));
        out[o++] = (char)(0x80 | (cp & 0x3F));
      }
      break;
    }
    default: free(out); return NULL;
    }
  }
  out[o] = '\0';
  return out;
}

static bool js_get_i64(const char *v, int64_t *out) {
  if (v == NULL) return false;
  v = js_ws(v);
  char *e = NULL;
  long long r = strtoll(v, &e, 10);
  if (e == v) return false;
  *out = (int64_t)r;
  return true;
}

static bool js_get_bool(const char *v, bool *out) {
  if (v == NULL) return false;
  v = js_ws(v);
  if (strncmp(v, "true", 4) == 0) { *out = true; return true; }
  if (strncmp(v, "false", 5) == 0) { *out = false; return true; }
  return false;
}

static bool js_is_null(const char *v) {
  if (v == NULL) return true;
  v = js_ws(v);
  return strncmp(v, "null", 4) == 0;
}

/* Array iteration: init *elem to NULL; returns false at end/on error. */
static bool js_arr_next(const char *arr, const char **elem) {
  const char *p;
  if (*elem == NULL) {
    if (arr == NULL) return false;
    p = js_ws(arr);
    if (*p != '[') return false;
    p = js_ws(p + 1);
    if (*p == ']') return false;
  } else {
    p = js_skip_value(*elem);
    if (p == NULL) return false;
    p = js_ws(p);
    if (*p != ',') return false;
    p = js_ws(p + 1);
  }
  *elem = p;
  return true;
}

static int32_t js_arr_count(const char *arr) {
  int32_t n = 0;
  const char *el = NULL;
  while (js_arr_next(arr, &el)) n++;
  return n;
}

/* Object {k:v} iteration: init *pos to NULL; the returned key is malloc'd
 * (caller frees). */
static bool js_obj_next(const char *obj, const char **pos, char **key_out,
                        const char **val_out) {
  const char *p;
  if (*pos == NULL) {
    if (obj == NULL) return false;
    p = js_ws(obj);
    if (*p != '{') return false;
    p = js_ws(p + 1);
    if (*p == '}') return false;
  } else {
    p = js_skip_value(*pos);
    if (p == NULL) return false;
    p = js_ws(p);
    if (*p != ',') return false;
    p = js_ws(p + 1);
  }
  if (*p != '"') return false;
  char *k = js_str_dup(p);
  if (k == NULL) return false;
  const char *after = js_skip_str(p);
  after = js_ws(after);
  if (*after != ':') {
    free(k);
    return false;
  }
  *key_out = k;
  *val_out = js_ws(after + 1);
  *pos = *val_out;
  return true;
}

/* ==========================================================================
 * Metadata (unchanged since M4 part 1)
 * ========================================================================== */

/* JSON shape matches getMetadata() of confluent-kafka-javascript/node-rdkafka
 * so the callback layer can pass it through untouched. */
static void sb_metadata_json(brk_sb *sb, const struct rd_kafka_metadata *md) {
  sb_str(sb, "{\"orig_broker_id\":");
  sb_int(sb, md->orig_broker_id);
  sb_str(sb, ",\"orig_broker_name\":");
  sb_json_str(sb, md->orig_broker_name);
  sb_str(sb, ",\"brokers\":[");
  for (int i = 0; i < md->broker_cnt; i++) {
    if (i) sb_str(sb, ",");
    sb_str(sb, "{\"id\":");
    sb_int(sb, md->brokers[i].id);
    sb_str(sb, ",\"host\":");
    sb_json_str(sb, md->brokers[i].host);
    sb_str(sb, ",\"port\":");
    sb_int(sb, md->brokers[i].port);
    sb_str(sb, "}");
  }
  sb_str(sb, "],\"topics\":[");
  for (int t = 0; t < md->topic_cnt; t++) {
    const struct rd_kafka_metadata_topic *mt = &md->topics[t];
    if (t) sb_str(sb, ",");
    sb_str(sb, "{\"name\":");
    sb_json_str(sb, mt->topic);
    sb_str(sb, ",\"partitions\":[");
    for (int p = 0; p < mt->partition_cnt; p++) {
      const struct rd_kafka_metadata_partition *mp = &mt->partitions[p];
      if (p) sb_str(sb, ",");
      sb_str(sb, "{\"id\":");
      sb_int(sb, mp->id);
      sb_str(sb, ",\"leader\":");
      sb_int(sb, mp->leader);
      sb_str(sb, ",\"replicas\":[");
      for (int r = 0; r < mp->replica_cnt; r++) {
        if (r) sb_str(sb, ",");
        sb_int(sb, mp->replicas[r]);
      }
      sb_str(sb, "],\"isrs\":[");
      for (int r = 0; r < mp->isr_cnt; r++) {
        if (r) sb_str(sb, ",");
        sb_int(sb, mp->isrs[r]);
      }
      sb_str(sb, "]}");
    }
    sb_str(sb, "]}");
  }
  sb_str(sb, "]}");
}

BRK_EXPORT int32_t brk_metadata(void *hv, const char *topic_or_null,
                                int32_t timeout_ms, char **out) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (out == NULL) {
    brk_set_err(h, BRK_ERR_BAD_ARGS, "brk_metadata: out == NULL");
    return BRK_ERR_BAD_ARGS;
  }
  *out = NULL;

  rd_kafka_topic_t *rkt = NULL;
  if (topic_or_null != NULL && topic_or_null[0] != '\0') {
    rkt = rd_kafka_topic_new(h->rk, topic_or_null, NULL);
    if (rkt == NULL) {
      rd_kafka_resp_err_t terr = rd_kafka_last_error();
      brk_set_err(h, BRK_KAFKA_ERR(terr),
                  "brk_metadata: rd_kafka_topic_new(%s): %s", topic_or_null,
                  rd_kafka_err2str(terr));
      return BRK_KAFKA_ERR(terr);
    }
  }

  /* Blocks up to timeout_ms (broker round-trip). Blocking is accepted: cold
   * path, the TS layer only calls this from connect()/getMetadata(). */
  const struct rd_kafka_metadata *md = NULL;
  rd_kafka_resp_err_t err =
      rd_kafka_metadata(h->rk, rkt == NULL, rkt, &md, timeout_ms);
  if (rkt != NULL) rd_kafka_topic_destroy(rkt);
  if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
    brk_set_err(h, BRK_KAFKA_ERR(err), "brk_metadata: %s",
                rd_kafka_err2str(err));
    return BRK_KAFKA_ERR(err);
  }

  brk_sb sb = {0};
  sb_metadata_json(&sb, md);
  rd_kafka_metadata_destroy(md);

  if (sb.oom || sb.p == NULL) {
    free(sb.p);
    brk_set_err(h, BRK_ERR_NOMEM, "brk_metadata: out of memory");
    return BRK_ERR_NOMEM;
  }
  *out = sb.p; /* JS copies then calls brk_mem_free */
  return (int32_t)sb.len;
}

/* ==========================================================================
 * Admin: result event → JSON
 * ========================================================================== */

bool brk_event_is_admin_result(int32_t evtype) {
  switch (evtype) {
  case RD_KAFKA_EVENT_CREATETOPICS_RESULT:
  case RD_KAFKA_EVENT_DELETETOPICS_RESULT:
  case RD_KAFKA_EVENT_CREATEPARTITIONS_RESULT:
  case RD_KAFKA_EVENT_LISTCONSUMERGROUPS_RESULT:
  case RD_KAFKA_EVENT_DESCRIBECONSUMERGROUPS_RESULT:
  case RD_KAFKA_EVENT_DELETEGROUPS_RESULT:
  case RD_KAFKA_EVENT_LISTCONSUMERGROUPOFFSETS_RESULT:
  case RD_KAFKA_EVENT_DELETERECORDS_RESULT:
  case RD_KAFKA_EVENT_DESCRIBETOPICS_RESULT:
  case RD_KAFKA_EVENT_LISTOFFSETS_RESULT: return true;
  default: return false;
  }
}

static void sb_kv_err(brk_sb *sb, rd_kafka_resp_err_t code, const char *str) {
  sb_str(sb, "\"error_code\":");
  sb_int(sb, code);
  sb_str(sb, ",\"error_string\":");
  sb_json_str(sb, str);
}

static void sb_error_fields(brk_sb *sb, const rd_kafka_error_t *e) {
  sb_kv_err(sb, e ? rd_kafka_error_code(e) : RD_KAFKA_RESP_ERR_NO_ERROR,
            e ? rd_kafka_error_string(e) : "");
}

static void sb_node(brk_sb *sb, const rd_kafka_Node_t *n) {
  if (n == NULL) {
    sb_str(sb, "null");
    return;
  }
  sb_str(sb, "{\"id\":");
  sb_int(sb, rd_kafka_Node_id(n));
  sb_str(sb, ",\"host\":");
  sb_json_str(sb, rd_kafka_Node_host(n));
  sb_str(sb, ",\"port\":");
  sb_int(sb, rd_kafka_Node_port(n));
  sb_str(sb, "}");
}

/* TP array (see the schema at the top of this file). */
static void sb_tpl_json(brk_sb *sb,
                        const rd_kafka_topic_partition_list_t *tpl) {
  sb_str(sb, "[");
  for (int i = 0; tpl != NULL && i < tpl->cnt; i++) {
    const rd_kafka_topic_partition_t *p = &tpl->elems[i];
    if (i) sb_str(sb, ",");
    sb_str(sb, "{\"topic\":");
    sb_json_str(sb, p->topic);
    sb_str(sb, ",\"partition\":");
    sb_int(sb, p->partition);
    sb_str(sb, ",\"offset\":");
    sb_int(sb, p->offset);
    sb_str(sb, ",\"leader_epoch\":");
    sb_int(sb, rd_kafka_topic_partition_get_leader_epoch(p));
    sb_str(sb, ",\"metadata\":");
    if (p->metadata != NULL && p->metadata_size > 0)
      sb_json_strn(sb, (const char *)p->metadata, p->metadata_size);
    else
      sb_str(sb, "null");
    sb_str(sb, ",\"error_code\":");
    sb_int(sb, p->err);
    sb_str(sb, "}");
  }
  sb_str(sb, "]");
}

static void sb_topic_results(brk_sb *sb, const rd_kafka_topic_result_t **arr,
                             size_t cnt) {
  sb_str(sb, "{\"topics\":[");
  for (size_t i = 0; i < cnt; i++) {
    if (i) sb_str(sb, ",");
    sb_str(sb, "{\"name\":");
    sb_json_str(sb, rd_kafka_topic_result_name(arr[i]));
    sb_str(sb, ",");
    sb_kv_err(sb, rd_kafka_topic_result_error(arr[i]),
              rd_kafka_topic_result_error_string(arr[i]));
    sb_str(sb, "}");
  }
  sb_str(sb, "]}");
}

static void sb_group_results(brk_sb *sb, const rd_kafka_group_result_t **arr,
                             size_t cnt, bool with_partitions) {
  sb_str(sb, "{\"groups\":[");
  for (size_t i = 0; i < cnt; i++) {
    if (i) sb_str(sb, ",");
    sb_str(sb, "{\"group_id\":");
    sb_json_str(sb, rd_kafka_group_result_name(arr[i]));
    sb_str(sb, ",");
    sb_error_fields(sb, rd_kafka_group_result_error(arr[i]));
    if (with_partitions) {
      sb_str(sb, ",\"partitions\":");
      sb_tpl_json(sb, rd_kafka_group_result_partitions(arr[i]));
    }
    sb_str(sb, "}");
  }
  sb_str(sb, "]}");
}

int32_t brk_admin_result_json(brk_handle *h, rd_kafka_event_t *ev,
                              char **out) {
  brk_sb sb = {0};
  size_t cnt = 0;

  switch (rd_kafka_event_type(ev)) {
  case RD_KAFKA_EVENT_CREATETOPICS_RESULT: {
    const rd_kafka_topic_result_t **arr = rd_kafka_CreateTopics_result_topics(
        rd_kafka_event_CreateTopics_result(ev), &cnt);
    sb_topic_results(&sb, arr, cnt);
    break;
  }
  case RD_KAFKA_EVENT_DELETETOPICS_RESULT: {
    const rd_kafka_topic_result_t **arr = rd_kafka_DeleteTopics_result_topics(
        rd_kafka_event_DeleteTopics_result(ev), &cnt);
    sb_topic_results(&sb, arr, cnt);
    break;
  }
  case RD_KAFKA_EVENT_CREATEPARTITIONS_RESULT: {
    const rd_kafka_topic_result_t **arr =
        rd_kafka_CreatePartitions_result_topics(
            rd_kafka_event_CreatePartitions_result(ev), &cnt);
    sb_topic_results(&sb, arr, cnt);
    break;
  }
  case RD_KAFKA_EVENT_LISTCONSUMERGROUPS_RESULT: {
    const rd_kafka_ListConsumerGroups_result_t *res =
        rd_kafka_event_ListConsumerGroups_result(ev);
    const rd_kafka_ConsumerGroupListing_t **groups =
        rd_kafka_ListConsumerGroups_result_valid(res, &cnt);
    sb_str(&sb, "{\"groups\":[");
    for (size_t i = 0; i < cnt; i++) {
      if (i) sb_str(&sb, ",");
      sb_str(&sb, "{\"group_id\":");
      sb_json_str(&sb, rd_kafka_ConsumerGroupListing_group_id(groups[i]));
      sb_str(&sb, ",\"is_simple\":");
      sb_str(&sb, rd_kafka_ConsumerGroupListing_is_simple_consumer_group(
                      groups[i])
                      ? "true"
                      : "false");
      sb_str(&sb, ",\"state\":");
      sb_json_str(&sb, rd_kafka_consumer_group_state_name(
                           rd_kafka_ConsumerGroupListing_state(groups[i])));
      sb_str(&sb, ",\"type\":");
      sb_json_str(&sb, rd_kafka_consumer_group_type_name(
                           rd_kafka_ConsumerGroupListing_type(groups[i])));
      sb_str(&sb, "}");
    }
    size_t ecnt = 0;
    const rd_kafka_error_t **errs =
        rd_kafka_ListConsumerGroups_result_errors(res, &ecnt);
    sb_str(&sb, "],\"errors\":[");
    for (size_t i = 0; i < ecnt; i++) {
      if (i) sb_str(&sb, ",");
      sb_str(&sb, "{");
      sb_error_fields(&sb, errs[i]);
      sb_str(&sb, "}");
    }
    sb_str(&sb, "]}");
    break;
  }
  case RD_KAFKA_EVENT_DESCRIBECONSUMERGROUPS_RESULT: {
    const rd_kafka_ConsumerGroupDescription_t **groups =
        rd_kafka_DescribeConsumerGroups_result_groups(
            rd_kafka_event_DescribeConsumerGroups_result(ev), &cnt);
    sb_str(&sb, "{\"groups\":[");
    for (size_t i = 0; i < cnt; i++) {
      const rd_kafka_ConsumerGroupDescription_t *g = groups[i];
      if (i) sb_str(&sb, ",");
      sb_str(&sb, "{\"group_id\":");
      sb_json_str(&sb, rd_kafka_ConsumerGroupDescription_group_id(g));
      sb_str(&sb, ",");
      sb_error_fields(&sb, rd_kafka_ConsumerGroupDescription_error(g));
      sb_str(&sb, ",\"is_simple\":");
      sb_str(&sb,
             rd_kafka_ConsumerGroupDescription_is_simple_consumer_group(g)
                 ? "true"
                 : "false");
      sb_str(&sb, ",\"partition_assignor\":");
      sb_json_str(&sb,
                  rd_kafka_ConsumerGroupDescription_partition_assignor(g));
      sb_str(&sb, ",\"state\":");
      sb_json_str(&sb, rd_kafka_consumer_group_state_name(
                           rd_kafka_ConsumerGroupDescription_state(g)));
      sb_str(&sb, ",\"coordinator\":");
      sb_node(&sb, rd_kafka_ConsumerGroupDescription_coordinator(g));
      sb_str(&sb, ",\"members\":[");
      size_t mcnt = rd_kafka_ConsumerGroupDescription_member_count(g);
      for (size_t m = 0; m < mcnt; m++) {
        const rd_kafka_MemberDescription_t *md =
            rd_kafka_ConsumerGroupDescription_member(g, m);
        if (m) sb_str(&sb, ",");
        sb_str(&sb, "{\"member_id\":");
        sb_json_str(&sb, rd_kafka_MemberDescription_consumer_id(md));
        sb_str(&sb, ",\"client_id\":");
        sb_json_str(&sb, rd_kafka_MemberDescription_client_id(md));
        sb_str(&sb, ",\"group_instance_id\":");
        const char *gi = rd_kafka_MemberDescription_group_instance_id(md);
        if (gi != NULL)
          sb_json_str(&sb, gi);
        else
          sb_str(&sb, "null");
        sb_str(&sb, ",\"host\":");
        sb_json_str(&sb, rd_kafka_MemberDescription_host(md));
        sb_str(&sb, ",\"assignment\":");
        const rd_kafka_MemberAssignment_t *ma =
            rd_kafka_MemberDescription_assignment(md);
        sb_tpl_json(&sb,
                    ma != NULL ? rd_kafka_MemberAssignment_partitions(ma)
                               : NULL);
        sb_str(&sb, "}");
      }
      sb_str(&sb, "]}");
    }
    sb_str(&sb, "]}");
    break;
  }
  case RD_KAFKA_EVENT_DELETEGROUPS_RESULT: {
    const rd_kafka_group_result_t **arr = rd_kafka_DeleteGroups_result_groups(
        rd_kafka_event_DeleteGroups_result(ev), &cnt);
    sb_group_results(&sb, arr, cnt, false);
    break;
  }
  case RD_KAFKA_EVENT_LISTCONSUMERGROUPOFFSETS_RESULT: {
    const rd_kafka_group_result_t **arr =
        rd_kafka_ListConsumerGroupOffsets_result_groups(
            rd_kafka_event_ListConsumerGroupOffsets_result(ev), &cnt);
    sb_group_results(&sb, arr, cnt, true);
    break;
  }
  case RD_KAFKA_EVENT_DELETERECORDS_RESULT: {
    sb_str(&sb, "{\"partitions\":");
    sb_tpl_json(&sb, rd_kafka_DeleteRecords_result_offsets(
                         rd_kafka_event_DeleteRecords_result(ev)));
    sb_str(&sb, "}");
    break;
  }
  case RD_KAFKA_EVENT_DESCRIBETOPICS_RESULT: {
    const rd_kafka_TopicDescription_t **topics =
        rd_kafka_DescribeTopics_result_topics(
            rd_kafka_event_DescribeTopics_result(ev), &cnt);
    sb_str(&sb, "{\"topics\":[");
    for (size_t i = 0; i < cnt; i++) {
      const rd_kafka_TopicDescription_t *td = topics[i];
      if (i) sb_str(&sb, ",");
      sb_str(&sb, "{\"name\":");
      sb_json_str(&sb, rd_kafka_TopicDescription_name(td));
      sb_str(&sb, ",");
      sb_error_fields(&sb, rd_kafka_TopicDescription_error(td));
      sb_str(&sb, ",\"is_internal\":");
      sb_str(&sb, rd_kafka_TopicDescription_is_internal(td) ? "true"
                                                            : "false");
      sb_str(&sb, ",\"partitions\":[");
      size_t pcnt = 0;
      const rd_kafka_TopicPartitionInfo_t **parts =
          rd_kafka_TopicDescription_partitions(td, &pcnt);
      for (size_t p = 0; p < pcnt; p++) {
        const rd_kafka_TopicPartitionInfo_t *pi = parts[p];
        if (p) sb_str(&sb, ",");
        sb_str(&sb, "{\"partition\":");
        sb_int(&sb, rd_kafka_TopicPartitionInfo_partition(pi));
        sb_str(&sb, ",\"leader\":");
        sb_node(&sb, rd_kafka_TopicPartitionInfo_leader(pi));
        size_t ncnt = 0;
        const rd_kafka_Node_t **nodes =
            rd_kafka_TopicPartitionInfo_isr(pi, &ncnt);
        sb_str(&sb, ",\"isr\":[");
        for (size_t n = 0; n < ncnt; n++) {
          if (n) sb_str(&sb, ",");
          sb_node(&sb, nodes[n]);
        }
        sb_str(&sb, "],\"replicas\":[");
        nodes = rd_kafka_TopicPartitionInfo_replicas(pi, &ncnt);
        for (size_t n = 0; n < ncnt; n++) {
          if (n) sb_str(&sb, ",");
          sb_node(&sb, nodes[n]);
        }
        sb_str(&sb, "]}");
      }
      sb_str(&sb, "]}");
    }
    sb_str(&sb, "]}");
    break;
  }
  case RD_KAFKA_EVENT_LISTOFFSETS_RESULT: {
    const rd_kafka_ListOffsetsResultInfo_t **infos =
        rd_kafka_ListOffsets_result_infos(rd_kafka_event_ListOffsets_result(ev),
                                          &cnt);
    sb_str(&sb, "{\"partitions\":[");
    for (size_t i = 0; i < cnt; i++) {
      const rd_kafka_topic_partition_t *p =
          rd_kafka_ListOffsetsResultInfo_topic_partition(infos[i]);
      if (i) sb_str(&sb, ",");
      sb_str(&sb, "{\"topic\":");
      sb_json_str(&sb, p->topic);
      sb_str(&sb, ",\"partition\":");
      sb_int(&sb, p->partition);
      sb_str(&sb, ",\"offset\":");
      sb_int(&sb, p->offset);
      sb_str(&sb, ",\"timestamp\":");
      sb_int(&sb, rd_kafka_ListOffsetsResultInfo_timestamp(infos[i]));
      sb_str(&sb, ",\"leader_epoch\":");
      sb_int(&sb, rd_kafka_topic_partition_get_leader_epoch(p));
      sb_str(&sb, ",");
      sb_kv_err(&sb, p->err, p->err ? rd_kafka_err2str(p->err) : "");
      sb_str(&sb, "}");
    }
    sb_str(&sb, "]}");
    break;
  }
  default:
    sb_str(&sb, "{}");
    break;
  }

  if (sb.oom || sb.p == NULL) {
    free(sb.p);
    brk_set_err(h, BRK_ERR_NOMEM, "admin result: out of memory");
    return BRK_ERR_NOMEM;
  }
  *out = sb.p;
  return (int32_t)sb.len;
}

/* ==========================================================================
 * Admin: request
 * ========================================================================== */

static rd_kafka_admin_op_t admin_op_of(int32_t op_id) {
  switch (op_id) {
  case BRK_ADMIN_CREATE_TOPICS: return RD_KAFKA_ADMIN_OP_CREATETOPICS;
  case BRK_ADMIN_DELETE_TOPICS: return RD_KAFKA_ADMIN_OP_DELETETOPICS;
  case BRK_ADMIN_CREATE_PARTITIONS: return RD_KAFKA_ADMIN_OP_CREATEPARTITIONS;
  case BRK_ADMIN_LIST_GROUPS: return RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPS;
  case BRK_ADMIN_DESCRIBE_GROUPS:
    return RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS;
  case BRK_ADMIN_DELETE_GROUPS: return RD_KAFKA_ADMIN_OP_DELETEGROUPS;
  case BRK_ADMIN_LIST_CONSUMER_GROUP_OFFSETS:
    return RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS;
  case BRK_ADMIN_DELETE_RECORDS: return RD_KAFKA_ADMIN_OP_DELETERECORDS;
  case BRK_ADMIN_DESCRIBE_TOPICS: return RD_KAFKA_ADMIN_OP_DESCRIBETOPICS;
  case BRK_ADMIN_LIST_OFFSETS: return RD_KAFKA_ADMIN_OP_LISTOFFSETS;
  default: return RD_KAFKA_ADMIN_OP_ANY;
  }
}

/* String array from JSON ["a","b"] → malloc'd char** (caller frees each
 * element + the array). Returns count or negative. */
static int32_t dup_str_array(const char *arr, char ***out) {
  int32_t cnt = js_arr_count(arr);
  if (cnt <= 0) return BRK_ERR_DECODE;
  char **v = calloc((size_t)cnt, sizeof(char *));
  if (v == NULL) return BRK_ERR_NOMEM;
  const char *el = NULL;
  int32_t i = 0;
  while (js_arr_next(arr, &el)) {
    v[i] = js_str_dup(el);
    if (v[i] == NULL) {
      for (int32_t j = 0; j < i; j++) free(v[j]);
      free(v);
      return BRK_ERR_DECODE;
    }
    i++;
  }
  *out = v;
  return cnt;
}

static void free_str_array(char **v, int32_t cnt) {
  if (v == NULL) return;
  for (int32_t i = 0; i < cnt; i++) free(v[i]);
  free(v);
}

/* [{"topic","partition","offset"?}] → tpl (caller destroys). NULL on error. */
static rd_kafka_topic_partition_list_t *tpl_from_json(const char *arr) {
  int32_t cnt = js_arr_count(arr);
  if (cnt < 0) return NULL;
  rd_kafka_topic_partition_list_t *tpl =
      rd_kafka_topic_partition_list_new(cnt);
  const char *el = NULL;
  while (js_arr_next(arr, &el)) {
    char *topic = js_str_dup(js_obj_find(el, "topic"));
    int64_t partition = -1, offset = RD_KAFKA_OFFSET_INVALID;
    if (topic == NULL || !js_get_i64(js_obj_find(el, "partition"), &partition)) {
      free(topic);
      rd_kafka_topic_partition_list_destroy(tpl);
      return NULL;
    }
    js_get_i64(js_obj_find(el, "offset"), &offset);
    rd_kafka_topic_partition_t *p =
        rd_kafka_topic_partition_list_add(tpl, topic, (int32_t)partition);
    p->offset = offset;
    free(topic);
  }
  return tpl;
}

BRK_EXPORT int32_t brk_admin_request(void *hv, int32_t op_id,
                                     uint64_t correlation_id,
                                     const char *req_json) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (req_json == NULL) {
    brk_set_err(h, BRK_ERR_BAD_ARGS, "brk_admin_request: req_json == NULL");
    return BRK_ERR_BAD_ARGS;
  }
  rd_kafka_admin_op_t op = admin_op_of(op_id);
  if (op == RD_KAFKA_ADMIN_OP_ANY) {
    brk_set_err(h, BRK_ERR_BAD_ARGS, "brk_admin_request: unknown op %d",
                op_id);
    return BRK_ERR_BAD_ARGS;
  }

  char errstr[512] = {0};
  rd_kafka_AdminOptions_t *opts = rd_kafka_AdminOptions_new(h->rk, op);
  if (opts == NULL) {
    brk_set_err(h, BRK_ERR_NOMEM, "brk_admin_request: AdminOptions_new");
    return BRK_ERR_NOMEM;
  }
  rd_kafka_AdminOptions_set_opaque(opts, (void *)(uintptr_t)correlation_id);

  int64_t t = 0;
  if (js_get_i64(js_obj_find(req_json, "timeout_ms"), &t))
    rd_kafka_AdminOptions_set_request_timeout(opts, (int)t, errstr,
                                              sizeof(errstr));
  if (js_get_i64(js_obj_find(req_json, "operation_timeout_ms"), &t) &&
      (op_id == BRK_ADMIN_CREATE_TOPICS || op_id == BRK_ADMIN_DELETE_TOPICS ||
       op_id == BRK_ADMIN_CREATE_PARTITIONS ||
       op_id == BRK_ADMIN_DELETE_RECORDS))
    rd_kafka_AdminOptions_set_operation_timeout(opts, (int)t, errstr,
                                                sizeof(errstr));
  bool b = false;
  if (js_get_bool(js_obj_find(req_json, "validate_only"), &b) && b)
    rd_kafka_AdminOptions_set_validate_only(opts, 1, errstr, sizeof(errstr));
  if (js_get_bool(js_obj_find(req_json, "include_authorized_operations"),
                  &b)) {
    rd_kafka_error_t *e =
        rd_kafka_AdminOptions_set_include_authorized_operations(opts, b ? 1
                                                                        : 0);
    if (e != NULL) rd_kafka_error_destroy(e); /* best-effort */
  }
  if (js_get_bool(js_obj_find(req_json, "require_stable"), &b)) {
    rd_kafka_error_t *e =
        rd_kafka_AdminOptions_set_require_stable_offsets(opts, b ? 1 : 0);
    if (e != NULL) rd_kafka_error_destroy(e);
  }
  if (js_get_i64(js_obj_find(req_json, "isolation_level"), &t)) {
    rd_kafka_error_t *e = rd_kafka_AdminOptions_set_isolation_level(
        opts, t == 1 ? RD_KAFKA_ISOLATION_LEVEL_READ_COMMITTED
                     : RD_KAFKA_ISOLATION_LEVEL_READ_UNCOMMITTED);
    if (e != NULL) rd_kafka_error_destroy(e); /* best-effort */
  }

  int32_t ret = BRK_OK;

  switch (op_id) {
  case BRK_ADMIN_CREATE_TOPICS: {
    const char *topics = js_obj_find(req_json, "topics");
    int32_t cnt = js_arr_count(topics);
    if (cnt <= 0) {
      ret = BRK_ERR_DECODE;
      brk_set_err(h, ret, "create_topics: 'topics' is empty/invalid");
      break;
    }
    rd_kafka_NewTopic_t **arr = calloc((size_t)cnt, sizeof(*arr));
    if (arr == NULL) {
      ret = BRK_ERR_NOMEM;
      break;
    }
    const char *el = NULL;
    int32_t i = 0;
    while (ret == BRK_OK && js_arr_next(topics, &el)) {
      char *name = js_str_dup(js_obj_find(el, "name"));
      int64_t np = -1, rf = -1;
      js_get_i64(js_obj_find(el, "num_partitions"), &np);
      js_get_i64(js_obj_find(el, "replication_factor"), &rf);
      if (name == NULL) {
        ret = BRK_ERR_DECODE;
        brk_set_err(h, ret, "create_topics: missing 'name'");
        break;
      }
      arr[i] = rd_kafka_NewTopic_new(name, (int)np, (int)rf, errstr,
                                     sizeof(errstr));
      free(name);
      if (arr[i] == NULL) {
        ret = BRK_ERR_BAD_ARGS;
        brk_set_err(h, ret, "create_topics: %s", errstr);
        break;
      }
      const char *cfg = js_obj_find(el, "config");
      const char *pos = NULL;
      char *k = NULL;
      const char *v = NULL;
      while (js_obj_next(cfg, &pos, &k, &v)) {
        char *vs = js_str_dup(v);
        if (vs != NULL) rd_kafka_NewTopic_set_config(arr[i], k, vs);
        free(vs);
        free(k);
      }
      i++;
    }
    if (ret == BRK_OK)
      rd_kafka_CreateTopics(h->rk, arr, (size_t)cnt, opts, h->main_q);
    rd_kafka_NewTopic_destroy_array(arr, (size_t)i);
    free(arr);
    break;
  }
  case BRK_ADMIN_DELETE_TOPICS: {
    char **names = NULL;
    int32_t cnt = dup_str_array(js_obj_find(req_json, "topics"), &names);
    if (cnt <= 0) {
      ret = cnt < 0 ? cnt : BRK_ERR_DECODE;
      brk_set_err(h, ret, "delete_topics: 'topics' is empty/invalid");
      break;
    }
    rd_kafka_DeleteTopic_t **arr = calloc((size_t)cnt, sizeof(*arr));
    if (arr == NULL) {
      free_str_array(names, cnt);
      ret = BRK_ERR_NOMEM;
      break;
    }
    for (int32_t i = 0; i < cnt; i++)
      arr[i] = rd_kafka_DeleteTopic_new(names[i]);
    rd_kafka_DeleteTopics(h->rk, arr, (size_t)cnt, opts, h->main_q);
    rd_kafka_DeleteTopic_destroy_array(arr, (size_t)cnt);
    free(arr);
    free_str_array(names, cnt);
    break;
  }
  case BRK_ADMIN_CREATE_PARTITIONS: {
    const char *topics = js_obj_find(req_json, "topics");
    int32_t cnt = js_arr_count(topics);
    if (cnt <= 0) {
      ret = BRK_ERR_DECODE;
      brk_set_err(h, ret, "create_partitions: 'topics' is empty/invalid");
      break;
    }
    rd_kafka_NewPartitions_t **arr = calloc((size_t)cnt, sizeof(*arr));
    if (arr == NULL) {
      ret = BRK_ERR_NOMEM;
      break;
    }
    const char *el = NULL;
    int32_t i = 0;
    while (ret == BRK_OK && js_arr_next(topics, &el)) {
      char *name = js_str_dup(js_obj_find(el, "name"));
      int64_t total = 0;
      if (name == NULL ||
          !js_get_i64(js_obj_find(el, "total_count"), &total) || total <= 0) {
        free(name);
        ret = BRK_ERR_DECODE;
        brk_set_err(h, ret, "create_partitions: missing name/total_count");
        break;
      }
      arr[i] = rd_kafka_NewPartitions_new(name, (size_t)total, errstr,
                                          sizeof(errstr));
      free(name);
      if (arr[i] == NULL) {
        ret = BRK_ERR_BAD_ARGS;
        brk_set_err(h, ret, "create_partitions: %s", errstr);
        break;
      }
      i++;
    }
    if (ret == BRK_OK)
      rd_kafka_CreatePartitions(h->rk, arr, (size_t)cnt, opts, h->main_q);
    rd_kafka_NewPartitions_destroy_array(arr, (size_t)i);
    free(arr);
    break;
  }
  case BRK_ADMIN_LIST_GROUPS: {
    rd_kafka_ListConsumerGroups(h->rk, opts, h->main_q);
    break;
  }
  case BRK_ADMIN_DESCRIBE_GROUPS: {
    char **groups = NULL;
    int32_t cnt = dup_str_array(js_obj_find(req_json, "groups"), &groups);
    if (cnt <= 0) {
      ret = cnt < 0 ? cnt : BRK_ERR_DECODE;
      brk_set_err(h, ret, "describe_groups: 'groups' is empty/invalid");
      break;
    }
    rd_kafka_DescribeConsumerGroups(h->rk, (const char **)groups, (size_t)cnt,
                                    opts, h->main_q);
    free_str_array(groups, cnt);
    break;
  }
  case BRK_ADMIN_DELETE_GROUPS: {
    char **groups = NULL;
    int32_t cnt = dup_str_array(js_obj_find(req_json, "groups"), &groups);
    if (cnt <= 0) {
      ret = cnt < 0 ? cnt : BRK_ERR_DECODE;
      brk_set_err(h, ret, "delete_groups: 'groups' is empty/invalid");
      break;
    }
    rd_kafka_DeleteGroup_t **arr = calloc((size_t)cnt, sizeof(*arr));
    if (arr == NULL) {
      free_str_array(groups, cnt);
      ret = BRK_ERR_NOMEM;
      break;
    }
    for (int32_t i = 0; i < cnt; i++)
      arr[i] = rd_kafka_DeleteGroup_new(groups[i]);
    rd_kafka_DeleteGroups(h->rk, arr, (size_t)cnt, opts, h->main_q);
    rd_kafka_DeleteGroup_destroy_array(arr, (size_t)cnt);
    free(arr);
    free_str_array(groups, cnt);
    break;
  }
  case BRK_ADMIN_LIST_CONSUMER_GROUP_OFFSETS: {
    char *group = js_str_dup(js_obj_find(req_json, "group_id"));
    if (group == NULL) {
      ret = BRK_ERR_DECODE;
      brk_set_err(h, ret, "list_consumer_group_offsets: missing 'group_id'");
      break;
    }
    const char *parts = js_obj_find(req_json, "partitions");
    rd_kafka_topic_partition_list_t *tpl = NULL;
    if (!js_is_null(parts)) {
      tpl = tpl_from_json(parts);
      if (tpl == NULL) {
        free(group);
        ret = BRK_ERR_DECODE;
        brk_set_err(h, ret, "list_consumer_group_offsets: bad 'partitions'");
        break;
      }
    }
    rd_kafka_ListConsumerGroupOffsets_t *req =
        rd_kafka_ListConsumerGroupOffsets_new(group, tpl); /* copies both */
    free(group);
    if (tpl != NULL) rd_kafka_topic_partition_list_destroy(tpl);
    rd_kafka_ListConsumerGroupOffsets(h->rk, &req, 1, opts, h->main_q);
    rd_kafka_ListConsumerGroupOffsets_destroy(req);
    break;
  }
  case BRK_ADMIN_DELETE_RECORDS: {
    rd_kafka_topic_partition_list_t *tpl =
        tpl_from_json(js_obj_find(req_json, "partitions"));
    if (tpl == NULL || tpl->cnt == 0) {
      if (tpl != NULL) rd_kafka_topic_partition_list_destroy(tpl);
      ret = BRK_ERR_DECODE;
      brk_set_err(h, ret, "delete_records: 'partitions' is empty/invalid");
      break;
    }
    rd_kafka_DeleteRecords_t *dr = rd_kafka_DeleteRecords_new(tpl); /* copy */
    rd_kafka_topic_partition_list_destroy(tpl);
    rd_kafka_DeleteRecords(h->rk, &dr, 1, opts, h->main_q);
    rd_kafka_DeleteRecords_destroy(dr);
    break;
  }
  case BRK_ADMIN_DESCRIBE_TOPICS: {
    char **names = NULL;
    int32_t cnt = dup_str_array(js_obj_find(req_json, "topics"), &names);
    if (cnt <= 0) {
      ret = cnt < 0 ? cnt : BRK_ERR_DECODE;
      brk_set_err(h, ret, "describe_topics: 'topics' is empty/invalid");
      break;
    }
    rd_kafka_TopicCollection_t *coll =
        rd_kafka_TopicCollection_of_topic_names((const char **)names,
                                                (size_t)cnt);
    free_str_array(names, cnt);
    if (coll == NULL) {
      ret = BRK_ERR_NOMEM;
      break;
    }
    rd_kafka_DescribeTopics(h->rk, coll, opts, h->main_q);
    rd_kafka_TopicCollection_destroy(coll);
    break;
  }
  case BRK_ADMIN_LIST_OFFSETS: {
    /* offset in the tpl = offset_spec (rd_kafka_OffsetSpec_t or a timestamp). */
    const char *parts = js_obj_find(req_json, "partitions");
    int32_t cnt = js_arr_count(parts);
    if (cnt <= 0) {
      ret = BRK_ERR_DECODE;
      brk_set_err(h, ret, "list_offsets: 'partitions' is empty/invalid");
      break;
    }
    rd_kafka_topic_partition_list_t *tpl =
        rd_kafka_topic_partition_list_new(cnt);
    const char *el = NULL;
    while (ret == BRK_OK && js_arr_next(parts, &el)) {
      char *topic = js_str_dup(js_obj_find(el, "topic"));
      int64_t partition = -1, spec = RD_KAFKA_OFFSET_SPEC_LATEST;
      if (topic == NULL ||
          !js_get_i64(js_obj_find(el, "partition"), &partition)) {
        free(topic);
        ret = BRK_ERR_DECODE;
        brk_set_err(h, ret, "list_offsets: missing topic/partition");
        break;
      }
      js_get_i64(js_obj_find(el, "offset_spec"), &spec);
      rd_kafka_topic_partition_t *p =
          rd_kafka_topic_partition_list_add(tpl, topic, (int32_t)partition);
      p->offset = spec;
      free(topic);
    }
    if (ret == BRK_OK) rd_kafka_ListOffsets(h->rk, tpl, opts, h->main_q);
    rd_kafka_topic_partition_list_destroy(tpl);
    break;
  }
  default:
    ret = BRK_ERR_BAD_ARGS;
    break;
  }

  rd_kafka_AdminOptions_destroy(opts);
  return ret;
}

/* ==========================================================================
 * SASL credentials
 * ========================================================================== */

BRK_EXPORT int32_t brk_sasl_set_credentials(void *hv, const char *username,
                                            const char *password) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (username == NULL || password == NULL) return BRK_ERR_BAD_ARGS;
  rd_kafka_error_t *e = rd_kafka_sasl_set_credentials(h->rk, username, password);
  if (e != NULL) {
    int32_t ret = BRK_KAFKA_ERR(rd_kafka_error_code(e));
    brk_set_err(h, ret, "sasl_set_credentials: %s", rd_kafka_error_string(e));
    rd_kafka_error_destroy(e);
    return ret;
  }
  return BRK_OK;
}

/* ==========================================================================
 * OAUTHBEARER
 * ========================================================================== */

BRK_EXPORT int32_t brk_oauthbearer_set_token(void *hv, const char *token,
                                             int64_t lifetime_ms,
                                             const char *principal,
                                             const uint8_t *extensions_buf,
                                             int32_t extensions_len,
                                             char *errstr,
                                             int32_t errstr_size) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  if (token == NULL || principal == NULL) return BRK_ERR_BAD_ARGS;

  /* extensions: STRING LIST (format 1) laid out as [k1, v1, k2, v2, ...] —
   * copied into NUL-terminated strings (the packed buffer carries no NULs). */
  char **exts = NULL;
  uint32_t ext_cnt = 0;
  if (extensions_buf != NULL && extensions_len >= 4) {
    brk_rbuf r = {extensions_buf, extensions_len, 0};
    if (!rb_u32(&r, &ext_cnt) || ext_cnt % 2 != 0 || ext_cnt > 512)
      return BRK_ERR_DECODE;
    if (ext_cnt > 0) {
      exts = calloc(ext_cnt, sizeof(char *));
      if (exts == NULL) return BRK_ERR_NOMEM;
      for (uint32_t i = 0; i < ext_cnt; i++) {
        uint16_t len;
        const uint8_t *p;
        if (!rb_u16(&r, &len) || (p = rb_bytes(&r, len)) == NULL) {
          free_str_array(exts, (int32_t)i);
          return BRK_ERR_DECODE;
        }
        exts[i] = malloc((size_t)len + 1);
        if (exts[i] == NULL) {
          free_str_array(exts, (int32_t)i);
          return BRK_ERR_NOMEM;
        }
        memcpy(exts[i], p, len);
        exts[i][len] = '\0';
      }
    }
  }

  rd_kafka_resp_err_t err = rd_kafka_oauthbearer_set_token(
      h->rk, token, lifetime_ms, principal, (const char **)exts,
      (size_t)ext_cnt, errstr != NULL ? errstr : (char[1]){0},
      errstr != NULL ? (size_t)errstr_size : 0);
  free_str_array(exts, (int32_t)ext_cnt);
  if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
    brk_set_err(h, BRK_KAFKA_ERR(err), "oauthbearer_set_token: %s",
                rd_kafka_err2str(err));
    return BRK_KAFKA_ERR(err);
  }
  return BRK_OK;
}

BRK_EXPORT int32_t brk_oauthbearer_set_token_failure(void *hv,
                                                     const char *errstr) {
  brk_handle *h = brk_check(hv);
  if (h == NULL) return BRK_ERR_INVALID_HANDLE;
  rd_kafka_resp_err_t err = rd_kafka_oauthbearer_set_token_failure(
      h->rk, errstr != NULL ? errstr : "token refresh failed");
  return err == RD_KAFKA_RESP_ERR_NO_ERROR ? BRK_OK : BRK_KAFKA_ERR(err);
}
