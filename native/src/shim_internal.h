/* Shim internals — not public ABI. The ABI lives in include/bunrdkafka.h */
#ifndef BRK_SHIM_INTERNAL_H
#define BRK_SHIM_INTERNAL_H

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Header layout differs by provider: vcpkg installs <librdkafka/rdkafka.h>
 * (Windows CI, BRK_USE_SYSTEM_RDKAFKA), while the FetchContent build adds
 * librdkafka's src/ dir where the header sits flat as <rdkafka.h>. */
#if defined(__has_include)
#if __has_include(<librdkafka/rdkafka.h>)
#include <librdkafka/rdkafka.h>
#else
#include "rdkafka.h"
#endif
#else
#include "rdkafka.h"
#endif
#include "bunrdkafka.h"

#if defined(_WIN32)
#include <windows.h>
typedef CRITICAL_SECTION brk_mutex_t;
#define brk_mutex_init(m) InitializeCriticalSection(m)
#define brk_mutex_destroy(m) DeleteCriticalSection(m)
#define brk_mutex_lock(m) EnterCriticalSection(m)
#define brk_mutex_unlock(m) LeaveCriticalSection(m)
typedef CONDITION_VARIABLE brk_cond_t;
#define brk_cond_init(c) InitializeConditionVariable(c)
#define brk_cond_destroy(c) ((void)0)
#define brk_cond_wait(c, m) SleepConditionVariableCS((c), (m), INFINITE)
#define brk_cond_signal(c) WakeConditionVariable(c)
#define brk_cond_broadcast(c) WakeAllConditionVariable(c)
typedef HANDLE brk_thread_t;
#else
#include <pthread.h>
typedef pthread_mutex_t brk_mutex_t;
#define brk_mutex_init(m) pthread_mutex_init((m), NULL)
#define brk_mutex_destroy(m) pthread_mutex_destroy(m)
#define brk_mutex_lock(m) pthread_mutex_lock(m)
#define brk_mutex_unlock(m) pthread_mutex_unlock(m)
typedef pthread_cond_t brk_cond_t;
#define brk_cond_init(c) pthread_cond_init((c), NULL)
#define brk_cond_destroy(c) pthread_cond_destroy(c)
#define brk_cond_wait(c, m) pthread_cond_wait((c), (m))
#define brk_cond_signal(c) pthread_cond_signal(c)
#define brk_cond_broadcast(c) pthread_cond_broadcast(c)
typedef pthread_t brk_thread_t;
#endif
/* Starts a thread running fn(arg); returns 0 on success. Defined in shim_common.c */
int brk_thread_start(brk_thread_t *t, void (*fn)(void *), void *arg);
void brk_thread_join(brk_thread_t *t);

#define BRK_HANDLE_MAGIC 0xB0BAF00Du

/* A pre-serialized frame (exact EVENT FRAME format 5) waiting to be picked up
 * by brk_events_poll. */
#ifdef _MSC_VER
/* C4200: MSVC flags C99/C11 flexible array members as a "nonstandard
 * extension"; the layout is well-defined and portable, so silence it. */
#pragma warning(push)
#pragma warning(disable : 4200)
#endif
typedef struct brk_frame {
  struct brk_frame *next;
  int32_t len; /* total bytes including the frame header */
  uint8_t data[];
} brk_frame;
#ifdef _MSC_VER
#pragma warning(pop)
#endif

/* Topic name → id intern table (id = index, stable for the handle's
 * lifetime). */
typedef struct {
  char **names;
  int32_t count;
  int32_t cap;
} brk_topic_tab;

/* One pre-serialized MESSAGE BATCH (format 4) produced by the prefetch thread. */
typedef struct {
  uint8_t *buf;
  int32_t cap;
  int32_t len;   /* bytes used */
  int32_t count; /* messages in the frame */
} brk_pf_frame;

/* Consumer prefetch thread state (opt-in via brk_consume_prefetch_start).
 * SPSC ring: the thread fills frames[tail], JS takes frames[head]. */
typedef struct {
  bool enabled;
  bool running;      /* cleared by stop; read under mu */
  brk_thread_t thread;
  brk_mutex_t mu;
  brk_cond_t cv;     /* signaled when a frame becomes ready or free */
  brk_pf_frame *frames;
  int32_t nframes;
  int32_t max_msgs;
  int32_t head, tail, ready;
  int64_t frames_filled; /* stats */
} brk_prefetch;

typedef struct brk_handle {
  uint32_t magic;
  int32_t type; /* BRK_CLIENT_* */
  rd_kafka_t *rk;
  rd_kafka_queue_t *main_q;     /* log/stats/error/DR/oauth (all clients) */
  rd_kafka_queue_t *consumer_q; /* consumer: fetch + rebalance + offset_commit */
  rd_kafka_event_t *cur_fetch;  /* FETCH event partially consumed (out buffer filled) */
  uint8_t *pending_msg;         /* serialized message that did not fit the out-buf */
  int32_t pending_msg_len;      /* (message_next cannot rewind, so keep it here) */
  brk_frame *stash_head, *stash_tail; /* events seen on the consume path, waiting for events_poll */
  brk_mutex_t mu;               /* guards stash + last_error + topics */
  int32_t last_err;
  int32_t last_required;
  char last_err_str[512];
  brk_topic_tab topics;
  /* reusable scratch used to serialize an event before knowing whether it fits
   * the out-buf */
  uint8_t *scratch;
  int32_t scratch_cap;
  /* consume-path scratch: separate from `scratch` so the prefetch thread
   * (consume path) never races the JS thread (events path). */
  uint8_t *cscratch;
  int32_t cscratch_cap;
  brk_prefetch pf;
  /* Reused 1-element list for brk_offset_store_single (JS thread only). */
  rd_kafka_topic_partition_list_t *store_tpl;
} brk_handle;

/* ---- shared helpers (defined in shim_common.c) ---------------------------- */
brk_handle *brk_check(void *h);              /* NULL if the handle is invalid */
void brk_set_err(brk_handle *h, int32_t code, const char *fmt, ...);
int32_t brk_intern_topic(brk_handle *h, const char *name); /* takes mu        */
void brk_stash_push(brk_handle *h, const uint8_t *frame, int32_t len); /* takes mu */
brk_frame *brk_stash_pop(brk_handle *h);     /* takes mu; caller free()s      */
uint8_t *brk_scratch_reserve(brk_handle *h, int32_t need); /* grows scratch   */
uint8_t *brk_buf_reserve(uint8_t **buf, int32_t *cap, int32_t need); /* generic grow */
/* Stops + frees the prefetch thread/ring if started (idempotent). */
void brk_consume_prefetch_teardown(brk_handle *h);

/* Serializes ONE event into an EVENT FRAME (format 5) in the handle's scratch.
 * Returns the frame's byte count, 0 = event type is not forwarded (skip),
 * negative = error. Defined in shim_events.c. */
int32_t brk_serialize_event(brk_handle *h, rd_kafka_event_t *ev);
/* Same, but serializes into an arbitrary growable buffer (used by the
 * consume path, which may run on the prefetch thread). */
int32_t brk_serialize_event_into(brk_handle *h, rd_kafka_event_t *ev,
                                 uint8_t **buf, int32_t *cap);

/* ---- admin (shim_admin.c) ------------------------------------------------- */
/* Whether the event type is one of the Admin API's *_RESULT types. */
bool brk_event_is_admin_result(int32_t evtype);
/* Builds the result JSON (malloc'd, caller frees) from an admin result event.
 * Returns the JSON length or a negative error. Schema: comment at the top of
 * shim_admin.c. */
int32_t brk_admin_result_json(brk_handle *h, rd_kafka_event_t *ev, char **out);

/* ---- little-endian writer/reader, no padding ------------------------------ */
typedef struct {
  uint8_t *p;
  int32_t cap;
  int32_t off;
} brk_wbuf;

static inline bool wb_has(brk_wbuf *w, int32_t n) { return w->off + n <= w->cap; }
static inline void wb_u8(brk_wbuf *w, uint8_t v) { w->p[w->off++] = v; }
static inline void wb_raw(brk_wbuf *w, const void *src, int32_t n) {
  if (n > 0) memcpy(w->p + w->off, src, (size_t)n);
  w->off += n;
}
static inline void wb_u16(brk_wbuf *w, uint16_t v) { wb_raw(w, &v, 2); }
static inline void wb_i16(brk_wbuf *w, int16_t v) { wb_raw(w, &v, 2); }
static inline void wb_u32(brk_wbuf *w, uint32_t v) { wb_raw(w, &v, 4); }
static inline void wb_i32(brk_wbuf *w, int32_t v) { wb_raw(w, &v, 4); }
static inline void wb_u64(brk_wbuf *w, uint64_t v) { wb_raw(w, &v, 8); }
static inline void wb_i64(brk_wbuf *w, int64_t v) { wb_raw(w, &v, 8); }

typedef struct {
  const uint8_t *p;
  int32_t len;
  int32_t off;
} brk_rbuf;

static inline bool rb_has(brk_rbuf *r, int32_t n) { return r->off + n <= r->len; }
static inline bool rb_u8(brk_rbuf *r, uint8_t *v) {
  if (!rb_has(r, 1)) return false;
  *v = r->p[r->off++];
  return true;
}
static inline bool rb_raw(brk_rbuf *r, void *dst, int32_t n) {
  if (!rb_has(r, n)) return false;
  memcpy(dst, r->p + r->off, (size_t)n);
  r->off += n;
  return true;
}
static inline bool rb_u16(brk_rbuf *r, uint16_t *v) { return rb_raw(r, v, 2); }
static inline bool rb_i32(brk_rbuf *r, int32_t *v) { return rb_raw(r, v, 4); }
static inline bool rb_u32(brk_rbuf *r, uint32_t *v) { return rb_raw(r, v, 4); }
static inline bool rb_i64(brk_rbuf *r, int64_t *v) { return rb_raw(r, v, 8); }
static inline bool rb_u64(brk_rbuf *r, uint64_t *v) { return rb_raw(r, v, 8); }
/* Returns a pointer to n bytes inside the buffer (no copy); NULL if short. */
static inline const uint8_t *rb_bytes(brk_rbuf *r, int32_t n) {
  if (n < 0 || !rb_has(r, n)) return NULL;
  const uint8_t *q = r->p + r->off;
  r->off += n;
  return q;
}

/* Read/write TOPIC-PARTITION LIST (format 2). Defined in shim_consumer.c but
 * shared (transactions, offset_commit event). */
rd_kafka_topic_partition_list_t *brk_tpl_decode(brk_handle *h,
                                                const uint8_t *buf,
                                                int32_t len);
/* Returns bytes written, or negative (BUFFER_TOO_SMALL). */
int32_t brk_tpl_encode(brk_handle *h, const rd_kafka_topic_partition_list_t *tpl,
                       brk_wbuf *w);

#endif /* BRK_SHIM_INTERNAL_H */
