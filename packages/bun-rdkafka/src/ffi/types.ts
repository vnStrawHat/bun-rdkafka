/**
 * ffi/types.ts — ABI constants mirroring `native/include/bunrdkafka.h` 1:1.
 *
 * SOURCE OF TRUTH: `native/include/bunrdkafka.h`. Every header change MUST be
 * reflected here along with a {@link BRK_ABI_VERSION} bump (design §3.2).
 *
 * This file does NOT import `bun:ffi` — which lets the decoder/unit tests use
 * it without the native library.
 */

/* ========================================================================== */
/* Version                                                                     */
/* ========================================================================== */

/** `#define BRK_ABI_VERSION 1` */
export const BRK_ABI_VERSION = 1;

/* ========================================================================== */
/* Shim-layer error codes (-1 .. -99)                                          */
/* ========================================================================== */

export const BRK_OK = 0;
/** out-buffer cannot fit even one record (see `brk_last_required_size`). */
export const BRK_ERR_BUFFER_TOO_SMALL = -1;
/** handle is NULL or already destroyed. */
export const BRK_ERR_INVALID_HANDLE = -2;
/** wrong client type / wrong state. */
export const BRK_ERR_INVALID_STATE = -3;
/** invalid argument / packed buffer. */
export const BRK_ERR_BAD_ARGS = -4;
/** packed buffer from JS failed to parse. */
export const BRK_ERR_DECODE = -5;
/** topic_id not present in the intern table. */
export const BRK_ERR_UNKNOWN_TOPIC_ID = -6;
export const BRK_ERR_NOMEM = -7;
export const BRK_ERR_UNSUPPORTED = -8;

/** Bounds of the shim error range (inclusive): `[-99, -1]`. */
export const BRK_SHIM_ERR_MIN = -99;
export const BRK_SHIM_ERR_MAX = -1;

/** Readable names for shim error codes, used to build messages. */
export const BRK_ERR_NAMES: Readonly<Record<number, string>> = Object.freeze({
  [BRK_ERR_BUFFER_TOO_SMALL]: "BRK_ERR_BUFFER_TOO_SMALL",
  [BRK_ERR_INVALID_HANDLE]: "BRK_ERR_INVALID_HANDLE",
  [BRK_ERR_INVALID_STATE]: "BRK_ERR_INVALID_STATE",
  [BRK_ERR_BAD_ARGS]: "BRK_ERR_BAD_ARGS",
  [BRK_ERR_DECODE]: "BRK_ERR_DECODE",
  [BRK_ERR_UNKNOWN_TOPIC_ID]: "BRK_ERR_UNKNOWN_TOPIC_ID",
  [BRK_ERR_NOMEM]: "BRK_ERR_NOMEM",
  [BRK_ERR_UNSUPPORTED]: "BRK_ERR_UNSUPPORTED",
});

/* ========================================================================== */
/* librdkafka error codes embedded in return values                            */
/* ========================================================================== */

/** `#define BRK_ERR_KAFKA_OFFSET (-1000)` */
export const BRK_ERR_KAFKA_OFFSET = -1000;

/** Lower/upper bounds of the kafka-error range per `BRK_IS_KAFKA_ERR` in the header. */
export const BRK_KAFKA_ERR_RET_MIN = -1300;
export const BRK_KAFKA_ERR_RET_MAX = -800;

/** `BRK_KAFKA_ERR(e)` — embeds an `rd_kafka_resp_err_t` into a return value. */
export function brkKafkaErr(err: number): number {
  return BRK_ERR_KAFKA_OFFSET + (err | 0);
}

/** `BRK_IS_KAFKA_ERR(ret)` — does ret carry a librdkafka error code? */
export function isKafkaErr(ret: number): boolean {
  return ret <= BRK_KAFKA_ERR_RET_MAX && ret >= BRK_KAFKA_ERR_RET_MIN;
}

/** `BRK_KAFKA_ERR_OF(ret)` — extracts the `rd_kafka_resp_err_t` from a return value. */
export function kafkaErrOf(ret: number): number {
  return (ret | 0) - BRK_ERR_KAFKA_OFFSET;
}

/** Is ret within the shim-layer error range (`-99 .. -1`)? */
export function isShimErr(ret: number): boolean {
  return ret <= BRK_SHIM_ERR_MAX && ret >= BRK_SHIM_ERR_MIN;
}

/** Any negative return value is an error (`>= 0` is success). */
export function isErrRet(ret: number): boolean {
  return ret < 0;
}

/* ========================================================================== */
/* Enum                                                                        */
/* ========================================================================== */

/** Client type for `brk_client_new`. */
export const BRK_CLIENT_PRODUCER = 0;
export const BRK_CLIENT_CONSUMER = 1;
export type BrkClientType = typeof BRK_CLIENT_PRODUCER | typeof BRK_CLIENT_CONSUMER;

/** Event types in the event frame (`brk_events_poll`). */
export const BRK_EVENT_DR = 1;
export const BRK_EVENT_ERROR = 2;
export const BRK_EVENT_LOG = 3;
export const BRK_EVENT_STATS = 4;
export const BRK_EVENT_REBALANCE = 5;
export const BRK_EVENT_OFFSET_COMMIT = 6;
export const BRK_EVENT_THROTTLE = 7;
export const BRK_EVENT_OAUTH_REFRESH = 8;
export const BRK_EVENT_ADMIN_RESULT = 9;

export type BrkEventType =
  | typeof BRK_EVENT_DR
  | typeof BRK_EVENT_ERROR
  | typeof BRK_EVENT_LOG
  | typeof BRK_EVENT_STATS
  | typeof BRK_EVENT_REBALANCE
  | typeof BRK_EVENT_OFFSET_COMMIT
  | typeof BRK_EVENT_THROTTLE
  | typeof BRK_EVENT_OAUTH_REFRESH
  | typeof BRK_EVENT_ADMIN_RESULT;

/** Modes for `brk_assign`. */
export const BRK_ASSIGN = 0;
export const BRK_ASSIGN_INCREMENTAL = 1;
export const BRK_UNASSIGN_INCREMENTAL = 2;
export const BRK_UNASSIGN = 3;
export type BrkAssignMode =
  | typeof BRK_ASSIGN
  | typeof BRK_ASSIGN_INCREMENTAL
  | typeof BRK_UNASSIGN_INCREMENTAL
  | typeof BRK_UNASSIGN;

/** Protocol in the REBALANCE payload. */
export const BRK_REBALANCE_PROTOCOL_EAGER = 0;
export const BRK_REBALANCE_PROTOCOL_COOPERATIVE = 1;
export type BrkRebalanceProtocol =
  | typeof BRK_REBALANCE_PROTOCOL_EAGER
  | typeof BRK_REBALANCE_PROTOCOL_COOPERATIVE;

/** Protocol names as librdkafka spells them (`rd_kafka_rebalance_protocol`). */
export const BRK_REBALANCE_PROTOCOL_NAMES = Object.freeze({
  [BRK_REBALANCE_PROTOCOL_EAGER]: "EAGER",
  [BRK_REBALANCE_PROTOCOL_COOPERATIVE]: "COOPERATIVE",
} as const);

/** op_id values for `brk_admin_request`. */
export const BRK_ADMIN_CREATE_TOPICS = 1;
export const BRK_ADMIN_DELETE_TOPICS = 2;
export const BRK_ADMIN_CREATE_PARTITIONS = 3;
export const BRK_ADMIN_LIST_GROUPS = 4;
export const BRK_ADMIN_DESCRIBE_GROUPS = 5;
export const BRK_ADMIN_DELETE_GROUPS = 6;
export const BRK_ADMIN_LIST_CONSUMER_GROUP_OFFSETS = 7;
export const BRK_ADMIN_DELETE_RECORDS = 8;
export const BRK_ADMIN_DESCRIBE_TOPICS = 9;
export const BRK_ADMIN_LIST_OFFSETS = 10;

/** `rd_kafka_OffsetSpec_t` — `offset_spec` values for the LIST_OFFSETS op. */
export const RD_KAFKA_OFFSET_SPEC_MAX_TIMESTAMP = -3;
export const RD_KAFKA_OFFSET_SPEC_EARLIEST = -2;
export const RD_KAFKA_OFFSET_SPEC_LATEST = -1;

export type BrkAdminOp =
  | typeof BRK_ADMIN_CREATE_TOPICS
  | typeof BRK_ADMIN_DELETE_TOPICS
  | typeof BRK_ADMIN_CREATE_PARTITIONS
  | typeof BRK_ADMIN_LIST_GROUPS
  | typeof BRK_ADMIN_DESCRIBE_GROUPS
  | typeof BRK_ADMIN_DELETE_GROUPS
  | typeof BRK_ADMIN_LIST_CONSUMER_GROUP_OFFSETS
  | typeof BRK_ADMIN_DELETE_RECORDS
  | typeof BRK_ADMIN_DESCRIBE_TOPICS
  | typeof BRK_ADMIN_LIST_OFFSETS;

/* ========================================================================== */
/* librdkafka constants that accompany the packed formats                      */
/* (rdkafka.h, tag v2.15.0 — matches `librdkafka.version`)                     */
/* ========================================================================== */

/** `RD_KAFKA_PARTITION_UA` — let librdkafka's partitioner pick the partition. */
export const RD_KAFKA_PARTITION_UA = -1;

/** `RD_KAFKA_OFFSET_BEGINNING` */
export const RD_KAFKA_OFFSET_BEGINNING = -2;
/** `RD_KAFKA_OFFSET_END` */
export const RD_KAFKA_OFFSET_END = -1;
/** `RD_KAFKA_OFFSET_STORED` */
export const RD_KAFKA_OFFSET_STORED = -1000;
/** `RD_KAFKA_OFFSET_INVALID` */
export const RD_KAFKA_OFFSET_INVALID = -1001;
/** `RD_KAFKA_OFFSET_TAIL_BASE` (librdkafka internal). */
export const RD_KAFKA_OFFSET_TAIL_BASE = -2000;
/** `RD_KAFKA_OFFSET_TAIL(CNT)` — start reading CNT messages before the end offset. */
export function rdKafkaOffsetTail(cnt: number): number {
  return RD_KAFKA_OFFSET_TAIL_BASE - cnt;
}

/** `rd_kafka_timestamp_type_t` */
export const RD_KAFKA_TIMESTAMP_NOT_AVAILABLE = 0;
export const RD_KAFKA_TIMESTAMP_CREATE_TIME = 1;
export const RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME = 2;
export type RdKafkaTimestampType =
  | typeof RD_KAFKA_TIMESTAMP_NOT_AVAILABLE
  | typeof RD_KAFKA_TIMESTAMP_CREATE_TIME
  | typeof RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME;

/** No leader epoch (used for both message batches and tpls). */
export const NO_LEADER_EPOCH = -1;
/** `len == -1` in packed formats = NULL (distinct from an empty len-0 string). */
export const NULL_LEN = -1;
