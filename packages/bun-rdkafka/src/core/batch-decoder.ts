/**
 * core/batch-decoder.ts — codec for every packed binary format of
 * `native/include/bunrdkafka.h` (design §5.3).
 *
 * | # | Format          | Direction | Function |
 * |---|-----------------|-------|-----|
 * | 1 | STRING LIST     | JS→C / C→JS | {@link encodeStringList} / {@link decodeStringList} |
 * | 2 | TOPIC-PARTITION LIST | both | {@link encodeTpl} / {@link decodeTpl} |
 * | 3 | PRODUCE BATCH   | JS→C  | {@link encodeProduceBatch} |
 * | 4 | MESSAGE BATCH   | C→JS  | {@link decodeMessageBatch} |
 * | 5 | EVENT FRAME     | C→JS  | {@link decodeEventFrames} |
 *
 * MESSAGE BATCH decoding has two modes: `copy: true` copies key/value/headers
 * out of the buffer (as Buffers); `copy: false` hands out views. The consume
 * path uses views over a buffer that is retired after every batch (a message
 * therefore lives independently of the next poll — ADR-6 — without a
 * per-message copy); see `NativeClient.consumeBatch`.
 */

import { Buffer } from "node:buffer";
import {
  BRK_EVENT_ADMIN_RESULT,
  BRK_EVENT_DR,
  BRK_EVENT_ERROR,
  BRK_EVENT_LOG,
  BRK_EVENT_OAUTH_REFRESH,
  BRK_EVENT_OFFSET_COMMIT,
  BRK_EVENT_REBALANCE,
  BRK_EVENT_STATS,
  BRK_EVENT_THROTTLE,
  NO_LEADER_EPOCH,
  RD_KAFKA_OFFSET_INVALID,
  RD_KAFKA_PARTITION_UA,
  type BrkRebalanceProtocol,
  type RdKafkaTimestampType,
} from "../ffi/types.ts";
import { ptr } from "bun:ffi";

import {
  BinaryDecodeError,
  BinaryEncodeError,
  BufReader,
  BufWriter,
  NULL_LENGTH,
} from "./binary.ts";

/* ========================================================================== */
/* Topic name interning                                                        */
/* ========================================================================== */

/** Fetches a topic name from native (`brk_topic_name`) on cache miss. */
export type TopicNameFetcher = (topicId: number) => string;

/**
 * `topic_id → name` cache for a handle's lifetime (design §5.3). Topic names
 * are never repeatedly copied on the hot path.
 */
export class TopicNameTable {
  private readonly names = new Map<number, string>();
  private readonly fetcher: TopicNameFetcher | undefined;

  constructor(fetcher?: TopicNameFetcher) {
    this.fetcher = fetcher;
  }

  get size(): number {
    return this.names.size;
  }

  get(topicId: number): string | undefined {
    return this.names.get(topicId);
  }

  has(topicId: number): boolean {
    return this.names.has(topicId);
  }

  set(topicId: number, name: string): void {
    if (topicId >= 0) this.names.set(topicId, name);
  }

  clear(): void {
    this.names.clear();
  }

  /**
   * Looks up a topic name; on a miss, calls `fetch` (the parameter or the
   * constructor's fetcher) exactly once and caches the result.
   */
  resolve(topicId: number, fetch?: TopicNameFetcher): string {
    const cachedName = this.names.get(topicId);
    if (cachedName !== undefined) return cachedName;
    const fn = fetch ?? this.fetcher;
    if (!fn) {
      throw new BinaryDecodeError(
        `topic_id ${topicId} is not in the TopicNameTable and no fetch function was given`,
        0,
      );
    }
    const name = fn(topicId);
    this.names.set(topicId, name);
    return name;
  }
}

/* ========================================================================== */
/* Format 1 — STRING LIST                                                      */
/* ========================================================================== */

/** `u32 count, count × { u16 len, bytes }`. */
export function encodeStringList(items: readonly string[], writer?: BufWriter): BufWriter {
  const w = writer ?? new BufWriter(256);
  w.u32(items.length);
  for (const item of items) w.stringU16(item);
  return w;
}

/** Decodes a STRING LIST (used by `brk_subscription`). */
export function decodeStringList(buf: Uint8Array): string[] {
  const r = new BufReader(buf);
  const count = r.u32();
  const out: string[] = new Array<string>(count);
  for (let i = 0; i < count; i++) out[i] = r.stringU16();
  return out;
}

/* ========================================================================== */
/* Format 2 — TOPIC-PARTITION LIST                                             */
/* ========================================================================== */

/** One tpl element when JS sends to C. */
export interface TopicPartitionInput {
  /** Required when `topicId` is absent. */
  topic?: string;
  /** Interned id (preferred when >= 0); omit when the topic has never been seen. */
  topicId?: number;
  partition: number;
  /** Defaults to `RD_KAFKA_OFFSET_INVALID` (-1001). */
  offset?: number | bigint;
  /** Defaults to -1 (absent). */
  leaderEpoch?: number;
  /** Commit metadata (UTF-8, ≤ ~4KB per broker limits). `null`/omitted = absent. */
  metadata?: string | null;
}

/** One tpl element when C returns to JS. */
export interface TopicPartitionEntry {
  topic: string;
  /** -1 when C sent it by name instead of id. */
  topicId: number;
  partition: number;
  offset: number;
  /** -1 if absent. */
  leaderEpoch: number;
  /** Commit metadata; `null` when absent (empty metadata ≠ absent). */
  metadata: string | null;
}

export interface TplDecodeOptions {
  /** Intern table for `topic_id` lookups; required when C sends entries by id. */
  topics?: TopicNameTable | undefined;
  /** Fetch function on cache miss (usually `brk_topic_name`). */
  fetchTopicName?: TopicNameFetcher | undefined;
}

const TPL_TEXT_ENCODER = /* @__PURE__ */ new TextEncoder();
const MAX_I16 = 0x7fff;

/**
 * `u32 count, count × { i32 topic_id, u16 name_len, bytes name, i32 partition,
 *  i64 offset, i32 leader_epoch, i16 metadata_len, bytes metadata }`
 *
 * Uses `topicId` when available (name_len = 0), otherwise writes -1 + the
 * topic name. `metadata_len == -1` = no metadata (distinct from an empty len-0
 * string).
 */
export function encodeTpl(
  entries: readonly TopicPartitionInput[],
  writer?: BufWriter,
): BufWriter {
  const w = writer ?? new BufWriter(256);
  w.u32(entries.length);
  for (const e of entries) {
    const hasId = e.topicId !== undefined && e.topicId >= 0;
    if (!hasId && (e.topic === undefined || e.topic === "")) {
      throw new BinaryEncodeError(
        `tpl entry needs "topic" or "topicId" (partition ${e.partition})`,
      );
    }
    w.i32(hasId ? (e.topicId as number) : -1);
    if (hasId) w.u16(0);
    else w.stringU16(e.topic as string);
    w.i32(e.partition);
    w.i64(e.offset ?? RD_KAFKA_OFFSET_INVALID);
    w.i32(e.leaderEpoch ?? NO_LEADER_EPOCH);
    if (e.metadata === undefined || e.metadata === null) {
      w.i16(-1);
    } else {
      const bytes = TPL_TEXT_ENCODER.encode(e.metadata);
      if (bytes.length > MAX_I16) {
        throw new BinaryEncodeError(
          `commit metadata of ${bytes.length} bytes exceeds the i16 length prefix limit`,
        );
      }
      w.i16(bytes.length);
      w.bytes(bytes);
    }
  }
  return w;
}

/** Decodes a tpl from a reader positioned at the start of the tpl region. */
export function decodeTpl(r: BufReader, opts: TplDecodeOptions = {}): TopicPartitionEntry[] {
  const count = r.u32();
  const out: TopicPartitionEntry[] = new Array<TopicPartitionEntry>(count);
  for (let i = 0; i < count; i++) {
    const topicId = r.i32();
    const nameLen = r.u16();
    let topic: string;
    if (nameLen > 0) {
      topic = r.utf8(nameLen);
      opts.topics?.set(topicId, topic);
    } else {
      if (!opts.topics) {
        throw new BinaryDecodeError(
          `tpl entry uses topic_id ${topicId} but no TopicNameTable was given`,
          r.offset,
        );
      }
      topic = opts.topics.resolve(topicId, opts.fetchTopicName);
    }
    const partition = r.i32();
    const offset = r.i64Number();
    const leaderEpoch = r.i32();
    const metadataLen = r.i16();
    const metadata = metadataLen < 0 ? null : r.utf8(metadataLen);
    out[i] = { topic, topicId, partition, offset, leaderEpoch, metadata };
  }
  return out;
}

/** Decodes a tpl from a whole buffer (`brk_assignment`, `brk_committed`, …). */
export function decodeTplBuffer(
  buf: Uint8Array,
  opts: TplDecodeOptions = {},
): TopicPartitionEntry[] {
  return decodeTpl(new BufReader(buf), opts);
}

/* ========================================================================== */
/* Format 3 — PRODUCE BATCH                                                    */
/* ========================================================================== */

export interface ProduceHeader {
  key: string;
  value: Uint8Array | string | null;
}

export interface ProduceRecord {
  topic: string;
  /** Defaults to `RD_KAFKA_PARTITION_UA` (-1) → librdkafka's partitioner picks. */
  partition?: number;
  /** Defaults to 0 → librdkafka uses the current time. */
  timestamp?: number | bigint;
  /** id assigned by JS (DeliveryLedger), returned verbatim in the DR. */
  opaqueId: bigint;
  key?: Uint8Array | string | null;
  value?: Uint8Array | string | null;
  headers?: readonly ProduceHeader[];
}

const MAX_U16 = 0xffff;

const PRODUCE_TEXT_ENCODER = /* @__PURE__ */ new TextEncoder();

/**
 * Writes one key/value field as `(u64 ptr, i32 len)` — bytes are NOT copied
 * into the packed buffer (1-copy produce path, header §format 3).
 *
 * Strings are UTF-8-encoded once and pushed into `keepAlive`; Uint8Arrays are
 * written straight via `ptr()` (bun:ffi adds the view's `byteOffset` itself —
 * verified, with a subarray unit test). The caller MUST keep `records` +
 * `keepAlive` alive until the synchronous FFI call returns.
 */
function writePtrField(
  w: BufWriter,
  v: Uint8Array | string | null,
  keepAlive: unknown[],
): void {
  if (v === null) {
    w.u64(0n);
    w.i32(NULL_LENGTH);
    return;
  }
  let bytes: Uint8Array;
  if (typeof v === "string") {
    bytes = PRODUCE_TEXT_ENCODER.encode(v);
    keepAlive.push(bytes);
  } else {
    bytes = v;
  }
  if (bytes.byteLength === 0) {
    w.u64(0n);
    w.i32(0);
    return;
  }
  w.u64(BigInt(ptr(bytes)));
  w.i32(bytes.byteLength);
}

/**
 * `u32 count, count × { u16 topic_len, bytes topic, i32 partition,
 *  i64 timestamp_ms, u64 opaque_id, u64 key_ptr, i32 key_len,
 *  u64 value_ptr, i32 value_len, u16 header_count,
 *  header_count × { u16 klen, bytes k, i32 vlen, bytes v } }`
 *
 * `keepAlive`: an array the caller keeps alive across the FFI call — receives
 * the intermediate buffers (string → UTF-8). Omitting it is only valid when
 * every key/value is already a Uint8Array the caller holds a reference to
 * (e.g. via `records` itself).
 */
export function encodeProduceBatch(
  records: readonly ProduceRecord[],
  writer?: BufWriter,
  keepAlive: unknown[] = [],
): BufWriter {
  const w = writer ?? new BufWriter(4096);
  w.u32(records.length);
  for (const rec of records) {
    w.stringU16(rec.topic);
    w.i32(rec.partition ?? RD_KAFKA_PARTITION_UA);
    w.i64(rec.timestamp ?? 0);
    w.u64(rec.opaqueId);
    writePtrField(w, rec.key ?? null, keepAlive);
    writePtrField(w, rec.value ?? null, keepAlive);
    const headers = rec.headers ?? [];
    if (headers.length > MAX_U16) {
      throw new BinaryEncodeError(
        `record has ${headers.length} headers, exceeding the u16 header_count limit`,
      );
    }
    w.u16(headers.length);
    for (const h of headers) {
      w.stringU16(h.key);
      w.bytesI32(h.value);
    }
  }
  return w;
}

/* ========================================================================== */
/* Format 4 — MESSAGE BATCH                                                    */
/* ========================================================================== */

export interface DecodedHeader {
  key: string;
  value: Uint8Array | null;
}

export interface DecodedMessage {
  /** id interned for the handle's lifetime. */
  topicId: number;
  topic: string;
  partition: number;
  offset: number;
  /** -1 when there is no timestamp. */
  timestamp: number;
  timestampType: RdKafkaTimestampType;
  /** `rd_kafka_resp_err_t`; 0 = normal message, non-zero (e.g. `_PARTITION_EOF`) = event. */
  err: number;
  key: Uint8Array | null;
  value: Uint8Array | null;
  headers: DecodedHeader[];
  /** -1 if absent. */
  leaderEpoch: number;
}

export interface MessageDecodeOptions {
  topics: TopicNameTable;
  fetchTopicName?: TopicNameFetcher | undefined;
  /**
   * `true` (default) copies key/value/header values out of `buf` (as Buffers);
   * `false` returns views into `buf` — only safe when `buf` is never reused,
   * which is how {@link NativeClient.consumeBatch} calls it (a fresh buffer
   * per batch, see the note there).
   */
  copy?: boolean;
}

/** Result of {@link decodeMessageBatchWithSize}. */
export interface DecodedMessageBatch {
  messages: DecodedMessage[];
  /** Bytes of `buf` consumed by the `count` records. */
  byteLength: number;
}

/**
 * Decodes `count` back-to-back records (no count prefix — `count` is
 * `brk_consume_batch`'s return value).
 */
export function decodeMessageBatch(
  buf: Uint8Array,
  count: number,
  opts: MessageDecodeOptions,
): DecodedMessage[] {
  return decodeMessageBatchWithSize(buf, count, opts).messages;
}

/**
 * {@link decodeMessageBatch} that also reports how many bytes the records
 * occupied (the consume path sizes its next buffer from it).
 *
 * This is the hottest decoder, so it reads through a local DataView/offset
 * instead of {@link BufReader} (one bounds check per fixed-size record header
 * and one per variable-length field, no per-field call overhead) and reads
 * i64s as two u32s (no BigInt). Layout per record:
 * `i32 topic_id, i32 partition, i64 offset, i64 timestamp, u8 ts_type, i16 err,
 *  i32 key_len, bytes, i32 value_len, bytes, u16 header_count,
 *  header_count × { u16 key_len, bytes, i32 value_len, bytes }, i32 leader_epoch`.
 */
export function decodeMessageBatchWithSize(
  buf: Uint8Array,
  count: number,
  opts: MessageDecodeOptions,
): DecodedMessageBatch {
  const copy = opts.copy !== false;
  const topics = opts.topics;
  const fetchTopicName = opts.fetchTopicName;
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const len = buf.byteLength;
  const out: DecodedMessage[] = new Array<DecodedMessage>(count);
  let pos = 0;
  let lastTopicId = -1;
  let lastTopic = "";

  for (let i = 0; i < count; i++) {
    // Fixed header: 4 + 4 + 8 + 8 + 1 + 2 = 27 bytes.
    if (pos + MSG_FIXED_HEADER > len) throw truncated(MSG_FIXED_HEADER, pos, len);
    const topicId = view.getInt32(pos, true);
    const partition = view.getInt32(pos + 4, true);
    const offset = readI64Number(view, pos + 8);
    const timestamp = readI64Number(view, pos + 16);
    const timestampType = view.getUint8(pos + 24) as RdKafkaTimestampType;
    const err = view.getInt16(pos + 25, true);
    pos += MSG_FIXED_HEADER;

    // key
    if (pos + 4 > len) throw truncated(4, pos, len);
    let n = view.getInt32(pos, true);
    pos += 4;
    let key: Uint8Array | null = null;
    if (n !== NULL_LENGTH) {
      if (n < 0 || pos + n > len) throw badLength(n, pos - 4, len);
      key = copy ? Buffer.copyBytesFrom(buf, pos, n) : buf.subarray(pos, pos + n);
      pos += n;
    }

    // value
    if (pos + 4 > len) throw truncated(4, pos, len);
    n = view.getInt32(pos, true);
    pos += 4;
    let value: Uint8Array | null = null;
    if (n !== NULL_LENGTH) {
      if (n < 0 || pos + n > len) throw badLength(n, pos - 4, len);
      value = copy ? Buffer.copyBytesFrom(buf, pos, n) : buf.subarray(pos, pos + n);
      pos += n;
    }

    // headers
    if (pos + 2 > len) throw truncated(2, pos, len);
    const headerCount = view.getUint16(pos, true);
    pos += 2;
    const headers: DecodedHeader[] = new Array<DecodedHeader>(headerCount);
    for (let h = 0; h < headerCount; h++) {
      if (pos + 2 > len) throw truncated(2, pos, len);
      const klen = view.getUint16(pos, true);
      pos += 2;
      if (pos + klen > len) throw truncated(klen, pos, len);
      const hkey = klen === 0 ? "" : HEADER_KEY_DECODER.decode(buf.subarray(pos, pos + klen));
      pos += klen;
      if (pos + 4 > len) throw truncated(4, pos, len);
      n = view.getInt32(pos, true);
      pos += 4;
      let hval: Uint8Array | null = null;
      if (n !== NULL_LENGTH) {
        if (n < 0 || pos + n > len) throw badLength(n, pos - 4, len);
        hval = copy ? Buffer.copyBytesFrom(buf, pos, n) : buf.subarray(pos, pos + n);
        pos += n;
      }
      headers[h] = { key: hkey, value: hval };
    }

    if (pos + 4 > len) throw truncated(4, pos, len);
    const leaderEpoch = view.getInt32(pos, true);
    pos += 4;

    // Consecutive records usually share a topic: skip the Map lookup.
    if (topicId !== lastTopicId) {
      lastTopic = topics.resolve(topicId, fetchTopicName);
      lastTopicId = topicId;
    }

    out[i] = {
      topicId,
      topic: lastTopic,
      partition,
      offset,
      timestamp,
      timestampType,
      err,
      key,
      value,
      headers,
      leaderEpoch,
    };
  }
  return { messages: out, byteLength: pos };
}

const MSG_FIXED_HEADER = 27;
const HEADER_KEY_DECODER = /* @__PURE__ */ new TextDecoder("utf-8", { fatal: false });

/** i64 → number via two u32 reads (no BigInt); throws beyond ±2^53. */
function readI64Number(view: DataView, at: number): number {
  const lo = view.getUint32(at, true);
  const hi = view.getInt32(at + 4, true);
  if (hi >= 0x200000 || hi < -0x200000) {
    throw new BinaryDecodeError(
      `i64 value ${view.getBigInt64(at, true)} is not representable as a number`,
      at,
    );
  }
  return hi * 4294967296 + lo;
}

function truncated(n: number, pos: number, len: number): BinaryDecodeError {
  return new BinaryDecodeError(
    `need ${n} bytes but only ${len - pos} remain (buffer is ${len} bytes)`,
    pos,
  );
}

function badLength(n: number, at: number, len: number): BinaryDecodeError {
  return n < 0
    ? new BinaryDecodeError(`invalid negative length: ${n}`, at)
    : truncated(n, at + 4, len);
}

/* ========================================================================== */
/* Format 5 — EVENT FRAME                                                      */
/* ========================================================================== */

export interface DeliveryReport {
  opaqueId: bigint;
  /** `rd_kafka_resp_err_t`; 0 = delivered successfully. */
  err: number;
  partition: number;
  offset: number;
  timestamp: number;
}

export interface BrkDrEvent {
  type: typeof BRK_EVENT_DR;
  reports: DeliveryReport[];
}

export interface BrkErrorEvent {
  type: typeof BRK_EVENT_ERROR;
  code: number;
  isFatal: boolean;
  reason: string;
}

export interface BrkLogEvent {
  type: typeof BRK_EVENT_LOG;
  level: number;
  fac: string;
  message: string;
}

export interface BrkStatsEvent {
  type: typeof BRK_EVENT_STATS;
  /** Raw JSON from librdkafka (NFR-8 — not parsed at the core layer). */
  json: string;
}

export interface BrkRebalanceEvent {
  type: typeof BRK_EVENT_REBALANCE;
  /** `_ASSIGN_PARTITIONS` (-175) or `_REVOKE_PARTITIONS` (-174). */
  code: number;
  protocol: BrkRebalanceProtocol;
  /** `rd_kafka_assignment_lost` at event time (only meaningful on REVOKE). */
  assignmentLost: boolean;
  partitions: TopicPartitionEntry[];
}

export interface BrkOffsetCommitEvent {
  type: typeof BRK_EVENT_OFFSET_COMMIT;
  code: number;
  partitions: TopicPartitionEntry[];
}

export interface BrkThrottleEvent {
  type: typeof BRK_EVENT_THROTTLE;
  brokerId: number;
  throttleMs: number;
  brokerName: string;
}

export interface BrkOauthRefreshEvent {
  type: typeof BRK_EVENT_OAUTH_REFRESH;
  /** Value of the `sasl.oauthbearer.config` property. */
  oauthbearerConfig: string;
}

export interface BrkAdminResultEvent {
  type: typeof BRK_EVENT_ADMIN_RESULT;
  correlationId: bigint;
  code: number;
  json: string;
}

export type BrkEvent =
  | BrkDrEvent
  | BrkErrorEvent
  | BrkLogEvent
  | BrkStatsEvent
  | BrkRebalanceEvent
  | BrkOffsetCommitEvent
  | BrkThrottleEvent
  | BrkOauthRefreshEvent
  | BrkAdminResultEvent;

export interface EventDecodeOptions {
  /** Needed for REBALANCE / OFFSET_COMMIT payloads when C sends the tpl by `topic_id`. */
  topics?: TopicNameTable | undefined;
  fetchTopicName?: TopicNameFetcher | undefined;
}

/**
 * Decodes `count` back-to-back frames `{ u8 event_type, u32 payload_len,
 * payload }` (`count` is `brk_events_poll`'s return value).
 */
export function decodeEventFrames(
  buf: Uint8Array,
  count: number,
  opts: EventDecodeOptions = {},
): BrkEvent[] {
  const r = new BufReader(buf);
  const out: BrkEvent[] = new Array<BrkEvent>(count);
  for (let i = 0; i < count; i++) {
    const eventType = r.u8();
    const payloadLen = r.u32();
    const start = r.offset;
    r.skip(payloadLen); // bounds check + jump to the next frame
    const p = new BufReader(r.bytesView, start, payloadLen);
    out[i] = decodeEventPayload(eventType, p, opts, start);
  }
  return out;
}

function decodeEventPayload(
  eventType: number,
  p: BufReader,
  opts: EventDecodeOptions,
  frameStart: number,
): BrkEvent {
  switch (eventType) {
    case BRK_EVENT_DR: {
      const n = p.u32();
      const reports: DeliveryReport[] = new Array<DeliveryReport>(n);
      for (let i = 0; i < n; i++) {
        reports[i] = {
          opaqueId: p.u64(),
          err: p.i16(),
          partition: p.i32(),
          offset: p.i64Number(),
          timestamp: p.i64Number(),
        };
      }
      return { type: BRK_EVENT_DR, reports };
    }
    case BRK_EVENT_ERROR: {
      const code = p.i32();
      const isFatal = p.u8() !== 0;
      return { type: BRK_EVENT_ERROR, code, isFatal, reason: p.stringU16() };
    }
    case BRK_EVENT_LOG: {
      const level = p.i32();
      const fac = p.stringU16();
      return { type: BRK_EVENT_LOG, level, fac, message: p.stringU32() };
    }
    case BRK_EVENT_STATS:
      return { type: BRK_EVENT_STATS, json: p.stringU32() };
    case BRK_EVENT_REBALANCE: {
      const code = p.i32();
      const protocol = p.u8() as BrkRebalanceProtocol;
      const assignmentLost = p.u8() !== 0;
      return {
        type: BRK_EVENT_REBALANCE,
        code,
        protocol,
        assignmentLost,
        partitions: decodeTpl(p, opts),
      };
    }
    case BRK_EVENT_OFFSET_COMMIT: {
      const code = p.i32();
      return { type: BRK_EVENT_OFFSET_COMMIT, code, partitions: decodeTpl(p, opts) };
    }
    case BRK_EVENT_THROTTLE: {
      const brokerId = p.i32();
      const throttleMs = p.i32();
      return { type: BRK_EVENT_THROTTLE, brokerId, throttleMs, brokerName: p.stringU16() };
    }
    case BRK_EVENT_OAUTH_REFRESH:
      return { type: BRK_EVENT_OAUTH_REFRESH, oauthbearerConfig: p.stringU16() };
    case BRK_EVENT_ADMIN_RESULT: {
      const correlationId = p.u64();
      const code = p.i32();
      return { type: BRK_EVENT_ADMIN_RESULT, correlationId, code, json: p.stringU32() };
    }
    default:
      throw new BinaryDecodeError(
        `unrecognized event_type: ${eventType} (ABI mismatch?)`,
        frameStart,
      );
  }
}
