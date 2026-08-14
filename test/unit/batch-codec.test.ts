/**
 * Round-trip encode↔decode for formats 1 (STRING LIST), 2 (TPL), 3 (PRODUCE
 * BATCH) and 4 (MESSAGE BATCH) of `native/include/bunrdkafka.h`, with
 * byte-level layout checks for the edge cases.
 */

import { describe, expect, test } from "bun:test";
import {
  TopicNameTable,
  decodeMessageBatch,
  decodeStringList,
  decodeTplBuffer,
  encodeProduceBatch,
  encodeStringList,
  encodeTpl,
} from "../../packages/bun-rdkafka/src/core/batch-decoder.ts";
import {
  BinaryDecodeError,
  BinaryEncodeError,
  BufReader,
  BufWriter,
} from "../../packages/bun-rdkafka/src/core/binary.ts";
import {
  RD_KAFKA_OFFSET_INVALID,
  RD_KAFKA_PARTITION_UA,
  RD_KAFKA_TIMESTAMP_CREATE_TIME,
  RD_KAFKA_TIMESTAMP_NOT_AVAILABLE,
} from "../../packages/bun-rdkafka/src/ffi/types.ts";
import { decodeProduceBatch, encodeMessageBatch } from "./helpers/c-side-encoders.ts";

const utf8 = (s: string) => new TextEncoder().encode(s);
const str = (b: Uint8Array | null) => (b === null ? null : new TextDecoder().decode(b));

/* ======================================================================== */
/* Format 1 — STRING LIST                                                    */
/* ======================================================================== */

describe("format 1 — STRING LIST", () => {
  test("round-trips a topic list (including multi-byte UTF-8 and empty strings)", () => {
    const topics = ["orders", "注文-topic", "主题", "🚀", ""];
    const bytes = encodeStringList(topics).toBytes();
    expect(decodeStringList(bytes)).toEqual(topics);
  });

  test("an empty list is just u32 count = 0", () => {
    const bytes = encodeStringList([]).toBytes();
    expect(Array.from(bytes)).toEqual([0, 0, 0, 0]);
    expect(decodeStringList(bytes)).toEqual([]);
  });

  test("byte-level layout: count + (u16 len, utf8)", () => {
    const bytes = encodeStringList(["ab"]).toBytes();
    expect(Array.from(bytes)).toEqual([
      1, 0, 0, 0, // u32 count = 1
      2, 0, // u16 len = 2
      0x61, 0x62, // "ab"
    ]);
  });

  test("the buffer grows when encoding many long topics", () => {
    const topics = Array.from({ length: 500 }, (_, i) => `topic-${"x".repeat(50)}-${i}`);
    const w = new BufWriter(16);
    encodeStringList(topics, w);
    expect(decodeStringList(w.toBytes())).toEqual(topics);
  });
});

/* ======================================================================== */
/* Format 2 — TOPIC-PARTITION LIST                                           */
/* ======================================================================== */

describe("format 2 — TPL", () => {
  test("JS→C by topic name, C→JS reads it back correctly", () => {
    const input = [
      { topic: "orders", partition: 0, offset: 42, leaderEpoch: 7 },
      { topic: "注文-topic", partition: 3 }, // default offset/leaderEpoch
    ];
    const bytes = encodeTpl(input).toBytes();
    const decoded = decodeTplBuffer(bytes);
    expect(decoded).toEqual([
      { topic: "orders", topicId: -1, partition: 0, offset: 42, leaderEpoch: 7, metadata: null },
      {
        topic: "注文-topic",
        topicId: -1,
        partition: 3,
        offset: RD_KAFKA_OFFSET_INVALID,
        leaderEpoch: -1,
        metadata: null,
      },
    ]);
  });

  test("commit metadata round-trip: null ≠ empty ≠ non-empty (including UTF-8)", () => {
    const input = [
      { topic: "orders", partition: 0, offset: 10, metadata: "checkpoint-α-42" },
      { topic: "orders", partition: 1, offset: 11, metadata: "" },
      { topic: "orders", partition: 2, offset: 12 }, // no metadata
      { topic: "orders", partition: 3, offset: 13, metadata: null },
    ];
    const decoded = decodeTplBuffer(encodeTpl(input).toBytes());
    expect(decoded.map((e) => e.metadata)).toEqual(["checkpoint-α-42", "", null, null]);
    // metadata_len is an i16 → the 32767-byte limit must be enforced at encode time.
    expect(() =>
      encodeTpl([{ topic: "t", partition: 0, metadata: "x".repeat(40_000) }]),
    ).toThrow();
  });

  test("using topicId gives name_len = 0 and requires a TopicNameTable to decode", () => {
    const bytes = encodeTpl([{ topicId: 5, partition: 1, offset: 100 }]).toBytes();

    const r = new BufReader(bytes);
    expect(r.u32()).toBe(1);
    expect(r.i32()).toBe(5); // topic_id
    expect(r.u16()).toBe(0); // name_len = 0

    expect(() => decodeTplBuffer(bytes)).toThrow(BinaryDecodeError);

    const topics = new TopicNameTable();
    topics.set(5, "orders");
    expect(decodeTplBuffer(bytes, { topics })).toEqual([
      { topic: "orders", topicId: 5, partition: 1, offset: 100, leaderEpoch: -1, metadata: null },
    ]);
  });

  test("a cache miss calls fetch exactly once, then caches", () => {
    const bytes = encodeTpl([
      { topicId: 9, partition: 0 },
      { topicId: 9, partition: 1 },
    ]).toBytes();
    let calls = 0;
    const topics = new TopicNameTable((id) => {
      calls++;
      return `topic-${id}`;
    });
    const decoded = decodeTplBuffer(bytes, { topics });
    expect(decoded.map((e) => e.topic)).toEqual(["topic-9", "topic-9"]);
    expect(calls).toBe(1);
    expect(topics.get(9)).toBe("topic-9");
  });

  test("an entry sent with a name also feeds the intern table", () => {
    const topics = new TopicNameTable();
    const w = new BufWriter(64);
    w.u32(1);
    w.i32(11);
    w.stringU16("from-c");
    w.i32(2);
    w.i64(5n);
    w.i32(-1);
    w.i16(-1); // metadata_len: absent
    decodeTplBuffer(w.toBytes(), { topics });
    expect(topics.get(11)).toBe("from-c");
  });

  test("special offsets (BEGINNING/END/INVALID) pass through untouched", () => {
    const input = [
      { topic: "t", partition: 0, offset: -2 },
      { topic: "t", partition: 1, offset: -1 },
      { topic: "t", partition: 2, offset: RD_KAFKA_OFFSET_INVALID },
      { topic: "t", partition: 3, offset: 9007199254740991n },
    ];
    expect(decodeTplBuffer(encodeTpl(input).toBytes()).map((e) => e.offset)).toEqual([
      -2, -1, RD_KAFKA_OFFSET_INVALID, 9007199254740991,
    ]);
  });

  test("an entry missing both topic and topicId is rejected", () => {
    expect(() => encodeTpl([{ partition: 0 }])).toThrow(BinaryEncodeError);
  });

  test("an empty tpl = u32 0", () => {
    expect(Array.from(encodeTpl([]).toBytes())).toEqual([0, 0, 0, 0]);
    expect(decodeTplBuffer(new Uint8Array([0, 0, 0, 0]))).toEqual([]);
  });
});

/* ======================================================================== */
/* Format 3 — PRODUCE BATCH                                                  */
/* ======================================================================== */

describe("format 3 — PRODUCE BATCH", () => {
  test("full-field round-trip", () => {
    const records = [
      {
        topic: "orders",
        partition: 3,
        timestamp: 1_700_000_000_000,
        opaqueId: 42n,
        key: utf8("k1"),
        value: utf8("wärte"),
        headers: [
          { key: "trace-id", value: utf8("abc") },
          { key: "schlüssel", value: "zeichenkette" },
          { key: "null-header", value: null },
          { key: "empty-header", value: new Uint8Array(0) },
        ],
      },
      {
        topic: "tombstones",
        opaqueId: 0xffffffffffffffffn,
        key: "nur-schlüssel",
        value: null,
      },
    ];

    const decoded = decodeProduceBatch(encodeProduceBatch(records).toBytes());
    expect(decoded).toHaveLength(2);

    const first = decoded[0]!;
    expect(first.topic).toBe("orders");
    expect(first.partition).toBe(3);
    expect(first.timestampMs).toBe(1_700_000_000_000n);
    expect(first.opaqueId).toBe(42n);
    expect(str(first.key)).toBe("k1");
    expect(str(first.value)).toBe("wärte");
    expect(first.headers.map((h) => h.key)).toEqual([
      "trace-id",
      "schlüssel",
      "null-header",
      "empty-header",
    ]);
    expect(str(first.headers[2]!.value)).toBeNull();
    expect(first.headers[3]!.value).toEqual(new Uint8Array(0));

    const second = decoded[1]!;
    expect(second.partition).toBe(RD_KAFKA_PARTITION_UA);
    expect(second.timestampMs).toBe(0n);
    expect(second.opaqueId).toBe(0xffffffffffffffffn);
    expect(str(second.key)).toBe("nur-schlüssel");
    expect(second.value).toBeNull();
    expect(second.headers).toEqual([]);
  });

  test("null key + null value + no headers", () => {
    const decoded = decodeProduceBatch(
      encodeProduceBatch([{ topic: "t", opaqueId: 1n }]).toBytes(),
    );
    expect(decoded[0]!.key).toBeNull();
    expect(decoded[0]!.value).toBeNull();
    expect(decoded[0]!.headers).toEqual([]);
  });

  test("byte-level layout of a minimal record", () => {
    const bytes = encodeProduceBatch([{ topic: "t", opaqueId: 1n }]).toBytes();
    expect(Array.from(bytes)).toEqual([
      1, 0, 0, 0, // u32 count
      1, 0, 0x74, // u16 topic_len=1, "t"
      0xff, 0xff, 0xff, 0xff, // i32 partition = -1 (UA)
      0, 0, 0, 0, 0, 0, 0, 0, // i64 timestamp = 0
      1, 0, 0, 0, 0, 0, 0, 0, // u64 opaque_id = 1
      0, 0, 0, 0, 0, 0, 0, 0, // u64 key_ptr = 0 (null)
      0xff, 0xff, 0xff, 0xff, // i32 key_len = -1
      0, 0, 0, 0, 0, 0, 0, 0, // u64 value_ptr = 0 (null)
      0xff, 0xff, 0xff, 0xff, // i32 value_len = -1
      0, 0, // u16 header_count = 0
    ]);
  });

  test("key/value as subarrays → ptr targets the right region (byteOffset applied)", () => {
    // Bury the real payload in the middle of a large buffer: if ptr() ignored
    // the view's byteOffset, C would read the 0xAA padding instead of the payload.
    const arena = new Uint8Array(256).fill(0xaa);
    arena.set(utf8("KEY!"), 100);
    arena.set(utf8("payload-in-müst"), 130);
    const key = arena.subarray(100, 104);
    const value = arena.subarray(130, 130 + utf8("payload-in-müst").byteLength);
    const bufKey = Buffer.from(arena.buffer, 100, 4); // Buffer views must work too

    const keepAlive: unknown[] = [];
    const decoded = decodeProduceBatch(
      encodeProduceBatch(
        [
          { topic: "t", opaqueId: 1n, key, value },
          { topic: "t", opaqueId: 2n, key: bufKey, value: "tmp-strüng" },
          { topic: "t", opaqueId: 3n, key: null, value: new Uint8Array(0) },
        ],
        undefined,
        keepAlive,
      ).toBytes(),
    );

    expect(str(decoded[0]!.key)).toBe("KEY!");
    expect(str(decoded[0]!.value)).toBe("payload-in-müst");
    expect(str(decoded[1]!.key)).toBe("KEY!");
    expect(str(decoded[1]!.value)).toBe("tmp-strüng");
    // strings are converted once and held in keepAlive (alive across the FFI call)
    expect(keepAlive.length).toBe(1);
    expect(decoded[2]!.key).toBeNull();
    expect(decoded[2]!.value).toEqual(new Uint8Array(0)); // empty ≠ null
  });

  test("a large batch forcing multiple writer grows stays correct", () => {
    const records = Array.from({ length: 2000 }, (_, i) => ({
      topic: `topic-${i % 7}`,
      opaqueId: BigInt(i + 1),
      value: utf8(`payload-${i}-${"füll".repeat(10)}`),
    }));
    const w = new BufWriter(32);
    const decoded = decodeProduceBatch(encodeProduceBatch(records, w).toBytes());
    expect(decoded).toHaveLength(2000);
    expect(decoded[1999]!.opaqueId).toBe(2000n);
    expect(str(decoded[1999]!.value)).toBe(`payload-1999-${"füll".repeat(10)}`);
  });

  test("the writer is reusable after reset for the next batch", () => {
    const w = new BufWriter(64);
    encodeProduceBatch([{ topic: "a", opaqueId: 1n }], w);
    w.reset();
    encodeProduceBatch([{ topic: "b", opaqueId: 2n }], w);
    const decoded = decodeProduceBatch(w.toBytes());
    expect(decoded).toHaveLength(1);
    expect(decoded[0]!.topic).toBe("b");
  });
});

/* ======================================================================== */
/* Format 4 — MESSAGE BATCH                                                  */
/* ======================================================================== */

describe("format 4 — MESSAGE BATCH", () => {
  const topics = () => {
    const t = new TopicNameTable();
    t.set(1, "orders");
    t.set(2, "注文-topic");
    return t;
  };

  test("decodes multiple back-to-back messages", () => {
    const buf = encodeMessageBatch([
      {
        topicId: 1,
        partition: 2,
        offset: 12345,
        timestampMs: 1_700_000_000_000,
        timestampType: RD_KAFKA_TIMESTAMP_CREATE_TIME,
        err: 0,
        key: "k",
        value: "v",
        headers: [{ key: "h1", value: "v1" }],
        leaderEpoch: 4,
      },
      {
        topicId: 2,
        partition: 0,
        offset: 1,
        timestampMs: -1,
        timestampType: RD_KAFKA_TIMESTAMP_NOT_AVAILABLE,
        err: 0,
        key: null,
        value: null,
      },
    ]);

    const msgs = decodeMessageBatch(buf, 2, { topics: topics() });
    expect(msgs).toHaveLength(2);
    expect(msgs[0]).toEqual({
      topicId: 1,
      topic: "orders",
      partition: 2,
      offset: 12345,
      timestamp: 1_700_000_000_000,
      timestampType: RD_KAFKA_TIMESTAMP_CREATE_TIME,
      err: 0,
      key: utf8("k"),
      value: utf8("v"),
      headers: [{ key: "h1", value: utf8("v1") }],
      leaderEpoch: 4,
    });
    expect(msgs[1]!.topic).toBe("注文-topic");
    expect(msgs[1]!.key).toBeNull();
    expect(msgs[1]!.value).toBeNull();
    expect(msgs[1]!.timestamp).toBe(-1);
    expect(msgs[1]!.leaderEpoch).toBe(-1);
  });

  test("error messages (_PARTITION_EOF) take the same path with empty key/value", () => {
    const buf = encodeMessageBatch([
      {
        topicId: 1,
        partition: 0,
        offset: 99,
        timestampMs: -1,
        timestampType: 0,
        err: -191, // ERR__PARTITION_EOF
        key: null,
        value: null,
      },
    ]);
    const [msg] = decodeMessageBatch(buf, 1, { topics: topics() });
    expect(msg!.err).toBe(-191);
    expect(msg!.value).toBeNull();
  });

  test("empty headers, empty values, and null values are distinguishable", () => {
    const buf = encodeMessageBatch([
      {
        topicId: 1,
        partition: 0,
        offset: 0,
        timestampMs: 0,
        timestampType: 1,
        err: 0,
        key: new Uint8Array(0),
        value: null,
        headers: [
          { key: "", value: new Uint8Array(0) },
          { key: "k", value: null },
        ],
      },
    ]);
    const [msg] = decodeMessageBatch(buf, 1, { topics: topics() });
    expect(msg!.key).toEqual(new Uint8Array(0));
    expect(msg!.value).toBeNull();
    expect(msg!.headers).toEqual([
      { key: "", value: new Uint8Array(0) },
      { key: "k", value: null },
    ]);
  });

  test("default copy: messages are independent of the reusable buffer", () => {
    const buf = encodeMessageBatch([
      {
        topicId: 1,
        partition: 0,
        offset: 0,
        timestampMs: 0,
        timestampType: 1,
        err: 0,
        key: null,
        value: "abc",
      },
    ]);
    const copied = decodeMessageBatch(buf, 1, { topics: topics() })[0]!;
    const zeroCopy = decodeMessageBatch(buf, 1, { topics: topics(), copy: false })[0]!;
    buf.fill(0);
    expect(str(copied.value)).toBe("abc");
    expect(zeroCopy.value).toEqual(new Uint8Array([0, 0, 0]));
  });

  test("a topic_id miss is resolved via fetch (brk_topic_name)", () => {
    const buf = encodeMessageBatch([
      {
        topicId: 77,
        partition: 0,
        offset: 0,
        timestampMs: 0,
        timestampType: 0,
        err: 0,
        key: null,
        value: null,
      },
    ]);
    const table = new TopicNameTable();
    const [msg] = decodeMessageBatch(buf, 1, {
      topics: table,
      fetchTopicName: (id) => `resolved-${id}`,
    });
    expect(msg!.topic).toBe("resolved-77");
    expect(table.get(77)).toBe("resolved-77");
  });

  test("a mid-record truncated buffer throws BinaryDecodeError", () => {
    const buf = encodeMessageBatch([
      {
        topicId: 1,
        partition: 0,
        offset: 0,
        timestampMs: 0,
        timestampType: 0,
        err: 0,
        key: null,
        value: null,
      },
    ]);
    expect(() => decodeMessageBatch(buf.subarray(0, 10), 1, { topics: topics() })).toThrow(
      BinaryDecodeError,
    );
  });

  test("decodes exactly count even with leftover buffer (C counts what it wrote)", () => {
    const buf = encodeMessageBatch([
      {
        topicId: 1,
        partition: 0,
        offset: 5,
        timestampMs: 0,
        timestampType: 0,
        err: 0,
        key: null,
        value: null,
      },
    ]);
    const padded = new Uint8Array(buf.length + 64);
    padded.set(buf);
    const msgs = decodeMessageBatch(padded, 1, { topics: topics() });
    expect(msgs).toHaveLength(1);
    expect(msgs[0]!.offset).toBe(5);
  });
});
