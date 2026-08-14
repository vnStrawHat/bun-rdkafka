/**
 * Format 5 — EVENT FRAME: `{ u8 event_type, u32 payload_len, bytes payload }`.
 * Every payload kind is hand-built byte for byte per `bunrdkafka.h`, then decoded.
 */

import { describe, expect, test } from "bun:test";
import {
  TopicNameTable,
  decodeEventFrames,
  type BrkAdminResultEvent,
  type BrkDrEvent,
  type BrkErrorEvent,
  type BrkLogEvent,
  type BrkOauthRefreshEvent,
  type BrkOffsetCommitEvent,
  type BrkRebalanceEvent,
  type BrkStatsEvent,
  type BrkThrottleEvent,
} from "../../packages/bun-rdkafka/src/core/batch-decoder.ts";
import { BinaryDecodeError } from "../../packages/bun-rdkafka/src/core/binary.ts";
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
  BRK_REBALANCE_PROTOCOL_COOPERATIVE,
  BRK_REBALANCE_PROTOCOL_EAGER,
} from "../../packages/bun-rdkafka/src/ffi/types.ts";
import {
  adminResultPayload,
  drPayload,
  encodeEventFrames,
  errorPayload,
  logPayload,
  oauthRefreshPayload,
  offsetCommitPayload,
  rebalancePayload,
  statsPayload,
  throttlePayload,
} from "./helpers/c-side-encoders.ts";

describe("khung frame", () => {
  test("hand-built byte-level layout: u8 type + u32 len + payload", () => {
    // An OAUTH_REFRESH frame with config "ab": payload = u16 len(2) + "ab" = 4 bytes.
    const buf = new Uint8Array([
      8, // event_type = BRK_EVENT_OAUTH_REFRESH
      4, 0, 0, 0, // u32 payload_len = 4
      2, 0, // u16 cfg_len = 2
      0x61, 0x62, // "ab"
    ]);
    const [event] = decodeEventFrames(buf, 1);
    expect(event).toEqual({ type: BRK_EVENT_OAUTH_REFRESH, oauthbearerConfig: "ab" });
  });

  test("multiple back-to-back frames decode in order", () => {
    const buf = encodeEventFrames([
      { type: BRK_EVENT_STATS, payload: statsPayload("{}") },
      { type: BRK_EVENT_THROTTLE, payload: throttlePayload(1, 2, "b1") },
      { type: BRK_EVENT_OAUTH_REFRESH, payload: oauthRefreshPayload("cfg") },
    ]);
    expect(decodeEventFrames(buf, 3).map((e) => e.type)).toEqual([
      BRK_EVENT_STATS,
      BRK_EVENT_THROTTLE,
      BRK_EVENT_OAUTH_REFRESH,
    ]);
  });

  test("decodes only `count` frames despite leftover buffer", () => {
    const buf = encodeEventFrames([
      { type: BRK_EVENT_STATS, payload: statsPayload("{\"a\":1}") },
      { type: BRK_EVENT_STATS, payload: statsPayload("{\"b\":2}") },
    ]);
    expect(decodeEventFrames(buf, 1)).toHaveLength(1);
  });

  test("count = 0 returns an empty array", () => {
    expect(decodeEventFrames(new Uint8Array(0), 0)).toEqual([]);
  });

  test("an unknown event_type → BinaryDecodeError (ABI mismatch)", () => {
    const buf = new Uint8Array([99, 0, 0, 0, 0]);
    expect(() => decodeEventFrames(buf, 1)).toThrow(BinaryDecodeError);
  });

  test("payload_len past the buffer → BinaryDecodeError", () => {
    const buf = new Uint8Array([4, 0xff, 0, 0, 0, 1, 2, 3]);
    expect(() => decodeEventFrames(buf, 1)).toThrow(BinaryDecodeError);
  });
});

describe("payload DR", () => {
  test("multiple delivery reports in one frame", () => {
    const buf = encodeEventFrames([
      {
        type: BRK_EVENT_DR,
        payload: drPayload([
          { opaqueId: 1n, err: 0, partition: 3, offset: 100, timestampMs: 1_700_000_000_000 },
          {
            opaqueId: 0xffffffffffffffffn,
            err: -184, // ERR__QUEUE_FULL
            partition: -1,
            offset: -1001,
            timestampMs: -1,
          },
        ]),
      },
    ]);
    const [event] = decodeEventFrames(buf, 1) as [BrkDrEvent];
    expect(event.type).toBe(BRK_EVENT_DR);
    expect(event.reports).toEqual([
      { opaqueId: 1n, err: 0, partition: 3, offset: 100, timestamp: 1_700_000_000_000 },
      { opaqueId: 0xffffffffffffffffn, err: -184, partition: -1, offset: -1001, timestamp: -1 },
    ]);
  });

  test("an empty DR frame (n = 0)", () => {
    const buf = encodeEventFrames([{ type: BRK_EVENT_DR, payload: drPayload([]) }]);
    expect((decodeEventFrames(buf, 1)[0] as BrkDrEvent).reports).toEqual([]);
  });

  test("one DR hand-built byte for byte", () => {
    const buf = new Uint8Array([
      1, // DR
      34, 0, 0, 0, // payload_len = 4 (u32 n) + 30 (one report)
      1, 0, 0, 0, // u32 n = 1
      7, 0, 0, 0, 0, 0, 0, 0, // u64 opaque_id = 7
      0, 0, // i16 err = 0
      2, 0, 0, 0, // i32 partition = 2
      5, 0, 0, 0, 0, 0, 0, 0, // i64 offset = 5
      0, 0, 0, 0, 0, 0, 0, 0, // i64 timestamp = 0
    ]);
    const [event] = decodeEventFrames(buf, 1) as [BrkDrEvent];
    expect(event.reports[0]).toEqual({
      opaqueId: 7n,
      err: 0,
      partition: 2,
      offset: 5,
      timestamp: 0,
    });
  });
});

describe("payload ERROR", () => {
  test("ordinary and fatal errors", () => {
    const buf = encodeEventFrames([
      { type: BRK_EVENT_ERROR, payload: errorPayload(-195, false, "Broker transport failure") },
      { type: BRK_EVENT_ERROR, payload: errorPayload(-150, true, "fatal-ish error: 🔥") },
    ]);
    const [normal, fatal] = decodeEventFrames(buf, 2) as [BrkErrorEvent, BrkErrorEvent];
    expect(normal).toEqual({
      type: BRK_EVENT_ERROR,
      code: -195,
      isFatal: false,
      reason: "Broker transport failure",
    });
    expect(fatal.isFatal).toBe(true);
    expect(fatal.reason).toBe("fatal-ish error: 🔥");
  });
});

describe("payload LOG", () => {
  test("level, fac, and message (msg uses a u32 len)", () => {
    const long = "x".repeat(70_000); // > u16 → proves msg_len is a u32
    const buf = encodeEventFrames([
      { type: BRK_EVENT_LOG, payload: logPayload(7, "FETCH", long) },
    ]);
    const [event] = decodeEventFrames(buf, 1) as [BrkLogEvent];
    expect(event.level).toBe(7);
    expect(event.fac).toBe("FETCH");
    expect(event.message).toHaveLength(70_000);
  });
});

describe("payload STATS", () => {
  test("JSON passes through verbatim", () => {
    const json = '{"name":"rdkafka#producer-1","ts":123,"größe":"ß"}';
    const buf = encodeEventFrames([{ type: BRK_EVENT_STATS, payload: statsPayload(json) }]);
    expect((decodeEventFrames(buf, 1)[0] as BrkStatsEvent).json).toBe(json);
  });
});

describe("payload REBALANCE", () => {
  test("ASSIGN_PARTITIONS + COOPERATIVE protocol + tpl by name", () => {
    const buf = encodeEventFrames([
      {
        type: BRK_EVENT_REBALANCE,
        payload: rebalancePayload(-175, BRK_REBALANCE_PROTOCOL_COOPERATIVE, [
          { topic: "orders", partition: 0, offset: -1001 },
          { topic: "orders", partition: 1, offset: 42, leaderEpoch: 3 },
        ]),
      },
    ]);
    const [event] = decodeEventFrames(buf, 1) as [BrkRebalanceEvent];
    expect(event.code).toBe(-175);
    expect(event.protocol).toBe(BRK_REBALANCE_PROTOCOL_COOPERATIVE);
    expect(event.partitions).toEqual([
      { topic: "orders", topicId: -1, partition: 0, offset: -1001, leaderEpoch: -1, metadata: null },
      { topic: "orders", topicId: -1, partition: 1, offset: 42, leaderEpoch: 3, metadata: null },
    ]);
  });

  test("REVOKE_PARTITIONS + protocol EAGER + tpl theo topic_id", () => {
    const topics = new TopicNameTable();
    topics.set(3, "注文-topic");
    const buf = encodeEventFrames([
      {
        type: BRK_EVENT_REBALANCE,
        payload: rebalancePayload(-174, BRK_REBALANCE_PROTOCOL_EAGER, [
          { topicId: 3, partition: 7 },
        ]),
      },
    ]);
    const [event] = decodeEventFrames(buf, 1, { topics }) as [BrkRebalanceEvent];
    expect(event.code).toBe(-174);
    expect(event.protocol).toBe(BRK_REBALANCE_PROTOCOL_EAGER);
    expect(event.partitions[0]!.topic).toBe("注文-topic");
  });

  test("the assignment_lost flag decodes correctly (REVOKE after fence/timeout)", () => {
    const buf = encodeEventFrames([
      {
        type: BRK_EVENT_REBALANCE,
        payload: rebalancePayload(-174, BRK_REBALANCE_PROTOCOL_EAGER, [], true),
      },
      {
        type: BRK_EVENT_REBALANCE,
        payload: rebalancePayload(-175, BRK_REBALANCE_PROTOCOL_EAGER, []),
      },
    ]);
    const events = decodeEventFrames(buf, 2) as [BrkRebalanceEvent, BrkRebalanceEvent];
    expect(events[0].assignmentLost).toBe(true);
    expect(events[1].assignmentLost).toBe(false);
  });

  test("a rebalance with an empty tpl", () => {
    const buf = encodeEventFrames([
      {
        type: BRK_EVENT_REBALANCE,
        payload: rebalancePayload(-174, BRK_REBALANCE_PROTOCOL_EAGER, []),
      },
    ]);
    expect((decodeEventFrames(buf, 1)[0] as BrkRebalanceEvent).partitions).toEqual([]);
  });
});

describe("payload OFFSET_COMMIT", () => {
  test("error code + the committed offset list", () => {
    const buf = encodeEventFrames([
      {
        type: BRK_EVENT_OFFSET_COMMIT,
        payload: offsetCommitPayload(0, [{ topic: "orders", partition: 2, offset: 900 }]),
      },
    ]);
    const [event] = decodeEventFrames(buf, 1) as [BrkOffsetCommitEvent];
    expect(event.code).toBe(0);
    expect(event.partitions).toEqual([
      { topic: "orders", topicId: -1, partition: 2, offset: 900, leaderEpoch: -1, metadata: null },
    ]);
  });
});

describe("payload THROTTLE", () => {
  test("broker id/name and the throttle time", () => {
    const buf = encodeEventFrames([
      { type: BRK_EVENT_THROTTLE, payload: throttlePayload(1001, 250, "broker-1:9092/1001") },
    ]);
    expect(decodeEventFrames(buf, 1)[0] as BrkThrottleEvent).toEqual({
      type: BRK_EVENT_THROTTLE,
      brokerId: 1001,
      throttleMs: 250,
      brokerName: "broker-1:9092/1001",
    });
  });
});

describe("payload OAUTH_REFRESH", () => {
  test("an empty cfg is still valid", () => {
    const buf = encodeEventFrames([
      { type: BRK_EVENT_OAUTH_REFRESH, payload: oauthRefreshPayload("") },
    ]);
    expect((decodeEventFrames(buf, 1)[0] as BrkOauthRefreshEvent).oauthbearerConfig).toBe("");
  });
});

describe("payload ADMIN_RESULT", () => {
  test("a large u64 correlation_id keeps full precision", () => {
    const correlationId = 18_446_744_073_709_551_615n;
    const json = '{"topics":[{"name":"注文-topic","error":0}]}';
    const buf = encodeEventFrames([
      { type: BRK_EVENT_ADMIN_RESULT, payload: adminResultPayload(correlationId, -186, json) },
    ]);
    expect(decodeEventFrames(buf, 1)[0] as BrkAdminResultEvent).toEqual({
      type: BRK_EVENT_ADMIN_RESULT,
      correlationId,
      code: -186,
      json,
    });
  });
});
