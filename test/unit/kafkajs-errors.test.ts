/**
 * M5a — the KafkaJS error system: subclass selection by code (upstream's
 * `createKafkaJsErrorFromLibRdKafkaError` table), distinguished via
 * `error.code`, with the retriable/fatal/abortable flags.
 */

import { describe, expect, test } from "bun:test";
import { ERROR_CODES, LibrdKafkaError } from "../../packages/bun-rdkafka/src/core/errors.ts";
import {
  ErrorCodes,
  KafkaJSAggregateError,
  KafkaJSConnectionError,
  KafkaJSDeleteTopicRecordsError,
  KafkaJSError,
  KafkaJSNoBrokerAvailableError,
  KafkaJSNotImplemented,
  KafkaJSOffsetOutOfRange,
  KafkaJSProtocolError,
  KafkaJSSASLAuthenticationError,
  KafkaJSTimeout,
  fromLibrdKafkaError,
  isKafkaJSError,
} from "../../packages/bun-rdkafka/src/kafkajs/errors.ts";

function lk(code: number, extra: Partial<Parameters<typeof LibrdKafkaError.fromKafkaCode>[2]> = {}) {
  return LibrdKafkaError.fromKafkaCode(code, undefined, extra);
}

describe("KafkaJSError", () => {
  test("defaults: code ERR_UNKNOWN, type from the code name, flags false", () => {
    const e = new KafkaJSError("boom");
    expect(e.code).toBe(ERROR_CODES.ERR_UNKNOWN);
    expect(e.type).toBe("ERR_UNKNOWN");
    expect(e.retriable).toBe(false);
    expect(e.fatal).toBe(false);
    expect(e.abortable).toBe(false);
    expect(e.name).toBe("KafkaJSError");
  });

  test("type is the error-code NAME (still distinguish by code)", () => {
    const e = new KafkaJSError("x", { code: ERROR_CODES.ERR__TIMED_OUT });
    expect(e.type).toBe("ERR__TIMED_OUT");
    expect(ErrorCodes.ERR__TIMED_OUT).toBe(-185);
  });

  test("accepts an Error as the message", () => {
    expect(new KafkaJSError(new Error("inner")).message).toBe("inner");
  });
});

describe("fromLibrdKafkaError — the subclass selection table", () => {
  const cases: [number, unknown][] = [
    [ERROR_CODES.ERR_OFFSET_OUT_OF_RANGE, KafkaJSOffsetOutOfRange],
    [ERROR_CODES.ERR__AUTHENTICATION, KafkaJSSASLAuthenticationError],
    [ERROR_CODES.ERR__NOT_IMPLEMENTED, KafkaJSNotImplemented],
    [ERROR_CODES.ERR__TIMED_OUT, KafkaJSTimeout],
    [ERROR_CODES.ERR__ALL_BROKERS_DOWN, KafkaJSNoBrokerAvailableError],
    [ERROR_CODES.ERR__TRANSPORT, KafkaJSConnectionError],
  ];
  for (const [code, cls] of cases) {
    test(`code ${code} → ${(cls as { name: string }).name}`, () => {
      const e = fromLibrdKafkaError(lk(code));
      expect(e).toBeInstanceOf(cls as never);
      expect(e.code).toBe(code);
      expect(isKafkaJSError(e)).toBe(true);
    });
  }

  test("code > 0 (protocol) → KafkaJSProtocolError; other local → KafkaJSError", () => {
    expect(fromLibrdKafkaError(lk(ERROR_CODES.ERR_TOPIC_ALREADY_EXISTS))).toBeInstanceOf(
      KafkaJSProtocolError,
    );
    const plain = fromLibrdKafkaError(lk(ERROR_CODES.ERR__STATE));
    expect(plain).toBeInstanceOf(KafkaJSError);
    expect(plain).not.toBeInstanceOf(KafkaJSProtocolError);
  });

  test("flags: retriable ← isRetriable, fatal ← isFatal, abortable ← isTxnRequiresAbort", () => {
    const e = fromLibrdKafkaError(
      new LibrdKafkaError("txn", {
        code: ERROR_CODES.ERR__STATE,
        isRetriable: true,
        isFatal: true,
        isTxnRequiresAbort: true,
      }),
    );
    expect(e.retriable).toBe(true);
    expect(e.fatal).toBe(true);
    expect(e.abortable).toBe(true);
  });
});

describe("aggregate errors", () => {
  test("KafkaJSAggregateError keeps the errors list", () => {
    const agg = new KafkaJSAggregateError("multi", [new KafkaJSError("a")]);
    expect(agg.errors).toHaveLength(1);
    expect(agg.name).toBe("KafkaJSAggregateError");
  });

  test("KafkaJSDeleteTopicRecordsError: retriable when EVERY error is", () => {
    const retriableErr = new KafkaJSError("r", { retriable: true });
    const finalErr = new KafkaJSError("f", { retriable: false });
    expect(
      new KafkaJSDeleteTopicRecordsError({
        partitions: [{ partition: 0, offset: "1", error: retriableErr }],
      }).retriable,
    ).toBe(true);
    expect(
      new KafkaJSDeleteTopicRecordsError({
        partitions: [
          { partition: 0, offset: "1", error: retriableErr },
          { partition: 1, offset: "2", error: finalErr },
        ],
      }).retriable,
    ).toBe(false);
  });
});
