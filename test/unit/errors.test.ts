import { describe, expect, test } from "bun:test";
import {
  CODES,
  ERROR_CODES,
  ERROR_DESCRIPTIONS,
  ERROR_NAMES,
  ERRSTR_FLAG_RETRIABLE,
  ERRSTR_FLAG_TXN_REQUIRES_ABORT,
  LibrdKafkaError,
  decodeReturn,
  errorDescription,
  errorName,
  isLocalError,
  parseErrstrFlags,
  throwOnError,
} from "../../packages/bun-rdkafka/src/core/errors.ts";
import {
  BRK_ERR_BUFFER_TOO_SMALL,
  BRK_ERR_DECODE,
  brkKafkaErr,
} from "../../packages/bun-rdkafka/src/ffi/types.ts";

describe("the error-code table (rdkafka.h v2.15.0)", () => {
  test("common codes have the right values", () => {
    expect(ERROR_CODES.ERR_NO_ERROR).toBe(0);
    expect(ERROR_CODES.ERR__QUEUE_FULL).toBe(-184);
    expect(ERROR_CODES.ERR__TIMED_OUT).toBe(-185);
    expect(ERROR_CODES.ERR__PARTITION_EOF).toBe(-191);
    expect(ERROR_CODES.ERR__ALL_BROKERS_DOWN).toBe(-187);
    expect(ERROR_CODES.ERR__MSG_TIMED_OUT).toBe(-192);
    expect(ERROR_CODES.ERR__TRANSPORT).toBe(-195);
    expect(ERROR_CODES.ERR__ASSIGN_PARTITIONS).toBe(-175);
    expect(ERROR_CODES.ERR__REVOKE_PARTITIONS).toBe(-174);
    expect(ERROR_CODES.ERR__FATAL).toBe(-150);
    expect(ERROR_CODES.ERR__STATE).toBe(-172);
    expect(ERROR_CODES.ERR__INVALID_ARG).toBe(-186);
    expect(ERROR_CODES.ERR__BEGIN).toBe(-200);
    expect(ERROR_CODES.ERR__END).toBe(-100);
    expect(ERROR_CODES.ERR_UNKNOWN).toBe(-1);
    expect(ERROR_CODES.ERR_OFFSET_OUT_OF_RANGE).toBe(1);
    expect(ERROR_CODES.ERR_GROUP_AUTHORIZATION_FAILED).toBe(30);
  });

  test("CODES.ERRORS is the public-API table (like upstream)", () => {
    expect(CODES.ERRORS).toBe(ERROR_CODES);
    expect(CODES.ERRORS.ERR__TIMED_OUT).toBe(-185);
  });

  test("constant names are unique; only upstream's 3 legacy aliases share values", () => {
    const values = Object.values(ERROR_CODES);
    // Upstream (types/errors.d.ts) keeps both legacy and new names for the 3
    // coordinator codes 14/15/16 — the conformance test requires all 6 names.
    const legacyAliases = {
      ERR_GROUP_LOAD_IN_PROGRESS: ERROR_CODES.ERR_COORDINATOR_LOAD_IN_PROGRESS,
      ERR_GROUP_COORDINATOR_NOT_AVAILABLE: ERROR_CODES.ERR_COORDINATOR_NOT_AVAILABLE,
      ERR_NOT_COORDINATOR_FOR_GROUP: ERROR_CODES.ERR_NOT_COORDINATOR,
    } as const;
    for (const [name, code] of Object.entries(legacyAliases)) {
      expect(ERROR_CODES[name as keyof typeof ERROR_CODES]).toBe(code);
    }
    expect(new Set(values).size).toBe(values.length - Object.keys(legacyAliases).length);
    expect(values.length).toBeGreaterThan(150);
    // The reverse mapping still prefers the new names (declared first).
    expect(errorName(15)).toBe("ERR_COORDINATOR_NOT_AVAILABLE");
  });

  test("ERROR_NAMES is the reverse mapping", () => {
    expect(errorName(-185)).toBe("ERR__TIMED_OUT");
    expect(ERROR_NAMES[-184]).toBe("ERR__QUEUE_FULL");
    expect(errorName(999_999)).toBeUndefined();
  });

  test("descriptions come from the header's doc comments", () => {
    expect(ERROR_DESCRIPTIONS.get(-184)).toBe("Queue is full");
    expect(errorDescription(-191)).toContain("Reached the end of the topic+partition queue");
    expect(errorDescription(999_999)).toBe("Unknown error 999999");
  });

  test("distinguishes local (internal) errors from broker errors", () => {
    expect(isLocalError(ERROR_CODES.ERR__TIMED_OUT)).toBe(true);
    expect(isLocalError(ERROR_CODES.ERR__END)).toBe(true);
    expect(isLocalError(ERROR_CODES.ERR_UNKNOWN)).toBe(false);
    expect(isLocalError(ERROR_CODES.ERR_NO_ERROR)).toBe(false);
  });
});

describe("LibrdKafkaError", () => {
  test("shape matches upstream: message/code/errno/origin/isFatal/…", () => {
    const err = LibrdKafkaError.fromKafkaCode(ERROR_CODES.ERR__QUEUE_FULL);
    expect(err).toBeInstanceOf(Error);
    expect(err.message).toBe("Queue is full");
    expect(err.code).toBe(-184);
    expect(err.errno).toBe(-184);
    expect(err.origin).toBe("local");
    expect(err.isFatal).toBe(false);
    expect(err.isRetriable).toBe(false);
    expect(err.isTxnRequiresAbort).toBe(false);
    expect(err.toJSON()).toMatchObject({ code: -184, origin: "local" });
  });

  test("broker errors carry origin kafka", () => {
    const err = LibrdKafkaError.fromKafkaCode(ERROR_CODES.ERR_OFFSET_OUT_OF_RANGE);
    expect(err.origin).toBe("kafka");
  });

  test("ERR__FATAL defaults to isFatal = true", () => {
    expect(LibrdKafkaError.fromKafkaCode(ERROR_CODES.ERR__FATAL).isFatal).toBe(true);
  });

  test("a custom message (from brk_last_error_string) beats the default description", () => {
    const err = LibrdKafkaError.fromKafkaCode(-185, "Local: Timed out (detail from the shim)");
    expect(err.message).toBe("Local: Timed out (detail from the shim)");
  });
});

describe("decoding brk_* return values", () => {
  test("ret >= 0 is not an error", () => {
    expect(decodeReturn(0)).toBeUndefined();
    expect(decodeReturn(42)).toBeUndefined();
    expect(throwOnError(7, "op")).toBe(7);
  });

  test("ret in the kafka range → LibrdKafkaError with the extracted code", () => {
    const err = decodeReturn(brkKafkaErr(ERROR_CODES.ERR__TIMED_OUT), "brk_flush")!;
    expect(err.code).toBe(-185);
    expect(err.origin).toBe("local");
    expect(err.context).toBe("brk_flush");
    expect(err.message).toBe("Operation timed out");
  });

  test("ret in the shim range → origin shim, code keeping the BRK_ERR_* value", () => {
    const err = decodeReturn(BRK_ERR_BUFFER_TOO_SMALL, "brk_consume_batch")!;
    expect(err.origin).toBe("shim");
    expect(err.code).toBe(-1);
    expect(err.message).toContain("BRK_ERR_BUFFER_TOO_SMALL");

    // The shim's -1 is NOT mistaken for librdkafka's ERR_UNKNOWN (-1).
    expect(err.origin).not.toBe("kafka");
    expect(decodeReturn(BRK_ERR_DECODE)!.message).toContain("BRK_ERR_DECODE");
  });

  test("a negative ret outside every range still yields a clear error", () => {
    const err = decodeReturn(-500, "brk_bogus")!;
    expect(err.origin).toBe("shim");
    expect(err.message).toContain("-500");
  });

  test("throwOnError throws the right kind", () => {
    expect(() => throwOnError(brkKafkaErr(-184), "brk_produce_batch")).toThrow(LibrdKafkaError);
  });
});

describe("transaction flags embedded in the errstr (contract with shim_producer.c)", () => {
  test("parses and strips the markers from the message", () => {
    const parsed = parseErrstrFlags(
      `Failed to commit transaction ${ERRSTR_FLAG_TXN_REQUIRES_ABORT} ${ERRSTR_FLAG_RETRIABLE}`,
    );
    expect(parsed.message).toBe("Failed to commit transaction");
    expect(parsed.isTxnRequiresAbort).toBe(true);
    expect(parsed.isRetriable).toBe(true);
  });

  test("without markers, both flags are false", () => {
    expect(parseErrstrFlags("Broker: Coordinator not available")).toEqual({
      message: "Broker: Coordinator not available",
      isRetriable: false,
      isTxnRequiresAbort: false,
    });
  });

  test("fromErrstr attaches the flags to the error", () => {
    const err = LibrdKafkaError.fromErrstr(
      brkKafkaErr(ERROR_CODES.ERR__TIMED_OUT),
      "brk_commit_transaction",
      `timed out ${ERRSTR_FLAG_RETRIABLE}`,
    );
    expect(err.code).toBe(-185);
    expect(err.isRetriable).toBe(true);
    expect(err.isTxnRequiresAbort).toBe(false);
    expect(err.message).toBe("brk_commit_transaction: timed out");
    expect(err.context).toBe("brk_commit_transaction");
  });

  test("fromErrstr with an empty errstr uses the default description", () => {
    const err = LibrdKafkaError.fromErrstr(brkKafkaErr(-185), "brk_flush", "");
    expect(err.message).toBe("Operation timed out");
  });
});
