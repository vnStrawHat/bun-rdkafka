import { describe, expect, test } from "bun:test";
import type { DeliveryReport } from "../../packages/bun-rdkafka/src/core/batch-decoder.ts";
import {
  DEFAULT_MAX_PENDING,
  DeliveryLedger,
} from "../../packages/bun-rdkafka/src/core/delivery-ledger.ts";
import {
  ERROR_CODES,
  LibrdKafkaError,
} from "../../packages/bun-rdkafka/src/core/errors.ts";

interface Rec {
  topic: string;
}

const report = (over: Partial<DeliveryReport> & { opaqueId: bigint }): DeliveryReport => ({
  err: 0,
  partition: 0,
  offset: 0,
  timestamp: 0,
  ...over,
});

describe("opaque_id assignment", () => {
  test("ids increase starting at 1", () => {
    const ledger = new DeliveryLedger<Rec>();
    expect(ledger.nextId).toBe(1n);
    const a = ledger.track({ topic: "t" }, () => {}, () => {});
    const b = ledger.track({ topic: "t" }, () => {}, () => {});
    expect([a, b]).toEqual([1n, 2n]);
    expect(ledger.nextId).toBe(3n);
  });

  test("custom startId, wrapping at the u64 boundary", () => {
    const ledger = new DeliveryLedger<Rec>({ startId: (1n << 64n) - 1n });
    expect(ledger.track({ topic: "t" }, () => {}, () => {})).toBe((1n << 64n) - 1n);
    expect(ledger.nextId).toBe(0n);
  });
});

describe("resolve theo DR payload", () => {
  test("a successful DR resolves with the original record", () => {
    const ledger = new DeliveryLedger<Rec>();
    const record = { topic: "orders" };
    let result: unknown;
    const id = ledger.track(record, (r) => (result = r), () => {});
    expect(ledger.pending).toBe(1);

    expect(
      ledger.settle(report({ opaqueId: id, partition: 3, offset: 77, timestamp: 1234 })),
    ).toBe(true);
    expect(result).toEqual({
      opaqueId: id,
      partition: 3,
      offset: 77,
      timestamp: 1234,
      record,
    });
    expect(ledger.pending).toBe(0);
  });

  test("a failed DR rejects with a LibrdKafkaError carrying the code", () => {
    const ledger = new DeliveryLedger<Rec>();
    let error: LibrdKafkaError | undefined;
    const id = ledger.track({ topic: "t" }, () => {}, (e) => (error = e));
    ledger.settle(report({ opaqueId: id, err: ERROR_CODES.ERR__MSG_TIMED_OUT }));
    expect(error).toBeInstanceOf(LibrdKafkaError);
    expect(error!.code).toBe(-192);
    expect(error!.origin).toBe("local");
    expect(error!.message).toBe("Produced message timed out");
  });

  test("a stray DR (opaque_id not in the ledger) returns false, never throws", () => {
    const ledger = new DeliveryLedger<Rec>();
    expect(ledger.settle(report({ opaqueId: 999n }))).toBe(false);
  });

  test("settleAll counts the matched DRs", () => {
    const ledger = new DeliveryLedger<Rec>();
    const a = ledger.track({ topic: "t" }, () => {}, () => {});
    const b = ledger.track({ topic: "t" }, () => {}, () => {});
    const matched = ledger.settleAll([
      report({ opaqueId: a }),
      report({ opaqueId: b }),
      report({ opaqueId: 12345n }),
    ]);
    expect(matched).toBe(2);
    expect(ledger.pending).toBe(0);
  });

  test("trackPromise resolves/rejects correctly", async () => {
    const ledger = new DeliveryLedger<Rec>();
    const ok = ledger.trackPromise({ topic: "t" });
    ledger.settle(report({ opaqueId: ok.opaqueId, offset: 5 }));
    await expect(ok.promise).resolves.toMatchObject({ offset: 5 });

    const bad = ledger.trackPromise({ topic: "t" });
    ledger.settle(report({ opaqueId: bad.opaqueId, err: ERROR_CODES.ERR__PARTITION_EOF }));
    await expect(bad.promise).rejects.toBeInstanceOf(LibrdKafkaError);
  });
});

describe("backpressure", () => {
  test("defaults to librdkafka's queue.buffering.max.messages", () => {
    expect(new DeliveryLedger().maxPending).toBe(DEFAULT_MAX_PENDING);
    expect(DEFAULT_MAX_PENDING).toBe(100_000);
  });

  test("exceeding maxPending throws ERR__QUEUE_FULL", () => {
    const ledger = new DeliveryLedger<Rec>({ maxPending: 2 });
    ledger.track({ topic: "t" }, () => {}, () => {});
    const second = ledger.track({ topic: "t" }, () => {}, () => {});
    expect(ledger.isFull).toBe(true);

    let thrown: unknown;
    try {
      ledger.track({ topic: "t" }, () => {}, () => {});
    } catch (err) {
      thrown = err;
    }
    expect(thrown).toBeInstanceOf(LibrdKafkaError);
    expect((thrown as LibrdKafkaError).code).toBe(ERROR_CODES.ERR__QUEUE_FULL);
    expect((thrown as LibrdKafkaError).code).toBe(-184);

    // Freeing one slot accepts a new record again.
    ledger.settle(report({ opaqueId: second }));
    expect(ledger.isFull).toBe(false);
    expect(() => ledger.track({ topic: "t" }, () => {}, () => {})).not.toThrow();
  });
});

describe("bulk cancellation", () => {
  test("failAll rejects every pending record", () => {
    const ledger = new DeliveryLedger<Rec>();
    const errors: LibrdKafkaError[] = [];
    ledger.track({ topic: "t" }, () => {}, (e) => errors.push(e));
    ledger.track({ topic: "t" }, () => {}, (e) => errors.push(e));
    const err = LibrdKafkaError.fromKafkaCode(ERROR_CODES.ERR__DESTROY);
    expect(ledger.failAll(err)).toBe(2);
    expect(errors).toEqual([err, err]);
    expect(ledger.pending).toBe(0);
  });

  test("fails one specific record", () => {
    const ledger = new DeliveryLedger<Rec>();
    let rejected = false;
    const id = ledger.track({ topic: "t" }, () => {}, () => (rejected = true));
    expect(ledger.has(id)).toBe(true);
    expect(ledger.peek(id)).toEqual({ topic: "t" });
    expect(ledger.fail(id, LibrdKafkaError.fromKafkaCode(-184))).toBe(true);
    expect(rejected).toBe(true);
    expect(ledger.fail(id, LibrdKafkaError.fromKafkaCode(-184))).toBe(false);
  });
});
