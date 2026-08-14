/**
 * Admin API integration via RAW bun:ffi (verifying the ABI independently of
 * the TS layer). Flow: create a 3-partition topic → describe/list sees it →
 * create_partitions to 5 → produce 5 msgs → delete_records before offset 3 →
 * delete the topic. ADMIN_RESULT must arrive with the right correlation_id
 * (event 9, payload:
 * u64 corr, i32 kafka_err, u32 json_len, json — schema: shim_admin.c).
 *
 * Broker: the docker-kafka helper (KEEP_KAFKA=1 when run with the suite).
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { dlopen, FFIType, ptr, suffix } from "bun:ffi";
import { integrationAvailable, startKafka, stopKafka } from "./docker-kafka";

const LIB_PATH =
  process.env.BUN_RDKAFKA_LIB_PATH ??
  new URL(`../../native/build/libbunrdkafka.${suffix}`, import.meta.url)
    .pathname;

const hasLib = existsSync(LIB_PATH);
const brokerOk = hasLib && (await integrationAvailable());

const OP = {
  CREATE_TOPICS: 1,
  DELETE_TOPICS: 2,
  CREATE_PARTITIONS: 3,
  LIST_GROUPS: 4,
  DELETE_RECORDS: 8,
  DESCRIBE_TOPICS: 9,
} as const;

describe.skipIf(!brokerOk)("native admin (raw bun:ffi)", () => {
  const lib = hasLib
    ? dlopen(LIB_PATH, {
        brk_conf_new: { args: [], returns: FFIType.ptr },
        brk_conf_set: {
          args: [FFIType.ptr, FFIType.cstring, FFIType.cstring, FFIType.ptr, FFIType.i32],
          returns: FFIType.i32,
        },
        brk_client_new: {
          args: [FFIType.i32, FFIType.ptr, FFIType.ptr, FFIType.i32],
          returns: FFIType.ptr,
        },
        brk_client_destroy: { args: [FFIType.ptr], returns: FFIType.void },
        brk_client_outq_len: { args: [FFIType.ptr], returns: FFIType.i32 },
        brk_events_poll: {
          args: [FFIType.ptr, FFIType.ptr, FFIType.i32, FFIType.i32, FFIType.i32],
          returns: FFIType.i32,
        },
        brk_produce_batch: {
          args: [FFIType.ptr, FFIType.ptr, FFIType.i32, FFIType.ptr, FFIType.i32],
          returns: FFIType.i32,
        },
        brk_admin_request: {
          args: [FFIType.ptr, FFIType.i32, FFIType.u64, FFIType.cstring],
          returns: FFIType.i32,
        },
        brk_last_error_string: {
          args: [FFIType.ptr, FFIType.ptr, FFIType.i32],
          returns: FFIType.i32,
        },
      })
    : (undefined as never);

  type Pointer = NonNullable<ReturnType<typeof lib.symbols.brk_conf_new>>;
  const cstr = (s: string) => Buffer.from(s + "\0");
  const errbuf = new Uint8Array(512);
  const evbuf = new Uint8Array(256 * 1024);

  let client: Pointer;
  const TOPIC = `m4c-admin-${Date.now()}`;

  beforeAll(async () => {
    const kafka = await startKafka();
    const conf = lib.symbols.brk_conf_new()!;
    lib.symbols.brk_conf_set(
      conf, cstr("bootstrap.servers"), cstr(kafka.brokers), ptr(errbuf), 512,
    );
    client = lib.symbols.brk_client_new(0, conf, ptr(errbuf), 512)!;
    expect(client).toBeTruthy();
  });

  afterAll(() => {
    if (client) lib.symbols.brk_client_destroy(client);
    return stopKafka(); // honors KEEP_KAFKA=1
  });

  /** Polls until an ADMIN_RESULT matching corr arrives; other frames are skipped. */
  function awaitAdminResult(corr: bigint, timeoutMs = 15_000) {
    const dv = () => new DataView(evbuf.buffer, evbuf.byteOffset);
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const n = lib.symbols.brk_events_poll(client, ptr(evbuf), evbuf.length, 64, 100);
      expect(n).toBeGreaterThanOrEqual(0);
      let off = 0;
      for (let i = 0; i < n; i++) {
        const type = dv().getUint8(off);
        const plen = dv().getUint32(off + 1, true);
        if (type === 9) {
          const c = dv().getBigUint64(off + 5, true);
          const kerr = dv().getInt32(off + 13, true);
          const jlen = dv().getUint32(off + 17, true);
          const json = JSON.parse(
            Buffer.from(evbuf.subarray(off + 21, off + 21 + jlen)).toString("utf8"),
          );
          if (c === corr) return { kerr, json };
          throw new Error(`ADMIN_RESULT correlation mismatch: expected ${corr}, got ${c}`);
        }
        off += 5 + plen;
      }
    }
    throw new Error(`timed out waiting for ADMIN_RESULT corr=${corr}`);
  }

  function adminOk(op: number, corr: bigint, req: object) {
    const rc = lib.symbols.brk_admin_request(client, op, corr, cstr(JSON.stringify(req)));
    if (rc !== 0) {
      lib.symbols.brk_last_error_string(client, ptr(errbuf), 512);
      throw new Error(`brk_admin_request rc=${rc}: ${Buffer.from(errbuf).toString().split("\0")[0]}`);
    }
    return awaitAdminResult(corr);
  }

  test("creates a 3-partition topic (with config)", () => {
    const { kerr, json } = adminOk(OP.CREATE_TOPICS, 101n, {
      topics: [{ name: TOPIC, num_partitions: 3, replication_factor: 1,
                 config: { "cleanup.policy": "delete" } }],
      timeout_ms: 10_000, operation_timeout_ms: 10_000,
    });
    expect(kerr).toBe(0);
    expect(json.topics).toHaveLength(1);
    expect(json.topics[0].name).toBe(TOPIC);
    expect(json.topics[0].error_code).toBe(0);
  }, 30_000);

  test("describe topics sees 3 partitions; list groups works", () => {
    const { json } = adminOk(OP.DESCRIBE_TOPICS, 102n, {
      topics: [TOPIC], timeout_ms: 10_000,
    });
    expect(json.topics[0].name).toBe(TOPIC);
    expect(json.topics[0].error_code).toBe(0);
    expect(json.topics[0].partitions).toHaveLength(3);
    expect(json.topics[0].partitions[0].leader.id).toBeGreaterThanOrEqual(0);

    const lg = adminOk(OP.LIST_GROUPS, 103n, { timeout_ms: 10_000 });
    expect(Array.isArray(lg.json.groups)).toBe(true);
    expect(Array.isArray(lg.json.errors)).toBe(true);
  }, 30_000);

  test("create_partitions 3 → 5", () => {
    const { json } = adminOk(OP.CREATE_PARTITIONS, 104n, {
      topics: [{ name: TOPIC, total_count: 5 }],
      timeout_ms: 10_000, operation_timeout_ms: 10_000,
    });
    expect(json.topics[0].error_code).toBe(0);
    const desc = adminOk(OP.DESCRIBE_TOPICS, 105n, { topics: [TOPIC], timeout_ms: 10_000 });
    expect(desc.json.topics[0].partitions).toHaveLength(5);
  }, 30_000);

  test("produces 5 msgs then delete_records before offset 3 → low_watermark 3", () => {
    // PRODUCE BATCH (format 3, pointer-based key/value): 5 records → partition 0.
    // `value` must stay referenced until brk_produce_batch returns (1-copy path).
    const value = Buffer.from("admin-test-value");
    const valuePtr = BigInt(ptr(value));
    const parts: Buffer[] = [Buffer.alloc(4)];
    parts[0]!.writeUInt32LE(5, 0);
    for (let i = 0; i < 5; i++) {
      const rec = Buffer.alloc(2 + TOPIC.length + 4 + 8 + 8 + (8 + 4) + (8 + 4) + 2);
      let o = 0;
      rec.writeUInt16LE(TOPIC.length, o); o += 2;
      rec.write(TOPIC, o); o += TOPIC.length;
      rec.writeInt32LE(0, o); o += 4;            // partition 0
      rec.writeBigInt64LE(0n, o); o += 8;        // timestamp: now
      rec.writeBigUInt64LE(BigInt(i + 1), o); o += 8; // opaque_id
      rec.writeBigUInt64LE(0n, o); o += 8;       // key_ptr = 0
      rec.writeInt32LE(-1, o); o += 4;           // key_len = -1 (null)
      rec.writeBigUInt64LE(valuePtr, o); o += 8; // value_ptr
      rec.writeInt32LE(value.length, o); o += 4; // value_len
      rec.writeUInt16LE(0, o);                   // headers
      parts.push(rec);
    }
    const batch = Buffer.concat(parts);
    const errs = new Int16Array(5);
    const rc = lib.symbols.brk_produce_batch(client, ptr(batch), batch.length, ptr(errs), 5);
    expect(rc).toBe(5);
    expect(Array.from(errs)).toEqual([0, 0, 0, 0, 0]);
    // wait for delivery (awaitAdminResult drains the DRs; outq is the source of truth)
    const dl = Date.now() + 10_000;
    while (lib.symbols.brk_client_outq_len(client) > 0 && Date.now() < dl)
      lib.symbols.brk_events_poll(client, ptr(evbuf), evbuf.length, 64, 50);
    expect(lib.symbols.brk_client_outq_len(client)).toBe(0);

    const { json } = adminOk(OP.DELETE_RECORDS, 106n, {
      partitions: [{ topic: TOPIC, partition: 0, offset: 3 }],
      timeout_ms: 10_000, operation_timeout_ms: 10_000,
    });
    const p0 = json.partitions.find(
      (p: { topic: string; partition: number }) => p.topic === TOPIC && p.partition === 0,
    );
    expect(p0).toBeDefined();
    expect(p0.error_code).toBe(0);
    expect(p0.offset).toBe(3); // the new low watermark
  }, 30_000);

  test("delete topic + independent correlation ids", () => {
    const { kerr, json } = adminOk(OP.DELETE_TOPICS, 107n, {
      topics: [TOPIC], timeout_ms: 10_000, operation_timeout_ms: 10_000,
    });
    expect(kerr).toBe(0);
    expect(json.topics[0].name).toBe(TOPIC);
    expect(json.topics[0].error_code).toBe(0);
  }, 30_000);

  test("an invalid op / broken req returns a synchronous error, no crash", () => {
    expect(lib.symbols.brk_admin_request(client, 999, 200n, cstr("{}"))).toBeLessThan(0);
    expect(
      lib.symbols.brk_admin_request(client, OP.CREATE_TOPICS, 201n, cstr("not json")),
    ).toBeLessThan(0);
    expect(
      lib.symbols.brk_admin_request(client, OP.CREATE_TOPICS, 202n, cstr('{"topics":[]}')),
    ).toBeLessThan(0);
  });
});
