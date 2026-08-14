/**
 * Smoke test for the native shim via RAW bun:ffi (bypassing the package's TS
 * layer — the point is verifying the ABI/binary independently). No broker
 * needed: a fake bootstrap proves the event path (ERROR/LOG) is alive.
 *
 * Skipped when native is unbuilt (BUN_RDKAFKA_LIB_PATH or native/build).
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { CString, dlopen, FFIType, ptr, suffix } from "bun:ffi";
import type { Pointer as FFIPointer } from "bun:ffi";
import {
  createTopic,
  integrationAvailable,
  startKafka,
  stopKafka,
} from "./docker-kafka";

const LIB_PATH =
  process.env.BUN_RDKAFKA_LIB_PATH ??
  new URL(`../../native/build/libbunrdkafka.${suffix}`, import.meta.url)
    .pathname;

const hasLib = existsSync(LIB_PATH);
const brokerOk = hasLib && (await integrationAvailable());

const EVENT = { DR: 1, ERROR: 2, LOG: 3, STATS: 4 } as const;

describe.skipIf(!hasLib)("native shim smoke", () => {
  const lib = hasLib
    ? dlopen(LIB_PATH, {
        brk_abi_version: { args: [], returns: FFIType.i32 },
        brk_librdkafka_version: { args: [], returns: FFIType.cstring },
        brk_conf_new: { args: [], returns: FFIType.ptr },
        brk_conf_destroy: { args: [FFIType.ptr], returns: FFIType.void },
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
        brk_metadata: {
          args: [FFIType.ptr, FFIType.cstring, FFIType.i32, FFIType.ptr],
          returns: FFIType.i32,
        },
        brk_mem_free: { args: [FFIType.ptr], returns: FFIType.void },
      })
    : (undefined as never);

  const cstr = (s: string) => Buffer.from(s + "\0");
  const errbuf = new Uint8Array(512);

  function confSet(conf: Pointer, k: string, v: string): number {
    return lib.symbols.brk_conf_set(conf, cstr(k), cstr(v), ptr(errbuf), 512);
  }
  type Pointer = NonNullable<ReturnType<typeof lib.symbols.brk_conf_new>>;

  test("abi + librdkafka version", () => {
    expect(lib.symbols.brk_abi_version()).toBe(1);
    const v = lib.symbols.brk_librdkafka_version();
    expect(String(v)).toMatch(/^2\.15\./);
  });

  test("conf set: the valid + the error path", () => {
    const conf = lib.symbols.brk_conf_new()!;
    expect(confSet(conf, "client.id", "smoke")).toBe(0);
    expect(confSet(conf, "linger.ms", "5")).toBe(0);
    // topic-level properties share the function (the shim routes them)
    expect(confSet(conf, "acks", "all")).toBe(0);
    // a nonexistent property → kafka INVALID_ARG error (-1000 + -186 = -1186)
    const bad = confSet(conf, "does.not.exist", "x");
    expect(bad).toBeLessThan(0);
    expect(new CString(ptr(errbuf)).toString()).toContain("does.not.exist");
    lib.symbols.brk_conf_destroy(conf);
  });

  test("producer: the events path + produce into the queue + clean destroy", async () => {
    const conf = lib.symbols.brk_conf_new()!;
    expect(confSet(conf, "bootstrap.servers", "localhost:19099")).toBe(0);
    expect(confSet(conf, "statistics.interval.ms", "100")).toBe(0);
    expect(confSet(conf, "message.timeout.ms", "2000")).toBe(0);
    const h = lib.symbols.brk_client_new(0, conf, ptr(errbuf), 512);
    expect(h).not.toBeNull();

    // ---- produce 3 messages (PRODUCE BATCH format 3 encoded by hand:
    //      key/value as (u64 ptr, i32 len), the 1-copy path) ----
    const topic = Buffer.from("smoke-topic");
    const value = Buffer.from("hello bun-rdkafka"); // must live across the FFI call
    const valuePtr = BigInt(ptr(value));
    const recSize = 2 + topic.length + 4 + 8 + 8 + (8 + 4) + (8 + 4) + 2;
    const inBuf = new Uint8Array(4 + 3 * recSize);
    const dv = new DataView(inBuf.buffer);
    let off = 0;
    dv.setUint32(off, 3, true); off += 4;
    for (let i = 0; i < 3; i++) {
      dv.setUint16(off, topic.length, true); off += 2;
      inBuf.set(topic, off); off += topic.length;
      dv.setInt32(off, -1, true); off += 4;          // partition UA
      dv.setBigInt64(off, 0n, true); off += 8;        // ts = now
      dv.setBigUint64(off, BigInt(100 + i), true); off += 8; // opaque_id
      dv.setBigUint64(off, 0n, true); off += 8;       // key_ptr = 0
      dv.setInt32(off, -1, true); off += 4;           // key_len = -1 (null)
      dv.setBigUint64(off, valuePtr, true); off += 8; // value_ptr
      dv.setInt32(off, value.length, true); off += 4; // value_len
      dv.setUint16(off, 0, true); off += 2;           // headers
    }
    const errOut = new Int16Array(3);
    const n = lib.symbols.brk_produce_batch(h, ptr(inBuf), off, ptr(errOut), 3);
    expect(n).toBe(3);
    expect([...errOut]).toEqual([0, 0, 0]);
    expect(lib.symbols.brk_client_outq_len(h)).toBeGreaterThan(0);

    // ---- events: broker down ⇒ ERROR must appear; stats on ⇒ STATS appears ----
    const evBuf = new Uint8Array(256 * 1024);
    const seen = new Set<number>();
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline && !(seen.has(EVENT.ERROR) && seen.has(EVENT.STATS))) {
      const frames = lib.symbols.brk_events_poll(h, ptr(evBuf), evBuf.length, 64, 0);
      expect(frames).toBeGreaterThanOrEqual(0);
      const edv = new DataView(evBuf.buffer);
      let p = 0;
      for (let f = 0; f < frames; f++) {
        const type = edv.getUint8(p);
        const len = edv.getUint32(p + 1, true);
        seen.add(type);
        p += 5 + len;
      }
      await Bun.sleep(20);
    }
    expect(seen.has(EVENT.ERROR)).toBe(true); // ALL_BROKERS_DOWN qua event path
    expect(seen.has(EVENT.STATS)).toBe(true);

    lib.symbols.brk_client_destroy(h);
  }, 15000);

  /** Calls brk_metadata: out is a char** passed via a BigUint64Array(1). */
  function callMetadata(h: Pointer, topic: string | null, timeoutMs: number) {
    const outHolder = new BigUint64Array(1);
    const ret = lib.symbols.brk_metadata(
      h,
      topic === null ? null : cstr(topic),
      timeoutMs,
      ptr(outHolder),
    );
    const p = Number(outHolder[0]);
    let json: string | null = null;
    if (ret >= 0 && p !== 0) {
      json = new CString(p as FFIPointer).toString();
      lib.symbols.brk_mem_free(p as FFIPointer);
    }
    return { ret, json };
  }

  test("metadata: broker down → a negative error per convention, no crash (twice)", () => {
    const conf = lib.symbols.brk_conf_new()!;
    expect(confSet(conf, "bootstrap.servers", "localhost:19099")).toBe(0);
    const h = lib.symbols.brk_client_new(0, conf, ptr(errbuf), 512)!;
    for (let i = 0; i < 2; i++) {
      const { ret, json } = callMetadata(h, null, 1200);
      expect(ret).toBeLessThan(0); // __TRANSPORT/__TIMED_OUT in the BRK_KAFKA_ERR range
      expect(ret).toBeLessThanOrEqual(-800);
      expect(json).toBeNull();
    }
    lib.symbols.brk_client_destroy(h);
  }, 15000);

  describe.skipIf(!brokerOk)("metadata with a real broker", () => {
    let brokers = "localhost:9092";
    beforeAll(async () => {
      brokers = (await startKafka()).brokers;
      await createTopic("smoke-md-topic", 2);
    }, 180_000);
    afterAll(() => stopKafka());

    test("NULL → all topics; a specific topic → the exact node-rdkafka shape", () => {
      const conf = lib.symbols.brk_conf_new()!;
      expect(confSet(conf, "bootstrap.servers", brokers)).toBe(0);
      const h = lib.symbols.brk_client_new(0, conf, ptr(errbuf), 512)!;

      for (let i = 0; i < 2; i++) {
        // twice in a row: no leak, no crash
        const all = callMetadata(h, null, 10_000);
        expect(all.ret).toBeGreaterThan(0);
        const md = JSON.parse(all.json!);
        expect(typeof md.orig_broker_id).toBe("number");
        expect(typeof md.orig_broker_name).toBe("string");
        expect(md.brokers.length).toBeGreaterThanOrEqual(1);
        expect(typeof md.brokers[0].host).toBe("string");
        expect(typeof md.brokers[0].port).toBe("number");
        expect(md.topics.map((t: { name: string }) => t.name)).toContain(
          "smoke-md-topic",
        );
      }

      const one = callMetadata(h, "smoke-md-topic", 10_000);
      expect(one.ret).toBeGreaterThan(0);
      const md1 = JSON.parse(one.json!);
      expect(md1.topics.length).toBe(1);
      expect(md1.topics[0].name).toBe("smoke-md-topic");
      expect(md1.topics[0].partitions.length).toBe(2);
      const p0 = md1.topics[0].partitions[0];
      expect(typeof p0.id).toBe("number");
      expect(typeof p0.leader).toBe("number");
      expect(Array.isArray(p0.replicas)).toBe(true);
      expect(Array.isArray(p0.isrs)).toBe(true);

      lib.symbols.brk_client_destroy(h);
    }, 30_000);
  });
});
