import { describe, expect, test } from "bun:test";
import {
  BinaryDecodeError,
  BinaryEncodeError,
  BufReader,
  BufWriter,
} from "../../packages/bun-rdkafka/src/core/binary.ts";

const UTF8 = new TextEncoder();

describe("BufWriter — little-endian integers", () => {
  test("writes the exact bytes for every type", () => {
    const w = new BufWriter(4);
    w.u8(0xff);
    w.i8(-1);
    w.u16(0x1234);
    w.i16(-2);
    w.u32(0xdeadbeef);
    w.i32(-3);
    w.i64(-4n);
    w.u64(0xfedcba9876543210n);

    const bytes = w.toBytes();
    expect(Array.from(bytes.subarray(0, 2))).toEqual([0xff, 0xff]);
    expect(Array.from(bytes.subarray(2, 4))).toEqual([0x34, 0x12]); // LE
    expect(Array.from(bytes.subarray(4, 6))).toEqual([0xfe, 0xff]);
    expect(Array.from(bytes.subarray(6, 10))).toEqual([0xef, 0xbe, 0xad, 0xde]);

    const r = new BufReader(bytes);
    expect(r.u8()).toBe(0xff);
    expect(r.i8()).toBe(-1);
    expect(r.u16()).toBe(0x1234);
    expect(r.i16()).toBe(-2);
    expect(r.u32()).toBe(0xdeadbeef);
    expect(r.i32()).toBe(-3);
    expect(r.i64()).toBe(-4n);
    expect(r.u64()).toBe(0xfedcba9876543210n);
    expect(r.eof).toBe(true);
  });

  test("i64 accepts safe integers, rejects fractional numbers", () => {
    const w = new BufWriter();
    w.i64(9007199254740991);
    expect(new BufReader(w.toBytes()).i64()).toBe(9007199254740991n);
    expect(() => new BufWriter().i64(1.5)).toThrow(BinaryEncodeError);
  });

  test("i64Number rejects values beyond Number.MAX_SAFE_INTEGER", () => {
    const w = new BufWriter();
    w.i64(9007199254740993n);
    expect(() => new BufReader(w.toBytes()).i64Number()).toThrow(BinaryDecodeError);
  });
});

describe("BufWriter — automatic growth", () => {
  test("doubles the capacity without changing the data", () => {
    const w = new BufWriter(16);
    expect(w.capacity).toBe(16);
    const chunk = new Uint8Array(1000).map((_, i) => i % 251);
    w.bytes(chunk);
    expect(w.length).toBe(1000);
    expect(w.capacity).toBeGreaterThanOrEqual(1000);
    expect(Array.from(w.toBytes())).toEqual(Array.from(chunk));
  });

  test("growing mid-string-write still patches the length prefix correctly", () => {
    const w = new BufWriter(16); // guaranteed to grow during encoding
    const long = "ä".repeat(500); // 2 bytes/char
    w.stringU16(long);
    const r = new BufReader(w.toBytes());
    expect(r.u16()).toBe(1000);
    expect(r.utf8(1000)).toBe(long);
  });

  test("reset reuses the allocated memory", () => {
    const w = new BufWriter(64);
    w.bytes(new Uint8Array(500));
    const cap = w.capacity;
    w.reset();
    expect(w.length).toBe(0);
    expect(w.capacity).toBe(cap);
    w.u8(7);
    expect(Array.from(w.toBytes())).toEqual([7]);
  });

  test("unsafeBytes is a view, toBytes is a copy", () => {
    const w = new BufWriter(16);
    w.u8(1);
    const view = w.unsafeBytes();
    const copy = w.toBytes();
    w.reset();
    w.u8(9);
    expect(view[0]).toBe(9); // the view sees the new data
    expect(copy[0]).toBe(1); // the copy is independent
  });
});

describe("multi-byte UTF-8 strings", () => {
  const cases = [
    ["ascii", "kafka"],
    ["latin diacritics", "münchen größe"],
    ["CJK", "主题名称"],
    ["emoji (surrogate pair)", "topic-🚀-🎉"],
    ["empty", ""],
  ] as const;

  for (const [label, value] of cases) {
    test(`stringU16 round-trip: ${label}`, () => {
      const w = new BufWriter(8);
      w.stringU16(value);
      const r = new BufReader(w.toBytes());
      const len = r.u16();
      expect(len).toBe(UTF8.encode(value).length);
      r.offset = 0;
      expect(r.stringU16()).toBe(value);
    });

    test(`stringU32 round-trip: ${label}`, () => {
      const w = new BufWriter(8);
      w.stringU32(value);
      expect(new BufReader(w.toBytes()).stringU32()).toBe(value);
    });
  }

  test("strings > 65535 bytes are rejected by stringU16", () => {
    const w = new BufWriter(16);
    expect(() => w.stringU16("あ".repeat(30000))).toThrow(BinaryEncodeError);
  });
});

describe("the len -1 = null convention", () => {
  test("null, empty, and non-empty are distinguishable", () => {
    const w = new BufWriter(16);
    w.bytesI32(null);
    w.bytesI32(new Uint8Array(0));
    w.bytesI32(UTF8.encode("hällo wörld"));
    w.bytesI32("direct-strüng");
    w.bytesI32(undefined);

    const r = new BufReader(w.toBytes());
    expect(r.bytesI32()).toBeNull();
    expect(r.bytesI32()).toEqual(new Uint8Array(0));
    expect(new TextDecoder().decode(r.bytesI32()!)).toBe("hällo wörld");
    expect(new TextDecoder().decode(r.bytesI32()!)).toBe("direct-strüng");
    expect(r.bytesI32()).toBeNull();
    expect(r.eof).toBe(true);
  });

  test("copy=false returns a view into the original buffer", () => {
    const w = new BufWriter(16);
    w.bytesI32(new Uint8Array([1, 2, 3]));
    const buf = w.toBytes();
    const r = new BufReader(buf);
    const view = r.bytesI32(false)!;
    buf[4] = 42;
    expect(view[0]).toBe(42);
  });

  test("a negative length other than -1 is a decode error", () => {
    const w = new BufWriter(16);
    w.i32(-2);
    expect(() => new BufReader(w.toBytes()).bytesI32()).toThrow(BinaryDecodeError);
  });
});

describe("BufReader — bounds checking", () => {
  test("reading past the buffer throws BinaryDecodeError with the offset", () => {
    const r = new BufReader(new Uint8Array(3));
    r.u8();
    expect(() => r.u32()).toThrow(BinaryDecodeError);
    try {
      r.u32();
    } catch (err) {
      expect((err as BinaryDecodeError).offset).toBe(1);
      expect((err as Error).message).toContain("only 2 remain");
    }
  });

  test("reading from a subarray honors byteOffset", () => {
    const backing = new Uint8Array([9, 9, 0x01, 0x02, 0x03, 0x04]);
    const r = new BufReader(backing, 2, 4);
    expect(r.u32()).toBe(0x04030201);
    expect(r.remaining).toBe(0);
  });
});
