/**
 * core/binary.ts — the low-level encoder/decoder shared by every packed format
 * of `bunrdkafka.h`.
 *
 * Conventions (header §PACKED BINARY FORMATS):
 *  - little-endian, no padding/alignment.
 *  - strings/bytes are length-prefixed; for signed types, `len == -1` means
 *    NULL (distinct from an empty `len == 0` string).
 *  - strings are UTF-8, NOT NUL-terminated inside packed buffers.
 */

const TEXT_ENCODER = /* @__PURE__ */ new TextEncoder();
const TEXT_DECODER = /* @__PURE__ */ new TextDecoder("utf-8", { fatal: false });

/** Error for buffers from C that violate the spec (or are truncated). */
export class BinaryDecodeError extends Error {
  override readonly name = "BinaryDecodeError";
  readonly offset: number;

  constructor(message: string, offset: number) {
    super(`${message} (at offset ${offset})`);
    this.offset = offset;
  }
}

/** Error for JS data that cannot be encoded into a packed format. */
export class BinaryEncodeError extends Error {
  override readonly name = "BinaryEncodeError";
}

/** `len == -1` = NULL in signed fields. */
export const NULL_LENGTH = -1;

const MAX_U16 = 0xffff;
const MAX_U32 = 0xffffffff;

/* ========================================================================== */
/* Writer                                                                      */
/* ========================================================================== */

/**
 * Sequential writer over an `ArrayBuffer`, auto-growing (doubling) when out of
 * room.
 *
 * Reuse the instance across produce calls to avoid hot-path allocation: call
 * {@link reset} and keep writing.
 */
export class BufWriter {
  private buf: Uint8Array;
  private view: DataView;
  private pos = 0;

  constructor(initialCapacity = 1024) {
    const cap = Math.max(16, initialCapacity | 0);
    this.buf = new Uint8Array(cap);
    this.view = new DataView(this.buf.buffer);
  }

  /** Bytes written so far. */
  get length(): number {
    return this.pos;
  }

  /** Current capacity (bytes). */
  get capacity(): number {
    return this.buf.length;
  }

  /** Clears the content (keeps the allocated memory). */
  reset(): void {
    this.pos = 0;
  }

  /**
   * A view (NO copy) over what has been written — only valid until the next
   * write or the next grow. This is what gets passed straight down to FFI.
   */
  unsafeBytes(): Uint8Array {
    return this.buf.subarray(0, this.pos);
  }

  /** An independent copy of what has been written. */
  toBytes(): Uint8Array {
    return this.buf.slice(0, this.pos);
  }

  /** Ensures at least `n` free bytes remain. */
  ensure(n: number): void {
    const need = this.pos + n;
    if (need <= this.buf.length) return;
    let cap = this.buf.length;
    while (cap < need) cap *= 2;
    const next = new Uint8Array(cap);
    next.set(this.buf);
    this.buf = next;
    this.view = new DataView(next.buffer);
  }

  u8(v: number): void {
    this.ensure(1);
    this.view.setUint8(this.pos, v);
    this.pos += 1;
  }

  i8(v: number): void {
    this.ensure(1);
    this.view.setInt8(this.pos, v);
    this.pos += 1;
  }

  u16(v: number): void {
    this.ensure(2);
    this.view.setUint16(this.pos, v, true);
    this.pos += 2;
  }

  i16(v: number): void {
    this.ensure(2);
    this.view.setInt16(this.pos, v, true);
    this.pos += 2;
  }

  u32(v: number): void {
    this.ensure(4);
    this.view.setUint32(this.pos, v, true);
    this.pos += 4;
  }

  i32(v: number): void {
    this.ensure(4);
    this.view.setInt32(this.pos, v, true);
    this.pos += 4;
  }

  i64(v: number | bigint): void {
    this.ensure(8);
    this.view.setBigInt64(this.pos, toBigInt(v), true);
    this.pos += 8;
  }

  u64(v: number | bigint): void {
    this.ensure(8);
    this.view.setBigUint64(this.pos, toBigInt(v), true);
    this.pos += 8;
  }

  /** Writes raw bytes, no length prefix. */
  bytes(src: Uint8Array): void {
    this.ensure(src.length);
    this.buf.set(src, this.pos);
    this.pos += src.length;
  }

  /** `u16 len, bytes` — UTF-8, no NUL. */
  stringU16(s: string): void {
    const lenOffset = this.reserveLen(2, s);
    const written = this.pos - lenOffset - 2;
    if (written > MAX_U16) {
      throw new BinaryEncodeError(`string of ${written} bytes exceeds the u16 length prefix limit`);
    }
    this.view.setUint16(lenOffset, written, true);
  }

  /** `u32 len, bytes` — UTF-8, no NUL. */
  stringU32(s: string): void {
    const lenOffset = this.reserveLen(4, s);
    const written = this.pos - lenOffset - 4;
    this.view.setUint32(lenOffset, written, true);
  }

  /** `i32 len, bytes` with `len == -1` when `value == null`. */
  bytesI32(value: Uint8Array | string | null | undefined): void {
    if (value === null || value === undefined) {
      this.i32(NULL_LENGTH);
      return;
    }
    if (typeof value === "string") {
      const lenOffset = this.reserveLen(4, value);
      this.view.setInt32(lenOffset, this.pos - lenOffset - 4, true);
      return;
    }
    if (value.length > MAX_U32) {
      throw new BinaryEncodeError(`${value.length} bytes exceed the i32 length prefix limit`);
    }
    this.i32(value.length);
    this.bytes(value);
  }

  /** Rewrites a u32 at a previously written offset (used to patch lengths). */
  patchU32(offset: number, value: number): void {
    if (offset + 4 > this.pos) {
      throw new BinaryEncodeError(`patchU32 outside the written region: ${offset}`);
    }
    this.view.setUint32(offset, value, true);
  }

  /** Reserves room for an integer, returning the offset to patch later. */
  reserve(nBytes: number): number {
    this.ensure(nBytes);
    const at = this.pos;
    this.pos += nBytes;
    return at;
  }

  /**
   * Writes a `lenBytes`-byte placeholder then encodes UTF-8 right after;
   * returns the placeholder's offset so the caller can write the real length.
   */
  private reserveLen(lenBytes: number, s: string): number {
    // Worst-case UTF-8: 3 bytes / UTF-16 code unit (surrogate pair = 4 bytes / 2 units).
    this.ensure(lenBytes + s.length * 3);
    const lenOffset = this.pos;
    this.pos += lenBytes;
    const { written } = TEXT_ENCODER.encodeInto(s, this.buf.subarray(this.pos));
    this.pos += written;
    return lenOffset;
  }
}

function toBigInt(v: number | bigint): bigint {
  if (typeof v === "bigint") return v;
  if (!Number.isSafeInteger(v)) {
    throw new BinaryEncodeError(`64-bit value is not a safe integer: ${v}`);
  }
  return BigInt(v);
}

/* ========================================================================== */
/* Reader                                                                      */
/* ========================================================================== */

/** Sequential reader over a `Uint8Array`, bounds-checked on every read. */
export class BufReader {
  readonly bytesView: Uint8Array;
  private readonly view: DataView;
  private pos = 0;

  constructor(source: Uint8Array | ArrayBuffer, byteOffset?: number, byteLength?: number) {
    let bytes: Uint8Array;
    if (source instanceof Uint8Array) {
      bytes =
        byteOffset === undefined
          ? source
          : source.subarray(byteOffset, byteLength === undefined ? undefined : byteOffset + byteLength);
    } else {
      bytes = new Uint8Array(source, byteOffset ?? 0, byteLength);
    }
    this.bytesView = bytes;
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }

  /** Current read position. */
  get offset(): number {
    return this.pos;
  }

  set offset(value: number) {
    if (value < 0 || value > this.bytesView.length) {
      throw new BinaryDecodeError(`offset ${value} is outside the buffer`, this.pos);
    }
    this.pos = value;
  }

  /** Bytes remaining. */
  get remaining(): number {
    return this.bytesView.length - this.pos;
  }

  get eof(): boolean {
    return this.pos >= this.bytesView.length;
  }

  private need(n: number): number {
    if (n < 0 || this.pos + n > this.bytesView.length) {
      throw new BinaryDecodeError(
        `need ${n} bytes but only ${this.remaining} remain (buffer is ${this.bytesView.length} bytes)`,
        this.pos,
      );
    }
    const at = this.pos;
    this.pos += n;
    return at;
  }

  u8(): number {
    return this.view.getUint8(this.need(1));
  }

  i8(): number {
    return this.view.getInt8(this.need(1));
  }

  u16(): number {
    return this.view.getUint16(this.need(2), true);
  }

  i16(): number {
    return this.view.getInt16(this.need(2), true);
  }

  u32(): number {
    return this.view.getUint32(this.need(4), true);
  }

  i32(): number {
    return this.view.getInt32(this.need(4), true);
  }

  i64(): bigint {
    return this.view.getBigInt64(this.need(8), true);
  }

  u64(): bigint {
    return this.view.getBigUint64(this.need(8), true);
  }

  /** i64 → number; throws if it exceeds `Number.MAX_SAFE_INTEGER`. */
  i64Number(): number {
    const v = this.i64();
    if (v > 9007199254740991n || v < -9007199254740991n) {
      throw new BinaryDecodeError(`i64 value ${v} is not representable as a number`, this.pos - 8);
    }
    return Number(v);
  }

  /** A view (no copy) over the next `n` bytes. */
  bytes(n: number): Uint8Array {
    const at = this.need(n);
    return this.bytesView.subarray(at, at + n);
  }

  /** A copy of the next `n` bytes. */
  bytesCopy(n: number): Uint8Array {
    const at = this.need(n);
    return this.bytesView.slice(at, at + n);
  }

  /** Decodes the next `n` bytes as a UTF-8 string. */
  utf8(n: number): string {
    const at = this.need(n);
    if (n === 0) return "";
    return TEXT_DECODER.decode(this.bytesView.subarray(at, at + n));
  }

  /** `u16 len, bytes` → string. */
  stringU16(): string {
    return this.utf8(this.u16());
  }

  /** `u32 len, bytes` → string. */
  stringU32(): string {
    return this.utf8(this.u32());
  }

  /**
   * `i32 len, bytes` with `-1` = null.
   * @param copy `true` (default) → an independent copy, detached from the
   *             reusable buffer; `false` → a zero-copy view, only valid until
   *             the next poll.
   */
  bytesI32(copy = true): Uint8Array | null {
    const len = this.i32();
    if (len === NULL_LENGTH) return null;
    if (len < 0) {
      throw new BinaryDecodeError(`invalid negative length: ${len}`, this.pos - 4);
    }
    return copy ? this.bytesCopy(len) : this.bytes(len);
  }

  skip(n: number): void {
    this.need(n);
  }
}
