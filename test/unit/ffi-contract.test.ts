/**
 * The ABI contract: `ffi/types.ts` and `ffi/symbols.ts` must match
 * `native/include/bunrdkafka.h` line for line. This test parses the header
 * directly, so any C-side change that forgets the TypeScript update fails
 * right here in unit tests (no .so needed).
 */

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { brkSymbols } from "../../packages/bun-rdkafka/src/ffi/symbols.ts";
import * as types from "../../packages/bun-rdkafka/src/ffi/types.ts";

const HEADER_PATH = join(import.meta.dir, "../../native/include/bunrdkafka.h");
const header = readFileSync(HEADER_PATH, "utf8");

/** `#define BRK_FOO 1` / `#define BRK_FOO (-1)` — parameterized macros skipped. */
function headerDefines(): Map<string, number> {
  const out = new Map<string, number>();
  const re = /^#define\s+(BRK_[A-Z0-9_]+)\s+(\(?-?\d+\)?)\s*(?:\/\*|$)/gm;
  for (const match of header.matchAll(re)) {
    const [, name, rawValue] = match;
    out.set(name!, Number.parseInt(rawValue!.replace(/[()]/g, ""), 10));
  }
  return out;
}

interface HeaderFn {
  name: string;
  paramCount: number;
}

/** Every `BRK_EXPORT <type> <name>(<params>);` declaration in the header. */
function headerFunctions(): HeaderFn[] {
  const out: HeaderFn[] = [];
  for (const match of header.matchAll(/^BRK_EXPORT[^;]*;/gm)) {
    const decl = match[0].replace(/\s+/g, " ");
    const open = decl.indexOf("(");
    const close = decl.lastIndexOf(")");
    const nameMatch = decl.slice(0, open).match(/([A-Za-z_][A-Za-z0-9_]*)\s*$/);
    const params = decl.slice(open + 1, close).trim();
    out.push({
      name: nameMatch![1]!,
      paramCount: params === "" || params === "void" ? 0 : splitTopLevel(params).length,
    });
  }
  return out;
}

function splitTopLevel(params: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let current = "";
  for (const ch of params) {
    if (ch === "(") depth++;
    else if (ch === ")") depth--;
    if (ch === "," && depth === 0) {
      parts.push(current.trim());
      current = "";
    } else {
      current += ch;
    }
  }
  if (current.trim() !== "") parts.push(current.trim());
  return parts;
}

describe("constants mirroring the header", () => {
  const defines = headerDefines();

  test("the header parses (sanity)", () => {
    expect(defines.size).toBeGreaterThan(25);
    expect(defines.get("BRK_ABI_VERSION")).toBe(2);
  });

  test("every integer #define has a TypeScript constant with the same name and value", () => {
    const exported = types as unknown as Record<string, unknown>;
    const mismatched: string[] = [];
    for (const [name, value] of defines) {
      if (exported[name] === undefined) {
        mismatched.push(`${name}: missing from ffi/types.ts`);
      } else if (exported[name] !== value) {
        mismatched.push(`${name}: header ${value} ≠ TS ${String(exported[name])}`);
      }
    }
    expect(mismatched).toEqual([]);
  });

  test("hard-pinned values guard against editing both sides by mistake", () => {
    expect(types.BRK_ABI_VERSION).toBe(2);
    expect(types.BRK_OK).toBe(0);
    expect(types.BRK_ERR_BUFFER_TOO_SMALL).toBe(-1);
    expect(types.BRK_ERR_KAFKA_OFFSET).toBe(-1000);
    expect(types.BRK_CLIENT_PRODUCER).toBe(0);
    expect(types.BRK_CLIENT_CONSUMER).toBe(1);
    expect(types.BRK_EVENT_DR).toBe(1);
    expect(types.BRK_EVENT_ADMIN_RESULT).toBe(9);
    expect(types.BRK_ASSIGN).toBe(0);
    expect(types.BRK_UNASSIGN).toBe(3);
    expect(types.BRK_ADMIN_DESCRIBE_TOPICS).toBe(9);
  });
});

describe("error-code decoding (BRK_KAFKA_ERR / BRK_IS_KAFKA_ERR / BRK_KAFKA_ERR_OF)", () => {
  test("embeds and re-extracts correctly across the whole rd_kafka_resp_err_t range", () => {
    for (const err of [-200, -191, -185, -184, -100, -1, 0, 1, 87, 120]) {
      const ret = types.brkKafkaErr(err);
      expect(types.isKafkaErr(ret)).toBe(true);
      expect(types.kafkaErrOf(ret)).toBe(err);
    }
  });

  test("the kafka and shim ranges never intersect", () => {
    for (const shim of [-1, -2, -3, -4, -5, -6, -7, -8, -99]) {
      expect(types.isShimErr(shim)).toBe(true);
      expect(types.isKafkaErr(shim)).toBe(false);
    }
    for (const err of [-200, 0, 120]) {
      expect(types.isShimErr(types.brkKafkaErr(err))).toBe(false);
    }
  });

  test("BRK_IS_KAFKA_ERR bounds match the macro (-1300 ≤ ret ≤ -800)", () => {
    expect(types.isKafkaErr(-800)).toBe(true);
    expect(types.isKafkaErr(-799)).toBe(false);
    expect(types.isKafkaErr(-1300)).toBe(true);
    expect(types.isKafkaErr(-1301)).toBe(false);
  });

  test("isErrRet: only negatives are errors", () => {
    expect(types.isErrRet(-1)).toBe(true);
    expect(types.isErrRet(0)).toBe(false);
    expect(types.isErrRet(42)).toBe(false);
  });
});

describe("librdkafka constants accompanying the packed formats", () => {
  test("special offsets and partition UA (rdkafka.h v2.15.0)", () => {
    expect(types.RD_KAFKA_PARTITION_UA).toBe(-1);
    expect(types.RD_KAFKA_OFFSET_BEGINNING).toBe(-2);
    expect(types.RD_KAFKA_OFFSET_END).toBe(-1);
    expect(types.RD_KAFKA_OFFSET_STORED).toBe(-1000);
    expect(types.RD_KAFKA_OFFSET_INVALID).toBe(-1001);
    expect(types.RD_KAFKA_OFFSET_TAIL_BASE).toBe(-2000);
    expect(types.rdKafkaOffsetTail(200)).toBe(-2200);
  });

  test("timestamp type", () => {
    expect(types.RD_KAFKA_TIMESTAMP_NOT_AVAILABLE).toBe(0);
    expect(types.RD_KAFKA_TIMESTAMP_CREATE_TIME).toBe(1);
    expect(types.RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME).toBe(2);
  });
});

describe("the symbol table", () => {
  const fns = headerFunctions();

  test("the header declares the expected function count (sanity)", () => {
    expect(fns.length).toBeGreaterThan(35);
  });

  test("symbols.ts declares exactly the header's function set, nothing more or less", () => {
    const inHeader = fns.map((f) => f.name).sort();
    const inSymbols = Object.keys(brkSymbols).sort();
    expect(inSymbols).toEqual(inHeader);
  });

  test("each symbol's parameter count matches the header", () => {
    const wrong: string[] = [];
    const table = brkSymbols as unknown as Record<string, { args: readonly unknown[] }>;
    for (const fn of fns) {
      const args = table[fn.name]?.args.length ?? -1;
      if (args !== fn.paramCount) {
        wrong.push(`${fn.name}: header ${fn.paramCount} tham số ≠ TS ${args}`);
      }
    }
    expect(wrong).toEqual([]);
  });

  test("every symbol declares returns", () => {
    const table = brkSymbols as unknown as Record<string, { returns?: string }>;
    for (const name of Object.keys(brkSymbols)) {
      expect(table[name]!.returns).toBeString();
    }
  });

  test("return signatures are right for a few key functions", () => {
    expect(brkSymbols.brk_abi_version.returns).toBe("i32");
    expect(brkSymbols.brk_librdkafka_version.returns).toBe("cstring");
    expect(brkSymbols.brk_conf_new.returns).toBe("ptr");
    expect(brkSymbols.brk_client_new.returns).toBe("ptr");
    expect(brkSymbols.brk_client_destroy.returns).toBe("void");
    expect(brkSymbols.brk_admin_request.args).toEqual(["ptr", "i32", "u64", "cstring"]);
    expect(brkSymbols.brk_seek.args).toEqual(["ptr", "cstring", "i32", "i64", "i32"]);
  });
});
