/**
 * Asserts the feature set compiled into the native library that the loader
 * resolves — in CI's `unit` job that is the freshly built `build-native`
 * artifact (via BUN_RDKAFKA_LIB_PATH), so every prebuilt target is gated on
 * shipping with the full codec/security feature set.
 *
 * Mechanism: librdkafka's `builtin.features` property doubles as a
 * required-features check — attempting to SET it to a comma-separated list
 * fails unless every listed feature was built in (documented in librdkafka's
 * CONFIGURATION.md). That path goes through the existing `brk_conf_set`
 * symbol, so no new ABI surface is needed.
 *
 * Skipped when no native library is present (unit tests must not require one).
 */
import { describe, expect, test } from "bun:test";

import {
  loadNative,
  resolveLibPath,
} from "../../packages/bun-rdkafka/src/ffi/loader.ts";
import { cstringBuffer } from "../../packages/bun-rdkafka/src/core/native-client.ts";
import { BRK_OK } from "../../packages/bun-rdkafka/src/ffi/types.ts";

/**
 * Exact librdkafka builtin feature names (see BUILT_WITH/rd_kafka_conf.c):
 * gzip, snappy, ssl, sasl, regex, lz4, sasl_gssapi, sasl_plain, sasl_scram,
 * plugins, zstd, sasl_oauthbearer, http, oidc.
 */
const REQUIRED_FEATURES = [
  "gzip",
  "snappy",
  "lz4",
  "zstd",
  "ssl",
  "sasl_scram",
  "sasl_oauthbearer",
] as const;

// Skip ONLY when no library can be resolved at all. When BUN_RDKAFKA_LIB_PATH
// is set (as in CI's unit job) resolveLibPath() honors it without further
// lookup, so a broken path fails loudly here instead of skipping silently —
// exactly what we want from a release gate.
let libAvailable = true;
try {
  resolveLibPath();
} catch {
  libAvailable = false;
}

const ERRSTR_CAP = 512;

describe.skipIf(!libAvailable)("native builtin.features", () => {
  /** Sets `builtin.features` to `list`; returns { ret, detail }. */
  function requireFeatures(
    native: ReturnType<typeof loadNative>,
    conf: ReturnType<ReturnType<typeof loadNative>["brk_conf_new"]>,
    list: string,
  ): { ret: number; detail: string } {
    const errBuf = new Uint8Array(ERRSTR_CAP);
    const ret = native.brk_conf_set(
      conf,
      cstringBuffer("builtin.features"),
      cstringBuffer(list),
      errBuf,
      ERRSTR_CAP,
    );
    const nul = errBuf.indexOf(0);
    const detail = new TextDecoder().decode(
      errBuf.subarray(0, nul < 0 ? errBuf.length : nul),
    );
    return { ret, detail };
  }

  test(`librdkafka is built with: ${REQUIRED_FEATURES.join(", ")}`, () => {
    const native = loadNative();
    const conf = native.brk_conf_new();
    expect(conf).not.toBeNull();
    try {
      // Per-feature first, so a failure names the exact missing feature.
      for (const feature of REQUIRED_FEATURES) {
        const { ret, detail } = requireFeatures(native, conf, feature);
        expect(
          ret,
          `librdkafka was built WITHOUT "${feature}" (${detail}) — ` +
            `the build is missing a dev dependency (see native/CMakeLists.txt)`,
        ).toBe(BRK_OK);
      }
      // The full list in one shot — the actual release gate.
      const all = requireFeatures(native, conf, REQUIRED_FEATURES.join(","));
      expect(all.ret, all.detail).toBe(BRK_OK);
    } finally {
      native.brk_conf_destroy(conf);
    }
  });

  test("the check mechanism itself rejects unknown features", () => {
    // Guards against builtin.features silently accepting anything, which
    // would make the assertions above meaningless.
    const native = loadNative();
    const conf = native.brk_conf_new();
    try {
      const { ret } = requireFeatures(native, conf, "not_a_real_feature");
      expect(ret).toBeLessThan(0);
    } finally {
      native.brk_conf_destroy(conf);
    }
  });
});
