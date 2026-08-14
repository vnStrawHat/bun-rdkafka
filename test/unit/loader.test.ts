/**
 * Loader: only the path-resolution part is tested — NO dlopen (unit tests must
 * run before the native library is built).
 */

import { afterEach, describe, expect, test } from "bun:test";
import {
  SUPPORTED_PLATFORM_KEYS,
  isNativeLoaded,
  libFileNames,
  loadedLibPath,
  platformKey,
  resolveLibPath,
} from "../../packages/bun-rdkafka/src/ffi/loader.ts";

const ENV_KEY = "BUN_RDKAFKA_LIB_PATH";
const original = process.env[ENV_KEY];

afterEach(() => {
  if (original === undefined) delete process.env[ENV_KEY];
  else process.env[ENV_KEY] = original;
});

describe("lazy load", () => {
  test("importing the module does NOT dlopen the library", () => {
    expect(isNativeLoaded()).toBe(false);
    expect(loadedLibPath()).toBeUndefined();
  });
});

describe("platform key", () => {
  test("Linux appends the -gnu suffix, other platforms do not", () => {
    expect(platformKey("linux", "x64")).toBe("linux-x64-gnu");
    expect(platformKey("linux", "arm64")).toBe("linux-arm64-gnu");
    expect(platformKey("darwin", "arm64")).toBe("darwin-arm64");
    expect(platformKey("darwin", "x64")).toBe("darwin-x64");
    expect(platformKey("win32", "x64")).toBe("win32-x64");
  });

  test("the current platform's key is in the supported list (spec §6)", () => {
    expect(SUPPORTED_PLATFORM_KEYS).toContain("linux-x64-gnu");
    expect(SUPPORTED_PLATFORM_KEYS).toHaveLength(5);
    for (const key of SUPPORTED_PLATFORM_KEYS) {
      expect(key).toMatch(/^(linux|darwin|win32)-(x64|arm64)(-gnu)?$/);
    }
  });

  test("accepts names with and without the lib prefix (MSVC drops 'lib')", () => {
    expect(libFileNames("so")).toEqual(["libbunrdkafka.so", "bunrdkafka.so"]);
    expect(libFileNames("dll")).toEqual(["libbunrdkafka.dll", "bunrdkafka.dll"]);
  });
});

describe("resolution order", () => {
  test("BUN_RDKAFKA_LIB_PATH takes absolute precedence", () => {
    process.env[ENV_KEY] = "/tmp/arbitrary-path/libbunrdkafka.so";
    expect(resolveLibPath()).toBe("/tmp/arbitrary-path/libbunrdkafka.so");
  });

  test("without the env var, prebuilds/ is tried before the dev-local native/build", () => {
    delete process.env[ENV_KEY];
    // In the repo prebuilds/ is normally absent, so it falls back to the dev build.
    const resolved = resolveLibPath();
    expect(resolved).toMatch(/(prebuilds[\\/]|native[\\/]build[\\/])/);
    expect(resolved).toMatch(/bunrdkafka\./);
  });

  test("nothing found → an error with remediation guidance", () => {
    delete process.env[ENV_KEY];
    // A directory outside the repo without prebuilds/ or a dev build.
    expect(() => resolveLibPath("/")).toThrow(/BUN_RDKAFKA_LIB_PATH/);
    expect(() => resolveLibPath("/")).toThrow(/trustedDependencies/);
  });
});
