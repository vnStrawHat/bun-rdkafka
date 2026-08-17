/**
 * Loader: only the path-resolution part is tested — NO dlopen (unit tests must
 * run before the native library is built).
 */

import { afterAll, afterEach, describe, expect, test } from "bun:test";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { suffix } from "bun:ffi";
import {
  SUPPORTED_PLATFORM_KEYS,
  libFileNames,
  platformKey,
  resolveLibPath,
} from "../../packages/bun-rdkafka/src/ffi/loader.ts";

const ENV_KEY = "BUN_RDKAFKA_LIB_PATH";
const original = process.env[ENV_KEY];

afterEach(() => {
  if (original === undefined) delete process.env[ENV_KEY];
  else process.env[ENV_KEY] = original;
});

/**
 * Hermetic fixture: a fake package root in a tmpdir. `resolveLibPath(fromDir)`
 * treats `fromDir` as `<pkg>/src/ffi`, looks for `<pkg>/prebuilds/<key>/` and
 * then walks up for a dev-local `native/build/`. Never depends on the state of
 * the real repo (CI has neither a dev build nor prebuilds).
 */
function makeFixtureRoot(opts: { prebuilds?: boolean; devBuild?: boolean }): {
  root: string;
  fromDir: string;
} {
  const root = mkdtempSync(join(tmpdir(), "brk-loader-"));
  fixtureRoots.push(root);
  const fromDir = join(root, "src", "ffi");
  mkdirSync(fromDir, { recursive: true });
  const libName = `libbunrdkafka.${suffix}`;
  if (opts.prebuilds) {
    const dir = join(root, "prebuilds", platformKey());
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, libName), "not a real library");
  }
  if (opts.devBuild) {
    const dir = join(root, "native", "build");
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, libName), "not a real library");
  }
  return { root, fromDir };
}

const fixtureRoots: string[] = [];
afterAll(() => {
  for (const root of fixtureRoots) rmSync(root, { recursive: true, force: true });
});

describe("lazy load", () => {
  test("importing the module does NOT dlopen the library", () => {
    // Checked in a fresh process: `bun test` shares one process across files, so
    // when unit and integration suites run together another file has already
    // loaded native by the time this one runs.
    const probe = Bun.spawnSync({
      cmd: [
        process.execPath,
        "-e",
        'const [pkg, loader] = await Promise.all([import(process.argv[1]), import(process.argv[2])]);' +
          "void pkg; console.log(JSON.stringify({ loaded: loader.isNativeLoaded(), path: loader.loadedLibPath() ?? null }));",
        join(import.meta.dir, "..", "..", "packages", "bun-rdkafka", "src", "index.ts"),
        join(import.meta.dir, "..", "..", "packages", "bun-rdkafka", "src", "ffi", "loader.ts"),
      ],
      env: Object.fromEntries(
        Object.entries(process.env).filter(([k, v]) => k !== ENV_KEY && v !== undefined),
      ) as Record<string, string>,
    });
    expect(probe.exitCode).toBe(0);
    const out = JSON.parse(probe.stdout.toString().trim().split("\n").at(-1) ?? "{}");
    expect(out.loaded).toBe(false);
    expect(out.path).toBeNull();
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
    // Both present → prebuilds/ wins (the installer's output beats the dev build).
    const both = makeFixtureRoot({ prebuilds: true, devBuild: true });
    expect(resolveLibPath(both.fromDir)).toBe(
      join(both.root, "prebuilds", platformKey(), `libbunrdkafka.${suffix}`),
    );
  });

  test("without prebuilds/, falls back to the dev-local native/build", () => {
    delete process.env[ENV_KEY];
    const devOnly = makeFixtureRoot({ devBuild: true });
    expect(resolveLibPath(devOnly.fromDir)).toBe(
      join(devOnly.root, "native", "build", `libbunrdkafka.${suffix}`),
    );
  });

  test("nothing found → an error with remediation guidance", () => {
    delete process.env[ENV_KEY];
    // A directory outside the repo without prebuilds/ or a dev build.
    expect(() => resolveLibPath("/")).toThrow(/BUN_RDKAFKA_LIB_PATH/);
    expect(() => resolveLibPath("/")).toThrow(/trustedDependencies/);
  });
});
