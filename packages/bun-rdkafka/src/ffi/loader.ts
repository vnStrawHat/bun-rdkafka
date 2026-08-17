/**
 * ffi/loader.ts — locates and `dlopen`s libbunrdkafka for the current platform (design §4).
 *
 * Resolution order:
 *   1. `process.env.BUN_RDKAFKA_LIB_PATH` (dev / CI / air-gapped)
 *   2. `<package>/prebuilds/<key>/libbunrdkafka.<suffix>` — placed there by
 *      `scripts/install.ts` (postinstall download from the GitHub Release, or
 *      its source-build fallback)
 *   3. dev-local fallback: `<repo>/native/build/libbunrdkafka.<suffix>`
 *
 * **Lazy**: this module does not open the library at import time — only on the
 * first {@link loadNative} call. This lets the decoder/config/ledger unit tests
 * run before the native library has been built.
 *
 * This layer contains no business logic.
 */

import { dlopen, suffix, type Library } from "bun:ffi";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { brkSymbols, type BrkSymbols } from "./symbols.ts";
import { BRK_ABI_VERSION } from "./types.ts";

type RawSymbols = Library<BrkSymbols>["symbols"];
/** Strips the `__ffi_function_callable` brand so the core layer can inject fakes in tests. */
type StripBrand<T> = T extends (...args: infer A) => infer R ? (...args: A) => R : never;

/** The native function table, type-safe per `bunrdkafka.h`. */
export type BrkNative = { [K in keyof RawSymbols]: StripBrand<RawSymbols[K]> };

/** Platforms with a prebuilt release asset (spec §6, Tier-1). */
export const SUPPORTED_PLATFORM_KEYS = [
  "linux-x64-gnu",
  "linux-arm64-gnu",
  "darwin-x64",
  "darwin-arm64",
  "win32-x64",
] as const;

export type PlatformKey = (typeof SUPPORTED_PLATFORM_KEYS)[number];

/** `<platform>-<arch>` (+ `-gnu` on Linux) — matches the prebuilds/ directory name. */
export function platformKey(
  platform: string = process.platform,
  arch: string = process.arch,
): string {
  return `${platform}-${arch}${platform === "linux" ? "-gnu" : ""}`;
}

/**
 * Library file names we may encounter. CMake drops the `lib` prefix on MSVC,
 * so Windows accepts both forms.
 */
export function libFileNames(sfx: string = suffix): string[] {
  return [`libbunrdkafka.${sfx}`, `bunrdkafka.${sfx}`];
}

function unsupportedPlatformMessage(key: string): string {
  return (
    `bun-rdkafka: no native library found for platform "${key}".\n` +
    `  - The library is normally downloaded by the postinstall script. Bun blocks\n` +
    `    dependency lifecycle scripts by default: add "@vnstrawhat/bun-rdkafka" to\n` +
    `    "trustedDependencies" in your package.json and reinstall, or run\n` +
    `    "bunx bun-rdkafka-install" once manually.\n` +
    `  - Prebuilt platforms: ${SUPPORTED_PLATFORM_KEYS.join(", ")}. Other platforms\n` +
    `    fall back to a source build (needs cmake + a C compiler).\n` +
    `  - If developing inside the repo: run "bun run build:native".\n` +
    `  - Or point at a library directly with BUN_RDKAFKA_LIB_PATH=/path/to/libbunrdkafka.${suffix}`
  );
}

/** Finds `native/build/libbunrdkafka.*` by walking up the directory tree. */
function findDevBuild(startDir: string): string | undefined {
  const names = libFileNames();
  let dir = startDir;
  for (let i = 0; i < 12; i++) {
    for (const sub of ["native/build", "native/build/Release", "native/build/Debug"]) {
      for (const name of names) {
        const candidate = join(dir, sub, name);
        if (existsSync(candidate)) return candidate;
      }
    }
    const parent = dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  return undefined;
}

/**
 * Returns the native library path that would be `dlopen`ed, without opening the
 * file. Throws a descriptive error when nothing is found.
 */
export function resolveLibPath(fromDir: string = import.meta.dir): string {
  const override = process.env["BUN_RDKAFKA_LIB_PATH"];
  if (override) return override;

  // prebuilds/ sits at the package root; this file lives at <pkg>/src/ffi/.
  const key = platformKey();
  for (const name of libFileNames()) {
    const candidate = join(fromDir, "..", "..", "prebuilds", key, name);
    if (existsSync(candidate)) return candidate;
  }

  const dev = findDevBuild(fromDir);
  if (dev) return dev;

  throw new Error(unsupportedPlatformMessage(key));
}

let cached: BrkNative | undefined;
let cachedLib: Library<BrkSymbols> | undefined;
let cachedError: Error | undefined;
let cachedPath: string | undefined;

/**
 * Opens the native library (once per process) and verifies the ABI version.
 * Subsequent calls return the cached result — including a cached error.
 */
export function loadNative(): BrkNative {
  if (cached) return cached;
  if (cachedError) throw cachedError;

  try {
    const path = resolveLibPath();
    const lib = dlopen(path, brkSymbols);
    const abi = lib.symbols.brk_abi_version();
    if (abi !== BRK_ABI_VERSION) {
      lib.close();
      throw new Error(
        `bun-rdkafka: native ABI mismatch — library "${path}" reports ABI ${abi}, ` +
          `TypeScript expects ${BRK_ABI_VERSION}. Reinstall dependencies or rebuild the native library.`,
      );
    }
    cachedLib = lib;
    cachedPath = path;
    cached = lib.symbols as unknown as BrkNative;
    return cached;
  } catch (err) {
    cachedError = err instanceof Error ? err : new Error(String(err));
    throw cachedError;
  }
}

/** Whether `dlopen` has already succeeded (does not trigger loading). */
export function isNativeLoaded(): boolean {
  return cached !== undefined;
}

/** Path of the opened library, `undefined` if not loaded yet. */
export function loadedLibPath(): string | undefined {
  return cachedPath;
}

/** Version of the statically linked librdkafka (NFR-6). Triggers loading if needed. */
export function librdkafkaVersion(): string {
  return loadNative().brk_librdkafka_version().toString();
}

/**
 * librdkafka's `builtin.features` as a list (e.g. `["gzip", "snappy", "ssl",
 * "sasl_scram", …]`) — upstream's `features` export. A FUNCTION for the same
 * reason as {@link librdkafkaVersion}: triggers loading on the first call.
 */
export function features(): string[] {
  const raw = loadNative().brk_features().toString();
  return raw
    .split(",")
    .map((f) => f.trim())
    .filter((f) => f.length > 0);
}

/**
 * Closes the library and clears the cache. Test-only — closing the library
 * while handles are alive is undefined behavior.
 * @internal
 */
export function unloadNativeForTests(): void {
  cachedLib?.close();
  cachedLib = undefined;
  cached = undefined;
  cachedError = undefined;
  cachedPath = undefined;
}
