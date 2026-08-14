#!/usr/bin/env bun
/**
 * install.ts — postinstall hook & manual installer (`bun-rdkafka-install`).
 *
 * Fetches the prebuilt native library matching this platform from the GitHub
 * Release of the installed package version, verifies its SHA-256 against the
 * release's SHA256SUMS, and unpacks it into `<package>/prebuilds/<target>/`
 * where the loader looks for it. When no matching asset exists (or the
 * download/checksum fails) it falls back to building from source (the npm
 * tarball ships `native/` + `librdkafka.version` for exactly this purpose).
 *
 * Environment variables:
 *   BUN_RDKAFKA_SKIP_DOWNLOAD=1   do nothing (opt out entirely)
 *   BUN_RDKAFKA_FORCE_BUILD=1     never download, always build from source
 *   BUN_RDKAFKA_BINARY_MIRROR     base URL replacing the GitHub release base;
 *                                 must serve `<mirror>/v<version>/<asset>`
 *   BUN_RDKAFKA_INSTALL_STRICT=1  exit non-zero on failure (default: warn and
 *                                 exit 0 so `bun install` isn't broken; the
 *                                 loader raises an actionable error at runtime)
 *   BUN_RDKAFKA_PKG_ROOT          package-root override (internal, for tests)
 *   BUN_RDKAFKA_TEST_BUILD_CMD    replaces the cmake build (internal, for
 *                                 tests); run via `sh -c` with
 *                                 BRK_INSTALL_OUT_DIR / BRK_INSTALL_LIB_NAME
 *
 * Note for Bun users: Bun blocks lifecycle scripts of dependencies by default.
 * Add "@vnstrawhat/bun-rdkafka" to `trustedDependencies` in your package.json (then
 * reinstall), or run `bunx bun-rdkafka-install` once manually.
 */

import {
  cpSync,
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import {
  assetName,
  decideInstall,
  detectTarget,
  libFileNamesForTarget,
  parseSha256Sums,
  releaseBaseUrl,
  type InstallAction,
  type PrebuiltTarget,
} from "./install-plan.ts";

const PREFIX = "bun-rdkafka:";
const log = (msg: string) => console.log(`${PREFIX} ${msg}`);
const warn = (msg: string) => console.warn(`${PREFIX} WARNING: ${msg}`);

const pkgRoot = process.env["BUN_RDKAFKA_PKG_ROOT"] ?? join(import.meta.dir, "..");
const strict = process.env["BUN_RDKAFKA_INSTALL_STRICT"] === "1";

/** `<platform>-<arch>` (+`-gnu` on Linux) — MUST mirror loader.ts#platformKey. */
function prebuildsDirName(): string {
  return `${process.platform}-${process.arch}${process.platform === "linux" ? "-gnu" : ""}`;
}

function isMusl(): boolean {
  return (
    process.platform === "linux" &&
    (existsSync("/lib/ld-musl-x86_64.so.1") || existsSync("/lib/ld-musl-aarch64.so.1"))
  );
}

/** True when running inside the bun-rdkafka development monorepo. */
function isDevRepo(): boolean {
  try {
    const rootPkg = join(pkgRoot, "..", "..", "package.json");
    if (!existsSync(rootPkg)) return false;
    const parsed = JSON.parse(readFileSync(rootPkg, "utf8"));
    return parsed?.name === "bun-rdkafka-workspace";
  } catch {
    return false;
  }
}

function packageVersion(): string {
  const pkg = JSON.parse(readFileSync(join(pkgRoot, "package.json"), "utf8"));
  return String(pkg.version);
}

function metaPath(dir: string): string {
  return join(dir, ".install-meta.json");
}

function alreadyInstalled(dir: string, libNames: string[], version: string): boolean {
  if (!libNames.some((n) => existsSync(join(dir, n)))) return false;
  try {
    const meta = JSON.parse(readFileSync(metaPath(dir), "utf8"));
    return meta?.version === version;
  } catch {
    return false;
  }
}

function sha256Hex(data: ArrayBuffer | Uint8Array): string {
  return new Bun.CryptoHasher("sha256").update(data).digest("hex");
}

/** Recursively finds the first file whose basename is in `names`. */
function findFile(root: string, names: string[]): string | undefined {
  const entries = readdirSync(root, { recursive: true, withFileTypes: false }) as string[];
  for (const rel of entries) {
    if (names.includes(basename(String(rel)))) {
      const p = join(root, String(rel));
      try {
        if (statSync(p).isFile()) return p;
      } catch {
        /* ignore */
      }
    }
  }
  return undefined;
}

async function run(cmd: string[], cwd?: string, env?: Record<string, string>): Promise<number> {
  const proc = Bun.spawn(cmd, {
    cwd,
    env: { ...process.env, ...env },
    stdout: "inherit",
    stderr: "inherit",
  });
  return await proc.exited;
}

/** Downloads + verifies + unpacks one release asset. Throws on any failure. */
async function downloadPrebuilt(target: PrebuiltTarget, version: string, destDir: string): Promise<void> {
  const base = releaseBaseUrl(version, process.env["BUN_RDKAFKA_BINARY_MIRROR"]);
  const asset = assetName(target);

  log(`downloading ${base}/${asset}`);
  const sumsRes = await fetch(`${base}/SHA256SUMS`);
  if (!sumsRes.ok) throw new Error(`SHA256SUMS not found (HTTP ${sumsRes.status}) at ${base}`);
  const sums = parseSha256Sums(await sumsRes.text());
  const expected = sums.get(asset);
  if (!expected) throw new Error(`no SHA256SUMS entry for ${asset}`);

  const assetRes = await fetch(`${base}/${asset}`);
  if (!assetRes.ok) throw new Error(`asset not found (HTTP ${assetRes.status}): ${asset}`);
  const bytes = new Uint8Array(await assetRes.arrayBuffer());
  const actual = sha256Hex(bytes);
  if (actual !== expected) {
    throw new Error(`checksum mismatch for ${asset}: expected ${expected}, got ${actual}`);
  }

  const tmp = join(tmpdir(), `bun-rdkafka-install-${process.pid}-${Date.now()}`);
  mkdirSync(tmp, { recursive: true });
  try {
    const tarball = join(tmp, asset);
    writeFileSync(tarball, bytes);
    // `tar` is available on Linux, macOS, and Windows 10+ (bsdtar).
    const code = await run(["tar", "-xzf", tarball, "-C", tmp]);
    if (code !== 0) throw new Error(`tar exited with code ${code}`);

    const libNames = libFileNamesForTarget(target);
    const lib = findFile(tmp, libNames);
    if (!lib) throw new Error(`no ${libNames.join("/")} inside ${asset}`);

    mkdirSync(destDir, { recursive: true });
    cpSync(lib, join(destDir, basename(lib)));
    writeFileSync(
      metaPath(destDir),
      JSON.stringify({ version, source: "download", assetSha256: actual }, null, 2),
    );
    log(`installed prebuilt ${basename(lib)} → ${destDir}`);
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

/** Builds the shim from source and copies the result into `destDir`. Throws on failure. */
async function buildFromSource(version: string, destDir: string): Promise<void> {
  const libNames = libFileNamesForTarget(prebuildsDirName());

  const testCmd = process.env["BUN_RDKAFKA_TEST_BUILD_CMD"];
  if (testCmd) {
    mkdirSync(destDir, { recursive: true });
    const code = await run(["sh", "-c", testCmd], pkgRoot, {
      BRK_INSTALL_OUT_DIR: destDir,
      BRK_INSTALL_LIB_NAME: libNames[0] ?? "libbunrdkafka.so",
    });
    if (code !== 0) throw new Error(`test build command exited with code ${code}`);
    writeFileSync(metaPath(destDir), JSON.stringify({ version, source: "build" }, null, 2));
    return;
  }

  // Sources: the npm tarball ships `native/` + `librdkafka.version` inside the
  // package (copied in by prepack); a git checkout has them at the repo root.
  const nativeDir = existsSync(join(pkgRoot, "native", "CMakeLists.txt"))
    ? join(pkgRoot, "native")
    : join(pkgRoot, "..", "..", "native");
  if (!existsSync(join(nativeDir, "CMakeLists.txt"))) {
    throw new Error("native sources not found — cannot build from source");
  }

  if (!Bun.which("cmake")) {
    throw new Error(
      "cmake not found. Install a C toolchain (cmake + gcc/clang) and re-run " +
        "`bunx bun-rdkafka-install`, or set BUN_RDKAFKA_LIB_PATH to an existing library.",
    );
  }
  if (!["cc", "gcc", "clang", "cl"].some((c) => Bun.which(c))) {
    throw new Error(
      "no C compiler found (cc/gcc/clang/cl). Install one and re-run `bunx bun-rdkafka-install`.",
    );
  }

  const buildDir = join(nativeDir, "build");
  log(`building from source (${nativeDir}) — this downloads and compiles librdkafka, please wait…`);
  let code = await run(["cmake", "-B", buildDir, "-S", nativeDir, "-DCMAKE_BUILD_TYPE=Release"]);
  if (code !== 0) throw new Error(`cmake configure exited with code ${code}`);
  code = await run(["cmake", "--build", buildDir, "--config", "Release", "--parallel", "2"]);
  if (code !== 0) throw new Error(`cmake build exited with code ${code}`);

  const lib = findFile(buildDir, libNames);
  if (!lib) throw new Error(`build succeeded but no ${libNames.join("/")} found under ${buildDir}`);
  mkdirSync(destDir, { recursive: true });
  cpSync(lib, join(destDir, basename(lib)));
  writeFileSync(metaPath(destDir), JSON.stringify({ version, source: "build" }, null, 2));
  log(`built and installed ${basename(lib)} → ${destDir}`);
}

async function main(): Promise<number> {
  const version = packageVersion();
  const dirName = prebuildsDirName();
  const destDir = join(pkgRoot, "prebuilds", dirName);
  const target = detectTarget(process.platform, process.arch, isMusl());

  const action: InstallAction = decideInstall({
    skipDownload: process.env["BUN_RDKAFKA_SKIP_DOWNLOAD"] === "1",
    forceBuild: process.env["BUN_RDKAFKA_FORCE_BUILD"] === "1",
    target,
    alreadyInstalled: alreadyInstalled(destDir, libFileNamesForTarget(dirName), version),
    devRepo: isDevRepo(),
  });

  if (action.kind === "skip") {
    log(`nothing to do (${action.reason})`);
    return 0;
  }

  try {
    if (action.kind === "download") {
      try {
        await downloadPrebuilt(action.target, version, destDir);
        return 0;
      } catch (err) {
        warn(`prebuilt download failed: ${(err as Error).message}`);
        warn("falling back to building from source");
      }
    } else {
      log(`building from source (${action.reason})`);
    }
    await buildFromSource(version, destDir);
    return 0;
  } catch (err) {
    warn((err as Error).message);
    warn(
      "bun-rdkafka has NO native library installed. Fix options:\n" +
        "  1. re-run:  bunx bun-rdkafka-install\n" +
        "  2. Bun users: add \"@vnstrawhat/bun-rdkafka\" to trustedDependencies in package.json, then reinstall\n" +
        "     (Bun blocks dependency lifecycle scripts by default, so postinstall may not have run)\n" +
        "  3. point BUN_RDKAFKA_LIB_PATH at an existing libbunrdkafka library\n" +
        "  4. install a C toolchain (cmake + gcc/clang) for the source-build fallback",
    );
    return strict ? 1 : 0;
  }
}

process.exit(await main());
