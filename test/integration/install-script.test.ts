/**
 * End-to-end test of packages/bun-rdkafka/scripts/install.ts against a local
 * HTTP mirror (Bun.serve) serving a real tarball built from the dev library.
 * Needs no broker/docker — only native/build/libbunrdkafka.so (skips otherwise).
 *
 * The build fallback is exercised via BUN_RDKAFKA_TEST_BUILD_CMD (internal test
 * hook) so the test never runs a real cmake build.
 */

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const repoRoot = join(import.meta.dir, "..", "..");
const devLib = join(repoRoot, "native", "build", "libbunrdkafka.so");
const installScript = join(
  repoRoot,
  "packages",
  "bun-rdkafka",
  "scripts",
  "install.ts",
);

const isLinuxX64 = process.platform === "linux" && process.arch === "x64";
const canRun = isLinuxX64 && existsSync(devLib);

const TARGET = "linux-x64-gnu";
const ASSET = `libbunrdkafka-${TARGET}.tar.gz`;
const VERSION = "0.1.0";

let workDir: string;
let tarballBytes: Uint8Array;
let goodSums: string;
let server: ReturnType<typeof Bun.serve> | undefined;
/** Per-request overrides: pathname → Response factory (undefined → default). */
let overrides: Map<string, () => Response>;

function sha256Hex(data: Uint8Array): string {
  return new Bun.CryptoHasher("sha256").update(data).digest("hex");
}

/** Creates a fresh fake npm-installed package root and returns its path. */
function makeFakePkgRoot(name: string): string {
  const dir = join(workDir, name);
  mkdirSync(dir, { recursive: true });
  writeFileSync(
    join(dir, "package.json"),
    JSON.stringify({ name: "@vnstrawhat/bun-rdkafka", version: VERSION }),
  );
  return dir;
}

async function runInstall(
  pkgRoot: string,
  extraEnv: Record<string, string> = {},
): Promise<{ code: number; out: string }> {
  const proc = Bun.spawn(["bun", installScript], {
    env: {
      ...process.env,
      BUN_RDKAFKA_PKG_ROOT: pkgRoot,
      BUN_RDKAFKA_BINARY_MIRROR: `http://127.0.0.1:${server!.port}`,
      BUN_RDKAFKA_INSTALL_STRICT: "1",
      ...extraEnv,
    },
    stdout: "pipe",
    stderr: "pipe",
  });
  const code = await proc.exited;
  const out =
    (await new Response(proc.stdout).text()) + (await new Response(proc.stderr).text());
  return { code, out };
}

beforeAll(async () => {
  if (!canRun) return;
  workDir = join(tmpdir(), `bun-rdkafka-install-test-${process.pid}`);
  mkdirSync(workDir, { recursive: true });

  // Real tarball from the dev build, matching the release.yml layout (lib at root).
  const tarPath = join(workDir, ASSET);
  const tarProc = Bun.spawn(
    ["tar", "-czf", tarPath, "-C", join(repoRoot, "native", "build"), "libbunrdkafka.so"],
    { stdout: "inherit", stderr: "inherit" },
  );
  expect(await tarProc.exited).toBe(0);
  tarballBytes = new Uint8Array(await Bun.file(tarPath).arrayBuffer());
  goodSums = `${sha256Hex(tarballBytes)}  ${ASSET}\n`;

  overrides = new Map();
  server = Bun.serve({
    port: 0,
    fetch(req) {
      const path = new URL(req.url).pathname;
      const override = overrides.get(path);
      if (override) return override();
      if (path === `/v${VERSION}/SHA256SUMS`) return new Response(goodSums);
      if (path === `/v${VERSION}/${ASSET}`) return new Response(tarballBytes);
      return new Response("not found", { status: 404 });
    },
  });
});

afterAll(() => {
  server?.stop(true);
  if (workDir) rmSync(workDir, { recursive: true, force: true });
});

describe.skipIf(!canRun)("install script (local mirror)", () => {
  test("happy path: downloads, verifies, unpacks into prebuilds/", async () => {
    const pkgRoot = makeFakePkgRoot("happy");
    const { code, out } = await runInstall(pkgRoot);
    expect(out).toContain("installed prebuilt");
    expect(code).toBe(0);

    const lib = join(pkgRoot, "prebuilds", TARGET, "libbunrdkafka.so");
    expect(existsSync(lib)).toBe(true);
    // Byte-identical to the dev library it was packed from.
    expect(sha256Hex(new Uint8Array(readFileSync(lib)))).toBe(
      sha256Hex(new Uint8Array(readFileSync(devLib))),
    );
    const meta = JSON.parse(readFileSync(join(pkgRoot, "prebuilds", TARGET, ".install-meta.json"), "utf8"));
    expect(meta).toMatchObject({ version: VERSION, source: "download" });

    // Idempotent: the second run skips without touching the network.
    const again = await runInstall(pkgRoot);
    expect(again.code).toBe(0);
    expect(again.out).toContain("nothing to do");
  });

  test("checksum mismatch falls back to the source build", async () => {
    const pkgRoot = makeFakePkgRoot("bad-checksum");
    overrides.set(`/v${VERSION}/SHA256SUMS`, () => new Response(`${"0".repeat(64)}  ${ASSET}\n`));
    try {
      const { code, out } = await runInstall(pkgRoot, {
        BUN_RDKAFKA_TEST_BUILD_CMD:
          'printf fake-built-library > "$BRK_INSTALL_OUT_DIR/$BRK_INSTALL_LIB_NAME"',
      });
      expect(out).toContain("checksum mismatch");
      expect(out).toContain("falling back to building from source");
      expect(code).toBe(0);
      const lib = join(pkgRoot, "prebuilds", TARGET, "libbunrdkafka.so");
      expect(readFileSync(lib, "utf8")).toBe("fake-built-library");
      const meta = JSON.parse(readFileSync(join(pkgRoot, "prebuilds", TARGET, ".install-meta.json"), "utf8"));
      expect(meta.source).toBe("build");
    } finally {
      overrides.clear();
    }
  });

  test("missing asset (404) falls back to the source build", async () => {
    const pkgRoot = makeFakePkgRoot("no-asset");
    overrides.set(`/v${VERSION}/${ASSET}`, () => new Response("nope", { status: 404 }));
    try {
      const { code, out } = await runInstall(pkgRoot, {
        BUN_RDKAFKA_TEST_BUILD_CMD:
          'printf fallback > "$BRK_INSTALL_OUT_DIR/$BRK_INSTALL_LIB_NAME"',
      });
      expect(out).toContain("falling back to building from source");
      expect(code).toBe(0);
      expect(
        readFileSync(join(pkgRoot, "prebuilds", TARGET, "libbunrdkafka.so"), "utf8"),
      ).toBe("fallback");
    } finally {
      overrides.clear();
    }
  });

  test("strict mode fails hard when download AND build both fail", async () => {
    const pkgRoot = makeFakePkgRoot("all-fail");
    overrides.set(`/v${VERSION}/SHA256SUMS`, () => new Response("gone", { status: 404 }));
    try {
      const { code, out } = await runInstall(pkgRoot, {
        BUN_RDKAFKA_TEST_BUILD_CMD: "exit 7",
      });
      expect(code).toBe(1); // BUN_RDKAFKA_INSTALL_STRICT=1
      expect(out).toContain("NO native library installed");
      expect(out).toContain("trustedDependencies");
    } finally {
      overrides.clear();
    }
  });

  test("BUN_RDKAFKA_SKIP_DOWNLOAD short-circuits everything", async () => {
    const pkgRoot = makeFakePkgRoot("skip");
    const { code, out } = await runInstall(pkgRoot, { BUN_RDKAFKA_SKIP_DOWNLOAD: "1" });
    expect(code).toBe(0);
    expect(out).toContain("nothing to do");
    expect(existsSync(join(pkgRoot, "prebuilds"))).toBe(false);
  });
});
