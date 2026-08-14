/**
 * Pure decision logic of the prebuilt installer (packages/bun-rdkafka/scripts/
 * install-plan.ts) — no network, no filesystem, no subprocesses.
 */

import { describe, expect, test } from "bun:test";
import {
  DEFAULT_RELEASE_BASE,
  PREBUILT_TARGETS,
  assetName,
  decideInstall,
  detectTarget,
  libFileNamesForTarget,
  parseSha256Sums,
  releaseBaseUrl,
} from "../../packages/bun-rdkafka/scripts/install-plan.ts";

describe("detectTarget", () => {
  test("maps the five Tier-1 platforms", () => {
    expect(detectTarget("linux", "x64", false)).toBe("linux-x64-gnu");
    expect(detectTarget("linux", "arm64", false)).toBe("linux-arm64-gnu");
    expect(detectTarget("darwin", "x64", false)).toBe("darwin-x64");
    expect(detectTarget("darwin", "arm64", false)).toBe("darwin-arm64");
    expect(detectTarget("win32", "x64", false)).toBe("win32-x64");
  });

  test("musl and unsupported combinations return null", () => {
    expect(detectTarget("linux", "x64", true)).toBeNull(); // musl
    expect(detectTarget("linux", "ia32", false)).toBeNull();
    expect(detectTarget("freebsd", "x64", false)).toBeNull();
    expect(detectTarget("win32", "arm64", false)).toBeNull();
  });
});

describe("asset naming", () => {
  test("matches the release.yml packaging convention", () => {
    expect(assetName("linux-x64-gnu")).toBe("libbunrdkafka-linux-x64-gnu.tar.gz");
    for (const t of PREBUILT_TARGETS) {
      expect(assetName(t)).toBe(`libbunrdkafka-${t}.tar.gz`);
    }
  });

  test("library file names per target (MSVC may drop the lib prefix)", () => {
    expect(libFileNamesForTarget("linux-x64-gnu")).toEqual(["libbunrdkafka.so"]);
    expect(libFileNamesForTarget("darwin-arm64")).toEqual(["libbunrdkafka.dylib"]);
    expect(libFileNamesForTarget("win32-x64")).toEqual([
      "libbunrdkafka.dll",
      "bunrdkafka.dll",
    ]);
  });
});

describe("releaseBaseUrl", () => {
  test("defaults to the GitHub release download base with a v-prefixed tag", () => {
    expect(releaseBaseUrl("0.1.0")).toBe(`${DEFAULT_RELEASE_BASE}/v0.1.0`);
    expect(DEFAULT_RELEASE_BASE).toContain("github.com/vnStrawHat/bun-rdkafka");
  });

  test("a mirror replaces the base and tolerates trailing slashes", () => {
    expect(releaseBaseUrl("0.1.0", "https://mirror.example/kafka/")).toBe(
      "https://mirror.example/kafka/v0.1.0",
    );
  });
});

describe("parseSha256Sums", () => {
  const hexA = "a".repeat(64);
  const hexB = "B".repeat(64);

  test("parses the sha256sum output format, including the binary-mode marker", () => {
    const sums = parseSha256Sums(
      `${hexA}  libbunrdkafka-linux-x64-gnu.tar.gz\n${hexB} *libbunrdkafka-win32-x64.tar.gz\n`,
    );
    expect(sums.get("libbunrdkafka-linux-x64-gnu.tar.gz")).toBe(hexA);
    // digests are normalized to lowercase
    expect(sums.get("libbunrdkafka-win32-x64.tar.gz")).toBe("b".repeat(64));
  });

  test("ignores malformed lines instead of throwing", () => {
    const sums = parseSha256Sums(`not-a-checksum-line\n${hexA}\n\n${hexA}  ok.tar.gz`);
    expect(sums.size).toBe(1);
    expect(sums.get("ok.tar.gz")).toBe(hexA);
  });
});

describe("decideInstall", () => {
  const base = {
    skipDownload: false,
    forceBuild: false,
    target: "linux-x64-gnu" as const,
    alreadyInstalled: false,
    devRepo: false,
  };

  test("normal case downloads the matching target", () => {
    expect(decideInstall(base)).toEqual({ kind: "download", target: "linux-x64-gnu" });
  });

  test("skip wins over everything", () => {
    expect(decideInstall({ ...base, skipDownload: true }).kind).toBe("skip");
    expect(decideInstall({ ...base, skipDownload: true, forceBuild: true }).kind).toBe("skip");
  });

  test("the dev repo never downloads or builds", () => {
    expect(decideInstall({ ...base, devRepo: true }).kind).toBe("skip");
  });

  test("an existing install is idempotent", () => {
    expect(decideInstall({ ...base, alreadyInstalled: true }).kind).toBe("skip");
  });

  test("force-build and unsupported platforms build from source", () => {
    expect(decideInstall({ ...base, forceBuild: true }).kind).toBe("build");
    expect(decideInstall({ ...base, target: null }).kind).toBe("build");
  });
});
