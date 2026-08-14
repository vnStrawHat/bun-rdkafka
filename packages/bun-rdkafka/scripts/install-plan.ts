/**
 * install-plan.ts — pure decision logic for the prebuilt-binary installer.
 *
 * Everything here is side-effect free so it can be unit-tested without network,
 * filesystem, or subprocesses. The orchestration (download / extract / build)
 * lives in ./install.ts.
 */

/** Platforms with a prebuilt release asset (spec §6, Tier-1). */
export const PREBUILT_TARGETS = [
  "linux-x64-gnu",
  "linux-arm64-gnu",
  "darwin-x64",
  "darwin-arm64",
  "win32-x64",
] as const;

export type PrebuiltTarget = (typeof PREBUILT_TARGETS)[number];

/** Default GitHub Releases download base (no trailing slash). */
export const DEFAULT_RELEASE_BASE =
  "https://github.com/vnStrawHat/bun-rdkafka/releases/download";

/**
 * Maps (platform, arch, musl?) to a prebuilt target key, or `null` when no
 * prebuilt asset exists for the combination (e.g. musl libc, unsupported arch).
 */
export function detectTarget(
  platform: string,
  arch: string,
  musl: boolean,
): PrebuiltTarget | null {
  if (platform === "linux" && musl) return null; // no musl prebuilts yet (spec §6 Tier-2)
  const key = `${platform}-${arch}${platform === "linux" ? "-gnu" : ""}`;
  return (PREBUILT_TARGETS as readonly string[]).includes(key)
    ? (key as PrebuiltTarget)
    : null;
}

/** Release asset file name for a target. Must match release.yml packaging. */
export function assetName(target: string): string {
  return `libbunrdkafka-${target}.tar.gz`;
}

/** Shared-library file names that may appear inside the asset (MSVC drops `lib`). */
export function libFileNamesForTarget(target: string): string[] {
  if (target.startsWith("win32")) return ["libbunrdkafka.dll", "bunrdkafka.dll"];
  if (target.startsWith("darwin")) return ["libbunrdkafka.dylib"];
  return ["libbunrdkafka.so"];
}

/**
 * Base URL for release assets of a given package version.
 * `mirror` (from BUN_RDKAFKA_BINARY_MIRROR) replaces the GitHub base; it must
 * serve the same layout: `<mirror>/v<version>/<asset>`.
 */
export function releaseBaseUrl(version: string, mirror?: string): string {
  const base = (mirror ?? DEFAULT_RELEASE_BASE).replace(/\/+$/, "");
  return `${base}/v${version}`;
}

/**
 * Parses the standard `sha256sum` output format:
 * `<64-hex>  <filename>` (also accepts the `*<filename>` binary-mode marker).
 * Returns a map of filename → lowercase hex digest.
 */
export function parseSha256Sums(text: string): Map<string, string> {
  const out = new Map<string, string>();
  for (const line of text.split("\n")) {
    const m = /^([0-9a-fA-F]{64})\s+\*?(.+?)\s*$/.exec(line.trim());
    if (m?.[1] && m[2]) out.set(m[2], m[1].toLowerCase());
  }
  return out;
}

/** What the installer should do, in order of preference. */
export type InstallAction =
  | { kind: "skip"; reason: string }
  | { kind: "download"; target: PrebuiltTarget }
  | { kind: "build"; reason: string };

export interface InstallEnv {
  /** BUN_RDKAFKA_SKIP_DOWNLOAD=1 — do nothing at all. */
  skipDownload: boolean;
  /** BUN_RDKAFKA_FORCE_BUILD=1 — skip the download, always build from source. */
  forceBuild: boolean;
  /** Detected prebuilt target, or null when unsupported (e.g. musl). */
  target: PrebuiltTarget | null;
  /** The library is already present in prebuilds/ with a matching install marker. */
  alreadyInstalled: boolean;
  /** Running inside the bun-rdkafka development repo (workspace layout). */
  devRepo: boolean;
}

/** Decides the initial action. Download failures later degrade to "build". */
export function decideInstall(env: InstallEnv): InstallAction {
  if (env.skipDownload) {
    return { kind: "skip", reason: "BUN_RDKAFKA_SKIP_DOWNLOAD is set" };
  }
  if (env.devRepo) {
    // Inside the monorepo `bun install` runs this hook on every install; devs
    // build via `bun run build:native` instead, so never download or build here.
    return { kind: "skip", reason: "development repo detected" };
  }
  if (env.alreadyInstalled) {
    return { kind: "skip", reason: "prebuilt library already installed" };
  }
  if (env.forceBuild) {
    return { kind: "build", reason: "BUN_RDKAFKA_FORCE_BUILD is set" };
  }
  if (env.target === null) {
    return { kind: "build", reason: "no prebuilt asset for this platform" };
  }
  return { kind: "download", target: env.target };
}
