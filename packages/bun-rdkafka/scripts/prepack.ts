/**
 * prepack.ts — runs automatically on `npm pack` / `npm publish`.
 *
 * Copies the native shim sources from the monorepo root into the package so the
 * npm tarball is self-contained for the source-build fallback of install.ts:
 *   <repo>/native/            → <package>/native/        (excluding build/)
 *   <repo>/librdkafka.version → <package>/librdkafka.version
 *
 * Rationale (see design ADR-8): the sources are tiny (~100 KB — librdkafka
 * itself is fetched by CMake at build time, not shipped), and shipping them
 * means the fallback build needs no extra download of the repo source tarball.
 * Both copies are gitignored; postpack.ts removes them again.
 */

import { cpSync, existsSync, rmSync } from "node:fs";
import { basename, join } from "node:path";

const pkgRoot = join(import.meta.dir, "..");
const repoNative = join(pkgRoot, "..", "..", "native");
const repoVersion = join(pkgRoot, "..", "..", "librdkafka.version");

if (!existsSync(join(repoNative, "CMakeLists.txt"))) {
  // Packing outside the monorepo (e.g. from an unpacked tarball): nothing to copy.
  console.log("bun-rdkafka prepack: no monorepo native/ found, skipping copy");
  process.exit(0);
}

rmSync(join(pkgRoot, "native"), { recursive: true, force: true });
cpSync(repoNative, join(pkgRoot, "native"), {
  recursive: true,
  filter: (src) => basename(src) !== "build", // never ship build artifacts
});
cpSync(repoVersion, join(pkgRoot, "librdkafka.version"));
console.log("bun-rdkafka prepack: copied native/ + librdkafka.version into the package");
