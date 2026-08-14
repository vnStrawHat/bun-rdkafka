/**
 * postpack.ts — runs automatically after `npm pack` / `npm publish`.
 * Removes the temporary copies created by prepack.ts, keeping the dev tree clean.
 */

import { existsSync, rmSync } from "node:fs";
import { join } from "node:path";

const pkgRoot = join(import.meta.dir, "..");
// Only clean up inside the monorepo — in an unpacked tarball these are real files.
if (existsSync(join(pkgRoot, "..", "..", "native", "CMakeLists.txt"))) {
  rmSync(join(pkgRoot, "native"), { recursive: true, force: true });
  rmSync(join(pkgRoot, "librdkafka.version"), { force: true });
  console.log("bun-rdkafka postpack: removed the temporary native/ copy");
}
