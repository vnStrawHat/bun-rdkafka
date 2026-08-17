/**
 * Hermetic guard: importing the package entrypoint (including its default
 * export with the lazy `features` / `librdkafkaVersion` getters) must NOT
 * dlopen the native library. Runs in a subprocess so no other test file's
 * `loadNative()` can pollute the check.
 */
import { expect, test } from "bun:test";
import { join } from "node:path";

const ROOT = join(import.meta.dir, "../..");

test("importing src/index.ts does not load the native library", async () => {
  const script = `
    const pkg = await import(${JSON.stringify(join(ROOT, "packages/bun-rdkafka/src/index.ts"))});
    const { isNativeLoaded } = await import(${JSON.stringify(join(ROOT, "packages/bun-rdkafka/src/ffi/loader.ts"))});
    // Touch the named exports and the default export object (NOT the getters).
    const keys = Object.keys(pkg.default);
    console.log(JSON.stringify({
      loaded: isNativeLoaded(),
      hasFeaturesFn: typeof pkg.features === "function",
      hasGetter: keys.includes("features") && keys.includes("librdkafkaVersion"),
    }));
  `;
  const proc = Bun.spawn(["bun", "-e", script], { cwd: ROOT, stdout: "pipe", stderr: "pipe" });
  const [out, err, code] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  expect(code, err).toBe(0);
  const result = JSON.parse(out.trim().split("\n").at(-1) ?? "{}") as Record<string, boolean>;
  expect(result["loaded"]).toBe(false);
  expect(result["hasFeaturesFn"]).toBe(true);
  expect(result["hasGetter"]).toBe(true);
});
