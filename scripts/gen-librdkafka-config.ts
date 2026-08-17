#!/usr/bin/env bun
/**
 * gen-librdkafka-config.ts — generates the typed librdkafka property
 * interfaces (`packages/bun-rdkafka/src/core/librdkafka-config.ts`) from the
 * CONFIGURATION.md of the librdkafka version the shim links against, so
 * editors can complete and document `new Producer({ ... })` keys.
 *
 * Usage:
 *   bun scripts/gen-librdkafka-config.ts [path/to/CONFIGURATION.md]
 *   (default: native/build/_deps/librdkafka-src/CONFIGURATION.md — present
 *    after `bun run build:native`)
 *
 * Only properties an application can set from JS are emitted; C-pointer
 * properties (`*_cb`, `opaque`, `default_topic_conf`, … — "see dedicated
 * API") are skipped. The callbacks bun-rdkafka does support (`dr_cb`,
 * `rebalance_cb`, …) are typed by hand next to the classes that use them.
 */

import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const ROOT = new URL("..", import.meta.url).pathname.replace(/\/$/, "");
const SRC = process.argv[2] ?? join(ROOT, "native/build/_deps/librdkafka-src/CONFIGURATION.md");
const OUT = join(ROOT, "packages/bun-rdkafka/src/core/librdkafka-config.ts");

if (!existsSync(SRC)) {
  console.error(`CONFIGURATION.md not found at ${SRC} — run \`bun run build:native\` first or pass a path.`);
  process.exit(1);
}

interface Prop {
  name: string;
  scope: "*" | "C" | "P";
  range: string;
  def: string;
  importance: string;
  description: string;
  type: string;
}

function parseTable(lines: string[]): Prop[] {
  const out: Prop[] = [];
  for (const line of lines) {
    if (!line.includes("|") || line.startsWith("Property") || line.startsWith("---")) continue;
    const cells = line.split("|").map((c) => c.trim());
    if (cells.length < 6) continue;
    const [name, scope, range, def, importance, ...rest] = cells as [string, string, string, string, string, ...string[]];
    const descRaw = rest.join("|");
    const m = /<br>\*Type: ([^*]+)\*\s*$/.exec(descRaw);
    const type = m ? m[1]!.trim() : "string";
    const description = descRaw.replace(/<br>\*Type: [^*]+\*\s*$/, "").trim();
    if (scope !== "*" && scope !== "C" && scope !== "P") continue;
    out.push({ name, scope, range, def, importance, description, type });
  }
  return out;
}

const text = readFileSync(SRC, "utf8");
/** librdkafka version: RD_KAFKA_VERSION (0xMMmmrrpp) from the sibling rdkafka.h when present. */
function detectVersion(): string {
  const header = join(SRC, "..", "src", "rdkafka.h");
  if (!existsSync(header)) return "unknown";
  const m = /#define RD_KAFKA_VERSION\s+0x([0-9a-fA-F]{8})/.exec(readFileSync(header, "utf8"));
  if (!m) return "unknown";
  const v = Number.parseInt(m[1]!, 16);
  return `${(v >> 24) & 0xff}.${(v >> 16) & 0xff}.${(v >> 8) & 0xff}`;
}
const [globalPart, topicPart] = text.split("## Topic configuration properties");
const globalProps = parseTable((globalPart ?? "").split("\n"));
const topicProps = parseTable((topicPart ?? "").split("\n"));

function tsType(p: Prop): string | null {
  switch (p.type) {
    case "integer":
    case "float":
      if (p.name === "request.required.acks" || p.name === "acks") return 'number | "all"';
      return "number";
    case "boolean":
      return "boolean";
    case "enum value": {
      const values = p.range.split(",").map((v) => v.trim()).filter(Boolean);
      return values.length > 0 ? values.map((v) => JSON.stringify(v)).join(" | ") : "string";
    }
    case "string":
    case "CSV flags":
    case "pattern list":
      return "string";
    default:
      return null; // "see dedicated API" — pointers/callbacks, not settable from JS
  }
}

function jsdoc(p: Prop): string {
  const lines: string[] = [];
  const desc = p.description.replace(/\*\//g, "*\\/");
  lines.push(desc);
  // Plain text (before the block tag, which would otherwise swallow it) rather than a
  // custom @range tag: TypeDoc warns on unknown block tags.
  if (p.range !== "" && p.type !== "enum value" && p.type !== "boolean") lines.push("", `Range: \`${p.range}\``);
  if (p.def !== "") lines.push("", `@default ${p.def}`);
  return ["  /**", ...lines.map((l) => (l === "" ? "   *" : `   * ${l}`)), "   */"].join("\n");
}

function emit(name: string, props: Prop[], base?: string): string {
  const members = props
    .map((p) => {
      const t = tsType(p);
      if (t === null) return null;
      return `${jsdoc(p)}\n  ${JSON.stringify(p.name)}?: ${t};`;
    })
    .filter((m): m is string => m !== null);
  const head = base ? `export type ${name} = ${base} & {` : `export type ${name} = {`;
  if (members.length === 0) return `${head}\n  // (no JS-settable properties in this librdkafka version)\n};\n`;
  return `${head}\n${members.join("\n\n")}\n};\n`;
}

const version = detectVersion();
const banner = `/* THIS FILE IS GENERATED — do not edit by hand.
 * Source: librdkafka CONFIGURATION.md (librdkafka ${version}), via
 * \`bun scripts/gen-librdkafka-config.ts\`.
 *
 * Typed librdkafka properties for editor completion and documentation. They
 * are type aliases (not interfaces) on purpose: object-literal types get an
 * implicit index signature, so a typed config is still assignable to the
 * \`Record<string, unknown>\` the ConfigBuilder consumes. Values are passed to
 * librdkafka unmodified; the library validates ranges at construction.
 */

`;

const out =
  banner +
  "/** Properties every client accepts (C/P = `*`). */\n" +
  emit("GlobalConfig", globalProps.filter((p) => p.scope === "*")) +
  "\n/** Producer-only global properties, on top of {@link GlobalConfig}. */\n" +
  emit("ProducerGlobalConfig", globalProps.filter((p) => p.scope === "P"), "GlobalConfig") +
  "\n/** Consumer-only global properties, on top of {@link GlobalConfig}. */\n" +
  emit("ConsumerGlobalConfig", globalProps.filter((p) => p.scope === "C"), "GlobalConfig") +
  "\n/** Topic-level properties every client accepts. */\n" +
  emit("TopicConfig", topicProps.filter((p) => p.scope === "*")) +
  "\n/** Producer topic-level properties, on top of {@link TopicConfig}. */\n" +
  emit("ProducerTopicConfig", topicProps.filter((p) => p.scope === "P"), "TopicConfig") +
  "\n/** Consumer topic-level properties, on top of {@link TopicConfig}. */\n" +
  emit("ConsumerTopicConfig", topicProps.filter((p) => p.scope === "C"), "TopicConfig") +
  `\n/** The librdkafka version whose CONFIGURATION.md produced these types. */\nexport const LIBRDKAFKA_CONFIG_VERSION = ${JSON.stringify(version)};\n`;

writeFileSync(OUT, out);
const count = (s: string) => (s.match(/^  "/gm) ?? []).length;
console.log(`wrote ${OUT}: ${count(out)} properties from librdkafka ${version}`);
