/**
 * api-surface.test.ts — conformance test: compares bun-rdkafka's API surface
 * against confluent-kafka-javascript's official `.d.ts` (spec §8, plan M5/M6).
 *
 * Approach:
 *  1. Parse `test/conformance/upstream-types/*.d.ts` with the **TypeScript
 *     Compiler API** (no hardcoded API lists — upgrading upstream lets the
 *     test see new APIs by itself). Only **runtime-valued** parts are taken:
 *     classes, enums, consts, functions, and members of classes/interfaces/
 *     type aliases describing instances.
 *  2. Extract the local package's surface via **reflection** on
 *     `packages/bun-rdkafka/src/index.ts`: module namespace exports + walking
 *     each class's prototype chain.
 *  3. Compare names, *kinds* (const/function/class), and parameter counts.
 *
 * Constraint: this test **must not load native (.so)**. Importing
 * `src/index.ts` must preserve `ffi/loader.ts`'s laziness; one test asserts
 * exactly that — if it fails, it is a lazy-load bug in src, not this test's
 * concern.
 *
 * Every missing API must be listed in `exclusions.ts` with a reason; and an
 * exclusion that is no longer missing also fails the test (forcing list
 * cleanup).
 */

import { describe, expect, test } from "bun:test";
import * as ts from "typescript";
import { join } from "node:path";

import { EXCLUSIONS, type Exclusion } from "./exclusions.ts";
import * as pkg from "../../packages/bun-rdkafka/src/index.ts";
import { isNativeLoaded } from "../../packages/bun-rdkafka/src/ffi/loader.ts";

/* ========================================================================== *
 * Common model                                                               *
 * ========================================================================== */

/** Kind of a surface name — used to catch const ↔ function drift. */
type SurfaceKind = "value" | "callable";

interface UpstreamMember {
  name: string;
  kind: SurfaceKind;
  /** Fewest required parameters across the overloads. */
  minArity: number;
  /** Most parameters across the overloads (`REST_ARITY` when a rest param exists). */
  maxArity: number;
}

interface LocalMember {
  kind: SurfaceKind;
  /** `Function.prototype.length` — parameters before the first optional/rest one. */
  arity: number;
}

interface Group {
  /** The `path` prefix in exclusions, e.g. `rdkafka:Producer`. */
  id: string;
  /** The API group label in the summary table. */
  bucket: "callback API" | "KafkaJS namespace" | "spec FR";
  upstream: UpstreamMember[];
  local: Map<string, LocalMember>;
}

const REST_ARITY = 99;

/* ========================================================================== *
 * 1. Extract the upstream surface with the TypeScript Compiler API           *
 * ========================================================================== */

const UPSTREAM_DIR = join(import.meta.dir, "upstream-types");
const RDKAFKA_DTS = join(UPSTREAM_DIR, "rdkafka.d.ts");
const KAFKAJS_DTS = join(UPSTREAM_DIR, "kafkajs.d.ts");

const program = ts.createProgram([RDKAFKA_DTS, KAFKAJS_DTS], {
  target: ts.ScriptTarget.ESNext,
  module: ts.ModuleKind.ESNext,
  moduleResolution: ts.ModuleResolutionKind.Bundler,
  // `noLib` — no lib.d.ts needed: we only read declarations, never type-check.
  noLib: true,
  skipLibCheck: true,
  strict: false,
});
const checker = program.getTypeChecker();

function sourceFileOf(path: string): ts.SourceFile {
  const sf = program.getSourceFile(path);
  if (sf === undefined) throw new Error(`could not read ${path}`);
  return sf;
}

function moduleSymbol(path: string): ts.Symbol {
  const sym = checker.getSymbolAtLocation(sourceFileOf(path));
  if (sym === undefined) throw new Error(`${path} is not a module`);
  return sym;
}

function unalias(sym: ts.Symbol): ts.Symbol {
  return sym.flags & ts.SymbolFlags.Alias ? checker.getAliasedSymbol(sym) : sym;
}

const VALUE_FLAGS =
  ts.SymbolFlags.Class |
  ts.SymbolFlags.Function |
  ts.SymbolFlags.Variable |
  ts.SymbolFlags.BlockScopedVariable |
  ts.SymbolFlags.RegularEnum |
  ts.SymbolFlags.ConstEnum;

/**
 * Computes `[minArity, maxArity]` across all overloads; `null` when not
 * callable. Classes count their construct signature (`new C(...)`) — under
 * reflection a class is just a function too.
 */
function arityOfType(type: ts.Type): { min: number; max: number } | null {
  const sigs =
    type.getCallSignatures().length > 0 ? type.getCallSignatures() : type.getConstructSignatures();
  if (sigs.length === 0) return null;
  let min = Number.POSITIVE_INFINITY;
  let max = 0;
  for (const sig of sigs) {
    let required = 0;
    let total = 0;
    for (const p of sig.getParameters()) {
      const decl = p.valueDeclaration;
      const param = decl !== undefined && ts.isParameter(decl) ? decl : undefined;
      if (param?.dotDotDotToken !== undefined) {
        total = REST_ARITY;
        continue;
      }
      total += 1;
      if (param?.questionToken === undefined && param?.initializer === undefined) required += 1;
    }
    min = Math.min(min, required);
    max = Math.max(max, total);
  }
  return { min: min === Number.POSITIVE_INFINITY ? 0 : min, max };
}

function typeOfSymbol(sym: ts.Symbol): ts.Type {
  const decl = sym.valueDeclaration ?? sym.declarations?.[0];
  return decl === undefined
    ? checker.getTypeOfSymbol(sym)
    : checker.getTypeOfSymbolAtLocation(sym, decl);
}

function memberOfSymbol(sym: ts.Symbol): UpstreamMember {
  const arity = arityOfType(typeOfSymbol(sym));
  return arity === null
    ? { name: sym.getName(), kind: "value", minArity: 0, maxArity: 0 }
    : { name: sym.getName(), kind: "callable", minArity: arity.min, maxArity: arity.max };
}

/** A `.d.ts` module's **runtime-valued** exports (interfaces/types dropped). */
function upstreamModuleValues(path: string): UpstreamMember[] {
  const out: UpstreamMember[] = [];
  for (const raw of checker.getExportsOfModule(moduleSymbol(path))) {
    const sym = unalias(raw);
    if ((sym.flags & VALUE_FLAGS) === 0) continue;
    out.push({ ...memberOfSymbol(sym), name: raw.getName() });
  }
  return out.sort((a, b) => a.name.localeCompare(b.name));
}

function exportedSymbol(path: string, name: string): ts.Symbol {
  const found = checker
    .getExportsOfModule(moduleSymbol(path))
    .find((s) => s.getName() === name);
  if (found === undefined) throw new Error(`${path} does not export "${name}"`);
  return unalias(found);
}

/**
 * Keeps only members declared DIRECTLY IN `owner` (not inherited).
 * `Producer extends Client extends EventEmitter` — Client's members are
 * checked by the `rdkafka:Client` group, and EventEmitter's members exist
 * locally since we also `extends EventEmitter`; without filtering, the report
 * would repeat uselessly.
 */
function declaredIn(owner: ts.Symbol, prop: ts.Symbol): boolean {
  const owners = owner.declarations ?? [];
  return (prop.declarations ?? []).some((d) => owners.includes(d.parent as ts.Declaration));
}

function propsToMembers(props: readonly ts.Symbol[]): UpstreamMember[] {
  return props
    .map((p) => memberOfSymbol(p))
    .filter((m) => m.name !== "prototype" && !m.name.startsWith("__"))
    .sort((a, b) => a.name.localeCompare(b.name));
}

/**
 * Instance members of a class / interface / type alias (intersections
 * included). `ownOnly` is on for classes & interfaces; off for `A & { ... }`
 * type aliases (e.g. `KafkaJS.Producer`), which carry no inheritance to split.
 */
function upstreamInstanceMembers(path: string, name: string, ownOnly: boolean): UpstreamMember[] {
  const sym = exportedSymbol(path, name);
  const props = checker.getPropertiesOfType(checker.getDeclaredTypeOfSymbol(sym));
  return propsToMembers(ownOnly ? props.filter((p) => declaredIn(sym, p)) : props);
}

/** Static members declared directly in the class (inherited EventEmitter statics dropped). */
function upstreamStaticMembers(path: string, name: string): UpstreamMember[] {
  const sym = exportedSymbol(path, name);
  const props = checker.getPropertiesOfType(typeOfSymbol(sym));
  return propsToMembers(props.filter((p) => declaredIn(sym, p)));
}

/** Members of an enum (or an object-literal type like `CODES.ERRORS`). */
function upstreamKeys(members: readonly ts.Symbol[]): UpstreamMember[] {
  return members
    .map((s) => ({ name: s.getName(), kind: "value" as const, minArity: 0, maxArity: 0 }))
    .sort((a, b) => a.name.localeCompare(b.name));
}

function upstreamEnumMembers(path: string, name: string): UpstreamMember[] {
  const sym = exportedSymbol(path, name);
  return upstreamKeys(checker.getPropertiesOfType(typeOfSymbol(sym)));
}

/** `CODES.ERRORS` in errors.d.ts (re-exported via rdkafka.d.ts). */
function upstreamErrorCodes(): UpstreamMember[] {
  const codes = exportedSymbol(RDKAFKA_DTS, "CODES");
  const errors = checker.getPropertyOfType(typeOfSymbol(codes), "ERRORS");
  if (errors === undefined) throw new Error("CODES.ERRORS does not exist in the upstream types");
  return upstreamKeys(checker.getPropertiesOfType(typeOfSymbol(errors)));
}

/* ========================================================================== *
 * 2. Extract the local surface via reflection                                *
 * ========================================================================== */

function describe1(value: unknown): LocalMember {
  return typeof value === "function"
    ? { kind: "callable", arity: (value as (...a: unknown[]) => unknown).length }
    : { kind: "value", arity: 0 };
}

/** Keys of a namespace/enum object (no prototype-chain walk). */
function localOwn(obj: object): Map<string, LocalMember> {
  const out = new Map<string, LocalMember>();
  for (const [k, v] of Object.entries(obj)) out.set(k, describe1(v));
  return out;
}

/** A class's statics: Function's length/name/prototype dropped. */
function localStatics(ctor: Function): Map<string, LocalMember> {
  const skip = new Set(["length", "name", "prototype"]);
  const out = new Map<string, LocalMember>();
  for (const k of Object.getOwnPropertyNames(ctor)) {
    if (skip.has(k)) continue;
    out.set(k, describe1((ctor as unknown as Record<string, unknown>)[k]));
  }
  return out;
}

/**
 * Walks the prototype chain (EventEmitter included) up to just before
 * `Object.prototype`. Getters are read via descriptors to avoid side effects.
 */
function localProtoChain(start: object): Map<string, LocalMember> {
  const out = new Map<string, LocalMember>();
  let cur: object | null = start;
  while (cur !== null && cur !== Object.prototype) {
    for (const k of Object.getOwnPropertyNames(cur)) {
      if (k === "constructor" || out.has(k)) continue;
      const d = Object.getOwnPropertyDescriptor(cur, k);
      if (d === undefined) continue;
      out.set(k, d.get !== undefined ? { kind: "value", arity: 0 } : describe1(d.value));
    }
    cur = Object.getPrototypeOf(cur) as object | null;
  }
  return out;
}

/** A local class's instance surface (no instantiation needed). */
function localInstance(ctor: Function): Map<string, LocalMember> {
  return localProtoChain((ctor as { prototype: object }).prototype);
}

/** An instantiated object's instance surface (own props + prototype chain). */
function localObjectInstance(obj: object): Map<string, LocalMember> {
  const out = localProtoChain(Object.getPrototypeOf(obj) as object);
  for (const k of Object.getOwnPropertyNames(obj)) {
    const d = Object.getOwnPropertyDescriptor(obj, k);
    if (d === undefined || out.has(k)) continue;
    out.set(k, d.get !== undefined ? { kind: "value", arity: 0 } : describe1(d.value));
  }
  return out;
}

/* -------------------------------------------------------------------------- *
 * KafkaJS client instantiation — Producer/Consumer/Admin are not exported     *
 * directly (like upstream), only creatable via `new Kafka(...)`. Constructors  *
 * never touch native: the real client is only created in `connect()`.          *
 * -------------------------------------------------------------------------- */
const kafkajsFactory = new pkg.KafkaJS.Kafka({ "metadata.broker.list": "127.0.0.1:9092" });
const kjsProducer = kafkajsFactory.producer();
const kjsConsumer = kafkajsFactory.consumer({ kafkaJS: { groupId: "conformance" } });
const kjsAdmin = kafkajsFactory.admin();

/* ========================================================================== *
 * 3. The table of API groups to cross-check                                  *
 * ========================================================================== */

const localModule = localOwn(pkg);
const localKafkaJs = localOwn(pkg.KafkaJS);

const GROUPS: Group[] = [
  /* ---------------------------------------------------- Callback API (FR-1) */
  {
    id: "rdkafka:module",
    bucket: "callback API",
    upstream: upstreamModuleValues(RDKAFKA_DTS),
    local: localModule,
  },
  {
    id: "rdkafka:Client",
    bucket: "callback API",
    upstream: upstreamInstanceMembers(RDKAFKA_DTS, "Client", true),
    local: localInstance(pkg.Client),
  },
  {
    id: "rdkafka:Producer",
    bucket: "callback API",
    upstream: upstreamInstanceMembers(RDKAFKA_DTS, "Producer", true),
    local: localInstance(pkg.Producer),
  },
  {
    id: "rdkafka:Producer.static",
    bucket: "callback API",
    upstream: upstreamStaticMembers(RDKAFKA_DTS, "Producer"),
    local: localStatics(pkg.Producer),
  },
  {
    id: "rdkafka:HighLevelProducer",
    bucket: "callback API",
    upstream: upstreamInstanceMembers(RDKAFKA_DTS, "HighLevelProducer", true),
    local: localInstance(pkg.HighLevelProducer),
  },
  {
    id: "rdkafka:KafkaConsumer",
    bucket: "callback API",
    upstream: upstreamInstanceMembers(RDKAFKA_DTS, "KafkaConsumer", true),
    local: localInstance(pkg.KafkaConsumer),
  },
  {
    id: "rdkafka:KafkaConsumer.static",
    bucket: "callback API",
    upstream: upstreamStaticMembers(RDKAFKA_DTS, "KafkaConsumer"),
    local: localStatics(pkg.KafkaConsumer),
  },
  {
    id: "rdkafka:AdminClient.static",
    bucket: "callback API",
    upstream: upstreamStaticMembers(RDKAFKA_DTS, "AdminClient"),
    local: localStatics(pkg.AdminClient),
  },
  {
    // `AdminClient.create()` returns `IAdminClient` — the actual instance surface.
    id: "rdkafka:IAdminClient",
    bucket: "callback API",
    upstream: upstreamInstanceMembers(RDKAFKA_DTS, "IAdminClient", true),
    local: localInstance(pkg.AdminClient),
  },
  {
    id: "rdkafka:CODES.ERRORS",
    bucket: "callback API",
    upstream: upstreamErrorCodes(),
    local: localOwn(pkg.CODES.ERRORS),
  },

  /* ------------------------------------------------ KafkaJS namespace (FR-2) */
  {
    id: "kafkajs:module",
    bucket: "KafkaJS namespace",
    upstream: upstreamModuleValues(KAFKAJS_DTS),
    local: localKafkaJs,
  },
  {
    id: "kafkajs:Kafka",
    bucket: "KafkaJS namespace",
    upstream: upstreamInstanceMembers(KAFKAJS_DTS, "Kafka", true),
    local: localInstance(pkg.KafkaJS.Kafka),
  },
  {
    id: "kafkajs:Producer",
    bucket: "KafkaJS namespace",
    upstream: upstreamInstanceMembers(KAFKAJS_DTS, "Producer", false),
    local: localObjectInstance(kjsProducer),
  },
  {
    id: "kafkajs:Consumer",
    bucket: "KafkaJS namespace",
    upstream: upstreamInstanceMembers(KAFKAJS_DTS, "Consumer", false),
    local: localObjectInstance(kjsConsumer),
  },
  {
    id: "kafkajs:Admin",
    bucket: "KafkaJS namespace",
    upstream: upstreamInstanceMembers(KAFKAJS_DTS, "Admin", false),
    local: localObjectInstance(kjsAdmin),
  },
  {
    id: "kafkajs:KafkaJSError",
    bucket: "KafkaJS namespace",
    upstream: upstreamInstanceMembers(KAFKAJS_DTS, "KafkaJSError", true),
    local: localObjectInstance(new pkg.KafkaJS.KafkaJSError("conformance")),
  },
  {
    id: "kafkajs:ErrorCodes",
    bucket: "KafkaJS namespace",
    upstream: upstreamErrorCodes(),
    local: localOwn(pkg.KafkaJS.ErrorCodes),
  },
  {
    id: "kafkajs:logLevel",
    bucket: "KafkaJS namespace",
    upstream: upstreamEnumMembers(KAFKAJS_DTS, "logLevel"),
    local: localOwn(pkg.KafkaJS.logLevel),
  },
  {
    id: "kafkajs:CompressionTypes",
    bucket: "KafkaJS namespace",
    upstream: upstreamEnumMembers(KAFKAJS_DTS, "CompressionTypes"),
    local: localOwn(pkg.KafkaJS.CompressionTypes),
  },
  {
    id: "kafkajs:PartitionAssigners",
    bucket: "KafkaJS namespace",
    upstream: upstreamEnumMembers(KAFKAJS_DTS, "PartitionAssigners"),
    local: localOwn(pkg.KafkaJS.PartitionAssigners),
  },
  {
    id: "kafkajs:PartitionAssignors",
    bucket: "KafkaJS namespace",
    upstream: upstreamEnumMembers(KAFKAJS_DTS, "PartitionAssignors"),
    local: localOwn(pkg.KafkaJS.PartitionAssignors),
  },
  {
    id: "kafkajs:ConsumerGroupStates",
    bucket: "KafkaJS namespace",
    upstream: upstreamEnumMembers(RDKAFKA_DTS, "ConsumerGroupStates"),
    local: localOwn(pkg.KafkaJS.ConsumerGroupStates),
  },
  {
    id: "kafkajs:ConsumerGroupTypes",
    bucket: "KafkaJS namespace",
    upstream: upstreamEnumMembers(RDKAFKA_DTS, "ConsumerGroupTypes"),
    local: localOwn(pkg.KafkaJS.ConsumerGroupTypes),
  },
  {
    id: "kafkajs:AclOperationTypes",
    bucket: "KafkaJS namespace",
    upstream: upstreamEnumMembers(RDKAFKA_DTS, "AclOperationTypes"),
    local: localOwn(pkg.KafkaJS.AclOperationTypes),
  },
  {
    id: "kafkajs:IsolationLevel",
    bucket: "KafkaJS namespace",
    upstream: upstreamEnumMembers(RDKAFKA_DTS, "IsolationLevel"),
    local: localOwn(pkg.KafkaJS.IsolationLevel),
  },
];

/* -------------------------------------------------------------------------- *
 * The "spec FR" group — docs/01-spec.md §4's own commitments (FR-1/FR-2).     *
 * Some spec-committed APIs are absent from the upstream `.d.ts` (e.g.          *
 * AdminClient.fetchOffsets, KafkaConsumer.incrementalAssign), so they get      *
 * their own check.                                                             *
 * -------------------------------------------------------------------------- */
function specGroup(id: string, names: readonly string[], local: Map<string, LocalMember>): Group {
  return {
    id,
    bucket: "spec FR",
    upstream: names
      .map((name) => ({ name, kind: "callable" as const, minArity: 0, maxArity: REST_ARITY }))
      .sort((a, b) => a.name.localeCompare(b.name)),
    local,
  };
}

GROUPS.push(
  // spec §4 FR-1 — Producer
  specGroup(
    "spec:FR-1.Producer",
    [
      "connect", "disconnect", "produce", "flush", "poll", "setPollInterval",
      "initTransactions", "beginTransaction", "commitTransaction", "abortTransaction",
      "sendOffsetsToTransaction",
    ],
    localInstance(pkg.Producer),
  ),
  // spec §4 FR-1 — KafkaConsumer
  specGroup(
    "spec:FR-1.KafkaConsumer",
    [
      "connect", "subscribe", "unsubscribe", "consume", "commit", "commitSync", "committed",
      "seek", "assign", "unassign", "incrementalAssign", "incrementalUnassign", "assignments",
      "position", "pause", "resume", "offsetsStore", "getWatermarkOffsets",
      "queryWatermarkOffsets",
    ],
    localInstance(pkg.KafkaConsumer),
  ),
  // spec §4 FR-1 — AdminClient
  specGroup(
    "spec:FR-1.AdminClient",
    [
      "createTopic", "deleteTopic", "createPartitions", "listTopics", "listGroups",
      "describeGroups", "deleteGroups", "fetchOffsets", "deleteRecords", "describeTopics",
      "listConsumerGroupOffsets",
    ],
    localInstance(pkg.AdminClient),
  ),
  // spec §4 FR-2 — Kafka / Producer / Consumer / Admin
  specGroup("spec:FR-2.Kafka", ["producer", "consumer", "admin"], localInstance(pkg.KafkaJS.Kafka)),
  specGroup(
    "spec:FR-2.Producer",
    ["connect", "disconnect", "send", "sendBatch", "flush", "transaction"],
    localObjectInstance(kjsProducer),
  ),
  specGroup(
    "spec:FR-2.Consumer",
    [
      "connect", "disconnect", "subscribe", "run", "commitOffsets", "seek", "pause", "resume",
      "assignment", "stop",
    ],
    localObjectInstance(kjsConsumer),
  ),
  specGroup(
    "spec:FR-2.Admin",
    [
      "createTopics", "deleteTopics", "listTopics", "listGroups", "describeGroups",
      "deleteGroups", "fetchOffsets", "fetchTopicMetadata", "fetchTopicOffsets",
      "fetchTopicOffsetsByTimestamp", "deleteTopicRecords",
    ],
    localObjectInstance(kjsAdmin),
  ),
);

/* ========================================================================== *
 * 4. Comparison                                                              *
 * ========================================================================== */

type FindingKind = "missing" | "kind" | "arity";

interface Finding {
  path: string;
  group: Group;
  kind: FindingKind;
  detail: string;
}

/** Arity differences that do NOT block (local is a superset) — informational. */
const SUPERSET_NOTES: string[] = [];

function diffGroup(g: Group): { findings: Finding[]; covered: number } {
  const findings: Finding[] = [];
  let covered = 0;

  for (const up of g.upstream) {
    const local = g.local.get(up.name);
    if (local === undefined) {
      findings.push({
        path: `${g.id}.${up.name}`,
        group: g,
        kind: "missing",
        detail: `upstream declares ${up.kind === "callable" ? `${up.name}(${up.minArity}..${up.maxArity} params)` : up.name}, missing locally`,
      });
      continue;
    }
    if (local.kind !== up.kind) {
      findings.push({
        path: `${g.id}.${up.name}#kind`,
        group: g,
        kind: "kind",
        detail: `upstream is a ${up.kind === "callable" ? "function" : "value (const/enum)"}, local is a ${local.kind === "callable" ? "function" : "value"}`,
      });
      continue;
    }
    // Only the genuinely incompatible direction is caught: local declaring
    // FEWER positional parameters than upstream requires → code written for
    // upstream would lose arguments. The other direction (local taking extra
    // optional parameters) is a valid superset — merely noted in the report.
    //
    // `arity === 0` proves nothing: `Function.length` = 0 for both
    // `(...args)` and `(a = {})`, both valid ways to accept upstream overloads.
    if (up.kind === "callable" && local.arity > 0 && local.arity < up.minArity) {
      findings.push({
        path: `${g.id}.${up.name}#arity`,
        group: g,
        kind: "arity",
        detail: `local takes ${local.arity} params, upstream requires ${up.minArity}`,
      });
      continue;
    }
    if (up.kind === "callable" && local.arity !== up.maxArity) {
      SUPERSET_NOTES.push(
        `${g.id}.${up.name} — local ${local.arity} tham số, upstream ${up.minArity}..${up.maxArity}`,
      );
    }
    covered += 1;
  }

  return { findings, covered };
}

const RESULTS = GROUPS.map((g) => ({ group: g, ...diffGroup(g) }));
const ALL_FINDINGS = RESULTS.flatMap((r) => r.findings);

const EXCLUSION_BY_PATH = new Map<string, Exclusion>(EXCLUSIONS.map((e) => [e.path, e]));
const FOUND_PATHS = new Set(ALL_FINDINGS.map((f) => f.path));

const UNEXPECTED = ALL_FINDINGS.filter((f) => !EXCLUSION_BY_PATH.has(f.path));
const STALE = EXCLUSIONS.filter((e) => !FOUND_PATHS.has(e.path));

/* ========================================================================== *
 * 5. Report                                                                   *
 * ========================================================================== */

function pad(s: string, n: number): string {
  return s.length >= n ? s : s + " ".repeat(n - s.length);
}
function padLeft(s: string, n: number): string {
  return s.length >= n ? s : " ".repeat(n - s.length) + s;
}

function printReport(): void {
  const lines: string[] = [];
  lines.push("");
  lines.push("=".repeat(84));
  lines.push("  CONFORMANCE REPORT — bun-rdkafka vs confluent-kafka-javascript");
  lines.push(`  upstream types: test/conformance/upstream-types (xem README.md — commit SHA)`);
  lines.push("=".repeat(84));
  lines.push(
    `  ${pad("API group", 34)}${padLeft("total", 7)}${padLeft("covered", 9)}${padLeft("excluded", 10)}${padLeft("MISSING", 9)}`,
  );
  lines.push("  " + "-".repeat(80));

  const byBucket = new Map<string, { total: number; covered: number; excluded: number; bad: number }>();

  for (const r of RESULTS) {
    const excluded = r.findings.filter((f) => EXCLUSION_BY_PATH.has(f.path)).length;
    const bad = r.findings.length - excluded;
    const total = r.group.upstream.length;
    lines.push(
      `  ${pad(r.group.id, 34)}${padLeft(String(total), 7)}${padLeft(String(r.covered), 9)}${padLeft(String(excluded), 10)}${padLeft(bad === 0 ? "-" : String(bad), 9)}`,
    );
    const b = byBucket.get(r.group.bucket) ?? { total: 0, covered: 0, excluded: 0, bad: 0 };
    b.total += total;
    b.covered += r.covered;
    b.excluded += excluded;
    b.bad += bad;
    byBucket.set(r.group.bucket, b);
  }

  lines.push("  " + "-".repeat(80));
  for (const [bucket, b] of byBucket) {
    const pct = b.total === 0 ? 100 : Math.round((b.covered / b.total) * 1000) / 10;
    lines.push(
      `  ${pad(`TOTAL ${bucket}`, 34)}${padLeft(String(b.total), 7)}${padLeft(String(b.covered), 9)}${padLeft(String(b.excluded), 10)}${padLeft(b.bad === 0 ? "-" : String(b.bad), 9)}   ${pct}% direct match`,
    );
  }

  const byMilestone = new Map<string, number>();
  for (const e of EXCLUSIONS) {
    const key = e.milestone ?? "not applicable";
    byMilestone.set(key, (byMilestone.get(key) ?? 0) + 1);
  }
  lines.push("  " + "-".repeat(80));
  lines.push(
    `  Exclusions: ${EXCLUSIONS.length} entries — ` +
      [...byMilestone].map(([k, v]) => `${k}: ${v}`).join(", "),
  );
  lines.push(
    `  Note: ${SUPERSET_NOTES.length} members differ from upstream in arity but stay compatible ` +
      `(local is a superset / uses default params).`,
  );

  if (UNEXPECTED.length > 0) {
    lines.push("");
    lines.push("  !! MISSING AND NOT ON THE EXCLUSION LIST:");
    for (const f of UNEXPECTED) lines.push(`     - ${f.path} — ${f.detail}`);
  }
  if (STALE.length > 0) {
    lines.push("");
    lines.push("  !! STALE EXCLUSIONS (APIs no longer missing — remove from exclusions.ts):");
    for (const e of STALE) lines.push(`     - ${e.path}`);
  }
  lines.push("=".repeat(84));
  console.log(lines.join("\n"));
}

printReport();

/* ========================================================================== *
 * 6. Test                                                                     *
 * ========================================================================== */

describe("conformance: API surface vs confluent-kafka-javascript", () => {
  test("importing the package does not dlopen native (lazy-load)", () => {
    expect(isNativeLoaded()).toBe(false);
  });

  test("the upstream .d.ts parses with all the main classes present", () => {
    const names = new Set(upstreamModuleValues(RDKAFKA_DTS).map((m) => m.name));
    for (const n of ["Client", "Producer", "HighLevelProducer", "KafkaConsumer", "AdminClient", "CODES"]) {
      expect(names.has(n)).toBe(true);
    }
    expect(upstreamErrorCodes().length).toBeGreaterThan(150);
  });

  test("no upstream API is missing outside the exclusion list", () => {
    const report = UNEXPECTED.map((f) => `${f.path} — ${f.detail}`);
    expect(report).toEqual([]);
  });

  test("the exclusion list has no stale entries (implemented APIs must be removed)", () => {
    expect(STALE.map((e) => e.path)).toEqual([]);
  });

  test("every exclusion carries a clear reason", () => {
    for (const e of EXCLUSIONS) {
      expect(e.reason.length).toBeGreaterThan(30);
      expect(e.path).toMatch(/^[a-z]+:[A-Za-z.]+\.[A-Za-z_.]+(#kind|#arity)?$/);
    }
    expect(new Set(EXCLUSIONS.map((e) => e.path)).size).toBe(EXCLUSIONS.length);
  });

  for (const r of RESULTS) {
    test(`${r.group.id} — ${r.covered}/${r.group.upstream.length} members match`, () => {
      const unexpected = r.findings
        .filter((f) => !EXCLUSION_BY_PATH.has(f.path))
        .map((f) => `${f.path} — ${f.detail}`);
      expect(unexpected).toEqual([]);
    });
  }
});
