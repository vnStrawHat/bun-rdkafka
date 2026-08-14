#!/usr/bin/env bun
/**
 * changelog.ts — generates a CHANGELOG.md section from Conventional Commits.
 *
 * Used by the release workflow (workflow_dispatch with a bump type): collects
 * commits since the last `v*` tag, groups them by conventional type, prepends
 * the section to CHANGELOG.md, and optionally writes the bare section body to
 * a notes file (used as the GitHub Release body).
 *
 * Usage:
 *   bun scripts/changelog.ts --next <version> [--notes-file <path>] [--dry-run]
 */

import { existsSync, readFileSync, writeFileSync } from "node:fs";

const REPO_URL = "https://github.com/vnStrawHat/bun-rdkafka";

export interface CommitInfo {
  subject: string;
  body: string;
}

export interface GroupedCommits {
  /** Section title → rendered bullet lines. */
  sections: Array<{ title: string; items: string[] }>;
}

const TYPE_SECTIONS: Array<{ title: string; types: string[] }> = [
  { title: "Features", types: ["feat"] },
  { title: "Bug Fixes", types: ["fix"] },
  { title: "Performance", types: ["perf"] },
  { title: "Documentation", types: ["docs"] },
  { title: "Maintenance", types: ["chore", "refactor", "test", "ci", "build", "style"] },
];

const CONVENTIONAL_RE = /^(\w+)(\(([^)]*)\))?(!)?:\s*(.+)$/;

/**
 * Groups commits into changelog sections per Conventional Commits.
 * Breaking changes (a `!` marker or a "BREAKING CHANGE" footer) always go into
 * a leading "Breaking Changes" section (in addition to their type section).
 * Non-conforming subjects land under "Other".
 */
export function groupCommits(commits: CommitInfo[]): GroupedCommits {
  const breaking: string[] = [];
  const byTitle = new Map<string, string[]>();
  const other: string[] = [];

  const add = (title: string, line: string) => {
    if (!byTitle.has(title)) byTitle.set(title, []);
    byTitle.get(title)!.push(line);
  };

  for (const c of commits) {
    const m = CONVENTIONAL_RE.exec(c.subject.trim());
    if (!m) {
      if (c.subject.trim()) other.push(`- ${c.subject.trim()}`);
      continue;
    }
    const [, type, , scope, bang, description] = m;
    const line = scope ? `- **${scope}:** ${description}` : `- ${description}`;
    const isBreaking = bang === "!" || /^BREAKING[ -]CHANGE:/m.test(c.body);
    if (isBreaking) breaking.push(line);

    const section = TYPE_SECTIONS.find((s) => s.types.includes(type!.toLowerCase()));
    if (section) add(section.title, line);
    else other.push(line);
  }

  const sections: GroupedCommits["sections"] = [];
  if (breaking.length) sections.push({ title: "Breaking Changes", items: breaking });
  for (const { title } of TYPE_SECTIONS) {
    const items = byTitle.get(title);
    if (items?.length) sections.push({ title, items });
  }
  if (other.length) sections.push({ title: "Other", items: other });
  return { sections };
}

/** Renders the section body (no version heading) as markdown. */
export function renderBody(grouped: GroupedCommits, prevTag: string | null, nextTag: string): string {
  const parts: string[] = [];
  for (const { title, items } of grouped.sections) {
    parts.push(`### ${title}\n\n${items.join("\n")}`);
  }
  if (parts.length === 0) parts.push("_No conventional commits found in this range._");
  if (prevTag) {
    parts.push(`**Full diff:** [${prevTag}...${nextTag}](${REPO_URL}/compare/${prevTag}...${nextTag})`);
  }
  return parts.join("\n\n") + "\n";
}

/* ------------------------------------------------------------------------- */
/* CLI                                                                        */
/* ------------------------------------------------------------------------- */

async function git(args: string[]): Promise<string> {
  const proc = Bun.spawn(["git", ...args], { stdout: "pipe", stderr: "pipe" });
  const out = await new Response(proc.stdout).text();
  if ((await proc.exited) !== 0) {
    throw new Error(`git ${args.join(" ")} failed: ${await new Response(proc.stderr).text()}`);
  }
  return out;
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const flag = (name: string): string | undefined => {
    const i = args.indexOf(name);
    return i >= 0 ? args[i + 1] : undefined;
  };
  const next = flag("--next");
  if (!next) {
    console.error("usage: bun scripts/changelog.ts --next <version> [--notes-file <path>] [--dry-run]");
    process.exit(2);
  }
  const nextTag = next.startsWith("v") ? next : `v${next}`;

  let prevTag: string | null = null;
  try {
    prevTag = (await git(["describe", "--tags", "--abbrev=0", "--match", "v*"])).trim() || null;
  } catch {
    prevTag = null; // first release: use the whole history
  }

  const range = prevTag ? `${prevTag}..HEAD` : "HEAD";
  let raw = "";
  try {
    raw = await git(["log", range, "--no-merges", "--pretty=format:%s%x1f%b%x1e"]);
  } catch {
    // A repository without commits (or an unborn branch) has no log — emit an
    // empty section rather than failing.
  }
  const commits: CommitInfo[] = raw
    .split("\x1e")
    .map((chunk) => chunk.split("\x1f"))
    .filter((parts) => (parts[0] ?? "").trim().length > 0)
    .map((parts) => ({ subject: (parts[0] ?? "").trim(), body: parts[1] ?? "" }));

  const body = renderBody(groupCommits(commits), prevTag, nextTag);
  const date = new Date().toISOString().slice(0, 10);
  const section = `## ${nextTag} (${date})\n\n${body}`;

  const notesFile = flag("--notes-file");
  if (notesFile) writeFileSync(notesFile, body);

  if (args.includes("--dry-run")) {
    console.log(section);
    return;
  }

  const path = "CHANGELOG.md";
  const header = "# Changelog\n";
  const existing = existsSync(path) ? readFileSync(path, "utf8") : `${header}\n`;
  const updated = existing.startsWith(header)
    ? `${header}\n${section}\n${existing.slice(header.length).trimStart()}`
    : `${header}\n${section}\n${existing}`;
  writeFileSync(path, updated.trimEnd() + "\n");
  console.log(`CHANGELOG.md updated with ${nextTag} (${commits.length} commits since ${prevTag ?? "the beginning"})`);
}

if (import.meta.main) await main();
