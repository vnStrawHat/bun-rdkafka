/** Pure Conventional-Commits grouping logic of scripts/changelog.ts. */

import { describe, expect, test } from "bun:test";
import { groupCommits, renderBody } from "../../scripts/changelog.ts";

describe("groupCommits", () => {
  test("groups by type with scope rendering", () => {
    const g = groupCommits([
      { subject: "feat(consumer): add eachBatch", body: "" },
      { subject: "fix: correct DR offset decoding", body: "" },
      { subject: "perf(producer): 1-copy staging", body: "" },
      { subject: "docs: rewrite install section", body: "" },
      { subject: "chore: bump librdkafka", body: "" },
    ]);
    const titles = g.sections.map((s) => s.title);
    expect(titles).toEqual(["Features", "Bug Fixes", "Performance", "Documentation", "Maintenance"]);
    expect(g.sections[0]?.items).toEqual(["- **consumer:** add eachBatch"]);
    expect(g.sections[1]?.items).toEqual(["- correct DR offset decoding"]);
  });

  test("breaking changes lead, via ! marker or footer", () => {
    const g = groupCommits([
      { subject: "feat!: drop Node compatibility", body: "" },
      { subject: "fix(abi): widen tpl metadata", body: "BREAKING CHANGE: TPL format changed" },
      { subject: "feat: harmless", body: "" },
    ]);
    expect(g.sections[0]?.title).toBe("Breaking Changes");
    expect(g.sections[0]?.items).toEqual([
      "- drop Node compatibility",
      "- **abi:** widen tpl metadata",
    ]);
    // breaking commits still appear in their type sections
    expect(g.sections.find((s) => s.title === "Features")?.items).toHaveLength(2);
  });

  test("non-conventional subjects fall into Other; empty subjects are dropped", () => {
    const g = groupCommits([
      { subject: "wip stuff", body: "" },
      { subject: "", body: "" },
      { subject: "unknown(scope): odd type", body: "" },
    ]);
    expect(g.sections).toHaveLength(1);
    expect(g.sections[0]?.title).toBe("Other");
    expect(g.sections[0]?.items).toHaveLength(2);
  });
});

describe("renderBody", () => {
  test("renders sections and the compare link", () => {
    const body = renderBody(
      groupCommits([{ subject: "feat: x", body: "" }]),
      "v0.1.0",
      "v0.2.0",
    );
    expect(body).toContain("### Features");
    expect(body).toContain("compare/v0.1.0...v0.2.0");
  });

  test("first release: no compare link, empty range noted", () => {
    const body = renderBody(groupCommits([]), null, "v0.1.0");
    expect(body).toContain("No conventional commits");
    expect(body).not.toContain("compare/");
  });
});
