---
name: update-changelog
description: Update CHANGELOG.md with merged PRs since the last changelog update, categorized by type
---

# Update Changelog

This skill updates CHANGELOG.md with merged PRs that aren't already listed.

## Step 1: Determine the changelog scope

Read `CHANGELOG.md` to identify the current unreleased version section at the top (e.g., `Tantivy 0.26 (Unreleased)`).

Collect all PR numbers already mentioned in the unreleased section by extracting `#NNNN` references.

## Step 2: Find merged PRs not yet in the changelog

Use `gh` to list recently merged PRs from the upstream repo:

```bash
gh pr list --repo quickwit-oss/tantivy --state merged --limit 100 --json number,title,author,labels,mergedAt
```

Filter out any PRs whose number already appears in the unreleased section of the changelog.

## Step 3: Consolidate related PRs

Before categorizing, group PRs that belong to the same logical change. This is critical for producing a clean changelog.

**Merge follow-up PRs into the original:**
- If a PR is a bugfix, refinement, or follow-up to another PR in the same unreleased cycle, combine them into a single changelog entry with multiple `[#N](url)` links. Look at PR descriptions, titles, and cross-references to identify these relationships.
- Example: PR #2760 "Fix edge case in AllScorer optimization" is a follow-up to PR #2745 "Replace saturated posting lists with AllScorer" → single entry referencing both.

**Filter out bugfixes on unreleased features:**
- If a bugfix PR fixes something introduced by another PR in the **same unreleased version**, it must NOT appear as a separate Bugfixes entry. Instead, silently fold it into the original feature/improvement entry. The changelog should describe the final shipped state, not the development history.
- Example: PR #100 adds a new aggregation type, PR #110 fixes a crash in that new aggregation → only one Features entry mentioning both PRs, nothing in Bugfixes.
- To detect this: check if the bugfix PR references or reverts changes from another PR in the same release cycle, or if it touches code that was newly added (not present in the previous release).

## Step 4: Categorize each PR group

For each PR (or consolidated group), determine its category based on title, labels, and (if needed) a brief look at the PR description:

- **Bugfixes** — fixes to behavior that existed in the **previous release**. NOT fixes to features introduced in this release cycle.
- **Features/Improvements** — new features, API additions, new options, improvements
- **Performance** — optimizations, speed improvements, memory reductions

If a PR doesn't clearly fit any category (e.g., CI-only changes, internal refactors with no user-facing impact, dependency bumps with no behavior change), skip it — not everything belongs in the changelog.

When unclear, use your best judgment or ask the user.

## Step 5: Format entries

Each entry must follow this exact format:

```
- Description [#NUMBER](https://github.com/quickwit-oss/tantivy/pull/NUMBER)(@author)
```

Rules:
- The description should be concise and describe the user-facing change (not the implementation). Describe the final shipped state, not the incremental development steps.
- Use sub-categories with bold headers when multiple entries relate to the same area (e.g., `- **Aggregation**` with indented entries beneath). Follow the existing grouping style in the changelog.
- Author is the GitHub username from the PR, prefixed with `@`. For consolidated entries, include all contributing authors.
- For consolidated PRs, list all PR links in a single entry: `[#100](url) [#110](url)` (see existing entries for examples).

## Step 6: Present changes to the user

Show the user the proposed changelog entries grouped by category **before** editing the file. Ask for confirmation or adjustments.

## Step 7: Update CHANGELOG.md

Insert the new entries into the appropriate sections of the unreleased version block. If a section doesn't exist yet, create it following the order: Bugfixes, Features/Improvements, Performance.

Append new entries at the end of each section (before the next section header or version header).

## Step 8: Verify

Read back the updated unreleased section and display it to the user for final review.
