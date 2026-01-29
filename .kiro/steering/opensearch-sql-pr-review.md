---
inclusion: manual
---

# OpenSearch SQL PR Review

Use this when someone asks for a PR/diff review in `opensearch-project/sql` that touches core/opensearch/ppl/calcite/integ-test/docs. Skip `sql/legacy` unless explicitly requested.

## Inputs to Pull Immediately
- PR metadata (title, body, labels, linked issues): `gh pr view <num> --repo opensearch-project/sql --json title,body,labels,number,author,createdAt,updatedAt,state,mergeable,reviewRequests`
- Diff/file list: `gh pr diff <num> --repo opensearch-project/sql` (or request a specific file list from the user if diff is missing).
- Tests/CI (if available): ask for failing logs; otherwise note as missing.

## Review Workflow (follow in order)
1) **Intake & triage** – classify change (bugfix/perf/refactor/docs/backport); note impacted modules; confirm no legacy-only scope unless requested.
2) **Risk scan (fast pass)** – flag behavior/API/config changes; check pagination/PIT, alias resolution, query size limits, exception handling, multi-valued and nested fields, and permissions.
3) **Test (integration-first)** – end-to-end confidence pass before deep review.
   - Prep: checkout the PR branch; start a local cluster `./gradlew opensearch-sql:run`; source `/Users/penghuo/release/python/myenv/bin/activate` for Python tooling.
   - Learn signals: from PR description, linked issues, and PR-added integration/doc tests, extract scenarios (data shape, queries, expected outputs, performance notes).
   - Plan: write a short test plan (data/setup, queries, expected results, perf/latency thresholds if stated, env assumptions). Default file: `.kiro/resources/pr-reviews/pr-<num>/tests/plan.md` (see template).
   - Execute (Python preferred): use a Python script to load/generate test data, issue the PPL queries against the local cluster, and capture responses. If PR adds yamlRestTest/doctest, rerun those targets and record results. Keep commands reproducible.
   - Report: save a markdown report (e.g., `.kiro/resources/pr-reviews/pr-<num>/tests/report-<YYYYMMDD-HHMM>.md`) with commit/PR ref, commands, scenarios run, pass/fail per scenario, logs/error snippets, and timing. If any failure, review the report with the reviewer and prepare PR review comments referencing the failing scenario; otherwise note “all planned scenarios passed” to raise confidence.
4) **Log session & follow-ups** – track the full review history until merge.
   - Append an entry after each review pass to `.kiro/resources/pr-reviews/pr-<num>/session.md` (use template) capturing: commit SHA reviewed, comments posted, concerns outstanding, tests run (link reports), next actions/owners, and whether re-run is needed after updates.
   - For subsequent passes (e.g., next day), pull latest PR changes/author responses, rerun the Test step on new commits, and append a new entry. Keep a concise delta of what changed and which concerns were cleared.
5) **Deep review by area**
   - Java logic: nullability, exception flow, resource cleanup, thread safety.
   - Calcite/Rel: pushdown correctness, plan shape, rule interactions, builder usage.
   - Core/OpenSearch integration: API usage, request lifecycle, permission checks.
   - Tests: ensure unit/integration tests cover behavior change and snapshots updated.
   - Docs/doctest: required for user-facing changes or explicitly call out why not needed.
6) **Output using the template** – keep comments small, specific, and actionable; ask clarifying questions when logic is non-obvious.
7) **Approval gate** – do not mark ready unless: relevant tests are added/updated or a reason is given; docs/doctests handled; risks acknowledged. Reference the latest session log/report when approving.

## Required Output Template
```
Findings
- [blocker|major|minor|nit|question] path:line - Impact. Actionable fix or next step.

Follow-up Questions
- Question 1
- Question 2

Testing Recommendations
- Recommendation 1
- Recommendation 2

Approval
- Ready for approval | Not ready for approval (list missing checklist items)
```

## Domain Checklist (core/opensearch/ppl/docs)
- PPL semantics and output correctness; Calcite pushdown must not change results.
- Query size limits vs performance expectations.
- Multi-value and nested fields covered and tested.
- Pagination/PIT lifecycle and fetch_size semantics; resource cleanup.
- Exception handling preserves root cause; request lifecycle and cursor management safe.
- Permission checks and required builder initialization are intact.
- Tests: behavior-change tests added/updated; snapshots updated when plan changes; flaky/CI issues acknowledged.
- Docs/doctest updated for user-visible changes or explicitly waived.

## Comment Patterns
- State impact first, then the fix; ask one question at a time.
- Propose concrete refactors/tests when flow is hard to follow.
- Remove redundant catch blocks when finally handles cleanup; avoid sysout/debug noise in tests.
- Preserve required side effects (e.g., explain() initializing builders); prefer helpers over raw relBuilder.aggregate when project trimming is required.

## Assets / References
- Checklist: `.kiro/skills/opensearch-sql-pr-review/references/checklist-opensearch-sql.md`
- Review patterns: `.kiro/skills/opensearch-sql-pr-review/references/review-patterns.md`
- Comment template: `.kiro/skills/opensearch-sql-pr-review/assets/templates/review-comment.md`
- Test plan template: `.kiro/resources/pr-reviews/templates/test-plan.md`
- Test report template: `.kiro/resources/pr-reviews/templates/test-report.md`
- Session log template: `.kiro/resources/pr-reviews/templates/session.md`

## Artifact Locations
- Per-PR folder: `.kiro/resources/pr-reviews/pr-<num>/`
  - Tests: `tests/plan.md`, `tests/report-<timestamp>.md`
  - Session log: `session.md` (append entries over time)
  - Optional extras: attach logs or data under the same folder to keep history until merge.

## Notes
- Keep scope minimal; prefer small, actionable comments.
- Explicitly note missing inputs (diff, logs, tests) instead of guessing.
