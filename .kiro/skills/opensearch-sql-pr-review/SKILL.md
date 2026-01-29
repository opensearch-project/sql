---
name: opensearch-sql-pr-review
description: PR/code review for opensearch-project/sql with focus on core, opensearch, ppl, calcite, integ-test, and docs. Use for review requests, PR diffs, or review comments in this repo. Exclude sql/legacy modules unless explicitly requested.
---

# Opensearch SQL PR Review

## Overview
Provide strict, engineer-to-engineer PR review feedback for opensearch-project/sql with clear, clean, actionable comments. Prioritize correctness, performance, test coverage, and maintainability.

## Inputs
- PR description, labels, linked issues.
- Diff or file list (including expected output snapshots).
- Tests run and CI results (if provided).
- Logs or failing test traces.

## Workflow (use in order)
1) Intake and triage
- Read PR description, labels, and linked issues.
- Classify change type: bugfix, performance, refactor, docs, backport.
- Identify impacted modules (core, opensearch, ppl, calcite, integ-test, docs). Ignore sql/legacy unless explicitly requested.

2) Risk scan (fast pass)
- Check for behavior changes, API changes, config changes, or performance impact.
- Flag fragile areas: Calcite pushdown, pagination/PIT, alias resolution, query size limit, exception handling, multi-valued fields, nested fields, and permissions.

3) Deep review by area
- Java logic: nullability, exception flow, resource cleanup, thread safety.
- Calcite/Rel: pushdown correctness, plan shape, rule interactions, consistent builder usage.
- Core/opensearch integration: API usage, permission checks, request lifecycle.
- Tests: unit vs integration expectations; snapshot update correctness.
- Docs/doctest: user-facing changes require docs or doctest updates.

4) Comment quality
- Prefer small, actionable comments over vague feedback.
- Ask clarifying questions when logic is non-obvious.
- Propose concrete refactors when flow is hard to follow.

5) Minimum checklist (required before approval)
- Tests: at least one relevant unit or integration test added or updated for behavior changes.
- Coverage: explain why no test is needed if tests are not added.
- Docs/doctest: update or explicitly state why not needed for user-facing changes.
- Risk acknowledgment: call out any unverified behavior or assumptions.

## Output format (strict template)
Use this structure exactly, with short, direct language.

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

## References
- Read `references/review-patterns.md` for recurring feedback patterns from prior reviews.
- Read `references/checklist-opensearch-sql.md` for the domain-specific checklist.

## Assets
- Use `assets/templates/review-comment.md` when you need a preformatted comment block.
