---
allowed-tools: Agent, Read, Bash(gh:*), Bash(git:*)
description: Run the PPL bugfix harness for a GitHub issue or follow up on an existing PR
---

Fix a PPL bug or follow up on an existing PR using the harness in `ppl-bugfix-harness.md`.

## Input

- `/ppl-bugfix #1234` or `/ppl-bugfix 1234` — issue number
- `/ppl-bugfix PR#5678` or `/ppl-bugfix pr 5678` — PR number
- `/ppl-bugfix https://github.com/opensearch-project/sql/issues/1234` — URL

If no argument given, ask for an issue or PR number.

## Step 1: Resolve Issue ↔ PR

```bash
# Issue → PR
gh pr list --search "Resolves #<issue_number>" --json number,url,state --limit 5

# PR → Issue
gh pr view <pr_number> --json body | jq -r '.body' | grep -oP 'Resolves #\K[0-9]+'
```

| State | Action |
|-------|--------|
| Issue exists, no PR | **Initial Fix** (Step 2A) |
| Issue exists, open PR found | **Follow-up** (Step 2B) |
| PR provided directly | **Follow-up** (Step 2B) |

## Step 2A: Initial Fix

1. `gh issue view <issue_number>`
2. `Read ppl-bugfix-harness.md`
3. Dispatch subagent:

```
Agent(
  mode: "acceptEdits",
  isolation: "worktree",
  name: "bugfix-<issue_number>",
  description: "PPL bugfix #<issue_number>",
  prompt: "<FULL harness content> + <issue details>
           Follow Phase 0 through Phase 3 in order.
           Phase 0.3 defines TDD execution flow. Do NOT skip any phase.
           Post the Decision Log (Phase 3.4) before completing."
)
```

The `isolation: "worktree"` creates a temporary git worktree branched from the current HEAD. The agent works in a clean, isolated copy of the repo — no interference with the user's working directory. If the agent makes changes, the worktree path and branch are returned in the result.

4. Report back: classification, fix summary, PR URL, **worktree branch name**, items needing human attention.

## Step 2B: Follow-up

1. Load PR state:
```bash
gh pr view <pr_number> --json title,body,state,reviews,comments,statusCheckRollup,mergeable
gh pr checks <pr_number>
```

2. Categorize:

| Signal | Type |
|--------|------|
| `statusCheckRollup` has failures | CI failure |
| `reviews` has CHANGES_REQUESTED | Review feedback |
| `mergeable` is CONFLICTING | Merge conflict |
| All pass + approved | Ready — run `gh pr ready` |

3. `Read ppl-bugfix-harness.md`
4. Dispatch follow-up subagent:

```
Agent(
  mode: "acceptEdits",
  isolation: "worktree",
  name: "bugfix-<issue_number>-followup",
  description: "PPL bugfix #<issue_number> followup",
  prompt: "<Phase 3.5 content from harness>
           PR: <pr_number> (<pr_url>), Issue: #<issue_number>
           Follow-up type: <CI failure / review feedback / merge conflict>
           Read the Decision Log PR comment FIRST before making any changes.
           Checkout the PR branch before starting: git checkout <pr_branch>"
)
```

5. Report back: what was addressed, current PR state, whether another round is needed.

## Subagent Lifecycle

Subagents are task-scoped. They complete and release context — they cannot poll for events.

```
Agent A (Phase 0-3) → creates PR → completes
  (CI runs, reviewers comment, conflicts arise)
Agent B (Phase 3.5) → handles feedback → completes
  (repeat as needed)
Agent N (Phase 3.5) → gh pr ready → done
```

Context is preserved across agents via:
- **Decision Log** (PR comment) — single source of truth for rejected alternatives, pitfalls, design rationale
- **GitHub state** (PR diff, review comments, CI logs) — reconstructed by each follow-up agent

## Rules

- Always inline harness content in the subagent prompt — subagents cannot read skill files
- If bug is not reproducible (Phase 0.1), stop and report — do not proceed
- Issue ↔ PR auto-resolution means the user never needs to track PR numbers manually
