---
allowed-tools: Agent, Read, Bash(gh:*), Bash(git:*)
description: Run the PPL bugfix harness for a GitHub issue or follow up on an existing PR
---

Fix a PPL bug or follow up on an existing PR using the harness in `.claude/harness/ppl-bugfix-harness.md`.

## Input

Accepts one or more issue/PR references. Multiple references are processed in parallel (each gets its own subagent + worktree).

- `/ppl-bugfix #1234` — single issue
- `/ppl-bugfix PR#5678` — single PR
- `/ppl-bugfix #1234 #5678 PR#9012` — multiple in parallel
- `/ppl-bugfix https://github.com/opensearch-project/sql/issues/1234` — URL

Optional mode flag (append to any of the above):
- `--safe` — `acceptEdits` mode. Auto-approve file edits only, Bash commands require manual approval. (Most conservative)
- `--yolo` — `bypassPermissions` mode. Fully trusted, no prompts. Subagent runs in an isolated worktree so this is safe. (Default)

Examples:
- `/ppl-bugfix #1234` — single issue, defaults to yolo
- `/ppl-bugfix #1234 #5678 --yolo` — two issues in parallel
- `/ppl-bugfix PR#5293 PR#5300` — two PRs in parallel
- `/ppl-bugfix #1234 PR#5678 --safe` — mix of issue and PR

If no argument given, ask for an issue or PR number.

## Step 0: Resolve Permission Mode

Parse the mode flag from the input arguments:

| Flag | Mode |
|------|------|
| `--safe` | `acceptEdits` |
| `--yolo` | `bypassPermissions` |
| _(no flag)_ | `bypassPermissions` (default) |

Use the resolved mode as the `mode` parameter when dispatching the subagent in Step 2A/2B.

## Step 1: Resolve Each Reference

For each issue/PR reference in the input, resolve its state. Run these lookups in parallel when there are multiple references.

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

## Step 2: Dispatch Subagents

Dispatch one subagent per reference. When there are multiple references, dispatch all subagents in a single message (parallel execution).

### 2A: Initial Fix

```
Agent(
  mode: "<resolved_mode>",
  isolation: "worktree",
  name: "bugfix-<issue_number>",
  description: "PPL bugfix #<issue_number>",
  prompt: "Read .claude/harness/ppl-bugfix-harness.md and follow it to fix GitHub issue #<issue_number>.
           Follow Phase 0 through Phase 3 in order.
           Phase 0.3 defines TDD execution flow. Do NOT skip any phase.
           Post the Decision Log (Phase 3.4) before completing."
)
```

### 2B: Follow-up

```
Agent(
  mode: "<resolved_mode>",
  isolation: "worktree",
  name: "bugfix-<issue_number>",
  description: "PPL bugfix #<issue_number> followup",
  prompt: "Read .claude/harness/ppl-bugfix-followup.md and follow it.
           PR: <pr_number> (<pr_url>), Issue: #<issue_number>
           Follow-up type: <CI failure / review feedback / merge conflict>"
)
```

## Step 3: Report Back

After all subagents complete, report a summary for each:
- Classification, fix summary, PR URL, worktree path and branch, items needing human attention (2A)
- What was addressed, current PR state, whether another round is needed (2B)

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

- Subagent reads `.claude/harness/ppl-bugfix-harness.md` and fetches issue/PR details itself — do NOT inline content into the prompt
- If bug is not reproducible (Phase 0.1), stop and report — do not proceed
- Issue ↔ PR auto-resolution means the user never needs to track PR numbers manually
- **Do NOT use `mode: "auto"` for subagents** — `auto` mode does not work for subagents; Bash commands still require manual approval. Only `bypassPermissions` reliably skips permission checks.
- **Always dispatch subagent** — even for trivial follow-ups (remove co-author, force push). Do NOT run commands directly in the main session; subagents with `bypassPermissions` skip permission prompts, the main session does not.
