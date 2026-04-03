# Claude Commands

Slash commands for Claude Code in this repository. Use them in any Claude Code session.

## `/ppl-bugfix`

Fix a PPL bug end-to-end or follow up on an existing PR.

**Usage:**

```
/ppl-bugfix #1234                    # Single issue
/ppl-bugfix PR#5678                  # Single PR follow-up
/ppl-bugfix #1234 #5678 PR#9012     # Multiple in parallel
/ppl-bugfix <github-url>            # By URL
```

**Permission mode flags** (optional, append to any input):

| Flag | Mode | Description |
|------|------|-------------|
| `--safe` | `acceptEdits` | File edits auto-approved, Bash commands need manual approval |
| `--yolo` | `bypassPermissions` | No prompts at all — subagent runs in isolated worktree (default) |

**What it does:**

1. Resolves issue/PR linkage automatically
2. For new issues: dispatches a subagent in an isolated git worktree that follows the full bugfix harness (triage → fix → test → PR)
3. For existing PRs: handles CI failures, review feedback, merge conflicts, or marks as ready

**Related files:** [`.claude/harness/ppl-bugfix-harness.md`](.claude/harness/ppl-bugfix-harness.md)

---

## `/dedupe`

Find duplicate GitHub issues for a given issue.

**Usage:**

```
/dedupe 1234
```

**What it does:**

1. Reads the target issue
2. Runs 3+ parallel search strategies to find potential duplicates (only older issues)
3. Verifies candidates by reading each one
4. Posts a structured comment on the issue listing 1-3 confirmed duplicates (if any)

**Skips:** closed issues, broad feedback issues, issues already checked

**Related files:** [`scripts/comment-on-duplicates.sh`](scripts/comment-on-duplicates.sh)
