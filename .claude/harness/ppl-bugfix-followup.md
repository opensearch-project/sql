# PPL Bugfix Follow-up

---

## Reconstruct Context

The follow-up agent runs in a fresh worktree. First checkout the PR branch, then load state:

```bash
# Checkout the PR branch in this worktree
gh pr checkout <pr_number>

# Load PR state — reviews, CI, mergeability
gh pr view <pr_number> --json title,body,state,reviews,statusCheckRollup,mergeable
gh pr checks <pr_number>

# Load ALL comments — includes bot comments (Code-Diff-Analyzer, PR Reviewer Guide, Code Suggestions) and human comments
gh pr view <pr_number> --json comments --jq '.comments[] | {author: .author.login, body: .body}'
```

Categorize ALL signals — not just CI and human reviews:

| Signal | Type |
|--------|------|
| `statusCheckRollup` has failures | CI failure |
| `reviews` has CHANGES_REQUESTED | Review feedback |
| `mergeable` is CONFLICTING | Merge conflict |
| Bot comments with actionable suggestions | Review feedback (treat like human review) |
| All pass + approved | Ready — run `gh pr ready` |

## Handle Review Feedback

For each comment (human OR bot), **cross-check against the Decision Log first**:

| Type | Action |
|------|--------|
| Code change | If already rejected in Decision Log, reply with reasoning. Otherwise make the change, new commit, push |
| Question | Reply with explanation — Decision Log often has the answer |
| Nit | Fix if trivial |
| Disagreement | Reply with Decision Log reasoning; if reviewer insists, escalate to user |

```bash
git add <files> && git commit -s -m "Address review feedback: <description>"
git push <your_fork_remote> <branch_name>
```

## Clean Up Commit History

When you need to amend a commit (e.g. remove Co-Authored-By, reword message) and the branch has a merge commit on top, don't try `git reset --soft origin/main` — it will include unrelated changes if main has moved. Instead cherry-pick the fix onto latest main:

```bash
git checkout -B clean-branch origin/main
git cherry-pick <fix_commit_sha>
git commit --amend -s -m "<updated message>"
git push <your_fork_remote> clean-branch:<pr_branch> --force
```

## Handle CI Failures

```bash
gh pr checks <pr_number>                  # Identify failures
gh run view <run_id> --log-failed         # Read logs
# Test failure → fix locally, push new commit
# Spotless → ./gradlew spotlessApply, push
# Flaky → gh run rerun <run_id> --failed
```

## Handle Merge Conflicts

```bash
git fetch origin && git merge origin/main  # Resolve conflicts
./gradlew spotlessApply && ./gradlew test && ./gradlew :integ-test:integTest  # Re-verify
git commit -s -m "Resolve merge conflicts with main"
git push <your_fork_remote> <branch_name>
```

## Mark Ready

```bash
gh pr ready <pr_number>
```
