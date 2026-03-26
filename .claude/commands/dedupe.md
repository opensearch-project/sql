---
allowed-tools: Bash(gh:*), Bash(./scripts/comment-on-duplicates.sh:*)
description: Find duplicate GitHub issues
---

Find up to 3 likely duplicate issues for a given GitHub issue.

Follow these steps precisely:

1. Use `gh issue view <number>` to read the issue. If the issue is closed, or is broad product feedback without a specific bug/feature request, or already has a duplicate detection comment (containing `<!-- duplicate-detection -->`), stop and report why you are not proceeding.

2. Summarize the issue's core problem in 2-3 sentences. Identify the key terms, error messages, and affected components.

3. Search for potential duplicates using **at least 3 different search strategies**. Run these searches in parallel:
   - `gh search issues "<exact error message or key phrase>" --repo $GITHUB_REPOSITORY --state open --limit 15`
   - `gh search issues "<component or feature keywords>" --repo $GITHUB_REPOSITORY --state open --limit 15`
   - `gh search issues "<alternate description of the problem>" --repo $GITHUB_REPOSITORY --state open --limit 15`
   - `gh search issues "<key terms>" --repo $GITHUB_REPOSITORY --state all --limit 10` (include closed issues for reference)

4. For each candidate issue that looks like a potential match, read it with `gh issue view <number>` to verify it is truly about the same problem. Filter out false positives — issues that merely share keywords but describe different problems.

5. If you find 1-3 genuine duplicates, post the result using the comment script:
   ```
   ./scripts/comment-on-duplicates.sh --base-issue <issue-number> --potential-duplicates <dup1> [dup2] [dup3]
   ```

6. If no genuine duplicates are found, report that no duplicates were detected and take no further action.

Important notes:
- Only flag issues as duplicates when you are confident they describe the **same underlying problem**
- Prefer open issues as duplicates, but closed issues can be referenced too
- Do not flag the issue as a duplicate of itself
- The base issue number is the last part of the issue reference (e.g., for `owner/repo/issues/42`, the number is `42`)
