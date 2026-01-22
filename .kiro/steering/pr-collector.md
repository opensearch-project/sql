---
inclusion: manual
---

# PR Review Collector

## Quick Commands

When user asks to collect PR data, IMMEDIATELY execute:

```bash
python3 .kiro/scripts/pr_collector/pr_collector.py \
  --repo REPO_NAME \
  --start-date YYYY-MM-DD \
  --end-date YYYY-MM-DD \
  [--state merged|open|closed|all] \
  [--limit NUMBER]
```

**Example:**
```bash
python3 .kiro/scripts/pr_collector/pr_collector.py \
  --repo opensearch-project/sql \
  --start-date 2025-12-01 \
  --end-date 2025-12-31
```

## Output Location
Data saved to: `.kiro/resources/{repo-name}/YYYY-MM-DD-TO-YYYY-MM-DD.md`

## Prerequisites Check

Before running, verify:
```bash
# Check GitHub CLI
gh --version

# Check authentication
gh auth status
```

If not installed: `brew install gh` (macOS)
If not authenticated: `gh auth login`

## Common Use Cases

### Collect all PRs for a month
```bash
python3 .kiro/scripts/pr_collector/pr_collector.py \
  --repo opensearch-project/sql \
  --start-date 2025-12-01 \
  --end-date 2025-12-31
```

### Collect only merged PRs, limited to 20
```bash
python3 .kiro/scripts/pr_collector/pr_collector.py \
  --repo opensearch-project/sql \
  --start-date 2025-12-01 \
  --end-date 2025-12-31 \
  --state merged \
  --limit 20
```

### Collect recent PRs (last 30 days)
```bash
python3 .kiro/scripts/pr_collector/pr_collector.py \
  --repo opensearch-project/sql \
  --start-date 2025-12-22 \
  --end-date 2026-01-21
```

## What Gets Collected

For each PR:
- Metadata (number, title, author, dates, state, URL)
- Description (checklist items filtered out)
- Reviews (human reviewers only, with full comment content)
- Review comments (inline code comments from humans only, with file/line context)
- General comments (discussion thread from humans only)
- Labels and assignees

**Filtered Out:**
- Checklist items (- [ ] and - [x] lines)
- Files changed section
- CodeRabbit bot reviews and comments
- Timestamps (only reviewer name and content shown)

## After Collection

Once data is collected, you can:

1. **Read the output file:**
```bash
cat .kiro/resources/sql/2025-12-01-TO-2025-12-31.md
```

2. **Analyze reviewers:**
```bash
grep "Review by @" .kiro/resources/sql/2025-12-01-TO-2025-12-31.md | sort | uniq -c | sort -rn
```

3. **Count PRs:**
```bash
grep "^# PR #" .kiro/resources/sql/2025-12-01-TO-2025-12-31.md | wc -l
```

4. **Find specific reviewer's comments:**
```bash
grep -A 5 "Review by @USERNAME" .kiro/resources/sql/2025-12-01-TO-2025-12-31.md
```

## Troubleshooting

**Error: gh: command not found**
→ Install GitHub CLI: `brew install gh`

**Error: authentication required**
→ Login: `gh auth login`

**Error: API rate limit exceeded**
→ Use `--limit` to reduce requests or wait an hour

**No PRs found**
→ Check date range and repo name are correct

## Tips

- Start with `--limit 10` to test
- Use `--state merged` to focus on completed PRs
- Break large date ranges into monthly chunks
- Authenticated requests have higher API limits (5000/hour vs 60/hour)

---

**Script Location:** `.kiro/scripts/pr_collector/pr_collector.py`
**Version:** 1.0.0
