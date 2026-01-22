# PR Review Collector

## Quick Start

### Prerequisites
1. Install GitHub CLI:
   ```bash
   brew install gh  # macOS
   ```

2. Authenticate:
   ```bash
   gh auth login
   ```

### Usage

**Basic collection:**
```bash
python3 ~/.kiro/scripts/pr_collector/pr_collector.py \
  --repo opensearch-project/sql \
  --start-date 2025-01-01 \
  --end-date 2025-01-31
```

**With filters:**
```bash
python3 ~/.kiro/scripts/pr_collector/pr_collector.py \
  --repo opensearch-project/sql \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --state merged \
  --limit 20
```

### Options
- `--repo`: Repository (owner/repo) - REQUIRED
- `--start-date`: Start date (YYYY-MM-DD) - REQUIRED
- `--end-date`: End date (YYYY-MM-DD) - REQUIRED
- `--state`: PR state filter (open, closed, merged, all) - default: all
- `--limit`: Maximum number of PRs to collect

### Output
Data is saved to: `~/.kiro/resources/{repo-name}/YYYY-MM-DD-TO-YYYY-MM-DD.md`

### Using with Kiro Agent
The `pr-collector` agent provides an interactive interface:
```bash
kiro-cli chat --agent pr-collector
```

Then simply describe what you want:
- "Collect PRs from opensearch-project/sql for January 2025"
- "Get merged PRs from last month, limit to 20"
- "Analyze the collected data"

## What Gets Collected

For each PR:
- Metadata (number, title, author, dates, state)
- Description
- Files changed (with additions/deletions)
- Reviews (with approval/rejection status)
- Review comments (inline code comments)
- General comments (discussion thread)
- Labels and assignees

## Troubleshooting

**GitHub CLI not found:**
```bash
gh --version  # Check if installed
brew install gh  # Install on macOS
```

**Authentication issues:**
```bash
gh auth status  # Check auth status
gh auth login   # Login if needed
```

**Rate limiting:**
- Use `--limit` to reduce API calls
- Authenticated requests have higher limits
- Break large collections into smaller date ranges

---
**Version:** 1.0.0
