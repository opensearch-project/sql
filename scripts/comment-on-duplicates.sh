#!/bin/bash
#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# Posts a formatted duplicate detection comment and adds the duplicate label.
#
# Usage:
#   ./scripts/comment-on-duplicates.sh --base-issue 123 --potential-duplicates 456 789

set -euo pipefail

REPO="${GITHUB_REPOSITORY:-}"
BASE_ISSUE=""
DUPLICATES=()

while [[ $# -gt 0 ]]; do
  case $1 in
    --base-issue)
      BASE_ISSUE="$2"
      shift 2
      ;;
    --potential-duplicates)
      shift
      while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
        DUPLICATES+=("$1")
        shift
      done
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$BASE_ISSUE" ]]; then
  echo "Error: --base-issue is required" >&2
  exit 1
fi

if [[ ${#DUPLICATES[@]} -eq 0 ]]; then
  echo "Error: --potential-duplicates requires at least one issue number" >&2
  exit 1
fi

REPO_FLAG=""
if [[ -n "$REPO" ]]; then
  REPO_FLAG="--repo $REPO"
fi

# Build duplicate list
DUP_LIST=""
for dup in "${DUPLICATES[@]}"; do
  TITLE=$(gh issue view "$dup" $REPO_FLAG --json title -q .title 2>/dev/null || echo "")
  if [[ -n "$TITLE" ]]; then
    DUP_LIST+="- #${dup} — ${TITLE}"$'\n'
  else
    DUP_LIST+="- #${dup}"$'\n'
  fi
done

# Build the comment body with a hidden marker for auto-close detection
BODY="<!-- duplicate-detection -->
### Possible Duplicate

Found **${#DUPLICATES[@]}** possible duplicate issue(s):

${DUP_LIST}
If this is **not** a duplicate:
- Add a comment on this issue, and the \`duplicate\` label will be removed automatically, or
- 👎 this comment to prevent auto-closure

Otherwise, this issue will be **automatically closed in 3 days**.

🤖 Generated with [Claude Code](https://claude.ai/code)"

# Post the comment
echo "$BODY" | gh issue comment "$BASE_ISSUE" $REPO_FLAG --body-file -

# Ensure the duplicate label exists
gh label create "duplicate" \
  --description "Issue is a duplicate of an existing issue" \
  --color "cccccc" \
  $REPO_FLAG 2>/dev/null || true

# Add duplicate label
gh issue edit "$BASE_ISSUE" $REPO_FLAG --add-label "duplicate"

echo "Posted duplicate comment and added duplicate label to issue #${BASE_ISSUE}"
