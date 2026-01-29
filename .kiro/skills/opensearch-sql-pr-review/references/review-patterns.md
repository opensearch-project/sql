# Review Patterns (opensearch-project/sql)

Use these patterns as prompts for review focus and comment phrasing.

## Common review themes
- Remove redundant catch blocks when finally already handles cleanup.
- Refactor complex flow into smaller methods; avoid controlling flow with boolean flags.
- Use clear, intent-based names for flags and state.
- Avoid sysout and debug noise in tests.
- Explain non-obvious behavior close to code or in comments.
- Preserve required side effects even if return values are unused (e.g., explain() initializing builders).
- Update expected output snapshots when plan behavior changes.
- When tests change behavior, explain why and adjust test data carefully.
- Add integration tests for permission-sensitive or user-visible behavior changes.
- Prefer helper methods over direct relBuilder.aggregate when project trimming is required.

## Comment phrasing
- State impact first, then the fix.
- Ask one question at a time.
- Offer a concrete refactor or test suggestion.
