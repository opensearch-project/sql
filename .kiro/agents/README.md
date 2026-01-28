# ppl-doctor agents

This folder contains the Kiro CLI agents for the PPL bug-fixing workflow.

## Prerequisites
- OpenSearch + SQL plugin built from this repo
- A running local test cluster when reproducing bugs

Start a local cluster:
```bash
./gradlew opensearch-sql:run
```

## Available agents
- `ppl-doctor` (orchestrator entry point)
- `issue-analyzer-agent`
- `reproducer-agent`
- `root-cause-agent`
- `fix-implementer-agent`
- `pr-commit-agent`

List agents discovered in this repo:
```bash
kiro-cli agent list
```

## Orchestrator usage (recommended)
Run the orchestrator with a GitHub issue link:
```bash
kiro-cli chat --agent ppl-doctor --trust-all-tools
```
Then provide a request envelope, for example:
```text
stage: issue-analyzer
issue_url: https://github.com/opensearch-project/sql/issues/5055
context:
  repo: opensearch-project/sql
  local_repo_root: /Users/penghuo/oss/os-ppl
inputs:
  sample_data_paths: []
  query: ""
  expected: ""
constraints:
  avoid_legacy: true
  max_source_files: 30
```

The orchestrator will delegate to sub-agents based on the stage.

## Slack notifications
- The orchestrator uses santos-slack-mcp-server to notify channel `C0ABN6XRY7N`
  when user input is required (for example: missing repro details).

## Agent-by-agent usage (manual)
You can run each sub-agent directly using the same envelope format.

Issue analysis:
```bash
kiro-cli chat --agent issue-analyzer-agent --trust-all-tools
```

Reproduce a bug:
```bash
kiro-cli chat --agent reproducer-agent --trust-all-tools
```

Root-cause analysis:
```bash
kiro-cli chat --agent root-cause-agent --trust-all-tools
```

Fix + test:
```bash
kiro-cli chat --agent fix-implementer-agent --trust-all-tools
```

Create PR + track review:
```bash
kiro-cli chat --agent pr-commit-agent --trust-all-tools
```

## Response envelope
Each agent responds with:
```text
stage: <same as request>
status: <ok|blocked|needs-info>
summary: <one-line result>
artifacts:
  files_changed: [<path>...]
  commands_run: [<cmd>...]
  tests_run: [<cmd>...]
notes:
  risks: <string>
  followups: <string>
```

## Tips
- If a repro fails, capture exact OpenSearch version and mappings in your input.
- Keep scope minimal: if a fix would touch more than 30 non-test files, pause and confirm.
- For issue #5055, the repro data and queries are already in the issue body.

## TODO
- Slack integration is tracked in `TODO.md`.
