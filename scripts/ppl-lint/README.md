# PPL grammar compatibility quick start

This directory contains the SQL-owned inputs and adapter used by
[PPL linter grammar compatibility CI](../../docs/dev/ppl-lint-grammar-compatibility-ci.md):

- [grammar-cases.json](grammar-cases.json) contains trigger/control cases and
  justified catalog exclusions.
- [validate-osd-grammar.mjs](validate-osd-grammar.mjs) resolves the paired OSD
  headless API, validates catalog coverage, and writes report schema version 2.
- [ppl-lint-rule-validation.sh](../ppl-lint-rule-validation.sh) verifies the
  selected checkouts, exports the candidate grammar, bootstraps OSD, and invokes
  the adapter once.

The grammar is a build artifact of the generated ANTLR classes. The wrapper
exports it through `:ppl:exportPplGrammarBundle`; it does not start OpenSearch,
open a port, call the plugin REST endpoint, create indices, or execute PPL.

## Prerequisites

- JDK 21;
- Node from the selected OSD checkout's `.nvmrc`;
- the Yarn version required by that checkout's `package.json`;
- `git` and `jq`; and
- either a local OSD checkout or permission to clone
  `opensearch-project/OpenSearch-Dashboards`.

## Run the CI-equivalent path

From the SQL repository root:

```bash
./scripts/ppl-lint-rule-validation.sh
```

With no options, the wrapper pairs local SQL with OSD `main` and manages a
checkout in `.ci/OpenSearch-Dashboards`. To use a sibling checkout:

```bash
./scripts/ppl-lint-rule-validation.sh \
  --osd-root ../OpenSearch-Dashboards \
  --target-branch main
```

For an exact release branch, check out the same `X.Y` line in both repositories
and use `--target-branch X.Y --osd-ref X.Y`. The wrapper requires both products
to report `X.Y.z`. On `main`, it records both product versions without requiring
their release lines to match.

The adapter is the only authority on OSD headless API availability. The wrapper
does not inspect module filenames or extensions. A truly absent legacy API
produces a visible `skipped` report; an entry point that resolves but fails to
load or lacks required exports is a structural failure.

Set `PPL_LINT_SKIP_OSD_BOOTSTRAP=1` only when the selected checkout has already
been bootstrapped and its generated targets are current.

## Exact CI reproduction

Download `resolved-target.json` from the
`ppl-lint-grammar-compatibility` artifact, then check out the exact tested SHAs:

```bash
git checkout --detach "$(jq -r '.sql.sha' /path/to/resolved-target.json)"
git -C ../OpenSearch-Dashboards checkout --detach \
  "$(jq -r '.osd.sha' /path/to/resolved-target.json)"

./scripts/ppl-lint-rule-validation.sh \
  --target /path/to/resolved-target.json \
  --osd-root ../OpenSearch-Dashboards
```

Fetch either SHA first if it is not present locally. The wrapper verifies both
checkout SHAs and both product versions before exporting or validating. For
pull requests, `.sql.sha` is the tested merge revision; `.sql.headSha` is
traceability metadata and is not a substitute.

## Local files

The wrapper defaults to these repository-root paths:

| Path | Role |
| --- | --- |
| `resolved-target.json` | Generated SQL/OSD metadata when `--target` is omitted |
| `ppl-grammar-bundle.json` | Direct grammar exporter output |
| `scripts/ppl-lint/grammar-cases.json` | Cases and explicit exclusions |
| `ppl-lint-grammar-compatibility-report.json` | Machine-readable result |
| `ppl-lint-grammar-summary.md` | Human-readable summary |

Override output/input paths with `--grammar`, `--cases`, `--report`, and
`--summary`.

## Primitive debugging

Export the candidate bundle directly:

```bash
./gradlew :ppl:exportPplGrammarBundle --no-daemon \
  -PpplGrammarBundleOutput="$PWD/ppl-grammar-bundle.json"

jq -e '
  (.grammarHash | test("^sha256:[0-9a-fA-F]{64}$")) and
  (.lexerSerializedATN | length > 0) and
  (.parserSerializedATN | length > 0)
' ppl-grammar-bundle.json
```

To invoke only the adapter, bootstrap the exact OSD checkout first:

```bash
SQL_ROOT=/absolute/path/to/sql
OSD_ROOT=/absolute/path/to/OpenSearch-Dashboards
cd "$OSD_ROOT"
yarn osd bootstrap

node -r ./src/setup_node_env \
  "$SQL_ROOT/scripts/ppl-lint/validate-osd-grammar.mjs" \
  --grammar "$SQL_ROOT/ppl-grammar-bundle.json" \
  --cases "$SQL_ROOT/scripts/ppl-lint/grammar-cases.json" \
  --target "$SQL_ROOT/resolved-target.json" \
  --osd-root "$OSD_ROOT" \
  --osd-sha "$(git rev-parse HEAD)" \
  --report "$SQL_ROOT/ppl-lint-grammar-compatibility-report.json" \
  --summary "$SQL_ROOT/ppl-lint-grammar-summary.md"
```

## Report results

Schema version 2 records catalog classification as well as behavior:

```json
{
  "schemaVersion": 2,
  "status": "passed",
  "sql": {"sha": "<sql-sha>", "targetBranch": "main", "version": "X.Y.Z"},
  "osd": {"ref": "main", "sha": "<osd-sha>", "version": "X.Y.Z"},
  "grammarHash": "sha256:<hash>",
  "coverage": {
    "catalogRuleIds": ["..."],
    "requiredRuleIds": ["..."],
    "coveredRuleIds": ["..."],
    "excludedRuleIds": ["..."],
    "excludedRules": [{"ruleId": "...", "reason": "..."}],
    "missingRuleIds": [],
    "unexpectedRuleIds": [],
    "counts": {
      "catalog": 18, "required": 16, "covered": 16,
      "excluded": 2, "missing": 0, "unexpected": 0
    }
  },
  "rules": {
    "catalog": 18, "required": 16, "excluded": 2,
    "selected": 16, "passed": 16, "failed": 0
  },
  "caseCounts": {"selected": 32, "passed": 32, "failed": 0},
  "failures": []
}
```

A legacy skip has `status: "skipped"` and
`skipReason: "osd-headless-grammar-api-unavailable"`.

| Code | Meaning |
| ---: | --- |
| `0` | Passed, or skipped because the paired OSD API is genuinely absent |
| `1` | A diagnostic count mismatch or per-case execution failure |
| `2` | Pairing, input, bundle, coverage, or advertised-API failure |

## Common outcomes

| Outcome | Action |
| --- | --- |
| Exact `X.Y` version mismatch | Verify both checkouts and branch-cut state; do not substitute OSD `main` |
| `osd-headless-grammar-api-unavailable` | Confirm the exact old OSD SHA and the visible skip |
| Module resolves but import/exports fail | Treat as an OSD headless API regression |
| Export task fails | Inspect the `:ppl` build and generated ANTLR sources |
| Bundle cannot deserialize | Check exporter schema and generated grammar compatibility |
| Catalog coverage is incomplete | Add trigger/control cases or a justified exclusion |
| Trigger count drops | Inspect SQL grammar changes and the OSD rule's parse-tree assumptions |
| Control count rises | Check whether the OSD rule broadened intentionally |
