# PPL grammar compatibility quick start

This directory contains the grammar cases and the adapter used by
[PPL linter grammar compatibility CI](../../docs/dev/ppl-lint-grammar-compatibility-ci.md):

- [grammar-cases.json](grammar-cases.json) contains grammar-only trigger and
  control queries with expected diagnostic counts.
- [validate-osd-grammar.mjs](validate-osd-grammar.mjs) loads the paired OSD
  headless linter API and writes the compatibility report.
- [ppl-lint-rule-validation.sh](../ppl-lint-rule-validation.sh) resolves local
  metadata, captures the candidate SQL grammar, bootstraps OSD, and invokes the
  adapter.

The temporary OpenSearch process serves only
`GET /_plugins/_ppl/_grammar`. This validation creates no indices or fixtures
and sends no backend PPL queries.

## Prerequisites

- JDK 21;
- Node from the selected OSD checkout's `.nvmrc`;
- the Yarn version required by that checkout's `package.json`;
- `curl`, `git`, `jq`, and an available local port `9200`; and
- either a local OSD checkout or permission to clone
  `opensearch-project/OpenSearch-Dashboards`.

## Run the CI-equivalent path

From the SQL repository root:

```bash
./scripts/ppl-lint-rule-validation.sh
```

With no options, the wrapper pairs local SQL with OSD `main` and clones OSD
into `.ci/OpenSearch-Dashboards` when that checkout does not exist. To use a
sibling checkout:

```bash
./scripts/ppl-lint-rule-validation.sh \
  --osd-root ../OpenSearch-Dashboards \
  --target-branch main
```

For an exact release branch, check out the same `X.Y` line in both repositories
and use `--target-branch X.Y --osd-ref X.Y`. The wrapper rejects SQL or OSD
versions outside that release line. On `main`, it records both product versions
without requiring their release lines to match.

The wrapper checks OSD capability before starting OpenSearch. If the headless
module is absent, it writes a `skipped` report and exits `0`.

## Exact CI reproduction

Start with `resolved-target.json` from the
`ppl-lint-grammar-compatibility` artifact. Check out the report's tested SQL SHA
and OSD SHA, not current branch tips:

```bash
git checkout --detach "$(jq -r '.sql.sha' /path/to/resolved-target.json)"
git -C ../OpenSearch-Dashboards checkout --detach \
  "$(jq -r '.osd.sha' /path/to/resolved-target.json)"

./scripts/ppl-lint-rule-validation.sh \
  --target /path/to/resolved-target.json \
  --osd-root ../OpenSearch-Dashboards
```

Fetch either SHA from its repository first if it is not present locally. The
wrapper verifies both checkout SHAs and both product versions against the
target file before doing any validation. For pull requests, `.sql.sha` is the
tested merge revision; `.sql.headSha` is traceability metadata, not a
substitute. A dispatch artifact with `releaseLineValidationBypassed: true`
retains that development-only bypass during exact reproduction.

## Local files

The wrapper defaults to these repository-root paths:

| Path | Role |
| --- | --- |
| `resolved-target.json` | Generated SQL/OSD metadata when `--target` is omitted |
| `ppl-grammar-bundle.json` | Captured production grammar endpoint response |
| `scripts/ppl-lint/grammar-cases.json` | SQL-owned trigger/control input |
| `ppl-lint-grammar-compatibility-report.json` | Machine-readable result |
| `ppl-lint-grammar-summary.md` | Local human-readable summary |
| `ppl-grammar-cluster.log` | Gradle development-cluster output |

Override these with `--grammar`, `--cases`, `--report`, `--summary`, and
`--cluster-log`.

A capability skip does not start the cluster, so no bundle or cluster log is
expected.

## Primitive debugging

Inspect the versions using the same sources as CI:

```bash
sed -nE \
  's/.*opensearch_version = System\.getProperty\("opensearch\.version", "([^"]+)"\).*/\1/p' \
  build.gradle

cd ../OpenSearch-Dashboards
nvm use
yarn --silent pkg-version
```

Check capability:

```bash
HEADLESS=../OpenSearch-Dashboards/src/plugins/data/public/antlr/opensearch_ppl/headless_ppl_lint
test -f "${HEADLESS}.ts" || test -f "${HEADLESS}.js"
```

Start the existing SQL development cluster from the SQL root:

```bash
./gradlew :opensearch-sql-plugin:run
```

In another terminal, capture and inspect the endpoint:

```bash
curl --fail --silent --show-error \
  http://127.0.0.1:9200/_plugins/_ppl/_grammar \
  --output ppl-grammar-bundle.json

jq -e '
  (.grammarHash | test("^sha256:[0-9a-fA-F]{64}$")) and
  (.lexerSerializedATN | length > 0) and
  (.parserSerializedATN | length > 0) and
  (.lexerRuleNames | length > 0) and
  (.parserRuleNames | length > 0)
' ppl-grammar-bundle.json
```

Stop the development cluster after capture. The wrapper is preferred for normal
use because its trap stops the process on success and failure.

To run only the adapter after `resolved-target.json` and the bundle exist,
bootstrap the exact OSD checkout first with `yarn osd bootstrap` unless its
generated targets already match that revision:

```bash
SQL_ROOT=/absolute/path/to/sql
OSD_ROOT=/absolute/path/to/OpenSearch-Dashboards
cd "$OSD_ROOT"

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

Minimal passed result:

```json
{
  "schemaVersion": 1,
  "status": "passed",
  "sql": {"sha": "<sql-sha>", "targetBranch": "main", "version": "X.Y.Z"},
  "osd": {"ref": "main", "sha": "<osd-sha>", "version": "X.Y.Z"},
  "manualOverride": false,
  "grammarHash": "sha256:<hash>",
  "rules": {"selected": 9, "passed": 9, "failed": 0},
  "caseCounts": {"selected": 18, "passed": 18, "failed": 0},
  "failures": []
}
```

Minimal skipped result:

```json
{
  "schemaVersion": 1,
  "status": "skipped",
  "skipReason": "osd-headless-grammar-api-unavailable",
  "sql": {"sha": "<sql-sha>", "targetBranch": "X.Y", "version": "X.Y.Z"},
  "osd": {"ref": "X.Y", "sha": "<osd-sha>", "version": "X.Y.Z"},
  "rules": {"selected": 0, "passed": 0, "failed": 0},
  "caseCounts": {"selected": 0, "passed": 0, "failed": 0}
}
```

Exit codes:

| Code | Meaning |
| ---: | --- |
| `0` | Passed, or skipped because the paired OSD API is absent |
| `1` | A diagnostic count mismatch or per-case execution failure |
| `2` | Structural input, pairing, bundle, coverage, or advertised-API failure |

## Common outcomes

| Outcome | Action |
| --- | --- |
| Exact `X.Y` version mismatch | Verify both checkouts and branch-cut state; do not use OSD `main` |
| `osd-headless-grammar-api-unavailable` | No product fix; confirm exact metadata and successful skip |
| Module exists but exports fail to load | Treat as an OSD supported-path API regression |
| Cluster startup or grammar GET fails | Inspect `ppl-grammar-cluster.log` |
| Bundle is malformed or cannot deserialize | Check the SQL grammar endpoint schema and generated grammar |
| Rule lacks trigger/control coverage | Add the missing grammar case before interpreting detector results |
| Case names a missing OSD rule | Review the OSD catalog change, then update the stale SQL case |
| Trigger count drops | SQL grammar and OSD rule owners inspect parser node names and tree shape |
| Control count rises | OSD rule owner checks whether matching broadened intentionally |
