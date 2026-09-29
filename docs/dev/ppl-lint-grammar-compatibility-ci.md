# PPL Linter Grammar Compatibility CI

This check verifies that the runtime PPL grammar built from a SQL revision
preserves the observable diagnostics of every applicable rule in the paired
OpenSearch Dashboards (OSD) PPL lint catalog.

SQL owns the candidate grammar exporter, branch pairing, cases, exclusions, and
CI artifacts. OSD owns the headless lint API, rule catalog, detectors, and
parse-tree behavior.

The check is deliberately grammar-only. It does not start OpenSearch, call the
plugin REST endpoint, execute PPL, create indices, require a syntax-clean parse
tree, compare compiled fallback grammars, or assert diagnostic wording,
severity, fixes, hover content, or UI rendering.

## Branch pairing

The SQL event target selects the OSD branch:

| SQL target | OSD ref | Version check |
| --- | --- | --- |
| `main` | `main` | Record both versions; their release lines may differ |
| Exact `X.Y` | Exact `X.Y` | SQL and OSD must both report `X.Y.z` |

Pull requests use `github.base_ref`; pushes and manual dispatches use
`github.ref_name`. The workflow tests the checked-out SQL revision and records
the pull request head SHA separately when available.

SQL's product version is the default `opensearch.version` in the root
`build.gradle`. OSD's product version comes from `yarn --silent pkg-version`
after selecting Node from OSD's `.nvmrc`. Patch versions may differ on an exact
release line.

The workflow does not infer an exact branch for version-family branches and
never falls back from a missing OSD `X.Y` branch to OSD `main`. There is no
release-line bypass.

## Validation path

The focused workflow is
[ppl-lint-grammar-compatibility.yml](../../.github/workflows/ppl-lint-grammar-compatibility.yml).
It:

1. Resolves the SQL target and paired OSD ref.
2. Checks out canonical `opensearch-project/OpenSearch-Dashboards` and records
   its immutable SHA before executing OSD code.
3. Validates product versions and writes `resolved-target.json`.
4. Runs `:ppl:exportPplGrammarBundle` to serialize
   `PPLGrammarBundleBuilder.getBundle()` directly.
5. Bootstraps the exact OSD checkout.
6. Invokes
   [validate-osd-grammar.mjs](../../scripts/ppl-lint/validate-osd-grammar.mjs)
   once under OSD's Node environment.
7. Publishes the report and evidence before enforcing the adapter exit code.

The workflow and shell wrapper do not probe for OSD module files. The adapter's
resolver and loader are the sole capability authority:

- a genuinely absent headless entry point produces `status: "skipped"` with
  `skipReason: "osd-headless-grammar-api-unavailable"`;
- a resolved entry point that cannot import, has a missing transitive
  dependency, or lacks required exports produces a structural error.

This prevents valid API layouts from becoming false-green skips.

The adapter deserializes the candidate bundle without invoking OSD's compiled
grammar fallback. For each case it enables only the named rule and compares the
target rule's exact diagnostic count. Recovered parse trees are valid; only
observable linter diagnostics determine case behavior.

Case schema version 2 classifies the complete paired catalog. It requires:

```text
coveredRuleIds == catalogRuleIds - excludedRuleIds
```

Every covered rule needs trigger and control cases. Every exclusion needs a
non-empty reason. Missing rules, unknown cases, stale exclusions, overlaps, and
duplicate IDs fail structurally. The blocking set is the 12 catalog rules
enabled by default in the approved OSD release inventory. Four default-off
headless rules are explicitly excluded from the active gate, and the two
default-off explain-backed rules are excluded because a grammar-only run has no
backend explain plan. The separately configured `command-suggestion` check is a
syntax-channel feature, not a catalog detector, so it is outside this headless
lint adapter.

## Events

| Event | Purpose | OSD source |
| --- | --- | --- |
| `pull_request` | Pre-merge candidate validation | Canonical paired branch |
| `push` | Validation of the exact merged revision | Canonical paired branch |
| `workflow_dispatch` | Non-required canary against another canonical ref | Optional `osd_ref` in the canonical repository |

Required pull request and push runs cannot redirect OSD. Manual dispatch may
select another ref only from
`opensearch-project/OpenSearch-Dashboards`; it cannot select another repository
or bypass release-line version checks.

Product path triggers are limited to the PPL grammar sources, grammar bundle
builder/exporter, and their build inputs. Tooling paths cover the adapter,
cases, wrapper, tests, and workflow itself. Plugin startup and REST action files
are not inputs to this check.

## Reports and artifacts

The `ppl-lint-grammar-compatibility` artifact contains the files available for
the run:

- `resolved-target.json`: SQL and OSD branches, versions, and immutable SHAs;
- `osd-revision.txt`: the recorded OSD SHA;
- `ppl-grammar-bundle.json`: direct exporter output; and
- `ppl-lint-grammar-compatibility-report.json`: machine-readable result.

Report schema version 2 includes status, provenance, grammar hash, case results,
failures, and catalog classification:

- catalog, required, covered, excluded, missing, and unexpected rule sets;
- reasons for every excluded rule;
- selected/passed/failed rule and case counts; and
- expected and actual target-rule diagnostic counts.

A failure before the adapter writes a report produces a schema-version-2 error
report. The workflow always creates that fallback when needed, uploads all
available evidence, and only then enforces the result. This preserves evidence
for exporter, bootstrap, and adapter startup failures.

The wrapper and adapter use these exit codes:

| Code | Meaning |
| ---: | --- |
| `0` | All cases passed, or the paired OSD branch genuinely lacks the API |
| `1` | One or more target-rule diagnostic counts did not match |
| `2` | Pairing, input, bundle, catalog coverage, or advertised-API failure |

See the [operational quick start](../../scripts/ppl-lint/README.md) for local
commands and exact-SHA reproduction.

## Security

- Required runs execute only the canonical paired OSD branch.
- Manual dispatch can override only the canonical repository's ref.
- Exact release-line validation cannot be disabled.
- The job has `contents: read`, persists no checkout credentials, and receives
  no repository secrets.
- The immutable OSD SHA is recorded before dependency or build code runs.

## Release branches

When a new exact `X.Y` branch is cut, carry the workflow, scripts, cases, and
this documentation with the SQL branch. Confirm that the OSD `X.Y` branch
exists and both builds report `X.Y.z`, then run a manual canary.

An older paired OSD branch without the headless API may continue to produce a
visible successful skip. If the API is backported, the same adapter begins
validating automatically. No workflow or wrapper capability table needs
updating.

## Triage

Always begin with the SQL and OSD SHAs in the report. Reproducing against a
newer branch tip does not reproduce the completed run.

| Failure | First owner or action |
| --- | --- |
| SQL or OSD version disagrees with exact `X.Y` | CI owner checks branch selection and branch-cut state |
| Paired OSD branch is missing | CI owner fixes pairing; never substitute `main` |
| Headless API is genuinely absent | Verify the visible skip and exact old OSD SHA |
| Headless module import or exports fail | OSD linter owner treats it as an API regression |
| Direct grammar export fails | SQL grammar owner inspects the `:ppl` build |
| Bundle validation or deserialization fails | SQL grammar-bundle owner checks exporter schema and generated grammar |
| Catalog classification is incomplete | Add cases or a justified exclusion; do not remove failing coverage |
| Trigger stops firing | SQL grammar and OSD rule owners inspect the parse-tree contract |
| Control starts firing | OSD rule owner checks whether matching broadened intentionally |
| Post-merge push fails | Fix the merged revision before relying on its compatibility evidence |
