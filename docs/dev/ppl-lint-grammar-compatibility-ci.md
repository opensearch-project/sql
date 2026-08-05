# PPL Linter Grammar Compatibility CI

This check verifies that the runtime PPL grammar built from a SQL revision
remains compatible with the grammar-dependent linter rules in the paired
OpenSearch Dashboards (OSD) revision.

SQL owns the candidate grammar endpoint, branch pairing, grammar cases, and CI
artifacts. OSD owns the headless lint API, rule catalog, detectors, and
parse-tree behavior.

The check is deliberately grammar-only. It does not send PPL queries to an
OpenSearch backend, create indices or fixtures, compare historical engines, run
Analytics Engine, or assert diagnostic wording, severity, fixes, hover content,
or UI rendering. OpenSearch runs only long enough to serve
`GET /_plugins/_ppl/_grammar`.

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
`build.gradle`. OSD's product version is read from the checked-out build with
`yarn --silent pkg-version`, after selecting Node from OSD's `.nvmrc`. Patch
versions may differ on an exact release line.

The workflow does not infer an exact branch for `X.x` branches and never falls
back from a missing OSD `X.Y` branch to OSD `main`. There is no fixed
historical-release lane on `main`: the contract is between the two branches
that ship together, not between current SQL and an unrelated old OSD grammar.

## Validation path

The focused workflow is
[ppl-lint-grammar-compatibility.yml](../../.github/workflows/ppl-lint-grammar-compatibility.yml).
For each supported event it:

1. Resolves the SQL target and paired OSD ref.
2. Checks out OSD and records its immutable SHA before running OSD code.
3. Validates product versions and writes `resolved-target.json`.
4. Probes the
   `src/plugins/data/public/antlr/opensearch_ppl/headless_ppl_lint` module,
   accepting its `.ts` or `.js` form.
5. If the module is absent, writes a successful capability-skip report and
   stops before starting OpenSearch or bootstrapping OSD.
6. Otherwise starts the existing `:opensearch-sql-plugin:run` task, captures
   and validates `ppl-grammar-bundle.json`, then stops the cluster.
7. Bootstraps OSD and runs
   [validate-osd-grammar.mjs](../../scripts/ppl-lint/validate-osd-grammar.mjs)
   against [grammar-cases.json](../../scripts/ppl-lint/grammar-cases.json).
8. Publishes the report and evidence before enforcing the result.

Capability is detected from the checkout, not from a product-version constant.
An absent module means:

```json
{
  "status": "skipped",
  "skipReason": "osd-headless-grammar-api-unavailable"
}
```

This is exit code `0` and executes zero cases. If the module exists but cannot
load `deserializeBundleOrThrow` and `lintQueryWithBundle`, the supported API is
broken and the run fails structurally. It must not become a skip.

The adapter deserializes the candidate bundle without a compiled-grammar
fallback. For each case it enables only the named rule, supplies the normalized
SQL version, builds a parse tree, and compares the target rule's diagnostic
count. Every selected rule needs trigger and control coverage, and every case
must name a rule in the paired OSD catalog. Missing cases, rules, parse trees,
syntax-clean trees, or bundles fail rather than pass vacuously.

## Events

| Event | Purpose | OSD source |
| --- | --- | --- |
| `pull_request` | Pre-merge candidate validation | Canonical paired branch |
| `push` | Validation of the exact merged revision | Canonical paired branch |
| `workflow_dispatch` | Non-required OSD branch, fork, or SHA evidence | Optional `osd_repo` and `osd_ref` |

Pull request and push runs use no repository variables to redirect OSD.
Manual overrides remain diagnostic evidence and do not satisfy branch
protection.

## Reports and artifacts

The `ppl-lint-grammar-compatibility` CI artifact contains the files that were
available for the run:

- `resolved-target.json`: SQL and OSD branches, versions, and immutable SHAs;
- `osd-revision.txt`: the recorded OSD SHA;
- `ppl-lint-grammar-compatibility-report.json`: final machine-readable result;
- `ppl-grammar-bundle.json`: candidate bundle when capability is available; and
- `ppl-grammar-cluster.log`: candidate cluster output when it was started.

Every report has `schemaVersion`, `status`, and rule counts. Reports produced
after target resolution also include SQL and OSD metadata, the manual-override
flag, and the release-line-bypass flag. A validating report additionally
includes `grammarHash`, case counts, normalized case results, and failures with
rule ID, case ID, query, and expected and actual diagnostic counts. A skipped
report includes `skipReason` and zero selected rules. Structural adapter
failures use `status: "error"` and include `error`.
Failures before target resolution still produce an error report, but cannot
include SQL or OSD metadata. The GitHub step summary presents the available
provenance and failed cases.

The wrapper and adapter use these exit codes:

| Code | Meaning |
| ---: | --- |
| `0` | All cases passed, or the paired OSD branch lacks the headless API |
| `1` | One or more grammar/linter diagnostic counts did not match |
| `2` | Pairing, input, bundle, case coverage, or advertised-API failure |

See the [operational quick start](../../scripts/ppl-lint/README.md) for local
commands and exact-SHA reproduction.

## Security

- Required runs execute only the canonical paired OSD branch.
- OSD repository/ref overrides and release-line bypasses are
  `workflow_dispatch`-only.
- The job has `contents: read`, persists no checkout credentials, and receives
  no repository secrets.
- The immutable OSD SHA is recorded before OSD dependency or build code runs.
- Manual fork/ref runs are non-required and receive no privileged credentials.

## Release branches

When a new exact `X.Y` branch is cut, carry the workflow, scripts, grammar
cases, and this documentation with the SQL branch. Confirm that the OSD `X.Y`
branch exists and both builds report `X.Y.z`, then run a manual canary.
Capability detection decides whether the branch validates or reports
`skipped`; no version constant or documentation rewrite is required.

An already-cut branch without the OSD API may continue to report a visible
successful skip. If the API is later backported, the same workflow starts
validating automatically. Version-family branches remain unsupported until
they receive an explicit pairing policy.

## Triage

Always begin with the SQL and OSD SHAs in the report. Reproducing against a
newer `main` does not reproduce the completed run.

| Failure | First owner or action |
| --- | --- |
| SQL or OSD version disagrees with exact `X.Y` | CI owner checks branch selection and branch-cut state |
| Paired OSD branch is missing | CI owner fixes pairing; never substitute `main` |
| Headless module is absent | No product action; verify `skipped` and exact metadata |
| Module exists but imports or exports fail | OSD linter owner treats it as a headless API regression |
| Cluster startup or grammar GET fails | SQL plugin owner inspects `ppl-grammar-cluster.log` |
| Bundle validation or deserialization fails | SQL grammar-bundle owner checks the endpoint schema and generated grammar |
| Trigger stops firing | SQL grammar and OSD rule owners inspect the parse-tree contract |
| Control starts firing | OSD rule owner checks whether matching broadened intentionally |
| Case names a missing OSD rule | Review the OSD change, then update or remove the stale SQL case |
| Post-merge push fails | Fix the merged revision before relying on its compatibility evidence |
