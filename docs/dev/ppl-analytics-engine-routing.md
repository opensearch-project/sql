# Routing PPL Commands Through the Analytics-Engine Path

A guide for wiring an existing PPL command — already working on the v2 / Calcite engine — through the analytics-engine route so that `CalciteXxxCommandIT` passes against the force-routed analytics-engine path, without needing a per-command code change in the SQL plugin.

This doc is the playbook distilled from landing PPL `fillnull` end-to-end through the analytics engine ([opensearch-project/OpenSearch#21472][pr-fillnull]). Pair with [`ppl-commands.md`](ppl-commands.md) (which covers adding a *new* PPL command on the v2 / Calcite engine).

[pr-fillnull]: https://github.com/opensearch-project/OpenSearch/pull/21472

## Goal

For an existing PPL command `Xxx` whose `CalciteXxxCommandIT` currently passes on the v2 / Calcite engine but fails on the analytics-engine route, drive the analytics-engine route to parity. Two acceptance criteria:

1. `CalciteXxxCommandIT` (in `opensearch-project/sql`) passes when run via `:integ-test:integTestRemote` with `-Dtests.analytics.force_routing=true -Dtests.analytics.parquet_indices=true`.
2. A self-contained `XxxCommandIT` lives in `opensearch-project/OpenSearch` under `sandbox/qa/analytics-engine-rest`, exercising the command through `POST /_analytics/ppl` against a parquet-backed dataset. CI for OpenSearch core can verify the analytics-engine path without checking out the SQL plugin.

The QA IT in OpenSearch core is the long-term verification surface — once it lands, the SQL-plugin IT becomes a duplicate maintained for the v2 / Calcite path.

## Mental model — three layers

A PPL query on the analytics-engine route passes through three independent layers:

```
PPL text
  │
  ▼
[ Layer 1 ] SQL plugin lowering            (opensearch-project/sql)
  │   PPLSyntaxParser → AstBuilder → CalciteRelNodeVisitor → RelNode tree
  │
  ▼
[ Layer 2 ] Analytics-engine planner       (opensearch-project/OpenSearch · sandbox/plugins/analytics-engine)
  │   Capability registry, OpenSearch{Project,Filter,Aggregate}Rule,
  │   AnnotatedProjectExpression, fragment driver
  │
  ▼
[ Layer 3 ] Backend                        (opensearch-project/OpenSearch · sandbox/plugins/analytics-backend-datafusion)
      Substrait conversion via isthmus → DataFusion native execution
```

Knowing which layer a missing function lives in tells you the effort:

| Bucket | Symptom (failure stack frame) | Layer | Effort |
|---|---|---|---|
| 1 (S0) | `OpenSearchProjectRule` / `OpenSearchFilterRule` throws `No backend supports scalar function [X] among [datafusion]` | Layer 2 capability registry | One line |
| 2 (S1) | Substrait isthmus throws `Unable to convert call X` (the function exists in DataFusion but the call shape doesn't match) | Layer 2 adapter | Small adapter |
| 3 (S1–M) | DataFusion runtime throws (function doesn't exist natively) | Layer 3 UDF | Half-day–multi-day Rust |

**Layer 1 is almost always already done** for any command that works on the v2 Calcite engine — `CalciteRelNodeVisitor.visitXxx` produces a clean RelNode that's the same regardless of which backend executes it. Verify by reading the v2 unit test (`CalcitePPLXxxTest` in `ppl/src/test/java/.../calcite/`); the printed `LogicalPlan` shows exactly what reaches Layer 2.

## Local environment setup

### One-time

1. **JDK 25** — `sandbox/libs/dataformat-native` targets JDK 25 (FFM API). Without it you'll see `error: release version 25 not supported`.
   ```bash
   # mise / asdf / sdkman
   export JAVA_HOME=/path/to/temurin-25.0.x
   export PATH=$JAVA_HOME/bin:$PATH
   ```
2. **Native lib built** — confirm `OpenSearch/sandbox/libs/dataformat-native/rust/target/release/libopensearch_native.dylib` (macOS) or `.so` (Linux) exists. Built automatically by `:run`/`:integTest` if missing, but a stale build is the most common source of weird FFM errors. Rebuild explicitly via the cargo target if in doubt.
3. **`gh` CLI** authenticated against your OpenSearch fork for opening PRs.

### Repos

| Repo | Branch convention | Purpose |
|---|---|---|
| `opensearch-project/OpenSearch` | `feature/<descriptive>` off `upstream/main` | Where the actual fix lands (analytics-engine and DataFusion backend code) |
| `opensearch-project/sql` | `feature/<descriptive>` off `ahkcs/feature/ppl-coverage-bundle` until the analytics-compatibility test task lands upstream; thereafter off `upstream/feature/mustang-ppl-integration` | Test-infrastructure work, IT carryovers (rare for new functions once QA-side ITs exist) |

In the OpenSearch checkout: `feature/mustang-fillnull-command` was the dev branch; `pr/fillnull-poc` was the rebased clean branch I pushed for the PR. Use the same split when working on a new function — keep your dev branch with extra debugging/printf commits, then cherry-pick the clean ones onto a `pr/...` branch for PR submission.

### Maven-local prerequisites (full publish-and-run flow from scratch)

The `:run` task pulls `opensearch-job-scheduler:3.7.0.0-SNAPSHOT` and `opensearch-sql-plugin:3.7.0.0-SNAPSHOT` from `~/.m2/repository`. If those aren't published yet, the cluster bootstrap fails with `Could not resolve ...` or `Failed installing file:.../opensearch-sql-plugin...zip`. End-to-end:

```bash
# 1. Publish OpenSearch core + sandbox plugins to maven local
cd /path/to/OpenSearch
JAVA_HOME=/path/to/temurin-25 ./gradlew publishToMavenLocal -Dsandbox.enabled=true

# 2. Publish SQL plugin to maven local (any JDK 21+ works for the SQL repo)
cd /path/to/sql
./gradlew publishToMavenLocal

# 3. (Optional) Publish opensearch-job-scheduler from its own repo if not already in maven local
#    Most devs already have this from prior work — check via:
ls ~/.m2/repository/org/opensearch/plugin/opensearch-job-scheduler/3.7.0.0-SNAPSHOT/

# 4. Run the server with all required plugins (back in OpenSearch)
cd /path/to/OpenSearch
JAVA_HOME=/path/to/temurin-25 ./gradlew :run -Dsandbox.enabled=true \
  -PinstalledPlugins="['opensearch-job-scheduler:3.7.0.0-SNAPSHOT', \
    'analytics-engine', 'parquet-data-format', 'analytics-backend-datafusion', \
    'analytics-backend-lucene', 'composite-engine', \
    'opensearch-sql-plugin:3.7.0.0-SNAPSHOT']"
```

**`tests.jvm.argline` and `--debug-server-jvm`.** The user-facing Mustang task brief shows:
```bash
-Dtests.jvm.argline="-Djava.library.path=$(pwd)/sandbox/libs/dataformat-native/rust/target/release \
  -Dopensearch.experimental.feature.pluggable.dataformat.enabled=true" \
--debug-server-jvm
```
You don't need `-Dtests.jvm.argline` for `:run` — `OpenSearch/gradle/run.gradle` already injects the native-lib path and the `pluggable.dataformat.enabled` flag automatically when `parquet-data-format` or `analytics-backend-datafusion` is in `installedPlugins` (lines 93–101). The flag is only needed for `:internalClusterTest` style tasks where the test JVM (not the cluster JVM) loads the native bridge — committed to `gradle.properties` for those use cases.

`--debug-server-jvm` is the JVM-debug-attach flag for `:run`. Add it when you want to attach a debugger on port 5005; omit for normal runs.

**Plugin install order matters.** `opensearch-sql-plugin` declares `analytics-engine` and `opensearch-job-scheduler` as `extendedPlugins`, so those must be installed first. Putting `opensearch-sql-plugin` first in the list yields:
```
IllegalArgumentException: Missing plugin [analytics-engine], dependency of [opensearch-sql]
```

## The development loop (Bucket 1 / S0)

> **Branch base for the SQL plugin checkout.** Until the
> `analyticsCompatibilityTest` task and `tests.analytics.{force_routing,parquet_indices}`
> system properties land in `upstream/feature/mustang-ppl-integration`, base
> your SQL plugin work on `ahkcs/feature/ppl-coverage-bundle` (which rebases on
> top of the integration branch and adds those bits). See
> [`ppl-analytics-engine-coverage-sop.md`](ppl-analytics-engine-coverage-sop.md)
> for the standalone daily-report recipe.

### 1. Triage — find the failure

Start an OpenSearch test cluster locally and run the SQL-plugin's `CalciteXxxCommandIT` through the analytics-engine route. The cluster needs all 7 sandbox plugins.

```bash
# Terminal A — OpenSearch checkout, JDK 25
cd /path/to/OpenSearch
JAVA_HOME=/path/to/temurin-25 ./gradlew :run -Dsandbox.enabled=true \
  -PinstalledPlugins="['opensearch-job-scheduler:3.7.0.0-SNAPSHOT', \
    'analytics-engine', 'parquet-data-format', 'analytics-backend-datafusion', \
    'analytics-backend-lucene', 'composite-engine', \
    'opensearch-sql-plugin:3.7.0.0-SNAPSHOT']"
```

> **Plugin order matters.** `opensearch-sql-plugin` declares `analytics-engine` and `opensearch-job-scheduler` as `extendedPlugins`, so those must install first. The order above is correct.
> **Maven local prerequisites.** `opensearch-sql-plugin:3.7.0.0-SNAPSHOT` and `opensearch-job-scheduler:3.7.0.0-SNAPSHOT` need to be in `~/.m2/repository`. Build/publish from their respective repos via `./gradlew publishToMavenLocal -Dsandbox.enabled=true` once.

```bash
# Terminal B — sql checkout, any JDK 21+
cd /path/to/sql
./gradlew :integ-test:integTestRemote \
  -Dtests.rest.cluster=localhost:9200 \
  -Dtests.cluster=localhost:9300 \
  -Dtests.clustername=runTask \
  -Dtests.analytics.force_routing=true \
  -Dtests.analytics.parquet_indices=true \
  --tests "org.opensearch.sql.calcite.remote.CalciteXxxCommandIT"
```

The standard `:integTestRemote` runs against the externally-managed `:run` cluster; the two `tests.analytics.*` system properties are what flip the route. JUnit XML lands in `integ-test/build/test-results/integTestRemote/`.

**Why those two flags exist:**
- `tests.analytics.force_routing=true` → `PPLIntegTestCase.init()` flips the cluster setting `plugins.calcite.analytics.force_routing=true`, which makes `RestUnifiedQueryAction.isAnalyticsIndex()` return `true` unconditionally so every PPL query routes through the analytics-engine path regardless of the index name pattern.
- `tests.analytics.parquet_indices=true` → `TestUtils.createIndexByRestClient` injects the parquet/composite settings on every index it creates:
  ```java
  indexSettings.put("number_of_shards", 1);
  indexSettings.put("pluggable.dataformat.enabled", true);
  indexSettings.put("pluggable.dataformat", "composite");
  indexSettings.put("composite.primary_data_format", "parquet");
  ```
  Without parquet-backed storage, the analytics-engine planner fails with `IllegalStateException: No backend can scan all requested fields on index [...]` — DataFusion can't read Lucene-backed indices.

Without those two flags the same `:integTestRemote` invocation runs the v2 / Calcite path — that's the comparison baseline.

**Note on `:integ-test:analyticsCompatibilityTest` / `analyticsCompatibilityReport`.** These tasks (defined on `ahkcs/feature/ppl-coverage-bundle`) are *report-generation* helpers — they sweep every PPL IT through the analytics path with `ignoreFailures=true` and bucket the failures into a markdown report (see [`ppl-analytics-engine-coverage-sop.md`](ppl-analytics-engine-coverage-sop.md)). They are not needed for verifying a specific command — `:integTestRemote --tests "..."` with the two system properties is the right primitive for per-command work.

### 2. Read the failure

```bash
grep -E "<failure" integ-test/build/test-results/integTestRemote/TEST-org.opensearch.sql.calcite.remote.CalciteXxxCommandIT.xml \
  | grep -oE 'details=&quot;[^&]+'
```

Look for the **deepest cluster-side stack frame** in the failure body — that's where the analytics-engine path actually broke. Two common shapes:

**Bucket 1:**
```
No backend supports scalar function [<NAME>] among [datafusion]
  at OpenSearchProjectRule.annotateExpr(OpenSearchProjectRule.java:123)
```

**Bucket 2:**
```
Unable to convert call ANNOTATED_PROJECT_EXPR(fp64?)
```
or
```
io.substrait.isthmus.SubstraitRelVisitorException: cannot convert <X> ...
```

If the failure is something else entirely (e.g. `Project rule encountered unmarked child [LogicalJoin]`), the command depends on a non-S0 planner gap (joins, sub-queries, etc.) — file a separate issue, don't try to wedge it into the function PR.

### 3. Wire the function (Bucket 1 / S0)

For a Bucket-1 / S0 fix, edit one file:

`OpenSearch/sandbox/plugins/analytics-backend-datafusion/src/main/java/org/opensearch/be/datafusion/DataFusionAnalyticsBackendPlugin.java`

```java
private static final Set<ScalarFunction> STANDARD_PROJECT_OPS = Set.of(
    ScalarFunction.COALESCE,
    ScalarFunction.CEIL,
    ScalarFunction.<NEW_FUNCTION>     // ← add here
);
```

`STANDARD_PROJECT_OPS` is fanned out across `SUPPORTED_FIELD_TYPES` and the backend's supported formats by the `projectCapabilities()` override. The constant for `<NEW_FUNCTION>` must already exist in `sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/spi/ScalarFunction.java` — if it doesn't, add it (see "Adding a new ScalarFunction enum constant" below).

**No Rust-side, convertor, or Substrait-extension changes needed for Bucket 1.** DataFusion's native runtime executes the call directly via the default Substrait extension catalog already loaded by `DataFusionPlugin.loadSubstraitExtensions`.

### Adding a new `ScalarFunction` enum constant

If the function isn't yet in the `ScalarFunction` enum:

1. Open `sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/spi/ScalarFunction.java`.
2. Pick the right `Category` (`COMPARISON`, `STRING`, `MATH`, `SCALAR`, etc.).
3. Pick the right `SqlKind`. If Calcite has a dedicated `SqlKind` for the function (e.g. `SqlKind.COALESCE`, `SqlKind.CEIL`), use it. Otherwise use `SqlKind.OTHER` and rely on `fromSqlFunction` (which matches by name; the enum constant name must match `SqlFunction.getName().toUpperCase(Locale.ROOT)`).

```java
NEW_FUNCTION(Category.MATH, SqlKind.OTHER),
```

### 4. Verify locally

Restart the cluster (the `:run` task rebuilds plugins automatically from source on restart), then re-run the IT.

```bash
# Terminal A — Ctrl-C, then re-run :run
# Terminal B — re-run :integTestRemote with the two analytics-engine system properties
```

Or, faster: kill the gradle wrapper PID and re-launch.

### 5. Add the QA IT in OpenSearch core

This is the one-time investment that pays off across every future function: a self-contained IT in `sandbox/qa/analytics-engine-rest` that exercises the command through `POST /_analytics/ppl`, no SQL plugin needed.

#### 5a. Dataset resources

Pick or add a dataset under `sandbox/qa/analytics-engine-rest/src/test/resources/datasets/<name>/`:

```
datasets/<name>/
  ├── mapping.json    ← OpenSearch mapping with a "settings" block containing "number_of_shards"
  └── bulk.json       ← NDJSON bulk body, MUST end with a trailing newline (see "Common pitfalls")
```

`DatasetProvisioner.injectParquetSettings` injects parquet/composite settings adjacent to `"number_of_shards"`, so the mapping's `settings` block must contain that key. **Existing datasets to reuse:** `clickbench` (43-column wide table, dates, lots of types), `calcs` (fillnull-friendly: nullable int/double/keyword/boolean/date columns; verbose enough for `value=` syntax tests). Add a new dataset only if neither fits.

#### 5b. Test class

Place under `sandbox/qa/analytics-engine-rest/src/test/java/org/opensearch/analytics/qa/<Function>CommandIT.java`. Mirror the v2 / Calcite IT in the SQL plugin (`integ-test/src/test/java/org/opensearch/sql/calcite/remote/CalciteXxxCommandIT.java`) one-test-method-to-one — same query strings, same expected rows.

```java
public class FillNullCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");
    private static boolean dataProvisioned = false;

    /**
     * Lazily provision the dataset on first invocation. Must be called inside a test method
     * (not setUp) — OpenSearchRestTestCase's static client() is not initialized until after
     * @BeforeClass but is reliably available inside test bodies.
     */
    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    public void testFillNullSameValueOneField() throws IOException {
        assertRows(
            "source=" + DATASET.indexName + " | fields str2, num0 | fillnull with -1 in num0",
            row("one", 12.3),
            row("two", -12.3),
            // …
            row(null, -1)
        );
    }

    // ── helpers ────────────────────────────────────────────────────────────

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRows(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
        assertNotNull(actualRows);
        assertEquals(expected.length, actualRows.size());
        for (int i = 0; i < expected.length; i++) {
            List<Object> want = expected[i];
            List<Object> got = actualRows.get(i);
            assertEquals(want.size(), got.size());
            for (int j = 0; j < want.size(); j++) {
                assertCellEquals(want.get(j), got.get(j));
            }
        }
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }

    /** Numeric-tolerant cell comparison — JSON parsing returns Integer/Long/Double interchangeably. */
    private static void assertCellEquals(Object expected, Object actual) {
        if (expected == null || actual == null) { assertEquals(expected, actual); return; }
        if (expected instanceof Number && actual instanceof Number) {
            if (Double.compare(((Number) expected).doubleValue(), ((Number) actual).doubleValue()) != 0) {
                fail("expected <" + expected + "> but was <" + actual + ">");
            }
            return;
        }
        assertEquals(expected, actual);
    }
}
```

For tests that should fail with a planner-side validation error (e.g. type-incompatibility errors raised in `CalciteRelNodeVisitor` preflight), use a sibling `assertErrorContains` helper that catches `ResponseException` and asserts on the response body substring.

#### 5c. Run the QA IT

```bash
cd /path/to/OpenSearch
JAVA_HOME=/path/to/temurin-25 ./gradlew \
  :sandbox:qa:analytics-engine-rest:integTest \
  -Dsandbox.enabled=true \
  --tests "*<Function>CommandIT"
```

The QA module boots its own self-contained test cluster with all 7 sandbox plugins; no need for a `:run` cluster running. JUnit XML lands at `sandbox/qa/analytics-engine-rest/build/test-results/integTest/TEST-org.opensearch.analytics.qa.<Function>CommandIT.xml`.

### 6. Run the sandbox-wide `check` before pushing

Always run this before opening or updating a PR:

```bash
cd /path/to/OpenSearch
JAVA_HOME=/path/to/temurin-25 ./gradlew check -p sandbox -Dsandbox.enabled=true
```

This is the sandbox-tree equivalent of `./gradlew check` and runs everything OpenSearch core CI runs over the sandbox subprojects: compile (`-Werror`), unit tests, integ tests where applicable, `forbiddenApis`, `licenseHeaders`, `spotless`/`checkstyle`, `dependencyLicenses`, `thirdPartyAudit`, etc. CI fails on any of these — running locally first avoids the rebase-and-force-push churn when CI catches a missing license header or a forbidden API call.

Common things this surfaces that focused IT runs miss:
- Missing Apache 2.0 license headers on new files (`licenseHeaders`)
- Code-style violations on the new IT class (`checkstyle`/`spotless`)
- An import that pulls in a forbidden API (e.g. `java.util.Date`, `Optional.get()` in some places)
- A unit test in a sibling module broken by a planner change

Expect 5–15 min on a warm build, longer on a clean one. If `check` is green, the PR's CI almost always is too.

## When you need more than capabilities (Bucket 2 / S1)

If the planner accepts the function but Substrait conversion fails (`Unable to convert call ...`):

1. Check whether `OpenSearchProjectRule.annotateExpr` is wrapping a sub-call you weren't expecting. The recursive `AnnotatedProjectExpression` strip in `OpenSearchProject.stripAnnotations` should handle every wrapper, but if the function lives somewhere other than a `Project` (e.g. `Filter`, `Aggregate`), the corresponding rel's `stripAnnotations` may not be recursive yet — same fix applies (`RexShuttle` that recursively unwraps).
2. If the call shape itself is wrong for DataFusion (e.g. PPL emits a 3-arg call where DataFusion expects 2), add a `ScalarFunctionAdapter` in the DataFusion backend that rewrites the call into the equivalent DF-native shape. See `BackendPlanAdapter` and the `ScalarFunctionAdapter` SPI for the pattern.

## When the runtime doesn't support the function (Bucket 3 / M)

Implement a UDF in `sandbox/plugins/analytics-backend-datafusion/rust/src/udf.rs` and register it in `DataFusionService` startup. Roughly half a day per UDF; see existing UDF registrations for the boilerplate.

## Common pitfalls

- **`refresh=wait_for` hangs >60s on parquet-backed indices.** `analytics-backend-lucene`'s `LuceneCommitter.getSafeCommitInfo` is a `TODO:: with index deleter` stub. The QA module's `DatasetProvisioner` already uses `refresh=true` to sidestep this. The SQL plugin's `loadDataByRestClient` has a workaround keyed on `tests.analytics.parquet_indices=true`. Symptom: first test in a class run fails with `IllegalStateException: Failed to perform request` and elapsed time exactly 60s.
- **Bulk file missing trailing newline.** `DatasetProvisioner` loads `bulk.json` via `BufferedReader.lines().collect(joining("\n"))`, which drops the final newline. The bulk endpoint requires a trailing `\n`. Workaround: ensure your `bulk.json` has at least two trailing newlines (one row of `\n\n`), or the joined output will be missing the terminator and the bulk fails with `The bulk request must be terminated by a newline [\n]`.
- **`-Werror` blocks `@SafeVarargs`-eligible methods.** OpenSearch's javac is `-Werror`. Generic varargs assertion helpers need `@SafeVarargs @SuppressWarnings("varargs") private final void ...` (the `final` keyword is required for `@SafeVarargs` on instance methods; `private` alone won't compile until JDK 9+, and even then `final` is the safe choice).
- **Plugin install order during `:run`.** `opensearch-sql-plugin` extends `analytics-engine` and `opensearch-job-scheduler` — install both before the SQL plugin or you'll see `Missing plugin [analytics-engine], dependency of [opensearch-sql]`.
- **Gradle 9.x cached transform corruption.** Occasionally `:integTestRemote` / `:analyticsCompatibilityTest` will fail with `The contents of the immutable workspace ... have been modified`. Fix: `rm -rf ~/.gradle/caches/9.x/transforms/<hash>` and re-run.
- **Number-format pitfalls in QA assertions.** `assertCellEquals` must use `Double.compare(e.doubleValue(), a.doubleValue())` — Jackson parsing returns `Integer`/`Long`/`Double` based on JSON shape, and naive `.equals()` on cross-type numbers fails even when values are equal.
- **`ScalarFunction.fromSqlKind` returns null for `SqlKind.OTHER`.** If your function uses `SqlKind.OTHER`, the planner falls back to name-based resolution via `fromSqlFunction`. Make sure the enum constant name matches the function's SQL name uppercased.

## PR shape

For a single Bucket-1 function, a clean PR against `opensearch-project/OpenSearch:main` is:

| Commit | Files | Purpose |
|---|---|---|
| 1 | `DataFusionAnalyticsBackendPlugin.java` | Add the `ScalarFunction.<NAME>` to `STANDARD_PROJECT_OPS` |
| 2 | `<Function>CommandIT.java`, `datasets/<name>/{mapping,bulk}.json` (new or reused) | QA IT exercising the function via `POST /_analytics/ppl` |

If you also need a planner fix for the same function, prepend it as commit 0 (e.g. fillnull's nested-strip fix).

PR description should:
- State the bucket (S0 / S1 / M).
- Show the failure mode the fix addresses (paste the cluster-log stack trace).
- List the test file count: `CalciteXxxCommandIT` v2-side go from `N/M → M/M`, and `XxxCommandIT` QA-side is `M/M`.
- Frame it as the ongoing pattern: future Bucket-1 functions are one-line additions to `STANDARD_PROJECT_OPS`.

**Before pushing**, run the sandbox-wide check (see Step 6 in the dev loop):
```bash
./gradlew check -p sandbox -Dsandbox.enabled=true
```

## Files to read once before starting

- `OpenSearch/sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/spi/ScalarFunction.java`
- `OpenSearch/sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/spi/{Project,Filter,Aggregate}Capability.java`
- `OpenSearch/sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/spi/BackendCapabilityProvider.java`
- `OpenSearch/sandbox/plugins/analytics-engine/src/main/java/org/opensearch/analytics/planner/CapabilityRegistry.java`
- `OpenSearch/sandbox/plugins/analytics-engine/src/main/java/org/opensearch/analytics/planner/rules/OpenSearchProjectRule.java`
- `OpenSearch/sandbox/plugins/analytics-backend-datafusion/src/main/java/org/opensearch/be/datafusion/DataFusionAnalyticsBackendPlugin.java`
- `OpenSearch/sandbox/qa/analytics-engine-rest/src/test/java/org/opensearch/analytics/qa/{AnalyticsRestTestCase,DatasetProvisioner,Dataset}.java`
- `sql/core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java` — find the `visitXxx` for your command to confirm the RelNode shape it produces
- `sql/ppl/src/test/java/org/opensearch/sql/ppl/calcite/CalcitePPL<Xxx>Test.java` — the printed `LogicalPlan` shows exactly what reaches Layer 2

## Reference: fillnull as the worked example

The fillnull walkthrough that produced this guide:
- PR: [opensearch-project/OpenSearch#21472][pr-fillnull]
- Function: `COALESCE` (Layer 3 wire-up via `STANDARD_PROJECT_OPS`)
- Adjacent: `CEIL` (one fillnull test uses `with ceil(...) in ...`)
- Planner fix bundled: recursive `AnnotatedProjectExpression` strip in `OpenSearchProject.stripAnnotations` (general bug exposed by nested project calls; surfaced once both COALESCE and CEIL became project-capable)
- Result: `CalciteFillNullCommandIT` 2/13 → 13/13; QA-side `FillNullCommandIT` 13/13

The IT-infrastructure scaffolding (`tests.analytics.force_routing`, `tests.analytics.parquet_indices`, parquet-backed index helper in `TestUtils.makeParquetBacked`) was carried over from [`ahkcs/sql@29339c9c`](https://github.com/ahkcs/sql/commit/29339c9c0316d5bad43b5bcb13cc91627b54b1d9) on `feature/mustang-bin-command` — that commit is the canonical reference for the SQL-side test infra. The bundle branch's `analyticsCompatibilityTest` / `analyticsCompatibilityReport` Gradle tasks are report-generation helpers built on top of those system properties; per-command verification just needs the two `-D` flags on `:integTestRemote`. Once a function ships its QA-side IT in `sandbox/qa/analytics-engine-rest`, the SQL-side IT route becomes verification-only (the QA IT is the source of truth).
