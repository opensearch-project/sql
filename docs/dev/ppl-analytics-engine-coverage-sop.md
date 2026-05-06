# SOP — PPL Analytics-Engine IT Coverage Report

Run all PPL ITs through the analytics-engine path against an externally-managed
cluster, get a bucketed markdown report. Daily.

Output: `integ-test/build/reports/analytics-compatibility/REPORT.md`

---

## One-time setup

JDK 25 + JDK 21, Rust stable, ~15 GB free disk.

```bash
mkdir -p ~/workspace && cd ~/workspace
git clone git@github.com:opensearch-project/OpenSearch.git
git clone git@github.com:opensearch-project/sql.git
( cd OpenSearch && git remote add upstream https://github.com/opensearch-project/OpenSearch.git )
( cd sql       && git remote add ahkcs    git@github.com:ahkcs/sql.git )
```

**OpenSearch — track `upstream/main` + apply commons-text patch:**

`upstream/main` already has the full analytics-engine sandbox plugin set
(`analytics-engine`, `analytics-backend-{datafusion,lucene}`, `composite-engine`,
`dsl-query-executor`, `parquet-data-format`) and the `dataformat-native` rust
crate. Track main; rebase daily.

```bash
cd ~/workspace/OpenSearch
git fetch upstream main && git checkout -B main upstream/main
```

In `sandbox/plugins/analytics-engine/build.gradle`, add next to the existing
`commons-math3` runtimeOnly:

```groovy
runtimeOnly "org.apache.commons:commons-text:1.11.0"
```

`commons-text` is only listed under `resolutionStrategy.force` on main, which
pins the version but doesn't bundle the jar — without this `runtimeOnly` line
the cluster dies on `LevenshteinDistance` the first time a query reaches
Calcite's `SqlFunctions.<clinit>`. Track upstreaming; once it lands, drop this
patch.

**Build the native lib (one-time + after rust changes):**

The cluster JVM loads `libopensearch_native.dylib` (rust + JNI/FFM) at startup
for DataFusion execution, parquet I/O, and native repository ops. Without it
the cluster crashes in `NativeBridge.<clinit>`.

```bash
cd ~/workspace/OpenSearch/sandbox/libs/dataformat-native/rust
cargo build --release -p opensearch-native-lib   # ~5 min
```

---

## Daily run (~30 min)

### 1. Get the SQL bundle worktree

First time:
```bash
cd ~/workspace/sql
git fetch ahkcs feature/ppl-coverage-bundle
DATE=$(date +%Y-%m-%d)
git worktree add "../sql-ppl-coverage-${DATE}" ahkcs/feature/ppl-coverage-bundle
```

Reuse same day:
```bash
cd ~/workspace/sql-ppl-coverage-${DATE}
git fetch ahkcs && git rebase ahkcs/feature/ppl-coverage-bundle
```

### 2. Publish the SQL plugin to mavenLocal

`:run` installs `opensearch-sql-plugin:3.7.0.0-SNAPSHOT` from maven coords —
mavenLocal first, then OpenSearch CI snapshots. The SQL plugin isn't on CI
snapshots, so without this step the cluster fails to bootstrap with
`Could not resolve …`. Re-publish whenever the bundle worktree is updated.

```bash
cd ~/workspace/sql-ppl-coverage-${DATE}
./gradlew publishToMavenLocal   # ~1 min, JDK 21+
```

### 3. Bring up cluster (terminal A — leave running)

```bash
cd ~/workspace/OpenSearch
JAVA_HOME=$(mise where java@temurin-25.0.1+8.0.LTS) ./gradlew :run \
  -Dsandbox.enabled=true \
  -PinstalledPlugins="['opensearch-job-scheduler:3.7.0.0-SNAPSHOT', \
'analytics-engine', 'parquet-data-format', 'analytics-backend-datafusion', \
'analytics-backend-lucene', 'composite-engine', \
'opensearch-sql-plugin:3.7.0.0-SNAPSHOT']"
```

Wait for it:
```bash
until curl -sf http://localhost:9200/ >/dev/null; do sleep 4; done
```

### 4. Run the report (terminal B)

```bash
cd ~/workspace/sql-ppl-coverage-${DATE}
rm -rf integ-test/build/test-results/analyticsCompatibility \
       integ-test/build/test-results/analyticsCompatibilityTest \
       integ-test/build/reports/analytics-compatibility

./gradlew :integ-test:analyticsCompatibilityReport \
  -Dtests.rest.cluster=localhost:9200 \
  -Dtests.cluster=localhost:9300 \
  -Dtests.clustername=runTask
```

`BUILD SUCCESSFUL` is expected — failures are the report payload, not a build
break.

### 5. Open the report

```bash
open integ-test/build/reports/analytics-compatibility/REPORT.md
```

### 6. Tear down

`Ctrl-C` terminal A.

---

## When something breaks

| Symptom | Fix |
|---|---|
| `BUILD FAILED … java.io.EOFException … KryoBackedDecoder` | `rm -rf integ-test/build/test-results/analyticsCompatibilityTest` and rerun. |
| Run finishes <10 min, top bucket is `ConnectException` | Cluster died. Tail `~/workspace/OpenSearch/build/testclusters/runTask-0/logs/runTask.log` for `fatal error`; the missing class usually needs a `runtimeOnly` dep added to `analytics-engine/build.gradle`. |
| Top bucket is `No backend can scan all requested fields` (thousands) | Parquet flag isn't reaching `TestUtils`. Confirm worktree is on `ahkcs/feature/ppl-coverage-bundle`. |
| `BUILD FAILED` immediately, log shows `[BUILD] Starting OpenSearch process / Stopping node` | `task.getClusters().clear()` is missing. Re-fetch `feature/ppl-coverage-bundle`. |

---

## What the bundle branch is

`ahkcs/feature/ppl-coverage-bundle` rebases on
`upstream/feature/mustang-ppl-integration` and adds the report task, parquet
flag, origin classifier, pass-rate fix, and testcluster auto-bind workaround.
Daily upstream sync: `git rebase upstream/feature/mustang-ppl-integration`.
