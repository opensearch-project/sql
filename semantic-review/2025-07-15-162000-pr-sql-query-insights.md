# Query phase tracking and source-header propagation for query-insights

A new `QueryPhaseTracker` collects wall-clock, CPU, and memory-allocation timings per query execution phase (parse, analyze, plan, execute) and serializes them into thread-context headers so the query-insights plugin can attribute costs to SQL/PPL queries. The entry points (`RestSqlAction`, `TransportPPLQueryAction`) stamp source-identification headers, and `SQLPlugin.getTaskHeaders` registers them for cross-node transport. The tracker uses Log4j `ThreadContext` as a shuttle to cross from the REST thread to the sql-worker thread.

Watch for:
- **Unconditional `putHeader` in `RestSqlAction`** (confirmed) — unlike the PPL path, the SQL REST handler calls `putHeader` without a null-guard, which will throw `IllegalArgumentException` if the header is already present.
- **`Thread.currentThread().getId()` deprecation** (confirmed) — deprecated since Java 19, replaced by `threadId()`. The project targets Java 21.
- **ThreadLocal leak on exception paths** (likely) — if `executeWithCalcite` throws before reaching `endAll()` / `writePhaseHeader()`, the tracker remains in the ThreadLocal and the Log4j key is never cleaned.
- **Missing imports / FQN usage in PPLService and SQLService** (confirmed) — both files use fully-qualified `org.opensearch.sql.common.utils.QueryPhaseTracker` inline instead of an import statement, inconsistent with the rest of the codebase.

## High-level view

The header-writing entry points differ in safety: `TransportPPLQueryAction` guards each `putHeader` with a null-check on `getHeader`, while `RestSqlAction` writes unconditionally. OpenSearch's `ThreadContext.putHeader` throws if the key already exists, so the SQL path is fragile in any scenario where the handler runs more than once per request context (retries, plugin chaining).

The lifecycle management has a gap in the Calcite path: if an exception escapes between `beginPhase("analyze")` and `endAll()`, neither `endAll()` nor `clear()` is reached, leaking the ThreadLocal and Log4j entry on the pooled thread.

`writePhaseHeader` in `OpenSearchExecutionEngine` is best-effort (catch-all around `putHeader`), which is the right call for observability plumbing — a failure to write metrics should never fail the query.

<details>
<summary>Issues (6)</summary>

1. **Unconditional putHeader in RestSqlAction** — wrap each `putHeader` call with a `getHeader == null` guard, matching the PPL pattern, to prevent `IllegalArgumentException` if the header already exists.
2. **Deprecated `Thread.currentThread().getId()`** — replace with `Thread.currentThread().threadId()` (available since Java 19; project targets 21).
3. **ThreadLocal leak on exception in executeWithCalcite** — add a `finally` block (or catch) that calls `QueryPhaseTracker.clear()` so the tracker and Log4j key are cleaned on failure paths.
4. **FQN usage instead of imports in PPLService/SQLService** — add proper import statements for `QueryPhaseTracker` to `PPLService.java` and `SQLService.java` for consistency and readability.
5. **Unused `isEmpty()` method** — `isEmpty()` is public but never called anywhere in the diff or existing code. Either document its intended consumer or remove dead code.
6. **No `tracker.endAll()` on legacy V2 exception path** — in `executeWithLegacy`, the tracker begins the "plan" phase but `endAll()` only fires inside `executePlan` if `current()` is non-null. If `plan(plan)` throws before that point, phases are left dangling.

</details>

<details>
<summary>Details</summary>

## Unconditional putHeader in RestSqlAction vs guarded PPL path

In `RestSqlAction` (lines 161-175), headers are set without checking whether they already exist:

```java
client.threadPool().getThreadContext()
    .putHeader(QuerySourceHeaders.QUERY_SOURCE_HEADER, "sql");
client.threadPool().getThreadContext()
    .putHeader(QuerySourceHeaders.QUERY_EXECUTION_ID_HEADER, UUID.randomUUID().toString());
```

OpenSearch's `ThreadContext.putHeader` throws `IllegalArgumentException` if the key is already present. The PPL transport action correctly guards with `if (threadContext.getHeader(...) == null)`. The SQL path should follow the same pattern.

## Deprecated Thread.currentThread().getId()

`QueryPhaseTracker` calls `Thread.currentThread().getId()` on lines 134 and 148 to pass to `getThreadAllocatedBytes(long)`. Since Java 19, `Thread.getId()` is deprecated in favour of `Thread.threadId()`. With the project targeting Java 21, this will produce deprecation warnings and should be:

```java
SUN_THREAD_MX.getThreadAllocatedBytes(Thread.currentThread().threadId())
```

## ThreadLocal and Log4j key lifecycle on exception paths

In `QueryService.executeWithCalcite`, the tracker is created at the top of the try block:

```java
QueryPhaseTracker tracker = QueryPhaseTracker.startOrRestore();
tracker.beginPhase("analyze");
```

If `StageErrorHandler.executeStage` throws (e.g. `CalciteUnsupportedException`), control jumps to the catch block which calls `executeWithLegacy`. That path creates a *new* tracker via `startOrRestore()`, which overwrites `CURRENT` — so the first tracker's ThreadLocal slot is released. However, the Log4j `ThreadContext` key `_sql_phase_tracker` from the initial `persist()` (set on the REST thread in `SQLService`) is never cleaned if the whole request fails before reaching `writePhaseHeader`. The fix: call `QueryPhaseTracker.clear()` in a `finally` block at the outermost scope of `executeWithCalcite`.

## FQN usage in PPLService and SQLService

Both `PPLService.java` and `SQLService.java` use the fully-qualified class name inline:

```java
org.opensearch.sql.common.utils.QueryPhaseTracker tracker =
    org.opensearch.sql.common.utils.QueryPhaseTracker.start();
```

Every other file in this diff uses an import statement. Add `import org.opensearch.sql.common.utils.QueryPhaseTracker;` to each file and use the short name.

## Serialization format delimiter assumption

The format `phase:wallNanos|cpu:cpuNanos|mem:memBytes` uses `:` and `|` as delimiters without escaping. Currently all phase names are hardcoded safe strings ("parse", "analyze", "plan", "total"), so this is not a bug today. Adding a defensive check in `beginPhase` (e.g. `assert !name.contains(":") && !name.contains("|")`) guards against future misuse.

</details>

<details>
<summary>File map</summary>

| File | Change |
|------|--------|
| `common/.../QueryPhaseTracker.java` | New. Thread-local phase tracker with wall/CPU/mem metrics and Log4j shuttle. |
| `common/.../QuerySourceHeaders.java` | New. Constants for x-query-source/original-query/execution-id/phases headers. |
| `common/.../QueryPhaseTrackerTest.java` | New. Unit tests covering lifecycle, serialization, cross-thread restore. |
| `core/.../QueryService.java` | Integrates tracker into `executeWithCalcite` and `executeWithLegacy` paths. |
| `legacy/.../RestSqlAction.java` | Stamps source-identification headers on the REST thread for SQL queries. |
| `opensearch/.../OpenSearchExecutionEngine.java` | `writePhaseHeader` writes serialized phases into thread-context header. |
| `plugin/.../SQLPlugin.java` | Registers query-insights headers via `getTaskHeaders`. |
| `plugin/.../TransportPPLQueryAction.java` | Stamps source-identification headers for PPL queries (guarded). |
| `ppl/.../PPLService.java` | Starts tracker and tracks parse phase for PPL. |
| `sql/.../SQLService.java` | Starts tracker and tracks parse phase for SQL. |

</details>
