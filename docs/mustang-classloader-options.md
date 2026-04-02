# Mustang Plugin Integration: Classloader Architecture Options

## Problem Statement

The SQL plugin needs to communicate with the analytics-engine plugin to execute queries against Parquet-backed indices. The SQL plugin generates a Calcite `RelNode` (logical query plan), and the analytics-engine executes it via DataFusion.

Both plugins use Apache Calcite. Calcite uses Janino (a Java compiler) to compile generated Java code at runtime. Janino resolves classes using the classloader of the Calcite class that invokes it — specifically `EnumerableInterpretable.class.getClassLoader()` and `RexExecutable.class.getClassLoader()`.

### The Classloader Issue

With OpenSearch's `extendedPlugins` mechanism, one plugin becomes the **parent classloader** of the other. In a parent-child classloader hierarchy:
- The **child** can see parent classes
- The **parent** cannot see child classes

When analytics-engine is the parent (owns Calcite), Janino uses the parent's classloader. This classloader cannot see SQL plugin classes (child). Any generated code that references SQL plugin classes — UDFs, custom operators, table scan implementations — fails with `CompileException: Cannot determine simple type name "org"`.

### Affected Code Paths

Janino compilation is hardcoded in 4 places inside Calcite:

| Code Path | Purpose | Impact When Broken |
|-----------|---------|-------------------|
| `EnumerableInterpretable.getBindable()` | Compile Enumerable plans (UNION ALL, cross-cluster) | Queries crash with CompileException |
| `RexExecutable` in `CalciteScriptEngine` | Compile pushdown filter/expression scripts | Pushdown scripts crash on the OpenSearch shard |
| `RexExecutable` in `RexExecutorImpl.reduce()` | Constant fold expressions during planning | UDFs like `TIMESTAMP('...')` can't be folded to literals |
| `RexExecutable` in `Prepare.optimize()` | Overwrites custom executor before optimization | Sarg range creation fails, filters fall back to script queries |

Fixing these requires patching each code path individually, and each fix creates new edge cases. After extensive investigation, we identified that this approach is not sustainable.

---

## Option C: Reverse the Parent-Child Relationship

### Summary

Make the SQL plugin the parent classloader and analytics-engine the child, instead of the current arrangement.

### How It Works

```
opensearch-sql (PARENT classloader)
├── calcite-core, janino, commons-*, guava, etc.
├── core, ppl, sql, opensearch modules
│
└── analytics-engine (CHILD classloader)
    ├── analytics-engine.jar
    ├── analytics-framework.jar
    └── DataFusion JNI bindings
```

Analytics-engine declares `extendedPlugins = ['opensearch-sql']` in its `plugin-descriptor.properties`. The SQL plugin loads first and provides Calcite to the analytics-engine via the parent classloader.

### Why It Solves the Problem

Janino uses `SomeCalciteClass.class.getClassLoader()` to resolve classes. Since Calcite is in the SQL plugin (parent), this returns the SQL plugin's classloader, which has BOTH Calcite AND SQL plugin classes. Janino can resolve everything — zero patches needed.

### Changes Required

**Analytics-engine side (OpenSearch sandbox repo):**
```properties
# plugin-descriptor.properties
extendedPlugins=opensearch-sql
```
- Remove Calcite JARs from analytics-engine ZIP (inherited from parent)

**SQL plugin side (this repo):**
- Remove `extendedPlugins = ['analytics-engine']` from `plugin/build.gradle`
- Remove all `bundlePlugin` excludes (SQL plugin bundles its own Calcite)
- Remove ALL classloader fixes (CalciteToolsHelper, CalciteScriptEngine, etc.)
- Test clusters: load SQL plugin first, analytics-engine second

### Pros
- Zero Calcite/Janino patches — all 4 code paths work automatically
- Minimal code changes — mostly build config
- We can implement entirely ourselves (we have access to both repos)
- Immediate fix, no architectural redesign needed

### Cons
- Semantically unusual: SQL plugin (consumer) is parent of analytics-engine (core infrastructure)
- Hard dependency: analytics-engine cannot start without SQL plugin installed
- If analytics-engine wants to serve other consumers (not just SQL plugin), each would need to be a parent
- `QueryPlanExecutor` interface must be visible from the parent (SQL plugin) classloader

### When to Choose This
- Need a fix immediately with minimal effort
- Acceptable that analytics-engine depends on SQL plugin at runtime
- Analytics-engine won't have other consumers besides SQL plugin

---

## Option D: Transport Actions (Serialized Communication)

### Summary

Instead of sharing Java objects (RelNode) between plugins via a shared classloader, serialize the query plan to JSON, send it via an OpenSearch transport action, and deserialize on the other side.

### How It Works

```
SQL Plugin (own classloader + own Calcite)
┌────────────────────────────────────────┐
│ 1. Parse PPL query                     │
│ 2. Build RelNode plan                  │
│ 3. Serialize RelNode to JSON string    │
│    (using Calcite's RelJsonWriter)     │
│ 4. Send JSON via transport action      │──── transport action ────┐
│                                        │                          │
│ 7. Receive serialized result rows      │                          │
│ 8. Deserialize to QueryResponse        │                          │
│ 9. Format and return to user           │                          │
└────────────────────────────────────────┘                          │
                                                                    │
Analytics Engine (own classloader + own Calcite)                    │
┌────────────────────────────────────────┐                          │
│ 5. Receive JSON plan                   │←─────────────────────────┘
│    Deserialize to RelNode              │
│    (using Calcite's RelJsonReader)     │
│ 6. Execute via DataFusion              │
│    Serialize result rows               │
│    Send response                       │
└────────────────────────────────────────┘
```

### Why It Solves the Problem

Each plugin has its own isolated classloader with its own Calcite JARs. There is no `extendedPlugins` declaration — no parent-child relationship. Janino in each plugin uses that plugin's own classloader, which contains everything it needs.

The only data crossing the plugin boundary is a JSON string (the serialized plan) and byte arrays (the result rows). No shared Java objects means no shared classloader needed.

### Changes Required

**SQL plugin side:**

1. **Define transport action types** (small):
   ```java
   // New request/response classes
   public class AnalyticsQueryRequest extends TransportRequest {
       String jsonPlan;        // RelNode serialized as JSON
       String indexName;       // Target index
       boolean explain;        // Explain mode flag
   }

   public class AnalyticsQueryResponse extends TransportResponse {
       byte[] schema;          // Column names and types
       byte[] rows;            // Serialized result rows
   }
   ```

2. **Serialize RelNode to JSON** (small — Calcite provides this):
   ```java
   // Calcite's built-in JSON serialization
   RelJsonWriter writer = new RelJsonWriter();
   relNode.explain(writer);
   String jsonPlan = writer.asString();
   ```

3. **Modify AnalyticsExecutionEngine** (small):
   ```java
   // Before (direct Java call — needs shared classloader):
   Iterable<Object[]> rows = planExecutor.execute(relNode);

   // After (transport action — no shared classloader):
   String jsonPlan = RelJsonWriter.serialize(relNode);
   AnalyticsQueryResponse response = client.execute(
       AnalyticsQueryAction.INSTANCE,
       new AnalyticsQueryRequest(jsonPlan, indexName));
   Iterable<Object[]> rows = deserializeRows(response.rows);
   ```

4. **Create stub transport action handler for testing** (small):
   ```java
   // Replaces StubQueryPlanExecutor — same canned data, different transport
   public class StubAnalyticsTransportAction
       extends HandledTransportAction<AnalyticsQueryRequest, AnalyticsQueryResponse> {
       @Override
       protected void doExecute(...) {
           // Return canned rows for parquet_logs/parquet_metrics
       }
   }
   ```

5. **Remove classloader workarounds** (cleanup):
   - Remove `extendedPlugins` and `bundlePlugin` excludes
   - Remove ALL Janino classloader fixes
   - Remove analytics-engine from test clusters
   - SQL plugin bundles its own Calcite (no exclusions)

6. **Thin shared interface JAR** (small):
   - `analytics-framework.jar` contains only transport action types
   - No Calcite classes in the interface — just `String` and `byte[]`
   - Both plugins depend on this JAR at compile time

**Analytics-engine side (future, requires their team):**

1. Register transport action handler
2. Receive JSON plan string
3. Deserialize to RelNode (using their own Calcite + `RelJsonReader`)
4. Execute via DataFusion
5. Serialize and return results

### Implementation Phases

**Phase 1 (we do now, independently):**
- Define transport action contract
- Implement SQL plugin sender side
- Create stub handler for testing
- Ship with all existing ITs passing

**Phase 2 (analytics team, when ready):**
- Implement real transport action handler in analytics-engine
- Handle plan deserialization and DataFusion execution
- Integration testing

### Pros
- Clean architecture — each plugin fully independent
- No classloader issues at all — no `extendedPlugins`
- Each plugin bundles and manages its own dependencies
- Analytics-engine can serve multiple consumers
- No Calcite version coupling between plugins (each can upgrade independently)
- Aligns with microservice-style plugin isolation

### Cons
- Serialization/deserialization overhead (RelNode → JSON → RelNode)
- More code to write than Option C
- RelNode JSON serialization may not handle all custom operators — needs validation
- Analytics team must implement the receiver (but we use stubs until then)
- Potential schema/type mapping issues across the serialization boundary

### When to Choose This
- Want a clean long-term architecture
- Both plugins need to evolve independently
- Analytics-engine may have multiple consumers
- Acceptable to coordinate with analytics team for Phase 2

---

## Comparison Summary

| Aspect | Option C (Reverse Parent) | Option D (Transport Actions) |
|--------|--------------------------|------------------------------|
| Effort to implement | Small (config change) | Medium (1-2 weeks) |
| Classloader patches needed | None | None |
| Plugin independence | analytics-engine depends on SQL plugin | Fully independent |
| Calcite version coupling | Shared (must match) | Independent (each owns its own) |
| Runtime overhead | None (direct Java calls) | Serialization cost |
| Analytics team coordination | Minimal (change plugin descriptor) | Required for Phase 2 |
| Can we do it ourselves? | Yes | Phase 1 yes, Phase 2 needs them |
| Long-term maintainability | Moderate | Best |
| Risk of future classloader issues | Low (but possible if roles change) | None |

## Recommendation

**Short-term**: Option C is the fastest path to unblock this PR. It requires changing one line in analytics-engine's plugin descriptor and removing our classloader hacks.

**Long-term**: Option D is the right architecture. It eliminates classloader coupling entirely and allows both plugins to evolve independently. We recommend planning Option D as the follow-up work after Option C ships.

**Pragmatic path**: Ship Option C now to unblock the PR, then migrate to Option D before GA release. Option C → D migration is straightforward since the SQL plugin side changes are isolated in `AnalyticsExecutionEngine`.
