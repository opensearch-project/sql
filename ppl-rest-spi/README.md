# ppl-rest-spi

The extension point for the PPL `rest` command. Any OpenSearch plugin can contribute read-only,
fixed-schema management endpoints that become queryable as `rest '<name>'`, without touching the
PPL grammar and without depending on the sql plugin's internals. The built-in `/_cluster/health`
endpoint is just one client of this same contract.

This module is intentionally thin: it depends only on OpenSearch core, and the row values it
exchanges are plain `java.lang` types (String / Number / Boolean / nested Map), so there is no
cross-classloader type-identity problem.

## Contract

| Type | Role |
|---|---|
| `RestEndpointProvider` | Your entry point: `List<RestEndpointDefinition> getEndpoints()`. |
| `RestEndpointDefinition` | One endpoint: `name()`, `schema()`, `argSpec()`, `handler()`. Build with `RestEndpointDefinition.builder()`. |
| `Column` / `ColumnType` | A fixed output column. `ColumnType` is `STRING`, `INTEGER`, `LONG`, `DOUBLE`, or `BOOLEAN`. The schema is pinned before execution. |
| `ArgSpec` | The query args the endpoint accepts and each arg's allowed value domain; an unknown or out-of-domain arg is rejected. `ArgSpec.NONE` accepts none. |
| `RestEndpointHandler` | `List<Map<String, Object>> fetch(RestEndpointContext)`, run at scan execution (not planning), so the scan is lazy and `EXPLAIN` is side-effect free. |
| `RestEndpointContext` | The validated `args()` plus an optional core `NodeClient` (`client()`) for a handler that issues its own read-only transport action. |

## Add an endpoint

1. Depend on this module `compileOnly`, and declare the sql plugin as an extended plugin so
   `loadExtensions` discovers your provider:

   ```gradle
   dependencies { compileOnly project(':ppl-rest-spi') }   // or the published artifact
   opensearchplugin { extendedPlugins = ['opensearch-sql'] }
   ```

2. Implement `RestEndpointProvider`:

   ```java
   public final class MyRestProvider implements RestEndpointProvider {
     @Override
     public List<RestEndpointDefinition> getEndpoints() {
       return List.of(
           RestEndpointDefinition.builder()
               .name("/_my/thing")
               .schema(List.of(
                   Column.of("name", ColumnType.STRING),
                   Column.of("count", ColumnType.LONG)))
               .argSpec(ArgSpec.builder().arg("verbose", Set.of("true", "false")).build())
               .handler(MyRestProvider::fetch)
               .build());
     }

     private static List<Map<String, Object>> fetch(RestEndpointContext ctx) {
       // ctx.args() is already validated; ctx.client() is a NodeClient for transport calls (may be null).
       return List.of(Map.of("name", "example", "count", 1L));
     }
   }
   ```

3. Register the provider as a service in
   `META-INF/services/org.opensearch.sql.spi.rest.RestEndpointProvider`, containing your class's
   fully-qualified name.

4. Enable the endpoint on the cluster. `plugins.ppl.rest.allowed_endpoints` is a node-level setting
   listing the endpoint names a deployment permits; it defaults to `["/_cluster/health"]`. Add your
   name to run it:

   ```yaml
   plugins.ppl.rest.allowed_endpoints: ["/_cluster/health", "/_my/thing"]
   ```

Then `rest '/_my/thing' verbose=true | where count > 0 | stats sum(count)` composes like any scan.

## Rules and guarantees

- Endpoints are read-only. The handler produces rows; the sql side coerces each value to the
  declared `ColumnType` and rejects an uncoercible value with a clear error.
- Endpoint names are global. If two providers register the same name, the first registration wins
  and the duplicate is ignored with a logged warning.
- An endpoint must be both registered by a provider and present in `allowed_endpoints`; anything
  else is rejected before any transport call.
- Redaction of sensitive values is applied centrally at the row-shaping choke point; a provider
  does not implement it.
