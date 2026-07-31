# ppl-rest-spi

A generic extension point that lets any OpenSearch plugin surface its own read-only, fixed-schema
data as a PPL table, queryable as `rest '<name>'`. A plugin integrates with PPL this way without a
new command or grammar keyword and without a compile dependency on the sql plugin's internals; each
endpoint then composes with ordinary PPL (`| where`, `| stats`, `| head`).

This module is intentionally thin: it depends only on OpenSearch core, and the row values it
exchanges are plain `java.lang` types (String / Number / Boolean / nested Map), so there is no
cross-classloader type-identity problem.

## Example

The built-in `/_cluster/health` endpoint is just one client of this contract:

```
rest '/_cluster/health' | where status != 'green'
```

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
   dependencies {
       // Published artifact, provided at runtime by the installed sql plugin, so compileOnly (not bundled).
       compileOnly "org.opensearch.plugin:ppl-rest-spi:${version}"
   }
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

- Endpoints are read-only. The handler produces rows; the ppl side coerces each value to the
  declared `ColumnType` and rejects an uncoercible value with a clear error.
- `plugins.ppl.rest.allowed_endpoints` is the operator's explicit enable list: a name is queryable
  only when it is both registered by a provider and listed there; anything else is rejected before
  any transport call. Listing a name no provider registered has no effect.
- Endpoint names are global across providers; a duplicate name is a registration-time concern,
  independent of `allowed_endpoints`.
- Redaction is a platform policy, not an endpoint concern. Sensitive values are masked centrally at
  the row-shaping choke point, uniformly for every endpoint, so an endpoint author never
  re-implements masking and cannot accidentally bypass it. The OSS default is a no-op
  (`Redactor.NONE`). A deployment can install one masking policy that applies to all endpoints. For
  example, masking an `ip` column the same way wherever it appears:

  ```java
  // OSS default: Redactor.NONE. Rows pass through unchanged.
  // A deployment can supply a single Redactor, applied once per row at the choke point:
  Redactor mask = (endpoint, row) -> {
    row.computeIfPresent("ip", (col, val) -> maskIp(val));
    return row;
  };
  ```
