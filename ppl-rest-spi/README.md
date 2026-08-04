# ppl-rest-spi

A generic way for any OpenSearch plugin to integrate with PPL: contribute your own read-only data
as a table queryable via `rest '<name>'`, without adding a new PPL command or grammar keyword and
without a compile dependency on the sql plugin's internals. Each contributed endpoint composes with
ordinary PPL (`| where`, `| stats`, `| head`), so a plugin extends the query surface without any
change to the language itself.

This module is intentionally thin: it depends only on OpenSearch core, and the row values it
exchanges are plain `java.lang` types (String / Number / Boolean / nested Map), so there is no
cross-classloader type-identity problem. Whatever type a handler returns, PPL surfaces it as a
string column, and a query casts the fields it needs.

## Example: `/_cluster/health`

The built-in `/_cluster/health` endpoint is implemented against this SPI exactly like an external
provider would. It declares a single `response` column and, at execution time, calls the
cluster-health transport action and serializes the whole response into that column:

```java
RestEndpointDefinition.builder()
    .name("/_cluster/health")
    .argSpec(ArgSpec.builder().arg("local", Set.of("true", "false")).build())
    .handler(ctx -> {
      ClusterHealthResponse health =
          ctx.client().admin().cluster().health(new ClusterHealthRequest()).actionGet();
      XContentBuilder json = XContentFactory.jsonBuilder();
      health.toXContent(json, ToXContent.EMPTY_PARAMS); // serialize the full response as-is
      return List.of(json.toString());
    })
    .build();
```

Querying it returns one row whose `response` column holds the full health JSON:

```
> rest '/_cluster/health'

response
--------------------------------------------------------------------------------------------
{"cluster_name":"opensearch-cluster","status":"green","timed_out":false,"number_of_nodes":1,
"number_of_data_nodes":1,"discovered_cluster_manager":true,"active_primary_shards":0,
"active_shards":0,"relocating_shards":0,"initializing_shards":0,"unassigned_shards":0,
"delayed_unassigned_shards":0,"number_of_pending_tasks":0,"number_of_in_flight_fetch":0,
"task_max_waiting_in_queue_millis":0,"active_shards_percent_as_number":100.0}
```

Pull individual fields downstream with `spath` (or `json_extract`), casting where a numeric type
is needed:

```
> rest '/_cluster/health' | spath input=response path=status output=status | fields status

status
------
green
```

```
> rest '/_cluster/health'
  | spath input=response path=number_of_nodes output=nodes
  | where cast(nodes as int) >= 1
  | fields nodes

nodes
-----
1
```

## Contract

| Type | Role |
|---|---|
| `RestEndpointProvider` | Your entry point: `List<RestEndpointDefinition> getEndpoints()`. |
| `RestEndpointDefinition` | One endpoint: `name()`, `argSpec()`, `handler()`. Build with `RestEndpointDefinition.builder()`. Every endpoint surfaces a single `response` string column. |
| `ArgSpec` | The query args the endpoint accepts and each arg's allowed value domain; an unknown or out-of-domain arg is rejected. `ArgSpec.NONE` accepts none. |
| `RestEndpointHandler` | `List<String> fetch(RestEndpointContext)`: each string is one row's `response` cell (typically a serialized JSON document). Runs at scan execution (not planning), so the scan is lazy and `EXPLAIN` is side-effect free. |
| `RestEndpointContext` | The validated `args()` plus an optional core `NodeClient` (`client()`) for a handler that issues its own read-only transport action. |

## Add an endpoint

1. Depend on the published `ppl-rest-spi` artifact `compileOnly` (the installed sql plugin
   provides it at runtime, so it is never bundled), and declare the sql plugin as an extended
   plugin so `loadExtensions` discovers your provider:

   ```gradle
   dependencies {
       compileOnly "org.opensearch.query:ppl-rest-spi:${sqlPluginVersion}"
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
               .argSpec(ArgSpec.builder().arg("verbose", Set.of("true", "false")).build())
               .handler(MyRestProvider::fetch)
               .build());
     }

     private static List<String> fetch(RestEndpointContext ctx) {
       // ctx.args() is already validated; ctx.client() is a NodeClient for transport calls (may be null).
       MyThing thing = readMyThing(ctx);                // your own read-only transport call
       XContentBuilder json = XContentFactory.jsonBuilder();
       thing.toXContent(json, ToXContent.EMPTY_PARAMS); // serialize the whole response
       return List.of(json.toString());
     }
   }
   ```

3. Register the provider as a service in
   `META-INF/services/org.opensearch.sql.spi.rest.RestEndpointProvider`, containing your class's
   fully-qualified name.

4. Add the endpoint name to the sql plugin's default allow list. Steps 1 to 3 only *register* the
   provider, which does not make the endpoint queryable on its own. A name is queryable only when it
   is also present in `plugins.ppl.rest.allowed_endpoints`. Enable your endpoint by submitting a
   change to the sql plugin that adds its name to the default list in `OpenSearchSettings.java`:

   ```java
   public static final Setting<List<String>> PPL_REST_ALLOWED_ENDPOINTS_SETTING =
       Setting.listSetting(
           Key.PPL_REST_ALLOWED_ENDPOINTS.getKeyValue(),
           List.of("/_cluster/health", "/_my/thing"), // add your endpoint name here
           Function.identity(),
           Setting.Property.NodeScope);
   ```

   Once that change is merged and released, the endpoint is enabled by default on every cluster
   running that sql version. The sql maintainers' review of this change is the gate that decides
   which endpoint names PPL is allowed to expose.

Then `rest '/_my/thing' verbose=true | spath input=response path=count output=count | where cast(count as int) > 0` composes like any scan; pull fields out of the `response` JSON with `spath` (or `json_extract`) and cast the ones you compute on.

## Rules and guarantees

- Endpoints are read-only. The handler produces rows; every value is surfaced as a string column,
  and a query extracts and casts the fields it needs (for example with `json_extract` or `spath`).
- `plugins.ppl.rest.allowed_endpoints` is the enable list, whose default is maintained in the sql
  plugin's `OpenSearchSettings.java`: a name is queryable only when it is both registered by a
  provider and listed there; anything else is rejected before any transport call. Listing a name no
  provider registered has no effect.
- Endpoint names are global across all providers, and `allowed_endpoints` does not resolve name
  collisions: it only enables names, it does not make two providers that claim the same name
  coexist. Collisions are handled separately, at registration time: a built-in name cannot be
  shadowed by an external provider (the built-in wins and the external duplicate is dropped with a
  logged warning), and if two external providers register the same name that name is disabled
  entirely (logged warning, and the node still starts). So even when every name is listed in
  `allowed_endpoints`, a name owned by two providers will not silently pick a winner.
- Redaction is the endpoint's own responsibility, applied to the response before it is streamed
  back. The framework surfaces exactly what the handler returns and adds no masking step, so there
  is no central redaction seam to implement and an endpoint with nothing sensitive does nothing.
  Because an endpoint returns a single `response` JSON column, redact the sensitive fields (IPs,
  hostnames, tokens) on the response object *before* serializing it into that column, so the masked
  value is what is streamed out:

  ```java
  .handler(ctx -> {
    MyStats stats = fetchStats(ctx);                  // your own read-only transport call
    stats.maskIp();                                   // redact on the object first
    XContentBuilder json = XContentFactory.jsonBuilder();
    stats.toXContent(json, ToXContent.EMPTY_PARAMS);  // then serialize the masked response
    return List.of(json.toString());
  })
  ```
