# PPL Settings  

## Introduction  

When OpenSearch bootstraps, PPL plugin will register a few settings in OpenSearch cluster settings. Most of the settings are able to change dynamically so you can control the behavior of PPL plugin without need to bounce your cluster.
## plugins.ppl.enabled

### Version

1.0

### Description  

You can disable SQL plugin to reject all coming requests.
1. The default value is true.  
2. This setting is node scope.  
3. This setting can be updated dynamically.  
  
Notes. Calls to _plugins/_ppl include index names in the request body, so they have the same access policy considerations as the bulk, mget, and msearch operations. if rest.action.multi.allow_explicit_index set to false, PPL plugin will be disabled.
### Example 1  

You can update the setting with a new value like this.
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X PUT localhost:9200/_plugins/_query/settings \
-d '{"transient" : {"plugins.ppl.enabled" : "false"}}'
```
  
Expected output:
  
```json
{
  "acknowledged": true,
  "persistent": {},
  "transient": {
    "plugins": {
      "ppl": {
        "enabled": "false"
      }
    }
  }
}

```
  
Note: the legacy settings of `opendistro.ppl.enabled` is deprecated, it will fallback to the new settings if you request an update with the legacy name.
### Example 2  

Query result after the setting updated is like:
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X POST localhost:9200/_plugins/_ppl \
-d '{"query": "source=my_prometheus"}'
```
  
Expected output:
  
```json
{
  "error": {
    "reason": "Invalid Query",
    "details": "Either plugins.ppl.enabled or rest.action.multi.allow_explicit_index setting is false",
    "type": "IllegalAccessException"
  },
  "status": 400
}

```
  
## plugins.ppl.query.timeout

### Version

3.4

### Description  

This setting controls the maximum execution time for PPL queries. When a query exceeds this timeout, it will be interrupted and return a timeout error.
1. The default value is 300s (5 minutes).  
2. This setting is node scope.  
3. This setting can be updated dynamically.  
  
### Example  

You can configure the query timeout:
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X PUT localhost:9200/_plugins/_query/settings \
-d '{"transient" : {"plugins.ppl.query.timeout" : "60s"}}'
```
  
Expected output:
  
```json
{
  "acknowledged": true,
  "persistent": {},
  "transient": {
    "plugins": {
      "ppl": {
        "query": {
          "timeout": "60s"
        }
      }
    }
  }
}
```
  
## plugins.query.async.max_retained_jobs

### Version

3.10

### Description

This setting limits how many asynchronous PPL queries a node retains for polling. A query is retained from the moment its submit returns an `id`, while it runs and after it finishes, until it is deleted with `DELETE /_plugins/_async_query/{id}` or its `keep_alive` expires; fetching its result with GET does not release it. Queries whose results are returned inline and synchronous queries are not counted. While the limit is reached, an asynchronous submit fails with `429`. A query that is still running when its wait ends and finds the limit reached is cancelled, and its submit fails with `429`.
1. The default value is 1000. The minimum is 1.
2. This setting is node scope.
3. This setting can be updated dynamically. A lowered limit applies to the next submit; retained queries are not removed.

### Example

```bash ignore
curl -sS -H 'Content-Type: application/json' \
-X PUT localhost:9200/_plugins/_query/settings \
-d '{"transient" : {"plugins.query.async.max_retained_jobs" : 100}}'
```

## plugins.query.async.batched_reduce_size

### Version

3.11

### Description

This setting controls how many shard results the coordinating node collects before it merges them, for an asynchronous PPL query whose aggregation runs entirely in OpenSearch (other than a `stats count()` alone, which uses the hit count). Each merge is a running result that the query returns while it is still running. A smaller value gives more frequent running results and takes more merges on the coordinating node. Synchronous queries and other asynchronous queries are not affected.
1. The default value is 5. A value below 2 turns running results of aggregations off, and the search uses OpenSearch's default.
2. This setting is node scope.
3. This setting can be updated dynamically. A new value applies to the next query.

### Example

```bash ignore
curl -sS -H 'Content-Type: application/json' \
-X PUT localhost:9200/_plugins/_query/settings \
-d '{"transient" : {"plugins.query.async.batched_reduce_size" : 10}}'
```

## plugins.query.memory_limit

### Version

1.0

### Description  

You can set heap memory usage limit for the query engine. When query running, it will detected whether the heap memory usage under the limit, if not, it will terminated the current query. The default value is: 85%
### Example  
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X PUT localhost:9200/_plugins/_query/settings \
-d '{"persistent" : {"plugins.query.memory_limit" : "80%"}}'
```
  
Expected output:
  
```json
{
  "acknowledged": true,
  "persistent": {
    "plugins": {
      "query": {
        "memory_limit": "80%"
      }
    }
  },
  "transient": {}
}

```
  
Note: the legacy settings of `opendistro.ppl.query.memory_limit` is deprecated, it will fallback to the new settings if you request an update with the legacy name.
## plugins.query.size_limit

### Version

1.0

### Description  

The size configures the maximum amount of rows to be fetched from PPL execution results. The default value is: 10000
### Example  

Change the size_limit to 1000
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X PUT localhost:9200/_plugins/_query/settings \
-d '{"persistent" : {"plugins.query.size_limit" : "1000"}}'
```
  
Expected output:
  
```json
{
  "acknowledged": true,
  "persistent": {
    "plugins": {
      "query": {
        "size_limit": "1000"
      }
    }
  },
  "transient": {}
}
```
  
Note: the legacy settings of `opendistro.query.size_limit` is deprecated, it will fallback to the new settings if you request an update with the legacy name.
## plugins.query.buckets

### Version

3.4

### Description  

This configuration indicates how many aggregation buckets will return in a single response. The default value equals to `plugins.query.size_limit`.
You can change the value to any value not greater than the maximum number of aggregation buckets allowed in a single response (`search.max_buckets`), here is an example
  
```bash ppl
curl -sS -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_query/settings -d '{
  "transient" : {
    "plugins.query.buckets" : 1000
  }
}'
```
  
Expected output:
  
```json
{
  "acknowledged": true,
  "persistent": {},
  "transient": {
    "plugins": {
      "query": {
        "buckets": "1000"
      }
    }
  }
}
```
  
### Limitations  

The number of aggregation buckets is fixed to `1000` in v2. `plugins.query.buckets` can only effect the number of aggregation buckets when calcite enabled.
## plugins.calcite.all_join_types.allowed

### Version

3.3

### Description  

Since 3.3.0, join types `inner`, `left`, `outer` (alias of `left`), `semi` and `anti` are supported by default. `right`, `full`, `cross` are performance sensitive join types which are disabled by default. Set config `plugins.calcite.all_join_types.allowed = true` to enable.
### Example  
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X PUT localhost:9200/_plugins/_query/settings \
-d '{"transient" : {"plugins.calcite.all_join_types.allowed" : "true"}}'
```
  
Expected output:
  
```json
{
  "acknowledged": true,
  "persistent": {},
  "transient": {
    "plugins": {
      "calcite": {
        "all_join_types": {
            "allowed": "true"
        }
      }
    }
  }
}
```
  
## plugins.ppl.syntax.legacy.preferred

### Version

3.3

### Description  

This configuration is introduced since 3.3.0 which is used to switch some behaviours in PPL syntax. The current default value is `true`.
The behaviours it controlled includes:
- The default value of argument `bucket_nullable` in `stats` command. Check [stats command](../cmd/stats.md) for details.
- The return value of `divide` and `/` operator. Check [expressions](../functions/expressions.md) for details.
- The default value of argument `usenull` in `top` and `rare` commands. Check [top command](../cmd/top.md) and [rare command](../cmd/rare.md) for details.
- The default value of argument `max` in `join` command. Check [join command](../cmd/join.md) for details.
  
### Example 1  

You can update the setting with a new value like this.
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X PUT localhost:9200/_plugins/_query/settings \
-d '{"transient" : {"plugins.ppl.syntax.legacy.preferred" : "false"}}'
```
  
Expected output:
  
```json
{
  "acknowledged": true,
  "persistent": {},
  "transient": {
    "plugins": {
      "ppl": {
        "syntax": {
          "legacy": {
            "preferred": "false"
          }
        }
      }
    }
  }
}

```
  
### Example 2  

Reset to default (true) by setting to null:
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X PUT localhost:9200/_plugins/_query/settings \
-d '{"transient" : {"plugins.ppl.syntax.legacy.preferred" : null}}'
```
  
Expected output:
  
```json
{
  "acknowledged": true,
  "persistent": {},
  "transient": {}
}
```
  
## plugins.ppl.values.max.limit

### Version

3.3

### Description  

This setting controls the maximum number of unique values that the `VALUES` aggregation function can return. When set to 0 (the default), there is no limit on the number of unique values returned. When set to a positive integer, the function will return at most that many unique values.
1. The default value is 0 (unlimited).  
2. This setting is node scope.  
3. This setting can be updated dynamically.  
  
The `VALUES` function collects all unique values from a field and returns them in lexicographical order. This setting helps manage memory usage by limiting the number of values collected.
### Example 1  

Set the limit to 1000 unique values:
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X PUT localhost:9200/_plugins/_query/settings \
-d '{"transient" : {"plugins.ppl.values.max.limit" : "1000"}}'
```
  
Expected output:
  
```json
{
  "acknowledged": true,
  "persistent": {},
  "transient": {
    "plugins": {
      "ppl": {
        "values": {
          "max": {
            "limit": "1000"
          }
        }
      }
    }
  }
}

```
  
### Example 2  

Set to 0 explicitly for unlimited values:
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X PUT localhost:9200/_plugins/_query/settings \
-d '{"transient" : {"plugins.ppl.values.max.limit" : "0"}}'
```
  
Expected output:
  
```json
{
  "acknowledged": true,
  "persistent": {},
  "transient": {
    "plugins": {
      "ppl": {
        "values": {
          "max": {
            "limit": "0"
          }
        }
      }
    }
  }
}


```
  
## plugins.ppl.subsearch.maxout

### Version

3.4

### Description

The size configures the maximum of rows to return from subsearch. The default value is: `10000`. A value of `0` indicates that the restriction is unlimited.

### Example  

Change the subsearch.maxout to unlimited
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X PUT localhost:9200/_plugins/_query/settings \
-d '{"persistent" : {"plugins.ppl.subsearch.maxout" : "0"}}'
```
  
Expected output:
  
```json
{
  "acknowledged": true,
  "persistent": {
    "plugins": {
      "ppl": {
        "subsearch": {
          "maxout": "0"
        }
      }
    }
  },
  "transient": {}
}
```
  
## plugins.ppl.join.subsearch_maxout

### Version

3.4

### Description

The size configures the maximum of rows from subsearch to join against. This configuration impacts `join` command. The default value is: `50000`. A value of `0` indicates that the restriction is unlimited.

### Example  

Change the join.subsearch_maxout to 5000
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X PUT localhost:9200/_plugins/_query/settings \
-d '{"persistent" : {"plugins.ppl.join.subsearch_maxout" : "5000"}}'
```
  
Expected output:
  
```json
{
  "acknowledged": true,
  "persistent": {
    "plugins": {
      "ppl": {
        "join": {
          "subsearch_maxout": "5000"
        }
      }
    }
  },
  "transient": {}
}
```
  

## search.insights.top_queries.ppl.enabled

### Version

3.11

### Description

This setting controls whether completed PPL queries are recorded into Query Insights Top N. When enabled, the PPL plugin reports each PPL query it runs through `_plugins/_ppl` to the Query Insights plugin when the query finishes: its query text, latency, CPU, memory, whether it failed, and, on a security-enabled cluster, the requesting user. It also tags the DSL searches the query runs, so Query Insights lists them as the query's sub-queries.

These are not recorded: `explain` (on the `_explain` endpoint or in the query text), `analyze`, asynchronous submissions, queries routed to the analytics engine, queries that fail to parse, and a PPL query that starts inside another one, whose searches belong to the outer query.

1. The default value is false (opt-in).
2. This setting is node scope, matching the rest of the `search.insights.top_queries.*` family.
3. It is dynamic: update it through the cluster settings API, as in the example below.

Note on query text and user identity: the record stores the query as it was submitted, including any literal values in it, the same way Query Insights stores the source of a DSL search. When a security plugin is installed, the record also includes the requesting user (username, roles, backend roles). Who can see these in Top N results is governed by the Query Insights plugin's own RBAC filter, `search.insights.top_queries.filter_by_mode` — default `none` (no per-user filtering), `username` (a user sees only their own queries; admins see all), or `backend_roles`. Set it to `username` or `backend_roles` if other users' queries and identities should not be visible to every Top N reader.

Note on CPU and memory: the PPL plugin reports the CPU time and the bytes allocated by the query's own engine threads, the same values `_tasks` shows for the PPL task. Query Insights adds the CPU and memory of the searches the query ran, so the figures in Top N cover the whole query. OpenSearch records these only while the cluster setting `task_resource_tracking.enabled` is on (the default). With it off, a PPL record reports 0 CPU and 0 memory.

Note: This setting is **registered by the Query Insights plugin** (it belongs to the `search.insights.top_queries.*` family), not by the PPL plugin. The PPL plugin reads its value to decide whether to report. It therefore only takes effect when the Query Insights plugin is installed; when Query Insights is absent the setting is unregistered and PPL reporting is a no-op.

### Example

With the Query Insights plugin installed, you can enable PPL recording like this:

```shell
curl -sS -H 'Content-Type: application/json' \
-X PUT localhost:9200/_cluster/settings \
-d '{"persistent" : {"search.insights.top_queries.ppl.enabled" : "true"}}'
```

This example is not executed as part of the documentation tests because the setting is registered by the Query Insights plugin, which is not installed in the PPL documentation test cluster.
