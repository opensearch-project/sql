# PPL Settings  

## Introduction  

When OpenSearch bootstraps, PPL plugin will register a few settings in OpenSearch cluster settings. Most of the settings are able to change dynamically so you can control the behavior of PPL plugin without need to bounce your cluster.
## plugins.ppl.enabled  

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
  
## plugins.query.memory_limit  

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

3.4.0
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

### Description  

The size configures the maximum of rows to return from subsearch. The default value is: `10000`. A value of `0` indicates that the restriction is unlimited.
### Version  

3.4.0
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

### Description  

The size configures the maximum of rows from subsearch to join against. This configuration impacts `join` command. The default value is: `50000`. A value of `0` indicates that the restriction is unlimited.
### Version  

3.4.0
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
  