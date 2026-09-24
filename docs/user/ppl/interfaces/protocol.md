# Protocol  

## Introduction  

For the protocol, PPL endpoint provides response formats in the JDBC format. JDBC format is widely used because it provides schema information and more functionality such as pagination. Besides JDBC driver, various clients can benefit from the detailed and well formatted response.
## Request/Response Format  

### Description  

The body of HTTP POST request can take PPL query.
### Example 1  
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X POST localhost:9200/_plugins/_ppl \
-d '{"query" : "source=accounts | fields firstname, lastname"}'
```
  
Expected output:
  
```json
{
  "schema": [
    {
      "name": "firstname",
      "type": "string"
    },
    {
      "name": "lastname",
      "type": "string"
    }
  ],
  "datarows": [
    [
      "Amber",
      "Duke"
    ],
    [
      "Hattie",
      "Bond"
    ],
    [
      "Nanette",
      "Bates"
    ],
    [
      "Dale",
      "Adams"
    ]
  ],
  "total": 4,
  "size": 4
}
```

## Warnings

### Description

A successful response carries a `warnings` array when the result is correct but incomplete, so a
consumer can tell an undercounted answer from a whole one. The field is absent when there is nothing
to report, and only the JSON format carries it -- CSV, raw and visualization responses have no
warning channel. Each entry has a machine-readable `type`, a one-line `message`, and a longer
`detail`:

| `type` | Meaning |
|---|---|
| `PARTIAL_RESULT` | The engine narrowed the query to a subset of the matched indices, e.g. an aggregation whose group-by field is not aggregatable in every index of a wildcard pattern. Controlled by `plugins.query.partial_result.on_mapping_conflict.enabled`. |
| `PARTIAL_RESULT_SHARD_FAILURE` | The search covered only the shards that responded: a shard failed, a shard had no available copy, or the search timed out. OpenSearch returns such a search with HTTP 200 because `search.default_allow_partial_results` defaults to `true`; set that to `false` to have these searches fail instead. |

### Example

A count whose search reached only three of its four shards:

```json
{
  "schema": [
    {
      "name": "n",
      "type": "bigint"
    }
  ],
  "datarows": [
    [
      750
    ]
  ],
  "total": 1,
  "size": 1,
  "warnings": [
    {
      "type": "PARTIAL_RESULT_SHARD_FAILURE",
      "message": "Results are partial: 1 of 4 shards did not return data.",
      "detail": "Rows and aggregate values from the shards that did not respond are missing, so counts may be undercounted. No copy of those shards was available -- a node may be down or a shard unassigned. Retry the query, or set search.default_allow_partial_results to false so such searches fail instead of returning a subset."
    }
  ]
}
```

## JDBC Format  

### Description  

By default the plugin return JDBC format. JDBC format is provided for JDBC driver and client side that needs both schema and result set well formatted.
### Example 1  

Here is an example for normal response. The `schema` includes field name and its type and `datarows` includes the result set.
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X POST localhost:9200/_plugins/_ppl \
-d '{"query" : "source=accounts | fields firstname, lastname"}'
```
  
Expected output:
  
```json
{
  "schema": [
    {
      "name": "firstname",
      "type": "string"
    },
    {
      "name": "lastname",
      "type": "string"
    }
  ],
  "datarows": [
    [
      "Amber",
      "Duke"
    ],
    [
      "Hattie",
      "Bond"
    ],
    [
      "Nanette",
      "Bates"
    ],
    [
      "Dale",
      "Adams"
    ]
  ],
  "total": 4,
  "size": 4
}
```
  
### Example 2  

If any error occurred, error message and the cause will be returned instead.
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X POST localhost:9200/_plugins/_ppl \
-d '{"query" : "source=unknown | fields firstname, lastname"}'
```
  
Expected output:
  
```json
{
  "error": {
    "context": {
      "stage": "analyzing",
      "index_name": "unknown",
      "stage_description": "Parsing and validating the query"
    },
    "reason": "no such index [unknown]",
    "details": "no such index [unknown]",
    "location": [
      "while preparing and validating the query plan",
      "while fetching index mappings"
    ],
    "code": "INDEX_NOT_FOUND",
    "type": "IndexNotFoundException"
  },
  "status": 404
}
```
  