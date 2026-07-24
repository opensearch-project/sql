# Endpoint  

## Introduction  

To send query request to PPL plugin, you MUST use HTTP POST request. POST request doesn't have length limitation and allows for other parameters passed to plugin for other functionality such as prepared statement. And also the explain endpoint is used very often for query translation and troubleshooting.
## POST  

### Description  

You can send HTTP POST request to endpoint **/_plugins/_ppl** with your query in request body.
### Example  
  
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
  
## Explain  

### Description  

You can send HTTP explain request to endpoint **/_plugins/_ppl/_explain** with your query in request body to understand the execution plan for the PPL query. The explain endpoint is useful when user want to get insight how the query is executed in the engine.
### Description  

To translate your query, send it to explain endpoint. The explain output is OpenSearch domain specific language (DSL) in JSON format. You can just copy and paste it to your console to run it against OpenSearch directly.
Explain API supports various mode: `standard` (the default format), `simple`, `extended`, `cost`.
And the explain output could be shown in different formats: `json` (the default format), `yaml`
### Example 1 default (standard) format  

Explain query
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X POST localhost:9200/_plugins/_ppl/_explain \
-d '{"query" : "source=state_country | where age>30"}'
```
  
Expected output:
  
```json
{
  "calcite": {
    "logical": "LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE_LIMIT])\n  LogicalProject(name=[$0], country=[$1], state=[$2], month=[$3], year=[$4], age=[$5])\n    LogicalFilter(condition=[>($5, 30)])\n      CalciteLogicalIndexScan(table=[[OpenSearch, state_country]])\n",
    "physical": "CalciteEnumerableIndexScan(table=[[OpenSearch, state_country]], PushDownContext=[[PROJECT->[name, country, state, month, year, age], FILTER->>($5, 30), LIMIT->10000], OpenSearchRequestBuilder(sourceBuilder={\"from\":0,\"size\":10000,\"timeout\":\"1m\",\"query\":{\"range\":{\"age\":{\"from\":30,\"to\":null,\"include_lower\":false,\"include_upper\":true,\"boost\":1.0}}},\"_source\":{\"includes\":[\"name\",\"country\",\"state\",\"month\",\"year\",\"age\"]}}, requestedTotalSize=10000, pageSize=null, startFrom=0)])\n"
  }
}

```
  
### Example 2 simple mode  

Explain query
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X POST localhost:9200/_plugins/_ppl/_explain?mode=simple \
-d '{"query" : "source=state_country | where age>30"}'
```
  
Expected output:
  
```json
{
  "calcite": {
    "logical": "LogicalSystemLimit\n  LogicalProject\n    LogicalFilter\n      CalciteLogicalIndexScan\n"
  }
}
```
  
### Example 3 extended mode

Explain query
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X POST localhost:9200/_plugins/_ppl/_explain?mode=extended \
-d '{"query" : "source=state_country | head 10 | where age>30"}'
```
  
Expected output:
  
```json
{
  "calcite": {
    "logical": "LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE_LIMIT])\n  LogicalProject(name=[$0], country=[$1], state=[$2], month=[$3], year=[$4], age=[$5])\n    LogicalFilter(condition=[>($5, 30)])\n      LogicalSort(fetch=[10])\n        CalciteLogicalIndexScan(table=[[OpenSearch, state_country]])\n",
    "physical": "EnumerableLimit(fetch=[10000])\n  EnumerableCalc(expr#0..5=[{inputs}], expr#6=[30], expr#7=[>($t5, $t6)], proj#0..5=[{exprs}], $condition=[$t7])\n    CalciteEnumerableIndexScan(table=[[OpenSearch, state_country]], PushDownContext=[[PROJECT->[name, country, state, month, year, age], LIMIT->10], OpenSearchRequestBuilder(sourceBuilder={\"from\":0,\"size\":10,\"timeout\":\"1m\",\"_source\":{\"includes\":[\"name\",\"country\",\"state\",\"month\",\"year\",\"age\"]}}, requestedTotalSize=10, pageSize=null, startFrom=0)])\n",
    "extended": "public org.apache.calcite.linq4j.Enumerable bind(final org.apache.calcite.DataContext root) {\n  final org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan v1stashed = (org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan) root.get(\"v1stashed\");\n  final org.apache.calcite.linq4j.Enumerable _inputEnumerable = v1stashed.scan();\n  final org.apache.calcite.linq4j.AbstractEnumerable child = new org.apache.calcite.linq4j.AbstractEnumerable(){\n    public org.apache.calcite.linq4j.Enumerator enumerator() {\n      return new org.apache.calcite.linq4j.Enumerator(){\n          public final org.apache.calcite.linq4j.Enumerator inputEnumerator = _inputEnumerable.enumerator();\n          public void reset() {\n            inputEnumerator.reset();\n          }\n\n          public boolean moveNext() {\n            while (inputEnumerator.moveNext()) {\n              final Long input_value = (Long) ((Object[]) inputEnumerator.current())[5];\n              final Boolean binary_call_value = input_value == null ? null : Boolean.valueOf(input_value.longValue() > (long) 30);\n              if (binary_call_value != null && org.apache.calcite.runtime.SqlFunctions.toBoolean(binary_call_value)) {\n                return true;\n              }\n            }\n            return false;\n          }\n\n          public void close() {\n            inputEnumerator.close();\n          }\n\n          public Object current() {\n            final Object[] current = (Object[]) inputEnumerator.current();\n            final Object input_value = current[0];\n            final Object input_value0 = current[1];\n            final Object input_value1 = current[2];\n            final Object input_value2 = current[3];\n            final Object input_value3 = current[4];\n            final Object input_value4 = current[5];\n            return new Object[] {\n                input_value,\n                input_value0,\n                input_value1,\n                input_value2,\n                input_value3,\n                input_value4};\n          }\n\n        };\n    }\n\n  };\n  return child.take(10000);\n}\n\n\npublic Class getElementType() {\n  return java.lang.Object[].class;\n}\n\n\n"
  }
}
```
  
### Example 4 YAML format (experimental)  

   YAML explain output is an experimental feature and not intended for
   production use. The interface and output may change without notice.
Return Explain response in `yaml` format.
Explain query
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X POST localhost:9200/_plugins/_ppl/_explain?format=yaml \
-d '{"query" : "source=state_country | where age>30"}'
```
  
Expected output:
  
```yaml
calcite:
  logical: |
    LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE_LIMIT])
      LogicalProject(name=[$0], country=[$1], state=[$2], month=[$3], year=[$4], age=[$5])
        LogicalFilter(condition=[>($5, 30)])
          CalciteLogicalIndexScan(table=[[OpenSearch, state_country]])
  physical: |
    CalciteEnumerableIndexScan(table=[[OpenSearch, state_country]], PushDownContext=[[PROJECT->[name, country, state, month, year, age], FILTER->>($5, 30), LIMIT->10000], OpenSearchRequestBuilder(sourceBuilder={"from":0,"size":10000,"timeout":"1m","query":{"range":{"age":{"from":30,"to":null,"include_lower":false,"include_upper":true,"boost":1.0}}},"_source":{"includes":["name","country","state","month","year","age"]}}, requestedTotalSize=10000, pageSize=null, startFrom=0)])
```

## Profile (Experimental)

You can enable profiling on the PPL endpoint to capture per-stage timings in milliseconds. Profiling is returned only for regular query execution (not explain) and only when using the default `format=jdbc`.

### Example

```bash ppl ignore
curl -sS -H 'Content-Type: application/json' \
  -X POST localhost:9200/_plugins/_ppl \
  -d '{
        "profile": true,
        "query" : "source=accounts | fields firstname, lastname"
      }'
```

Expected output (trimmed):

```json
{
   "profile": {
      "summary": {
         "total_time_ms": 33.34
      },
      "phases": {
         "analyze": { "time_ms": 8.68 },
         "optimize": { "time_ms": 18.2 },
         "execute": { "time_ms": 4.87 },
         "format": { "time_ms": 0.05 }
      },
      "plan": {
         "node": "EnumerableCalc",
         "time_ms": 4.82,
         "rows": 2,
         "children": [
            { "node": "CalciteEnumerableIndexScan", "time_ms": 4.12, "rows": 2 }
         ]
      }
   }
}
```

### Notes

- Profile output is only returned when the query finishes successfully.
- Profiling runs only when Calcite is enabled.
- Plan node names use Calcite physical operator names (for example, `EnumerableCalc` or `CalciteEnumerableIndexScan`).
- Plan `time_ms` is inclusive of child operators and represents wall-clock time; overlapping work can make summed plan times exceed `summary.total_time_ms`.
- Scan nodes reflect operator wall-clock time; background prefetch can make scan time smaller than total request latency.

## Highlight

You can add a `highlight` parameter to the PPL request body to enable search-result highlighting. This parameter follows the same semantics as the [OpenSearch highlight API](https://docs.opensearch.org/latest/search-plugins/searching-data/highlight/). When enabled, the response includes a `_highlight` column in `schema` and `datarows` containing matching fragments with the specified tags. Each `_highlight` value in a datarow is an object whose keys are field names and whose values are arrays of highlight fragments for the corresponding row.

Two formats are supported:

### Simple array format

Pass a JSON array of field names or wildcards. Use `["*"]` to highlight all fields that match the search query.

```bash ppl ignore
curl -sS -H 'Content-Type: application/json' \
  -X POST localhost:9200/_plugins/_ppl \
  -d '{
        "query": "source=accounts \"Holmes\"",
        "highlight": ["*"]
      }'
```

### Object format (OpenSearch Dashboards)

Pass a JSON object with `fields`, `pre_tags`, `post_tags`, and `fragment_size`. This is the format used by OpenSearch Dashboards.

```bash ppl ignore
curl -sS -H 'Content-Type: application/json' \
  -X POST localhost:9200/_plugins/_ppl \
  -d '{
        "query": "source=accounts \"Holmes\"",
        "highlight": {
          "pre_tags": ["@opensearch-dashboards-highlighted-field@"],
          "post_tags": ["@/opensearch-dashboards-highlighted-field@"],
          "fields": {"*": {}},
          "fragment_size": 2147483647
        }
      }'
```

Expected output (trimmed):

```json
{
  "schema": [
    { "name": "account_number", "type": "bigint" },
    { "name": "firstname", "type": "string" },
    { "name": "lastname", "type": "string" },
    { "name": "_highlight", "type": "struct" }
  ],
  "datarows": [
    [578, "Holmes", "Mcknight", {
      "firstname": ["@opensearch-dashboards-highlighted-field@Holmes@/opensearch-dashboards-highlighted-field@"],
      "firstname.keyword": ["@opensearch-dashboards-highlighted-field@Holmes@/opensearch-dashboards-highlighted-field@"]
    }],
    [828, "Blanche", "Holmes", {
      "lastname": ["@opensearch-dashboards-highlighted-field@Holmes@/opensearch-dashboards-highlighted-field@"],
      "lastname.keyword": ["@opensearch-dashboards-highlighted-field@Holmes@/opensearch-dashboards-highlighted-field@"]
    }],
    [1, "Amber", "Duke", {
      "address": ["880 @opensearch-dashboards-highlighted-field@Holmes@/opensearch-dashboards-highlighted-field@ Lane"]
    }]
  ],
  "total": 3,
  "size": 3
}
```

### Parameters (object format)

| Parameter       | Type            | Required | Description                                                                                                  |
|-----------------|-----------------|----------|--------------------------------------------------------------------------------------------------------------|
| `fields`        | Object          | Yes      | An object whose keys are field names or wildcards to highlight. Each value is an object of per-field options (see Notes). Use `{}` for defaults. Example: `{"*": {}}` or `{"title": {"fragment_size": 200}}`. |
| `pre_tags`      | Array of string | No       | Tags inserted before highlighted tokens. Defaults to `<em>`.                                                 |
| `post_tags`     | Array of string | No       | Tags inserted after highlighted tokens. Defaults to `</em>`.                                                 |
| `fragment_size` | Integer         | No       | Maximum character size of a highlight fragment. Defaults to `100`.                                            |

### Limits

| Constraint | Max value | Description |
|---|---|---|
| Highlight fields | 100 | Maximum number of fields in the `highlight` array or `fields` object. |
| Pre/post tags | 10 | Maximum number of entries in each `pre_tags` or `post_tags` array. |
| Fragment size | > 0 | Must be a positive integer. |

Exceeding these limits returns an error.

### Notes

- Highlighting requires a search term in the PPL statement (e.g. `source=accounts "Holmes"`). Without a search term (e.g. just `source=accounts`), the `_highlight` values in datarows will be empty objects.
- The `_highlight` column appears in `schema` and `datarows` as a regular column. Each `_highlight` value is an object whose keys are field names and whose values are arrays of highlight fragments.
- In the simple array format, `["*"]` highlights all fields. Specific field names like `["firstname", "lastname"]` scope highlighting to those fields only.
- In the object format, each key in the `fields` object is a field name or wildcard. Each value is an object of per-field highlight options. Supported per-field options: `fragment_size`, `number_of_fragments`, `type` (`plain`, `unified`, `fvh`), `pre_tags`, `post_tags`, `require_field_match`, `no_match_size`, `order`. Use `{}` for defaults. Example: `{"title": {"fragment_size": 200}, "body": {"type": "plain"}}`.
- Highlights may include fields that are not explicitly projected in the other columns. For example, using `{"*": {}}` highlights all fields that matched the search query, including fields not selected by `| fields`. In the example above, the `address` field appears in `_highlight` because it contains a match ("880 Holmes Lane") even though only `account_number`, `firstname`, and `lastname` are projected as separate columns.

## Grammar (Experimental)

### Description

You can send an HTTP GET request to endpoint **/_plugins/_ppl/_grammar** to fetch serialized PPL grammar metadata used by autocomplete clients.

### Example

```bash ppl ignore
curl -sS -X GET localhost:9200/_plugins/_ppl/_grammar
```

Expected output (trimmed):

```json
{
  "bundleVersion": "1.0",
  "antlrVersion": "4.13.2",
  "grammarHash": "sha256:...",
  "startRuleIndex": 0,
  "lexerSerializedATN": [4, ...],
  "parserSerializedATN": [4, ...],
  "lexerRuleNames": ["SEARCH", "..."],
  "parserRuleNames": ["root", "..."],
  "literalNames": [null, "'SEARCH'", "..."],
  "symbolicNames": [null, "SEARCH", "..."],
  "tokenDictionary": {"PIPE": 196, "...": 0},
  "ignoredTokens": [472, 473, "..."],
  "rulesToVisit": [200, 201, "..."]
}
```

## Include Metadata

### Description

You can add an `include_metadata` parameter to control whether metadata fields (such as `_id`, `_index`, `_score`, etc.) are included in wildcard field selections. This parameter can be specified either as a URL parameter or in the request body JSON. It only affects implicit field selections using `fields *` and does not impact explicit field selections.

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `include_metadata` | boolean | `false` | When `true`, metadata fields are included in wildcard field selections (`fields *`). When `false` (default), metadata fields are excluded from wildcard selections. Can be specified as a URL parameter or in the request body JSON. |

### Behavior

- **Default behavior (`include_metadata=false`)**: Wildcard field selections (`fields *`) exclude metadata fields like `_id`, `_index`, `_score`, etc.
- **With `include_metadata=true`**: Wildcard field selections include both regular data fields and metadata fields.
- **Explicit field selection**: The parameter does not affect explicit field selections. If you explicitly specify `fields _id, firstname`, the `_id` field will be included regardless of the `include_metadata` setting.
- **Aggregations**: The parameter does not affect aggregation results, which never include metadata fields.

### Example 1: Include metadata fields (URL parameter)

```bash ppl ignore
curl -sS -H 'Content-Type: application/json' \
-X POST "localhost:9200/_plugins/_ppl?include_metadata=true" \
-d '{"query" : "source=accounts | fields * | head 1"}'
```

### Example 2: Include metadata fields (request body)

```bash ppl ignore
curl -sS -H 'Content-Type: application/json' \
-X POST localhost:9200/_plugins/_ppl \
-d '{"query" : "source=accounts | fields * | head 1", "include_metadata": true}'
```

Expected output (metadata fields included):

```json
{
  "schema": [
    {
      "name": "account_number",
      "type": "bigint"
    },
    {
      "name": "firstname",
      "type": "string"
    },
    {
      "name": "address",
      "type": "string"
    },
    {
      "name": "balance",
      "type": "bigint"
    },
    {
      "name": "gender",
      "type": "string"
    },
    {
      "name": "city",
      "type": "string"
    },
    {
      "name": "employer",
      "type": "string"
    },
    {
      "name": "state",
      "type": "string"
    },
    {
      "name": "age",
      "type": "bigint"
    },
    {
      "name": "email",
      "type": "string"
    },
    {
      "name": "lastname",
      "type": "string"
    },
    {
      "name": "_id",
      "type": "string"
    },
    {
      "name": "_index",
      "type": "string"
    },
    {
      "name": "_score",
      "type": "float"
    },
    {
      "name": "_maxscore",
      "type": "float"
    },
    {
      "name": "_sort",
      "type": "bigint"
    },
    {
      "name": "_routing",
      "type": "string"
    }
  ],
  "datarows": [
    [
      1,
      "Amber",
      "880 Holmes Lane",
      39225,
      "M",
      "Brogan",
      "Pyrami",
      "IL",
      32,
      "amberduke@pyrami.com",
      "Duke",
      "pf4NQp4BHgpoAGEqpZkR",
      "accounts",
      1.0,
      1.0,
      -2,
      "[zgCLGDnFRDe3VQL4L9lNVw][accounts][0]"
    ]
  ],
  "total": 1,
  "size": 1
}
```

### Example 3: Explicit field selection (unaffected by include_metadata)

```bash ppl ignore
curl -sS -H 'Content-Type: application/json' \
-X POST localhost:9200/_plugins/_ppl?include_metadata=true \
-d '{"query" : "source=accounts | fields firstname, lastname | head 1"}'
```

Expected output (only explicitly selected fields):

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
    ]
  ],
  "total": 1,
  "size": 1
}
```

### Example 4: Explicit metadata field selection

```bash ppl ignore
curl -sS -H 'Content-Type: application/json' \
-X POST "localhost:9200/_plugins/_ppl?include_metadata=true" \
-d '{"query" : "source=accounts | fields firstname, _id | head 1"}'
```

Expected output (explicitly selected metadata field included):

```json
{
  "schema": [
    {
      "name": "firstname",
      "type": "string"
    },
    {
      "name": "_id",
      "type": "string"
    }
  ],
  "datarows": [
    [
      "Amber",
      "pf4NQp4BHgpoAGEqpZkR"
    ]
  ],
  "total": 1,
  "size": 1
}
```

### Notes

- The `include_metadata` parameter only affects wildcard field selections (`fields *`).
- **Current limitation**: Explicit metadata field selection (e.g., `fields firstname, _id`) currently requires `include_metadata=true` to work properly.
- Metadata fields include system fields like `_id`, `_index`, `_score`, `_maxscore`, `_sort`, `_routing`, etc.
- When using search queries with scoring (e.g., `source=accounts "Holmes"`), the `_score` field becomes available and will be included when `include_metadata=true`.
- Aggregation queries are not affected by this parameter, as they never include metadata fields in their results.
- The parameter can be specified as a URL parameter (`?include_metadata=true`) or in the request body JSON (`{"include_metadata": true}`).
- When both URL parameter and request body specify the parameter, the request body takes precedence.
