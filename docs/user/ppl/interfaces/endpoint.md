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
  
## Highlight

### Description

You can include an optional `highlight` object in the request body to request search result highlighting. When a PPL query contains a full-text search (e.g., `search source=logs "error"`), OpenSearch identifies where the search terms appear in document fields and returns the matching fragments wrapped in the specified tags.

The `highlight` object supports the following parameters:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `fields` | Object | Yes | Map of field names to per-field configuration. Use `"*"` to highlight all `text` and `keyword` fields that match. |
| `pre_tags` | Array of strings | No | Tags inserted before each highlighted term. Default: `["<em>"]`. |
| `post_tags` | Array of strings | No | Tags inserted after each highlighted term. Default: `["</em>"]`. |
| `fragment_size` | Integer | No | Maximum character length of each highlight fragment. Default is OpenSearch's default (100). Set to `2147483647` to return the entire field value without truncation. |

### What gets highlighted

- **Full-text search terms only.** The search term in `search source=logs "error"` is translated to a `query_string` query, and OpenSearch's highlighter identifies matches. Structured filters (`where`, `stats`, comparison operators) do not produce highlights.
- **`text` and `keyword` fields only.** Numeric, date, boolean, and other non-string field types never produce highlight fragments.

### Example 1: Wildcard highlight

```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X POST localhost:9200/_plugins/_ppl \
-d '{
  "query": "search source=accounts \"Holmes\"",
  "highlight": {
    "fields": { "*": {} },
    "pre_tags": ["<em>"],
    "post_tags": ["</em>"],
    "fragment_size": 2147483647
  }
}'
```

Expected output:

```json
{
  "schema": [
    { "name": "firstname", "type": "string" },
    { "name": "lastname", "type": "string" },
    { "name": "address", "type": "string" }
  ],
  "datarows": [
    ["Holmes", "Morgan", "123 Main St"],
    ["Jane", "Holmes", "456 Oak Ave"],
    ["John", "Smith", "880 Holmes Lane"]
  ],
  "highlights": [
    { "firstname": ["<em>Holmes</em>"], "firstname.keyword": ["<em>Holmes</em>"] },
    { "lastname": ["<em>Holmes</em>"], "lastname.keyword": ["<em>Holmes</em>"] },
    { "address": ["880 <em>Holmes</em> Lane"] }
  ],
  "total": 3,
  "size": 3,
  "status": 200
}
```

### Example 2: Specific field with custom tags

```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X POST localhost:9200/_plugins/_ppl \
-d '{
  "query": "search source=accounts \"Holmes\"",
  "highlight": {
    "fields": { "address": {} },
    "pre_tags": ["<mark>"],
    "post_tags": ["</mark>"]
  }
}'
```

Expected output:

```json
{
  "schema": [ ... ],
  "datarows": [ ... ],
  "highlights": [
    null,
    null,
    { "address": ["880 <mark>Holmes</mark> Lane"] }
  ],
  "total": 3,
  "size": 3,
  "status": 200
}
```

Only the `address` field is highlighted. Rows where "Holmes" appears in other fields have `null` highlight entries.

### Response format

- The `highlights` array is parallel to `datarows` — each entry corresponds to the row at the same index.
- Entries are `null` when a row has no highlight data for the requested fields.
- The `highlights` array is **omitted entirely** when no `highlight` config is provided in the request (backward compatible). When no highlight config is provided, there is zero impact on query processing — no extra columns, no extra overhead.

### Behavior with piped commands

- Piped commands (`where`, `sort`, `head`, `dedup`) narrow or reorder the result set but do not affect which terms are highlighted. Only the full-text search term produces highlights.
- Explicit field projection (`| fields name, age`) preserves highlight data, consistent with DSL where `_source` filtering does not affect highlights.

### Limitations

- Highlighting is supported only in the Calcite engine.
- The backend forwards the highlight config as-is to OpenSearch. The same highlighting behavior and limitations as [OpenSearch's highlighting API](https://opensearch.org/docs/latest/search-plugins/searching-data/highlight/) apply.
- Highlighting works with **single-source queries only**, consistent with DSL where highlighting is inherently single-index per request. Behavior with joins (`| join`), subqueries, and multi-source queries is not validated.

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
    "physical": "CalciteEnumerableIndexScan(table=[[OpenSearch, state_country]], PushDownContext=[[PROJECT->[name, country, state, month, year, age], FILTER->>($5, 30), LIMIT->10000], OpenSearchRequestBuilder(sourceBuilder={\"from\":0,\"size\":10000,\"timeout\":\"1m\",\"query\":{\"range\":{\"age\":{\"from\":30,\"to\":null,\"include_lower\":false,\"include_upper\":true,\"boost\":1.0}}},\"_source\":{\"includes\":[\"name\",\"country\",\"state\",\"month\",\"year\",\"age\"],\"excludes\":[]}}, requestedTotalSize=10000, pageSize=null, startFrom=0)])\n"
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
    "physical": "EnumerableLimit(fetch=[10000])\n  EnumerableCalc(expr#0..5=[{inputs}], expr#6=[30], expr#7=[>($t5, $t6)], proj#0..5=[{exprs}], $condition=[$t7])\n    CalciteEnumerableIndexScan(table=[[OpenSearch, state_country]], PushDownContext=[[PROJECT->[name, country, state, month, year, age], LIMIT->10], OpenSearchRequestBuilder(sourceBuilder={\"from\":0,\"size\":10,\"timeout\":\"1m\",\"_source\":{\"includes\":[\"name\",\"country\",\"state\",\"month\",\"year\",\"age\"],\"excludes\":[]}}, requestedTotalSize=10, pageSize=null, startFrom=0)])\n",
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
    CalciteEnumerableIndexScan(table=[[OpenSearch, state_country]], PushDownContext=[[PROJECT->[name, country, state, month, year, age], FILTER->>($5, 30), LIMIT->10000], OpenSearchRequestBuilder(sourceBuilder={"from":0,"size":10000,"timeout":"1m","query":{"range":{"age":{"from":30,"to":null,"include_lower":false,"include_upper":true,"boost":1.0}}},"_source":{"includes":["name","country","state","month","year","age"],"excludes":[]}}, requestedTotalSize=10000, pageSize=null, startFrom=0)])
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
