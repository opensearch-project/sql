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
Explain output could be set different formats: `standard` (the default format), `simple`, `extended`, `dsl`.
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
  
### Example 2 simple format  

Explain query
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X POST localhost:9200/_plugins/_ppl/_explain?format=simple \
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
  
### Example 3 extended format  

Explain query
  
```bash ppl
curl -sS -H 'Content-Type: application/json' \
-X POST localhost:9200/_plugins/_ppl/_explain?format=extended \
-d '{"query" : "source=state_country | where age>30"}'
```
  
Expected output:
  
```json
{
  "calcite": {
    "logical": "LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE_LIMIT])\n  LogicalProject(name=[$0], country=[$1], state=[$2], month=[$3], year=[$4], age=[$5])\n    LogicalFilter(condition=[>($5, 30)])\n      CalciteLogicalIndexScan(table=[[OpenSearch, state_country]])\n",
    "physical": "CalciteEnumerableIndexScan(table=[[OpenSearch, state_country]], PushDownContext=[[PROJECT->[name, country, state, month, year, age], FILTER->>($5, 30), LIMIT->10000], OpenSearchRequestBuilder(sourceBuilder={\"from\":0,\"size\":10000,\"timeout\":\"1m\",\"query\":{\"range\":{\"age\":{\"from\":30,\"to\":null,\"include_lower\":false,\"include_upper\":true,\"boost\":1.0}}},\"_source\":{\"includes\":[\"name\",\"country\",\"state\",\"month\",\"year\",\"age\"],\"excludes\":[]}}, requestedTotalSize=10000, pageSize=null, startFrom=0)])\n",
    "extended": "public org.apache.calcite.linq4j.Enumerable bind(final org.apache.calcite.DataContext root) {\n  final org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan v1stashed = (org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan) root.get(\"v1stashed\");\n  return v1stashed.scan();\n}\n\n\npublic Class getElementType() {\n  return java.lang.Object[].class;\n}\n\n\n"
  }
}
```
  
### Example 4 YAML format (experimental)  

   YAML explain output is an experimental feature and not intended for
   production use. The interface and output may change without notice.
Return Explain response format in In `yaml` format.
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
         "total_time_ms": 25.77
      },
      "phases": {
         "analyze": { "time_ms": 5.77 },
         "optimize": { "time_ms": 13.51 },
         "execute": { "time_ms": 0.77 },
         "format": { "time_ms": 0.04 }
      }
   }
}
```

### Notes

- Profile output is only returned when the query finishes successfully.
- Profiling runs only when Calcite is enabled.
