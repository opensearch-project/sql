
# explain

The `explain` command displays the execution plan of a query, which is often used for query translation and troubleshooting. The `explain` command can only be used as the first command in the PPL query.

## Syntax

The `explain` command has the following syntax:

```syntax
explain <mode> queryStatement
```

## Parameters

The `explain` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<queryStatement>` | Required | A PPL query to explain. |
| `<mode>` | Optional | The explain mode. Valid values are: <br> - `standard`: Displays the logical and physical plan along with pushdown information (query domain-specific language [DSL]). Available in both v2 and v3 engines. <br> - `simple`: Displays the logical plan tree without attributes. Requires the v3 engine (`plugins.calcite.enabled` = `true`). <br> - `cost`: Displays the standard information plus plan cost attributes. Requires the v3 engine (`plugins.calcite.enabled` = `true`). <br> - `extended`: Displays the standard information plus the generated code. If the whole plan is able to pushdown, it is equal to the standard mode. Requires the v3 engine (`plugins.calcite.enabled` = `true`). <br><br> Default is `standard`. |

## Example 1: Explain a PPL query in the v2 engine  

When Apache Calcite is disabled (`plugins.calcite.enabled` is set to `false`), `explain` obtains its physical plan and pushdown information from the v2 engine:
  
```ppl
explain source=state_country
| where country = 'USA' OR country = 'England'
| stats count() by country
```
  
The query returns the following results:
  
```json
{
  "root": {
    "name": "ProjectOperator",
    "description": {
      "fields": "[count(), country]"
    },
    "children": [
      {
        "name": "OpenSearchIndexScan",
        "description": {
          "request": """OpenSearchQueryRequest(indexName=state_country, sourceBuilder={"from":0,"size":10000,"timeout":"1m","query":{"bool":{"should":[{"term":{"country":{"value":"USA","boost":1.0}}},{"term":{"country":{"value":"England","boost":1.0}}}],"adjust_pure_negative":true,"boost":1.0}},"aggregations":{"composite_buckets":{"composite":{"size":1000,"sources":[{"country":{"terms":{"field":"country","missing_bucket":true,"missing_order":"first","order":"asc"}}}]},"aggregations":{"count()":{"value_count":{"field":"_index"}}}}}}, pitId=null, cursorKeepAlive=null, searchAfter=null, searchResponse=null)"""
        },
        "children": []
      }
    ]
  }
}
```
  

## Example 2: Explain a PPL query in the v3 engine  

When Apache Calcite is enabled (`plugins.calcite.enabled` is set to `true`), `explain` obtains its logical and physical plan and pushdown information from the v3 engine:  
  
```ppl
explain source=state_country
| where country = 'USA' OR country = 'England'
| stats count() by country
```
  
The query returns the following results:

```json
{
  "calcite": {
    "logical": """LogicalProject(count()=[$1], country=[$0])
  LogicalAggregate(group=[{1}], count()=[COUNT()])
    LogicalFilter(condition=[SEARCH($1, Sarg['England', 'USA':CHAR(7)]:CHAR(7))])
      CalciteLogicalIndexScan(table=[[OpenSearch, state_country]])
""",
    "physical": """EnumerableCalc(expr#0..1=[{inputs}], count()=[$t1], country=[$t0])
  CalciteEnumerableIndexScan(table=[[OpenSearch, state_country]], PushDownContext=[[FILTER->SEARCH($1, Sarg['England', 'USA':CHAR(7)]:CHAR(7)), AGGREGATION->rel#53:LogicalAggregate.NONE.[](input=RelSubset#43,group={1},count()=COUNT())], OpenSearchRequestBuilder(sourceBuilder={"from":0,"size":0,"timeout":"1m","query":{"terms":{"country":["England","USA"],"boost":1.0}},"aggregations":{"composite_buckets":{"composite":{"size":1000,"sources":[{"country":{"terms":{"field":"country","missing_bucket":true,"missing_order":"first","order":"asc"}}}]},"aggregations":{"count()":{"value_count":{"field":"_index"}}}}}}, requestedTotalSize=2147483647, pageSize=null, startFrom=0)])
"""
  }
}
```
  

## Example 3: Explain a PPL query in the simple mode  

The following query uses the `explain` command in the `simple` mode to show a simplified logical plan tree: 
  
```ppl
explain simple source=state_country
| where country = 'USA' OR country = 'England'
| stats count() by country
```
  
The query returns the following results: 
  
```json
{
  "calcite": {
    "logical": """LogicalProject
  LogicalAggregate
    LogicalFilter
      CalciteLogicalIndexScan
"""
  }
}
```
  

## Example 4: Explain a PPL query in the cost mode  

The following query uses the `explain` command in the `cost` mode to show plan cost attributes:
  
```ppl
explain cost source=state_country
| where country = 'USA' OR country = 'England'
| stats count() by country
```
  
The query returns the following results:

```json
{
  "calcite": {
    "logical": """LogicalProject(count()=[$1], country=[$0]): rowcount = 2.5, cumulative cost = {130.3125 rows, 206.0 cpu, 0.0 io}, id = 75
  LogicalAggregate(group=[{1}], count()=[COUNT()]): rowcount = 2.5, cumulative cost = {127.8125 rows, 201.0 cpu, 0.0 io}, id = 74
    LogicalFilter(condition=[SEARCH($1, Sarg['England', 'USA':CHAR(7)]:CHAR(7))]): rowcount = 25.0, cumulative cost = {125.0 rows, 201.0 cpu, 0.0 io}, id = 73
      CalciteLogicalIndexScan(table=[[OpenSearch, state_country]]): rowcount = 100.0, cumulative cost = {100.0 rows, 101.0 cpu, 0.0 io}, id = 72
""",
    "physical": """EnumerableCalc(expr#0..1=[{inputs}], count()=[$t1], country=[$t0]): rowcount = 100.0, cumulative cost = {200.0 rows, 501.0 cpu, 0.0 io}, id = 138
  CalciteEnumerableIndexScan(table=[[OpenSearch, state_country]], PushDownContext=[[FILTER->SEARCH($1, Sarg['England', 'USA':CHAR(7)]:CHAR(7)), AGGREGATION->rel#125:LogicalAggregate.NONE.[](input=RelSubset#115,group={1},count()=COUNT())], OpenSearchRequestBuilder(sourceBuilder={"from":0,"size":0,"timeout":"1m","query":{"terms":{"country":["England","USA"],"boost":1.0}},"aggregations":{"composite_buckets":{"composite":{"size":1000,"sources":[{"country":{"terms":{"field":"country","missing_bucket":true,"missing_order":"first","order":"asc"}}}]},"aggregations":{"count()":{"value_count":{"field":"_index"}}}}}}, requestedTotalSize=2147483647, pageSize=null, startFrom=0)]): rowcount = 100.0, cumulative cost = {100.0 rows, 101.0 cpu, 0.0 io}, id = 133
"""
  }
}
```
  

## Example 5: Explain a PPL query in the extended mode

The following query uses the `explain` command in the `extended` mode to show the generated code:

```ppl
explain extended source=state_country
| where country = 'USA' OR country = 'England'
| stats count() by country
```
  
The query returns the following results:

```json
{
  "calcite": {
    "logical": """LogicalProject(count()=[$1], country=[$0])
  LogicalAggregate(group=[{1}], count()=[COUNT()])
    LogicalFilter(condition=[SEARCH($1, Sarg['England', 'USA':CHAR(7)]:CHAR(7))])
      CalciteLogicalIndexScan(table=[[OpenSearch, state_country]])
""",
    "physical": """EnumerableCalc(expr#0..1=[{inputs}], count()=[$t1], country=[$t0])
  CalciteEnumerableIndexScan(table=[[OpenSearch, state_country]], PushDownContext=[[FILTER->SEARCH($1, Sarg['England', 'USA':CHAR(7)]:CHAR(7)), AGGREGATION->rel#193:LogicalAggregate.NONE.[](input=RelSubset#183,group={1},count()=COUNT())], OpenSearchRequestBuilder(sourceBuilder={"from":0,"size":0,"timeout":"1m","query":{"terms":{"country":["England","USA"],"boost":1.0}},"aggregations":{"composite_buckets":{"composite":{"size":1000,"sources":[{"country":{"terms":{"field":"country","missing_bucket":true,"missing_order":"first","order":"asc"}}}]},"aggregations":{"count()":{"value_count":{"field":"_index"}}}}}}, requestedTotalSize=2147483647, pageSize=null, startFrom=0)])
""",
    "extended": """public org.apache.calcite.linq4j.Enumerable bind(final org.apache.calcite.DataContext root) {
  final org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan v1stashed = (org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan) root.get("v1stashed");
  final org.apache.calcite.linq4j.Enumerable _inputEnumerable = v1stashed.scan();
  return new org.apache.calcite.linq4j.AbstractEnumerable(){
      public org.apache.calcite.linq4j.Enumerator enumerator() {
        return new org.apache.calcite.linq4j.Enumerator(){
            public final org.apache.calcite.linq4j.Enumerator inputEnumerator = _inputEnumerable.enumerator();
            public void reset() {
              inputEnumerator.reset();
            }
            public boolean moveNext() {
              return inputEnumerator.moveNext();
            }
            public void close() {
              inputEnumerator.close();
            }
            public Object current() {
              final Object[] current = (Object[]) inputEnumerator.current();
              final Object input_value = current[1];
              final Object input_value0 = current[0];
              return new Object[] {
                  input_value,
                  input_value0};
            }
          };
      }
    };
}
public Class getElementType() {
  return java.lang.Object[].class;
}
"""
  }
}
```