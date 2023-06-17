## Background

Scrolled search is performed by request to `http://localhost:9200/<index>/_search?scroll=<timeout>` endpoint and it has a mandatory field `size`, which defines page size:
```json
{
    "size" : 5
}
```
Regular (non-scrolled/non-paged) search is performed by request to `http://localhost:9200/<index</_search` endpoint and it has not mandatory fields. Parameter `size` though defines maximum number of docs to be returned by search.

## Problem statement

`LIMIT` clause is being converted to `size` by SQL plugin during push down operation.
Hereby comes the conflict in using `LIMIT` with pagination.

## Solution

Don't do push down for `LIMIT`. `LimitOperator` Physical Plan Tree node will cut off yielding search results with minimal overhead.
It seems simple, but here comes another problem.

## Impediments

`Optimizer` which calls push down operations sets page size after limit, which overwrites this settings. `Optimizer` does not apply rules in the order they are given.

## Fix

Rework [`Optimizer`](https://github.com/opensearch-project/sql/blob/57ce303740f64fe0279fc34aab0116f33f11fbe6/core/src/main/java/org/opensearch/sql/planner/optimizer/LogicalPlanOptimizer.java).
Current behavior:
```py
def Optimize:
    for node in PlanTree: # Traverse the Logical Plan Tree
        for rule in rules: # Enumerate rules
            tryApplyRule()
```

Expected behavior:
```py
def Optimize:
    for rule in rules: # Enumerate rules
        for node in PlanTree: # Traverse the Logical Plan Tree
            tryApplyRule()
```
Rules list:
```
...
CreateTableScanBuilder
PushDownPageSize
...
PUSH_DOWN_LIMIT
...
```

This gives us warranty that `pushDownLimit` operation would be rejected if `pushPageSize` called before. Then, not optimized Logical Plan Tree node `LogicalLimit` will be converted to `LimitOperator` Physical Plan tree node.

## Other changes

1. Make `Optimizer` rules applied only once.
2. Reorder `Optimizer` rules.
3. Make `LimitOperator` properly serialized and deserialized.
4. Make `OpenSearchIndexScanBuilder::pushDownLimit` return `false` if `pushDownPageSize` was called before.
5. (Optional) Groom `Optimizer` to reduce amount of unchecked casts and uses raw classes.
6. (Optional) Rework `Optimizer` to make it a tree visitor.