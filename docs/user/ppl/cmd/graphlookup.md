
# graphLookup (Experimental)

The `graphLookup` command performs recursive graph traversal on a collection using a breadth-first search (BFS) algorithm. It searches for documents matching a start value and recursively traverses connections between documents based on specified fields. This is useful for hierarchical data like organizational charts, social networks, or routing graphs.

## Syntax

The `graphLookup` command supports two modes:

### Piped mode (with source)

```syntax
source = <sourceIndex> | graphLookup <lookupIndex> start=<startField> edge=<fromField><operator><toField> [maxDepth=<maxDepth>] [depthField=<depthField>] [supportArray=(true | false)] [batchMode=(true | false)] [usePIT=(true | false)] [filter=(<condition>)] as <outputField>
```

### Top-level mode (with literal start values)

```syntax
graphLookup <lookupIndex> start=<literalValue> edge=<fromField><operator><toField> [maxDepth=<maxDepth>] [depthField=<depthField>] [usePIT=(true | false)] [filter=(<condition>)] as <outputField>
graphLookup <lookupIndex> start=(<literalValue1>, <literalValue2>, ...) edge=<fromField><operator><toField> [maxDepth=<maxDepth>] [depthField=<depthField>] [usePIT=(true | false)] [filter=(<condition>)] as <outputField>
```

The following are examples of the `graphLookup` command syntax:

```syntax
source = employees | graphLookup employees start=reportsTo edge=reportsTo-->name as reportingHierarchy
source = employees | graphLookup employees start=reportsTo edge=reportsTo-->name maxDepth=2 as reportingHierarchy
source = employees | graphLookup employees start=reportsTo edge=reportsTo-->name depthField=level as reportingHierarchy
source = employees | graphLookup employees start=reportsTo edge=reportsTo<->name as connections
source = travelers | graphLookup airports start=nearestAirport edge=connects-->airport supportArray=true as reachableAirports
source = airports | graphLookup airports start=airport edge=connects-->airport supportArray=true as reachableAirports
source = employees | graphLookup employees start=reportsTo edge=reportsTo-->name filter=(status = 'active' AND age > 18) as reportingHierarchy
graphLookup employees start="Eliot" edge=reportsTo-->name as reportingHierarchy
graphLookup employees start=("Eliot", "Andrew") edge=reportsTo-->name as reportingHierarchy
graphLookup employees start="Eliot" edge=reportsTo-->name maxDepth=1 depthField=level as reportingHierarchy
```

## Parameters

The `graphLookup` command supports the following parameters.

| Parameter | Required/Optional | Description |
|---|---|---|
| `<lookupIndex>` | Required | The name of the index to perform the graph traversal on. Can be the same as the source index for self-referential graphs. |
| `start=<startField>` or `start=<literal>` or `start=(<literal>, ...)` | Required | In **piped mode**, specifies the field in the source documents whose value initiates the recursive search. In **top-level mode**, specifies one or more literal values (e.g., `start="Eliot"` or `start=("Eliot", "Andrew")`) to seed the BFS traversal directly. The value is matched against `toField` in the lookup index. |
| `edge=<fromField><operator><toField>` | Required | Defines the traversal path between nodes, specifying the connection fields and the direction of traversal. See [Edge Sub-parameters](#edge-sub-parameters) below. |
| `maxDepth=<maxDepth>` | Optional | The maximum recursion depth (number of hops). Default is `0`. A value of `0` returns only direct connections to the start values. A value of `1` returns the initial matches plus one additional recursive step, and so on. |
| `depthField=<depthField>` | Optional | The name of the field added to each traversed document to indicate its recursion depth. If not specified, no depth field is added. Depth starts at `0` for the first level of matches. |
| `supportArray=(true \| false)` | Optional | When `true`, disables early visited-node filter pushdown to OpenSearch. Default is `false`. Set to `true` when `fromField` or `toField` contains array values to ensure correct traversal behavior. See [Array Field Handling](#array-field-handling) for details. |
| `batchMode=(true \| false)` | Optional | When `true`, collects all start values from all source rows and performs a single unified BFS traversal. Default is `false`. The output changes to two arrays: `[Array<sourceRows>, Array<lookupResults>]`. See [Batch Mode](#batch-mode) for details. |
| `usePIT=(true \| false)` | Optional | When `true`, enables Point In Time (PIT) search for the lookup index, allowing paginated retrieval of complete results without the `max_result_window` size limit. Default is `false`. See [PIT Search](#pit-search) for details. |
| `filter=(<condition>)` | Optional | A filter condition that restricts which lookup index documents participate in the graph traversal. Only documents matching the condition are considered as candidates during BFS. Parentheses around the condition are required. Example: `filter=(status = 'active' AND age > 18)`. |
| `as <outputField>` | Required | The name of the output array field that will contain all documents discovered during the graph traversal. |

### Edge Sub-parameters

The `edge` parameter uses the syntax `edge=<fromField><operator><toField>` and consists of three components:

| Component | Description |
|---|---|
| `fromField` | The field in the lookup index documents used for recursion. After a document is matched, the value of this field is used to find the next set of connected documents. Supports both single values and array values. |
| `toField` | The field in the lookup index documents to match against. Documents where `toField` equals the current traversal value are included in the results. |
| `operator` | Specifies the direction of traversal. `-->` performs a **unidirectional** traversal from `fromField` to `toField` only. `<->` performs a **bidirectional** traversal between `fromField` and `toField`. |

**Examples:**

- **Unidirectional:** `edge=reportsTo-->name` — traverses from `reportsTo` to `name` in one direction only.
- **Bidirectional:** `edge=reportsTo<->name` — traverses between `reportsTo` and `name` in both directions.

## How It Works

The `graphLookup` command performs a breadth-first search (BFS) traversal:

1. For each source document, extract the value of `start`
2. Query the lookup index to find documents where `toField` matches the start value
3. Add matched documents to the result array
4. Extract `fromField` values from matched documents to continue traversal
5. Repeat steps 2-4 until no new documents are found or `maxDepth` is reached

For bidirectional traversal (`<->`), the algorithm also follows edges in the reverse direction by additionally matching `fromField` values.

## Example 1: Employee Hierarchy Traversal

Given an `employees` index with the following documents:

| id | name | reportsTo |
|----|------|-----------|
| 1 | Dev | Eliot |
| 2 | Eliot | Ron |
| 3 | Ron | Andrew |
| 4 | Andrew | null |
| 5 | Asya | Ron |
| 6 | Dan | Andrew |

The following query finds the reporting chain for each employee:

```ppl ignore
source = employees
  | graphLookup employees
    start=reportsTo
    edge=reportsTo-->name
    as reportingHierarchy
```

The query returns the following results:

```text
+--------+----------+----+-----------------------------------------------+
| name   | reportsTo| id | reportingHierarchy                            |
+--------+----------+----+-----------------------------------------------+
| Dev    | Eliot    | 1  | [{name:Eliot, reportsTo:Ron, id:2}]           |
| Eliot  | Ron      | 2  | [{name:Ron, reportsTo:Andrew, id:3}]          |
| Ron    | Andrew   | 3  | [{name:Andrew, reportsTo:null, id:4}]         |
| Andrew | null     | 4  | []                                            |
| Asya   | Ron      | 5  | [{name:Ron, reportsTo:Andrew, id:3}]          |
| Dan    | Andrew   | 6  | [{name:Andrew, reportsTo:null, id:4}]         |
+--------+----------+----+-----------------------------------------------+
```

Each element in the `reportingHierarchy` array is a struct with named fields from the lookup index. For Dev, the traversal starts with `reportsTo="Eliot"`, finds the Eliot record, and returns it in the `reportingHierarchy` array.

## Example 2: Employee Hierarchy with Depth Tracking

The following query adds a depth field to track how many levels each manager is from the employee:

```ppl ignore
source = employees
  | graphLookup employees
    start=reportsTo
    edge=reportsTo-->name
    depthField=level
    as reportingHierarchy
```

The query returns the following results:

```text
+--------+----------+----+------------------------------------------------------+
| name   | reportsTo| id | reportingHierarchy                                   |
+--------+----------+----+------------------------------------------------------+
| Dev    | Eliot    | 1  | [{name:Eliot, reportsTo:Ron, id:2, level:0}]         |
| Eliot  | Ron      | 2  | [{name:Ron, reportsTo:Andrew, id:3, level:0}]        |
| Ron    | Andrew   | 3  | [{name:Andrew, reportsTo:null, id:4, level:0}]       |
| Andrew | null     | 4  | []                                                   |
| Asya   | Ron      | 5  | [{name:Ron, reportsTo:Andrew, id:3, level:0}]        |
| Dan    | Andrew   | 6  | [{name:Andrew, reportsTo:null, id:4, level:0}]       |
+--------+----------+----+------------------------------------------------------+
```

The depth field `level` is added to each struct in the result array. A value of `0` indicates the first level of matches.

## Example 3: Limited Depth Traversal

The following query limits traversal to 2 levels using `maxDepth=1`:

```ppl ignore
source = employees
  | graphLookup employees
    start=reportsTo
    edge=reportsTo-->name
    maxDepth=1
    as reportingHierarchy
```

The query returns the following results:

```text
+--------+----------+----+---------------------------------------------------------------------------------+
| name   | reportsTo| id | reportingHierarchy                                                              |
+--------+----------+----+---------------------------------------------------------------------------------+
| Dev    | Eliot    | 1  | [{name:Eliot, reportsTo:Ron, id:2}, {name:Ron, reportsTo:Andrew, id:3}]         |
| Eliot  | Ron      | 2  | [{name:Ron, reportsTo:Andrew, id:3}, {name:Andrew, reportsTo:null, id:4}]       |
| Ron    | Andrew   | 3  | [{name:Andrew, reportsTo:null, id:4}]                                           |
| Andrew | null     | 4  | []                                                                              |
| Asya   | Ron      | 5  | [{name:Ron, reportsTo:Andrew, id:3}, {name:Andrew, reportsTo:null, id:4}]       |
| Dan    | Andrew   | 6  | [{name:Andrew, reportsTo:null, id:4}]                                           |
+--------+----------+----+---------------------------------------------------------------------------------+
```

With `maxDepth=1`, the traversal goes two levels deep (depth 0 and depth 1).

## Example 4: Airport Connections Graph

Given an `airports` index with the following documents:

| airport | connects |
|---------|----------|
| JFK | [BOS, ORD] |
| BOS | [JFK, PWM] |
| ORD | [JFK] |
| PWM | [BOS, LHR] |
| LHR | [PWM] |

The following query finds reachable airports from each airport:

```ppl ignore
source = airports
  | graphLookup airports
    start=airport
    edge=connects-->airport
    as reachableAirports
```

The query returns the following results:

```text
+---------+------------+-----------------------------------------------+
| airport | connects   | reachableAirports                             |
+---------+------------+-----------------------------------------------+
| JFK     | [BOS, ORD] | [{airport:JFK, connects:[BOS, ORD]}]          |
| BOS     | [JFK, PWM] | [{airport:BOS, connects:[JFK, PWM]}]          |
| ORD     | [JFK]      | [{airport:ORD, connects:[JFK]}]               |
| PWM     | [BOS, LHR] | [{airport:PWM, connects:[BOS, LHR]}]          |
| LHR     | [PWM]      | [{airport:LHR, connects:[PWM]}]               |
+---------+------------+-----------------------------------------------+
```

## Example 5: Cross-Index Graph Lookup

The `graphLookup` command can use different source and lookup indexes. Given a `travelers` index:

| name | nearestAirport |
|------|----------------|
| Dev | JFK |
| Eliot | JFK |
| Jeff | BOS |

The following query finds reachable airports for each traveler:

```ppl ignore
source = travelers
  | graphLookup airports
    start=nearestAirport
    edge=connects-->airport
    as reachableAirports
```

The query returns the following results:

```text
+-------+----------------+-----------------------------------------------+
| name  | nearestAirport | reachableAirports                             |
+-------+----------------+-----------------------------------------------+
| Dev   | JFK            | [{airport:JFK, connects:[BOS, ORD]}]          |
| Eliot | JFK            | [{airport:JFK, connects:[BOS, ORD]}]          |
| Jeff  | BOS            | [{airport:BOS, connects:[JFK, PWM]}]          |
+-------+----------------+-----------------------------------------------+
```

## Example 6: Bidirectional Traversal

The following query performs bidirectional traversal to find both managers and colleagues who share the same manager:

```ppl ignore
source = employees
  | where name = 'Ron'
  | graphLookup employees
    start=reportsTo
    edge=reportsTo<->name
    as connections
```

The query returns the following results:

```text
+------+----------+----+-----------------------------------------------------------------------------------------------------+
| name | reportsTo| id | connections                                                                                          |
+------+----------+----+-----------------------------------------------------------------------------------------------------+
| Ron  | Andrew   | 3  | [{name:Ron, reportsTo:Andrew, id:3}, {name:Andrew, reportsTo:null, id:4}, {name:Dan, reportsTo:Andrew, id:6}] |
+------+----------+----+-----------------------------------------------------------------------------------------------------+
```

With bidirectional traversal, Ron's connections include:
- His own record (Ron reports to Andrew)
- His manager (Andrew)
- His peer (Dan, who also reports to Andrew)

## Batch Mode

When `batchMode=true`, the `graphLookup` command collects all start values from all source rows and performs a single unified BFS traversal instead of separate traversals per row.

### Output Format Change

In batch mode, the output is a **single row** with two arrays:
- First array: All source rows collected
- Second array: All lookup results from the unified BFS traversal

### When to Use Batch Mode

Use `batchMode=true` when:
- You want to find all nodes reachable from **any** of the source start values
- You need a global view of the graph connectivity from multiple starting points
- You want to avoid duplicate traversals when multiple source rows share overlapping paths

### Example

```ppl ignore
source = travelers
  | graphLookup airports
    start=nearestAirport
    edge=connects-->airport
    batchMode=true
    maxDepth=2
    as reachableAirports
```

**Normal mode** (default): Each traveler gets their own list of reachable airports
```text
| name  | nearestAirport | reachableAirports                    |
|-------|----------------|--------------------------------------|
| Dev   | JFK            | [{airport:JFK, connects:[BOS, ORD]}] |
| Jeff  | BOS            | [{airport:BOS, connects:[JFK, PWM]}] |
```

**Batch mode**: A single row with all travelers and all reachable airports combined
```text
| travelers                                                          | reachableAirports                                           |
|--------------------------------------------------------------------|-------------------------------------------------------------|
| [{name:Dev, nearestAirport:JFK}, {name:Jeff, nearestAirport:BOS}] | [{airport:JFK, connects:[BOS, ORD]}, {airport:BOS, ...}]   |
```

## Array Field Handling

When the `fromField` or `toField` contains array values, you should set `supportArray=true` to ensure correct traversal behavior.

## PIT Search

By default, each level of BFS traversal limits the number of returned documents to the `max_result_window` setting of the lookup index (typically 10,000). This avoids the overhead of PIT (Point In Time) search but may return incomplete results when a single traversal level matches more documents than the limit.

When `usePIT=true`, this limit is removed and the lookup table uses PIT-based pagination, which ensures all matching documents are retrieved at each traversal level. This provides complete and accurate results at the cost of additional search overhead.

### When to Use PIT Search

Use `usePIT=true` when:
- The graph has high-degree nodes where a single traversal level may match more than `max_result_window` documents
- Result completeness is more important than query performance
- You observe incomplete or missing results with the default setting

### Example

```ppl ignore
source = employees
  | graphLookup employees
    start=reportsTo
    edge=reportsTo-->name
    usePIT=true
    as reportingHierarchy
```

## Filtered Graph Traversal

The `filter` parameter restricts which documents in the lookup table are considered during the BFS traversal. Only documents matching the filter condition participate as candidates at each traversal level.

### Example

The following query traverses only active employees in the reporting hierarchy:

```ppl ignore
source = employees
  | graphLookup employees
    start=reportsTo
    edge=reportsTo-->name
    filter=(status = 'active')
    as reportingHierarchy
```

The filter is applied at the OpenSearch query level, so it combines efficiently with the BFS traversal queries. At each BFS level, the query sent to OpenSearch is effectively: `bool { filter: [user_filter, bfs_terms_query] }`.

## Top-Level Mode (Literal Start Values)

When used as a top-level command (without `source = ...`), `graphLookup` accepts literal start values instead of a field reference. This is useful when you know the specific starting points for graph traversal.

### Example: Single Start Value

```ppl ignore
graphLookup employees
  start="Eliot"
  edge=reportsTo-->name
  as reportingHierarchy
```

The query returns a single row containing the BFS results:

```text
+---------------------------------------------------------------+
| reportingHierarchy                                            |
+---------------------------------------------------------------+
| [{name:Eliot, reportsTo:Ron, id:2}, {name:Ron, ...}, ...]    |
+---------------------------------------------------------------+
```

### Example: Multiple Start Values

```ppl ignore
graphLookup employees
  start=("Eliot", "Andrew")
  edge=reportsTo-->name
  as reportingHierarchy
```

All literal start values are combined into a single BFS traversal. The output is a single row with all discovered nodes.

### Example: With Depth Tracking

```ppl ignore
graphLookup employees
  start="Eliot"
  edge=reportsTo-->name
  depthField=level
  as reportingHierarchy
```

## Limitations

- The source input, which provides the starting point for the traversal, has a limitation of 100 documents to avoid performance issues.
- When `usePIT=false` (default), each level of traversal search returns documents up to the `max_result_window` of the lookup index, which may result in incomplete data. Set `usePIT=true` to retrieve complete results.
