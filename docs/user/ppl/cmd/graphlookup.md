
# graphLookup

The `graphLookup` command performs recursive graph traversal on a collection using a breadth-first search (BFS) algorithm. It searches for documents matching a start value and recursively traverses connections between documents based on specified fields. This is useful for hierarchical data like organizational charts, social networks, or routing graphs.

## Syntax

The `graphLookup` command has the following syntax:

```syntax
graphLookup <lookupIndex> startField=<startField> fromField=<fromField> toField=<toField> [maxDepth=<maxDepth>] [depthField=<depthField>] [direction=(uni | bi)] [supportArray=(true | false)] [batchMode=(true | false)] as <outputField>
```

The following are examples of the `graphLookup` command syntax:

```syntax
source = employees | graphLookup employees startField=reportsTo fromField=reportsTo toField=name as reportingHierarchy
source = employees | graphLookup employees startField=reportsTo fromField=reportsTo toField=name maxDepth=2 as reportingHierarchy
source = employees | graphLookup employees startField=reportsTo fromField=reportsTo toField=name depthField=level as reportingHierarchy
source = employees | graphLookup employees startField=reportsTo fromField=reportsTo toField=name direction=bi as connections
source = travelers | graphLookup airports startField=nearestAirport fromField=connects toField=airport supportArray=true as reachableAirports
source = airports | graphLookup airports startField=airport fromField=connects toField=airport supportArray=true as reachableAirports
```

## Parameters

The `graphLookup` command supports the following parameters.

| Parameter | Required/Optional | Description                                                                                                                                                                                                                        |
| --- | --- |------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `<lookupIndex>` | Required | The name of the index to perform the graph traversal on. Can be the same as the source index for self-referential graphs.                                                                                                          |
| `startField=<startField>` | Required | The field in the source documents whose value is used to start the recursive search. The value of this field is matched against `toField` in the lookup index. We support both single value and array values as starting points.   |
| `fromField=<fromField>` | Required | The field in the lookup index documents that contains the value to recurse on. After matching a document, the value of this field is used to find the next set of documents. It supports both single value and array values.       |
| `toField=<toField>` | Required | The field in the lookup index documents to match against. Documents where `toField` equals the current traversal value are included in the results.                                                                                |
| `maxDepth=<maxDepth>` | Optional | The maximum recursion depth of hops. Default is `0`. A value of `0` means only the direct connections to the statr values are returned. A value of `1` means 1 hop connections (initial match plus one recursive step), and so on. |
| `depthField=<depthField>` | Optional | The name of the field to add to each traversed document indicating its recursion depth. If not specified, no depth field is added. Depth starts at `0` for the first level of matches.                                             |
| `direction=(uni \| bi)` | Optional | The traversal direction. `uni` (default) performs unidirectional traversal following edges in the forward direction only. `bi` performs bidirectional traversal, following edges in both directions.                               |
| `supportArray=(true \| false)` | Optional | When `true`, disables early visited-node filter pushdown to OpenSearch. Default is `false`. Set to `true` when `fromField` or `toField` contains array values to ensure correct traversal behavior. See [Array Field Handling](#array-field-handling) for details. |
| `batchMode=(true \| false)` | Optional | When `true`, collects all start values from all source rows and performs a single unified BFS traversal. Default is `false`. The output changes to two arrays: `[Array<sourceRows>, Array<lookupResults>]`. See [Batch Mode](#batch-mode) for details. |
| `as <outputField>` | Required | The name of the output array field that will contain all documents found during the graph traversal.                                                                                                                               |

## How It Works

The `graphLookup` command performs a breadth-first search (BFS) traversal:

1. For each source document, extract the value of `startField`
2. Query the lookup index to find documents where `toField` matches the start value
3. Add matched documents to the result array
4. Extract `fromField` values from matched documents to continue traversal
5. Repeat steps 2-4 until no new documents are found or `maxDepth` is reached

For bidirectional traversal (`direction=bi`), the algorithm also follows edges in the reverse direction by additionally matching `fromField` values.

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
    startField=reportsTo
    fromField=reportsTo
    toField=name
    as reportingHierarchy
```

The query returns the following results:

```text
+--------+----------+----+---------------------+
| name   | reportsTo| id | reportingHierarchy  |
+--------+----------+----+---------------------+
| Dev    | Eliot    | 1  | [{Eliot, Ron, 2}]   |
| Eliot  | Ron      | 2  | [{Ron, Andrew, 3}]  |
| Ron    | Andrew   | 3  | [{Andrew, null, 4}] |
| Andrew | null     | 4  | []                  |
| Asya   | Ron      | 5  | [{Ron, Andrew, 3}]  |
| Dan    | Andrew   | 6  | [{Andrew, null, 4}] |
+--------+----------+----+---------------------+
```

For Dev, the traversal starts with `reportsTo="Eliot"`, finds the Eliot record, and returns it in the `reportingHierarchy` array.

## Example 2: Employee Hierarchy with Depth Tracking

The following query adds a depth field to track how many levels each manager is from the employee:

```ppl ignore
source = employees
  | graphLookup employees
    startField=reportsTo
    fromField=reportsTo
    toField=name
    depthField=level
    as reportingHierarchy
```

The query returns the following results:

```text
+--------+----------+----+------------------------+
| name   | reportsTo| id | reportingHierarchy     |
+--------+----------+----+------------------------+
| Dev    | Eliot    | 1  | [{Eliot, Ron, 2, 0}]   |
| Eliot  | Ron      | 2  | [{Ron, Andrew, 3, 0}]  |
| Ron    | Andrew   | 3  | [{Andrew, null, 4, 0}] |
| Andrew | null     | 4  | []                     |
| Asya   | Ron      | 5  | [{Ron, Andrew, 3, 0}]  |
| Dan    | Andrew   | 6  | [{Andrew, null, 4, 0}] |
+--------+----------+----+------------------------+
```

The depth field `level` is appended to each document in the result array. A value of `0` indicates the first level of matches.

## Example 3: Limited Depth Traversal

The following query limits traversal to 2 levels using `maxDepth=1`:

```ppl ignore
source = employees
  | graphLookup employees
    startField=reportsTo
    fromField=reportsTo
    toField=name
    maxDepth=1
    as reportingHierarchy
```

The query returns the following results:

```text
+--------+----------+----+--------------------------------------+
| name   | reportsTo| id | reportingHierarchy                   |
+--------+----------+----+--------------------------------------+
| Dev    | Eliot    | 1  | [{Eliot, Ron, 2}, {Ron, Andrew, 3}]  |
| Eliot  | Ron      | 2  | [{Ron, Andrew, 3}, {Andrew, null, 4}]|
| Ron    | Andrew   | 3  | [{Andrew, null, 4}]                  |
| Andrew | null     | 4  | []                                   |
| Asya   | Ron      | 5  | [{Ron, Andrew, 3}, {Andrew, null, 4}]|
| Dan    | Andrew   | 6  | [{Andrew, null, 4}]                  |
+--------+----------+----+--------------------------------------+
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
    startField=airport
    fromField=connects
    toField=airport
    as reachableAirports
```

The query returns the following results:

```text
+---------+------------+---------------------+
| airport | connects   | reachableAirports   |
+---------+------------+---------------------+
| JFK     | [BOS, ORD] | [{JFK, [BOS, ORD]}] |
| BOS     | [JFK, PWM] | [{BOS, [JFK, PWM]}] |
| ORD     | [JFK]      | [{ORD, [JFK]}]      |
| PWM     | [BOS, LHR] | [{PWM, [BOS, LHR]}] |
| LHR     | [PWM]      | [{LHR, [PWM]}]      |
+---------+------------+---------------------+
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
    startField=nearestAirport
    fromField=connects
    toField=airport
    as reachableAirports
```

The query returns the following results:

```text
+-------+----------------+---------------------+
| name  | nearestAirport | reachableAirports   |
+-------+----------------+---------------------+
| Dev   | JFK            | [{JFK, [BOS, ORD]}] |
| Eliot | JFK            | [{JFK, [BOS, ORD]}] |
| Jeff  | BOS            | [{BOS, [JFK, PWM]}] |
+-------+----------------+---------------------+
```

## Example 6: Bidirectional Traversal

The following query performs bidirectional traversal to find both managers and colleagues who share the same manager:

```ppl ignore
source = employees
  | where name = 'Ron'
  | graphLookup employees
    startField=reportsTo
    fromField=reportsTo
    toField=name
    direction=bi
    as connections
```

The query returns the following results:

```text
+------+----------+----+------------------------------------------------+
| name | reportsTo| id | connections                                    |
+------+----------+----+------------------------------------------------+
| Ron  | Andrew   | 3  | [{Ron, Andrew, 3}, {Andrew, null, 4}, {Dan, Andrew, 6}] |
+------+----------+----+------------------------------------------------+
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
    startField=nearestAirport
    fromField=connects
    toField=airport
    batchMode=true
    maxDepth=2
    as reachableAirports
```

**Normal mode** (default): Each traveler gets their own list of reachable airports
```text
| name  | nearestAirport | reachableAirports |
|-------|----------------|-------------------|
| Dev   | JFK            | [JFK, BOS, ORD]   |
| Jeff  | BOS            | [BOS, JFK, PWM]   |
```

**Batch mode**: A single row with all travelers and all reachable airports combined
```text
| travelers                              | reachableAirports           |
|----------------------------------------|-----------------------------|
| [{Dev, JFK}, {Jeff, BOS}]              | [JFK, BOS, ORD, PWM, ...]   |
```

## Array Field Handling

When the `fromField` or `toField` contains array values, you should set `supportArray=true` to ensure correct traversal behavior.

## Limitations

- The source input, which provides the starting point for the traversal, has a limitation of 100 documents to avoid performance issues.
- To avoid PIT (Point in Time) search, each level of traversal search returns documents up to the "max result windows" of the lookup index.
