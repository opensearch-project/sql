
# reverse

The `reverse` command reverses the display order of the search results. It returns the same results but in the opposite order.

> **Note**: The `reverse` command processes the entire dataset. If applied directly to millions of records, it consumes significant coordinating node memory resources. Only apply the `reverse` command to smaller datasets, typically after aggregation operations.

## Performance optimization

The `reverse` command uses an optimized implementation that intelligently reverses existing sort collations instead of using a `ROW_NUMBER()` approach. The behavior depends on the context:

1. **Existing sort collation**: If a preceding `sort` command is detected, `reverse` flips the sort direction of each field (e.g., ASC becomes DESC and vice versa). This leverages database-native sort reversal for significantly better performance.
2. **`@timestamp` field**: If no explicit sort exists but the data source has an `@timestamp` field, `reverse` sorts by `@timestamp` in descending order.
3. **No sort or `@timestamp`**: If neither an explicit sort nor an `@timestamp` field is found, `reverse` is a no-op (ignored).

The optimization also supports **backtracking** through non-blocking operators like `where`, `eval`, and `fields` to find an upstream sort. However, blocking operators such as `stats` (aggregation), `join`, and set operations destroy the collation, so `reverse` after these operators is a no-op unless a new `sort` is added after them.

## Syntax

The `reverse` command has the following syntax:

```syntax
reverse
```

The `reverse` command takes no parameters.

## Example 1: Basic reverse operation  

The following query retrieves the top 4 errors sorted by service name, then reverses the order:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'FATAL')
| sort severityNumber, `resource.attributes.service.name`
| fields severityText, `resource.attributes.service.name`
| head 4
| reverse
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+--------------+----------------------------------+
| severityText | resource.attributes.service.name |
|--------------+----------------------------------|
| ERROR        | payment                          |
| ERROR        | frontend-proxy                   |
| ERROR        | checkout                         |
| ERROR        | checkout                         |
+--------------+----------------------------------+
```
  

## Example 2: Use the reverse and sort commands

The following query reverses results after sorting by severity in ascending order, effectively showing the most critical issues first:
  
```ppl
source=otellogs
| dedup severityText
| sort severityNumber
| fields severityText, severityNumber
| reverse
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 5/5
+--------------+----------------+
| severityText | severityNumber |
|--------------+----------------|
| FATAL        | 21             |
| ERROR        | 17             |
| WARN         | 13             |
| INFO         | 9              |
| DEBUG        | 5              |
+--------------+----------------+
```
  

## Example 3: Reverse aggregation results  

The following query reverses the order of aggregated error counts by service, showing the least active services first:
  
```ppl
source=otellogs
| stats count() as log_count by severityText
| sort - log_count
| reverse
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+-----------+--------------+
| log_count | severityText |
|-----------+--------------|
| 2         | FATAL        |
| 3         | DEBUG        |
| 4         | WARN         |
| 5         | ERROR        |
| 6         | INFO         |
+-----------+--------------+
```
  
