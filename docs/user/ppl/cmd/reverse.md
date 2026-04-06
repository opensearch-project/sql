
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

## Example 1: Basic reverse operation

The following query reverses the order of all documents in the results:

```ppl
source=otellogs
| fields severityText, `resource.attributes.service.name`
| head 5
| reverse
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+--------------+----------------------------------+
| severityText | resource.attributes.service.name |
|--------------+----------------------------------|
| DEBUG        | cart                             |
| ERROR        | payment                          |
| WARN         | product-catalog                  |
| INFO         | cart                             |
| INFO         | frontend                         |
+--------------+----------------------------------+
```

## Example 2: Use the reverse and sort commands

The following query reverses results after sorting by `severityNumber` in ascending order, effectively implementing descending order:

```ppl
source=otellogs
| sort severityNumber
| fields severityText, severityNumber
| head 5
| reverse
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+--------------+----------------+
| severityText | severityNumber |
|--------------+----------------|
| INFO         | 9              |
| INFO         | 9              |
| DEBUG        | 5              |
| DEBUG        | 5              |
| DEBUG        | 5              |
+--------------+----------------+
```

## Example 3: Use the reverse and head commands

The following query uses the `reverse` command together with the `head` command to retrieve the last two records from the original result order:

```ppl
source=otellogs
| reverse
| head 2
| fields severityText, `resource.attributes.service.name`
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+--------------+----------------------------------+
| severityText | resource.attributes.service.name |
|--------------+----------------------------------|
| ERROR        | checkout                         |
| DEBUG        | cart                             |
+--------------+----------------------------------+
```

## Example 4: Double reverse

The following query shows that applying `reverse` twice returns documents in the original order:

```ppl
source=otellogs
| reverse
| reverse
| fields severityText, `resource.attributes.service.name`
| head 5
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+--------------+----------------------------------+
| severityText | resource.attributes.service.name |
|--------------+----------------------------------|
| INFO         | frontend                         |
| INFO         | cart                             |
| WARN         | product-catalog                  |
| ERROR        | payment                          |
| DEBUG        | cart                             |
+--------------+----------------------------------+
```

## Example 5: Use the reverse command with filtering

The following query uses the `reverse` command with filtering and field selection:

```ppl
source=otellogs
| where severityText = 'ERROR'
| fields severityText, `resource.attributes.service.name`
| reverse
```

The query returns the following results:

```text
fetched rows / total rows = 7/7
+--------------+----------------------------------+
| severityText | resource.attributes.service.name |
|--------------+----------------------------------|
| ERROR        | checkout                         |
| ERROR        | product-catalog                  |
| ERROR        | recommendation                   |
| ERROR        | frontend-proxy                   |
| ERROR        | payment                          |
| ERROR        | checkout                         |
| ERROR        | payment                          |
+--------------+----------------------------------+
```
