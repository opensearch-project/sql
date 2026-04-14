
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

The following query reverses the order of all documents in the results:
  
```ppl
source=accounts
| fields account_number, age
| reverse
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+----------------+-----+
| account_number | age |
|----------------+-----|
| 6              | 36  |
| 18             | 33  |
| 1              | 32  |
| 13             | 28  |
+----------------+-----+
```
  

## Example 2: Use the reverse and sort commands

The following query reverses results after sorting documents by age in ascending order, effectively implementing descending order:
  
```ppl
source=accounts
| sort age
| fields account_number, age
| reverse
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+----------------+-----+
| account_number | age |
|----------------+-----|
| 6              | 36  |
| 18             | 33  |
| 1              | 32  |
| 13             | 28  |
+----------------+-----+
```
  

## Example 3: Use the reverse and head commands  

The following query uses the `reverse` command together with the `head` command to retrieve the last two records from the original result order:
  
```ppl
source=accounts
| reverse
| head 2
| fields account_number, age
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+----------------+-----+
| account_number | age |
|----------------+-----|
| 6              | 36  |
| 18             | 33  |
+----------------+-----+
```
  

## Example 4: Double reverse  

The following query shows that applying `reverse` twice returns documents in the original order:
  
```ppl
source=accounts
| reverse
| reverse
| fields account_number, age
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+----------------+-----+
| account_number | age |
|----------------+-----|
| 13             | 28  |
| 1              | 32  |
| 18             | 33  |
| 6              | 36  |
+----------------+-----+
```
  

## Example 5: Use the reverse command with a complex pipeline

The following query uses the `reverse` command with filtering and field selection:

```ppl
source=accounts
| where age > 30
| fields account_number, age
| reverse
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+----------------+-----+
| account_number | age |
|----------------+-----|
| 6              | 36  |
| 18             | 33  |
| 1              | 32  |
+----------------+-----+
```

## Example 6: Reverse with descending sort

The following query reverses a descending sort, effectively producing ascending order:

```ppl
source=accounts
| sort - account_number
| fields account_number
| reverse
```

The query returns the following results:

```text
fetched rows / total rows = 7/7
+----------------+
| account_number |
|----------------|
| 1              |
| 6              |
| 13             |
| 18             |
| 20             |
| 25             |
| 32             |
+----------------+
```

## Example 7: Reverse with mixed sort directions

The following query reverses a multi-field sort with mixed directions. Each field's sort direction is individually flipped:

```ppl
source=accounts
| sort - account_number, + firstname
| fields account_number, firstname
| reverse
```

The query returns the following results:

```text
fetched rows / total rows = 7/7
+----------------+--------------+
| account_number | firstname    |
|----------------+--------------|
| 1              | Amber JOHnny |
| 6              | Hattie       |
| 13             | Nanette      |
| 18             | Dale         |
| 20             | Elinor       |
| 25             | Virginia     |
| 32             | Dillard      |
+----------------+--------------+
```

## Example 8: Reverse with @timestamp field

When no explicit sort exists but the data source has an `@timestamp` field, `reverse` sorts by `@timestamp` in descending order:

```ppl
source=time_test_data
| fields value, category, `@timestamp`
| reverse
| head 5
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+-------+----------+---------------------+
| value | category | @timestamp          |
|-------+----------+---------------------|
| 8762  | A        | 2025-08-01 03:47:41 |
| 7348  | C        | 2025-08-01 02:00:56 |
| 9015  | B        | 2025-08-01 01:14:11 |
| 6489  | D        | 2025-08-01 00:27:26 |
| 8676  | A        | 2025-07-31 23:40:33 |
+-------+----------+---------------------+
```

## Example 9: Reverse is ignored without sort or @timestamp

When there is no explicit sort and the data source has no `@timestamp` field, `reverse` is a no-op and data remains in its natural order:

```ppl
source=accounts
| fields account_number
| reverse
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+----------------+
| account_number |
|----------------|
| 1              |
| 6              |
| 13             |
+----------------+
```

## Example 10: Reverse backtracks through filter and eval

The `reverse` command can detect sort collations through non-blocking operators like `where` and `eval`:

```ppl
source=accounts
| sort account_number
| where balance > 30000
| fields account_number, balance
| reverse
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+---------+
| account_number | balance |
|----------------+---------|
| 32             | 48086   |
| 25             | 40540   |
| 13             | 32838   |
| 1              | 39225   |
+----------------+---------+
```

## Example 11: Reverse is a no-op after aggregation

Aggregation (`stats`) destroys input ordering, so `reverse` after aggregation without a subsequent `sort` is a no-op:

```ppl
source=accounts
| stats count() as c by gender
| reverse
```

The query returns the following results (order not guaranteed):

```text
fetched rows / total rows = 2/2
+---+--------+
| c | gender |
|---+--------|
| 4 | M      |
| 3 | F      |
+---+--------+
```

## Example 12: Reverse works with sort after aggregation

Adding a `sort` after aggregation restores collation, allowing `reverse` to work:

```ppl
source=accounts
| stats count() as c by gender
| sort gender
| reverse
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+---+--------+
| c | gender |
|---+--------|
| 4 | M      |
| 3 | F      |
+---+--------+
```

## Example 13: Reverse with timechart

The `timechart` command adds a sort on the time field, so `reverse` flips it to return results in reverse chronological order:

```ppl
source=events
| timechart span=1m count()
| reverse
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+---------------------+----------+
| @timestamp          | count()  |
|---------------------+----------|
| 2024-07-01 00:04:00 | 1        |
| 2024-07-01 00:03:00 | 1        |
| 2024-07-01 00:02:00 | 1        |
| 2024-07-01 00:01:00 | 1        |
| 2024-07-01 00:00:00 | 1        |
+---------------------+----------+
```

