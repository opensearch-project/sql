# reverse

## Description

The `reverse` command reverses the display order of search results. The behavior depends on the query context:

1. **With existing sort**: Reverses the sort direction(s)
2. **With @timestamp field (no explicit sort)**: Sorts by @timestamp in descending order
3. **Without sort or @timestamp**: The command is ignored (no effect)

## Behavior

The `reverse` command follows a three-tier logic:

1. **If there's an explicit sort command before reverse**: The reverse command flips all sort directions (ASC ↔ DESC)
2. **If no explicit sort but the index has an @timestamp field**: The reverse command sorts by @timestamp in descending order (most recent first)
3. **If neither condition is met**: The reverse command is ignored and has no effect on the result order

This design optimizes performance by avoiding expensive operations when reverse has no meaningful semantic interpretation.

## Version

Available since version 3.2.0.

## Syntax

```
reverse
```

* No parameters: The reverse command takes no arguments or options.

## Note

The `reverse` command is optimized to avoid unnecessary memory consumption. When applied without an explicit sort or @timestamp field, it is ignored. When used with an explicit sort, it efficiently reverses the sort direction(s) without materializing the entire dataset.

## Example 1: Reverse with explicit sort

The example shows reversing the order of all documents.

```ppl
source=accounts
| sort age
| fields account_number, age
| reverse
```

Expected output:

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

## Example 2: Reverse with @timestamp field

The example shows reverse on a time-series index automatically sorts by @timestamp in descending order (most recent first).

```ppl
source=time_test
| fields value, @timestamp
| reverse
| head 3
```

Expected output:

```text
fetched rows / total rows = 3/3
+-------+---------------------+
| value | @timestamp          |
|-------+---------------------|
| 9243  | 2025-07-28 09:41:29 |
| 7654  | 2025-07-28 08:22:11 |
| 8321  | 2025-07-28 07:05:33 |
+-------+---------------------+
```

Note: When the index contains an @timestamp field and no explicit sort is specified, reverse will sort by @timestamp DESC to show the most recent events first. This is particularly useful for log analysis and time-series data.

## Example 3: Reverse ignored (no sort, no @timestamp)

The example shows that reverse is ignored when there's no explicit sort and no @timestamp field.

```ppl
source=accounts
| fields account_number, age
| reverse
| head 2
```

Expected output:

```text
fetched rows / total rows = 2/2
+----------------+-----+
| account_number | age |
|----------------+-----|
| 1              | 32  |
| 6              | 36  |
+----------------+-----+
```

Note: Results appear in natural order (same as without reverse) because accounts index has no @timestamp field and no explicit sort was specified.

## Example 4: Reverse with sort and head

The example shows using reverse with sort and head to get the top 2 records by age.

```ppl
source=accounts
| sort age
| reverse
| head 2
| fields account_number, age
```

Expected output:

```text
fetched rows / total rows = 2/2
+----------------+-----+
| account_number | age |
|----------------+-----|
| 6              | 36  |
| 18             | 33  |
+----------------+-----+
```

## Example 5: Double reverse with sort

The example shows that applying reverse twice with an explicit sort returns to the original sort order.

```ppl
source=accounts
| sort age
| reverse
| reverse
| fields account_number, age
```

Expected output:

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

## Example 6: Reverse with multiple sort fields

The example shows reverse flipping all sort directions when multiple fields are sorted.

```ppl
source=accounts
| sort +age, -account_number
| reverse
| fields account_number, age
```

Expected output:

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

Note: Original sort is ASC age, DESC account_number. After reverse, it becomes DESC age, ASC account_number.
