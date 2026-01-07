
# eventstats

The `eventstats` command enriches your event data with calculated summary statistics. It analyzes the specified fields within your events, computes various statistical measures, and then appends these results as new fields to each original event.

The `eventstats` command operates in the following way:

1. It performs calculations across the entire search results or within defined groups.
2. The original events remain intact, with new fields added to contain the statistical results.
3. The command is particularly useful for comparative analysis, identifying outliers, and providing additional context to individual events.

## Comparing stats and eventstats

For a comprehensive comparison of `stats`, `eventstats`, and `streamstats` commands, including their differences in transformation behavior, output format, aggregation scope, and use cases, see [Comparing stats, eventstats, and streamstats](streamstats.md/#comparing-stats-eventstats-and-streamstats).

## Syntax

The `eventstats` command has the following syntax:

```syntax
eventstats [bucket_nullable=bool] <function>... [by-clause]
```

The following are examples of the `eventstats` command syntax:

```syntax
source = table | eventstats avg(a)
source = table | where a < 50 | eventstats count(c)
source = table | eventstats min(c), max(c) by b
source = table | eventstats count(c) as count_by by b | where count_by > 1000
source = table | eventstats dc(field) as distinct_count
source = table | eventstats distinct_count(category) by region
```

## Parameters

The `eventstats` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<function>` | Required | An aggregation function or window function. |
| `bucket_nullable` | Optional | Controls whether the `eventstats` command considers `null` buckets as a valid group in group-by aggregations. When set to `false`, it does not treat `null` group-by values as a distinct group during aggregation. Default is determined by `plugins.ppl.syntax.legacy.preferred`. |
| `<by-clause>` | Optional | Groups results by specified fields or expressions. Syntax: `by [span-expression,] [field,]...` Default is aggregating over the entire search results. |
| `<span-expression>` | Optional | Splits a field into buckets by intervals (at most one). Syntax: `span(field_expr, interval_expr)`. For example, `span(age, 10)` creates 10-year age buckets, while `span(timestamp, 1h)` creates hourly buckets. |

### Time units

The following time units are available for span expressions:

* Milliseconds (`ms`)
* Seconds (`s`)
* Minutes (`m`, case sensitive)
* Hours (`h`)
* Days (`d`)
* Weeks (`w`)
* Months (`M`, case sensitive)
* Quarters (`q`)
* Years (`y`)  

## Aggregation functions

The `eventstats` command supports the following aggregation functions:

* `COUNT` -- Count of values
* `SUM` -- Sum of numeric values
* `AVG` -- Average of numeric values
* `MAX` -- Maximum value
* `MIN` -- Minimum value
* `VAR_SAMP` -- Sample variance
* `VAR_POP` -- Population variance
* `STDDEV_SAMP` -- Sample standard deviation
* `STDDEV_POP` -- Population standard deviation
* `DISTINCT_COUNT`/`DC` -- Distinct count of values
* `EARLIEST` -- Earliest value by timestamp
* `LATEST` -- Latest value by timestamp  

For detailed documentation of each function, see [Functions](../functions/aggregations.md).  

## Example 1: Calculate the average, sum, and count of a field by group  

The following query calculates the average age, sum of age, and count of events for all accounts grouped by gender:
  
```ppl
source=accounts
| fields account_number, gender, age
| eventstats avg(age), sum(age), count() by gender
| sort account_number
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+----------------+--------+-----+--------------------+----------+---------+
| account_number | gender | age | avg(age)           | sum(age) | count() |
|----------------+--------+-----+--------------------+----------+---------|
| 1              | M      | 32  | 33.666666666666664 | 101      | 3       |
| 6              | M      | 36  | 33.666666666666664 | 101      | 3       |
| 13             | F      | 28  | 28.0               | 28       | 1       |
| 18             | M      | 33  | 33.666666666666664 | 101      | 3       |
+----------------+--------+-----+--------------------+----------+---------+
```
  

## Example 2: Calculate the count by a gender and span  

The following query counts events by age intervals of 5 years, grouped by gender:
  
```ppl
source=accounts
| fields account_number, gender, age
| eventstats count() as cnt by span(age, 5) as age_span, gender
| sort account_number
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+----------------+--------+-----+-----+
| account_number | gender | age | cnt |
|----------------+--------+-----+-----|
| 1              | M      | 32  | 2   |
| 6              | M      | 36  | 1   |
| 13             | F      | 28  | 1   |
| 18             | M      | 33  | 2   |
+----------------+--------+-----+-----+
```
  

## Example 3: Null bucket handling

The following query uses the `eventstats` command with `bucket_nullable=false` to exclude null values from the group-by aggregation:

```ppl
source=accounts
| eventstats bucket_nullable=false count() as cnt by employer
| fields account_number, firstname, employer, cnt
| sort account_number
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+----------------+-----------+----------+------+
| account_number | firstname | employer | cnt  |
|----------------+-----------+----------+------|
| 1              | Amber     | Pyrami   | 1    |
| 6              | Hattie    | Netagy   | 1    |
| 13             | Nanette   | Quility  | 1    |
| 18             | Dale      | null     | null |
+----------------+-----------+----------+------+
```

The following query uses the `eventstats` command with `bucket_nullable=true` to include null values in the group-by aggregation:

```ppl
source=accounts
| eventstats bucket_nullable=true count() as cnt by employer
| fields account_number, firstname, employer, cnt
| sort account_number
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+----------------+-----------+----------+-----+
| account_number | firstname | employer | cnt |
|----------------+-----------+----------+-----|
| 1              | Amber     | Pyrami   | 1   |
| 6              | Hattie    | Netagy   | 1   |
| 13             | Nanette   | Quility  | 1   |
| 18             | Dale      | null     | 1   |
+----------------+-----------+----------+-----+
```
  