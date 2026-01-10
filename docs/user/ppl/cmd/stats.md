
# stats

The `stats` command calculates aggregations on the search results.

## Comparing stats, eventstats, and streamstats

For a comprehensive comparison of `stats`, `eventstats`, and `streamstats` commands, including their differences in transformation behavior, output format, aggregation scope, and use cases, see [Comparing stats, eventstats, and streamstats](streamstats.md/#comparing-stats-eventstats-and-streamstats).

## Syntax

The `stats` command has the following syntax:

```syntax
stats [bucket_nullable=bool] <aggregation>... [by-clause]
```

## Parameters

The `stats` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<aggregation>` | Required | An aggregation function. |
| `<by-clause>` | Optional | Groups results by specified fields or expressions. Syntax: `by [span-expression,] [field,]...` If no `by-clause` is specified, the stats command returns only one row, which is the aggregation over the entire search results. |
| `bucket_nullable` | Optional | Controls whether to include `null` buckets in group-by aggregations. When `false`, ignores records in which the `group-by` field is null, resulting in faster performance. Default is the value of `plugins.ppl.syntax.legacy.preferred`. |
| `<span-expression>` | Optional | Splits a field into buckets by intervals (maximum of one). Syntax: `span(field_expr, interval_expr)`. By default, the interval uses the field's default unit. For date/time fields, aggregation results ignore null values. Examples: `span(age, 10)` creates 10-year age buckets, and `span(timestamp, 1h)` creates hourly buckets. Valid time units are millisecond (`ms`), second (`s`), minute (`m`), hour (`h`), day (`d`), week (`w`), month (`M`), quarter (`q`), year (`y`). |

## Aggregation functions  

The `stats` command supports the following aggregation functions:

* `COUNT`/`C` -- Count of values
* `SUM` -- Sum of numeric values
* `AVG` -- Average of numeric values
* `MAX` -- Maximum value
* `MIN` -- Minimum value
* `VAR_SAMP` -- Sample variance
* `VAR_POP` -- Population variance
* `STDDEV_SAMP` -- Sample standard deviation
* `STDDEV_POP` -- Population standard deviation
* `DISTINCT_COUNT_APPROX` -- Approximate distinct count
* `TAKE` -- List of original values
* `PERCENTILE`/`PERCENTILE_APPROX` -- Percentile calculations
* `PERC<percent>`/`P<percent>` -- Percentile shortcut functions
* `MEDIAN` -- 50th percentile
* `EARLIEST` -- Earliest value by timestamp
* `LATEST` -- Latest value by timestamp
* `FIRST` -- First non-null value
* `LAST` -- Last non-null value
* `LIST` -- Collect all values into array
* `VALUES` -- Collect unique values into sorted array  
  
For detailed documentation of each function, see [Aggregation Functions](../functions/aggregations.md).

## Example 1: Calculate the count of events  

The following query calculates the count of events in the `accounts` index:
  
```ppl
source=accounts
| stats count()
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------+
| count() |
|---------|
| 4       |
+---------+
```
  

## Example 2: Calculate the average of a field  

The following query calculates the average age for all accounts:
  
```ppl
source=accounts
| stats avg(age)
```
  
The query returns the following results:

```text
fetched rows / total rows = 1/1
+----------+
| avg(age) |
|----------|
| 32.25    |
+----------+
```
  

## Example 3: Calculate the average of a field by group  

The following query calculates the average age for all accounts, grouped by gender:
  
```ppl
source=accounts
| stats avg(age) by gender
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+--------------------+--------+
| avg(age)           | gender |
|--------------------+--------|
| 28.0               | F      |
| 33.666666666666664 | M      |
+--------------------+--------+
```
  

## Example 4: Calculate the average, sum, and count of a field by group  

The following query calculates the average age, sum of ages, and count of events for all accounts, grouped by gender:
  
```ppl
source=accounts
| stats avg(age), sum(age), count() by gender
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+--------------------+----------+---------+--------+
| avg(age)           | sum(age) | count() | gender |
|--------------------+----------+---------+--------|
| 28.0               | 28       | 1       | F      |
| 33.666666666666664 | 101      | 3       | M      |
+--------------------+----------+---------+--------+
```
  

## Example 5: Calculate the maximum of a field  

The following query calculates the maximum age for all accounts:
  
```ppl
source=accounts
| stats max(age)
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------+
| max(age) |
|----------|
| 36       |
+----------+
```
  

## Example 6: Calculate the maximum and minimum of a field by group  

The following query calculates the maximum and minimum ages for all accounts, grouped by gender:
  
```ppl
source=accounts
| stats max(age), min(age) by gender
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+----------+----------+--------+
| max(age) | min(age) | gender |
|----------+----------+--------|
| 28       | 28       | F      |
| 36       | 32       | M      |
+----------+----------+--------+
```
  

## Example 7: Calculate the distinct count of a field  

To retrieve the count of distinct values of a field, you can use the `DISTINCT_COUNT` (or `DC`) function instead of `COUNT`. The following query calculates both the count and the distinct count of the `gender` field for all accounts:
  
```ppl
source=accounts
| stats count(gender), distinct_count(gender)
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------+------------------------+
| count(gender) | distinct_count(gender) |
|---------------+------------------------|
| 4             | 2                      |
+---------------+------------------------+
```
  

## Example 8: Calculate the count by a span  

The following query retrieves the count of `age` values grouped into 10-year intervals:
  
```ppl
source=accounts
| stats count(age) by span(age, 10) as age_span
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+------------+----------+
| count(age) | age_span |
|------------+----------|
| 1          | 20       |
| 3          | 30       |
+------------+----------+
```
  

## Example 9: Calculate the count by a gender and span  

The following query retrieves the count of `age` grouped into 5-year intervals and broken down by `gender`:
  
```ppl
source=accounts
| stats count() as cnt by span(age, 5) as age_span, gender
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-----+----------+--------+
| cnt | age_span | gender |
|-----+----------+--------|
| 1   | 25       | F      |
| 2   | 30       | M      |
| 1   | 35       | M      |
+-----+----------+--------+
```
  
The `span` expression is always treated as the first grouping key, regardless of its position in the `by` clause:
  
```ppl
source=accounts
| stats count() as cnt by gender, span(age, 5) as age_span
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-----+----------+--------+
| cnt | age_span | gender |
|-----+----------+--------|
| 1   | 25       | F      |
| 2   | 30       | M      |
| 1   | 35       | M      |
+-----+----------+--------+
```
  

## Example 10: Count and retrieve an email list by gender and age span

The following query calculates the count of `age` values grouped into 5-year intervals as well as by `gender` and also returns a list of up to 5 emails for each group:

```ppl
source=accounts
| stats count() as cnt, take(email, 5) by span(age, 5) as age_span, gender
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-----+--------------------------------------------+----------+--------+
| cnt | take(email, 5)                             | age_span | gender |
|-----+--------------------------------------------+----------+--------|
| 1   | []                                         | 25       | F      |
| 2   | [amberduke@pyrami.com,daleadams@boink.com] | 30       | M      |
| 1   | [hattiebond@netagy.com]                    | 35       | M      |
+-----+--------------------------------------------+----------+--------+
```
  

## Example 11: Calculate the percentile of a field  

The following query calculates the 90th percentile of `age` for all accounts:
  
```ppl
source=accounts
| stats percentile(age, 90)
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------+
| percentile(age, 90) |
|---------------------|
| 36                  |
+---------------------+
```
  

## Example 12: Calculate the percentile of a field by group  

The following query calculates the 90th percentile of `age` for all accounts, grouped by `gender`:
  
```ppl
source=accounts
| stats percentile(age, 90) by gender
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+---------------------+--------+
| percentile(age, 90) | gender |
|---------------------+--------|
| 28                  | F      |
| 36                  | M      |
+---------------------+--------+
```
  

## Example 13: Calculate the percentile by a gender and span  

The following query calculates the 90th percentile of `age`, grouped into 10-year intervals as well as by `gender`:
  
```ppl
source=accounts
| stats percentile(age, 90) as p90 by span(age, 10) as age_span, gender
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+-----+----------+--------+
| p90 | age_span | gender |
|-----+----------+--------|
| 28  | 20       | F      |
| 36  | 30       | M      |
+-----+----------+--------+
```
  

## Example 14: Collect all values in a field using LIST  

The following query collects all `firstname` values, preserving duplicates and order:
  
```ppl
source=accounts
| stats list(firstname)
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------+
| list(firstname)             |
|-----------------------------|
| [Amber,Hattie,Nanette,Dale] |
+-----------------------------+
```
  

## Example 15: Ignore a null bucket

The following query excludes null values from grouping by setting `bucket_nullable=false`:

```ppl
source=accounts
| stats bucket_nullable=false count() as cnt by email
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-----+-----------------------+
| cnt | email                 |
|-----+-----------------------|
| 1   | amberduke@pyrami.com  |
| 1   | daleadams@boink.com   |
| 1   | hattiebond@netagy.com |
+-----+-----------------------+
```
  

## Example 16: Collect unique values in a field using VALUES  

The following query collects all unique `firstname` values, sorted lexicographically with duplicates removed:
  
```ppl
source=accounts
| stats values(firstname)
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------+
| values(firstname)           |
|-----------------------------|
| [Amber,Dale,Hattie,Nanette] |
+-----------------------------+
```
  

## Example 17: Date span grouping with null handling  

The following example uses this sample index data:

```text
+-------+--------+------------+
| Name  | DEPTNO | birthday   |
|-------+--------+------------|
| Alice | 1      | 2024-04-21 |
| Bob   | 2      | 2025-08-21 |
| Jeff  | null   | 2025-04-22 |
| Adam  | 2      | null       |
+-------+--------+------------+
```

The following query groups data by yearly spans of the `birthday` field, automatically excluding null values:

```ppl ignore
source=example
| stats count() as cnt by span(birthday, 1y) as year
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-----+------------+
| cnt | year       |
|-----+------------|
| 1   | 2024-01-01 |
| 2   | 2025-01-01 |
+-----+------------+
```

Group by both yearly spans and department number (by default, null `DEPTNO` values are included in the results):

```ppl ignore
source=example
| stats count() as cnt by span(birthday, 1y) as year, DEPTNO
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-----+------------+--------+
| cnt | year       | DEPTNO |
|-----+------------+--------|
| 1   | 2024-01-01 | 1      |
| 1   | 2025-01-01 | 2      |
| 1   | 2025-01-01 | null   |
+-----+------------+--------+
```

Use `bucket_nullable=false` to exclude null `DEPTNO` values from the grouping:

```ppl ignore
source=example
| stats bucket_nullable=false count() as cnt by span(birthday, 1y) as year, DEPTNO
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-----+------------+--------+
| cnt | year       | DEPTNO |
|-----+------------+--------|
| 1   | 2024-01-01 | 1      |
| 1   | 2025-01-01 | 2      |
+-----+------------+--------+
```
  

## Example 18: Calculate the count by the implicit @timestamp field  

If you omit the `field` parameter in the `span` function, it automatically uses the implicit `@timestamp` field:
  
```ppl ignore
source=big5
| stats count() by span(1month)
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------+---------------------+
| count() | span(1month)        |
|---------+---------------------|
| 1       | 2023-01-01 00:00:00 |
+---------+---------------------+
```

## Limitations

The following limitations apply to the `stats` command.

### Bucket aggregation results may be approximate for high-cardinality fields

In OpenSearch, `doc_count` values for a `terms` bucket aggregation can be approximate. Thus, any aggregations (such as `sum` or `avg`) performed on those buckets may also be approximate.

For example, the following query retrieves the top 10 URLs:

```ppl ignore
source=hits
| stats bucket_nullable=false count() as c by URL
| sort - c
| head 10
```

This query is translated into a `terms` aggregation in OpenSearch with `"order": { "_count": "desc" }`. For fields with high cardinality, some buckets may be discarded, so the results may only be approximate.

### Sorting by doc_count in ascending order may produce inaccurate results

When retrieving the least frequent terms for high-cardinality fields, results may be inaccurate. Shard-level aggregations can miss globally rare terms or misrepresent their frequency, causing errors in the overall results.

For example, the following query retrieves the 10 least frequent URLs:

```ppl ignore
source=hits
| stats bucket_nullable=false count() as c by URL
| sort + c
| head 10
```

A globally rare term might not appear as rare on every shard or could be entirely absent from some shard results. Conversely, a term that is infrequent on one shard might be common on another. In both cases, shard-level approximations can cause rare terms to be missed, leading to inaccurate overall results.
