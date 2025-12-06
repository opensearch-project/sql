# stats  

## Description  

The `stats` command calculates the aggregation from the search result.
## Syntax  

stats [bucket_nullable=bool] \<aggregation\>... [by-clause]
* aggregation: mandatory. An aggregation function.  
* bucket_nullable: optional. Controls whether the stats command includes null buckets in group-by aggregations. When set to `false`, the aggregation ignores records where the group-by field is null, resulting in faster performance by excluding null bucket. **Default:** Determined by `plugins.ppl.syntax.legacy.preferred`.  
  * When `plugins.ppl.syntax.legacy.preferred=true`, `bucket_nullable` defaults to `true`  
  * When `plugins.ppl.syntax.legacy.preferred=false`, `bucket_nullable` defaults to `false`  
* by-clause: optional. Groups results by specified fields or expressions. Syntax: by [span-expression,] [field,]... **Default:** If no by-clause is specified, the stats command returns only one row, which is the aggregation over the entire result set.  
* span-expression: optional, at most one. Splits field into buckets by intervals. Syntax: span(field_expr, interval_expr). The unit of the interval expression is the natural unit by default. If the field is a date/time type field, the aggregation results always ignore null bucket. For example, `span(age, 10)` creates 10-year age buckets, `span(timestamp, 1h)` creates hourly buckets.  
  * Available time units  
    * millisecond (ms)  
    * second (s)  
    * minute (m, case sensitive)  
    * hour (h)  
    * day (d)  
    * week (w)  
    * month (M, case sensitive)  
    * quarter (q)  
    * year (y)  
  
## Aggregation Functions  

The stats command supports the following aggregation functions:
* COUNT/C: Count of values  
* SUM: Sum of numeric values  
* AVG: Average of numeric values  
* MAX: Maximum value  
* MIN: Minimum value  
* VAR_SAMP: Sample variance  
* VAR_POP: Population variance  
* STDDEV_SAMP: Sample standard deviation  
* STDDEV_POP: Population standard deviation  
* DISTINCT_COUNT_APPROX: Approximate distinct count  
* TAKE: List of original values  
* PERCENTILE/PERCENTILE_APPROX: Percentile calculations  
* PERC\<percent\>/P\<percent\>: Percentile shortcut functions  
* MEDIAN: 50th percentile  
* EARLIEST: Earliest value by timestamp  
* LATEST: Latest value by timestamp  
* FIRST: First non-null value  
* LAST: Last non-null value  
* LIST: Collect all values into array  
* VALUES: Collect unique values into sorted array  
  
For detailed documentation of each function, see [Aggregation Functions](../functions/aggregations.md).
## Example 1: Calculate the count of events  

This example shows calculating the count of events in the accounts.
  
```ppl
source=accounts
| stats count()
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+---------+
| count() |
|---------|
| 4       |
+---------+
```
  
## Example 2: Calculate the average of a field  

This example shows calculating the average age of all the accounts.
  
```ppl
source=accounts
| stats avg(age)
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------+
| avg(age) |
|----------|
| 32.25    |
+----------+
```
  
## Example 3: Calculate the average of a field by group  

This example shows calculating the average age of all the accounts group by gender.
  
```ppl
source=accounts
| stats avg(age) by gender
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+--------------------+--------+
| avg(age)           | gender |
|--------------------+--------|
| 28.0               | F      |
| 33.666666666666664 | M      |
+--------------------+--------+
```
  
## Example 4: Calculate the average, sum and count of a field by group  

This example shows calculating the average age, sum age and count of events of all the accounts group by gender.
  
```ppl
source=accounts
| stats avg(age), sum(age), count() by gender
```
  
Expected output:
  
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

The example calculates the max age of all the accounts.
  
```ppl
source=accounts
| stats max(age)
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------+
| max(age) |
|----------|
| 36       |
+----------+
```
  
## Example 6: Calculate the maximum and minimum of a field by group  

The example calculates the max and min age values of all the accounts group by gender.
  
```ppl
source=accounts
| stats max(age), min(age) by gender
```
  
Expected output:
  
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

To get the count of distinct values of a field, you can use `DISTINCT_COUNT` (or `DC`) function instead of `COUNT`. The example calculates both the count and the distinct count of gender field of all the accounts.
  
```ppl
source=accounts
| stats count(gender), distinct_count(gender)
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+---------------+------------------------+
| count(gender) | distinct_count(gender) |
|---------------+------------------------|
| 4             | 2                      |
+---------------+------------------------+
```
  
## Example 8: Calculate the count by a span  

The example gets the count of age by the interval of 10 years.
  
```ppl
source=accounts
| stats count(age) by span(age, 10) as age_span
```
  
Expected output:
  
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

The example gets the count of age by the interval of 10 years and group by gender.
  
```ppl
source=accounts
| stats count() as cnt by span(age, 5) as age_span, gender
```
  
Expected output:
  
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
  
Span will always be the first grouping key whatever order you specify.
  
```ppl
source=accounts
| stats count() as cnt by gender, span(age, 5) as age_span
```
  
Expected output:
  
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
  
## Example 10: Calculate the count and get email list by a gender and span  

The example gets the count of age by the interval of 10 years and group by gender, additionally for each row get a list of at most 5 emails.
  
```ppl
source=accounts
| stats count() as cnt, take(email, 5) by span(age, 5) as age_span, gender
```
  
Expected output:
  
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

This example shows calculating the percentile 90th age of all the accounts.
  
```ppl
source=accounts
| stats percentile(age, 90)
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+---------------------+
| percentile(age, 90) |
|---------------------|
| 36                  |
+---------------------+
```
  
## Example 12: Calculate the percentile of a field by group  

This example shows calculating the percentile 90th age of all the accounts group by gender.
  
```ppl
source=accounts
| stats percentile(age, 90) by gender
```
  
Expected output:
  
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

The example gets the percentile 90th age by the interval of 10 years and group by gender.
  
```ppl
source=accounts
| stats percentile(age, 90) as p90 by span(age, 10) as age_span, gender
```
  
Expected output:
  
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

The example shows how to collect all firstname values, preserving duplicates and order.
  
```ppl
source=accounts
| stats list(firstname)
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----------------------------+
| list(firstname)             |
|-----------------------------|
| [Amber,Hattie,Nanette,Dale] |
+-----------------------------+
```
  
## Example 15: Ignore null bucket  
  
```ppl
source=accounts
| stats bucket_nullable=false count() as cnt by email
```
  
Expected output:
  
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

The example shows how to collect all unique firstname values, sorted lexicographically with duplicates removed.
  
```ppl
source=accounts
| stats values(firstname)
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----------------------------+
| values(firstname)           |
|-----------------------------|
| [Amber,Dale,Hattie,Nanette] |
+-----------------------------+
```
  
## Example 17: Span on date/time field always ignore null bucket  

Index example data:
+-------+--------+------------+
Name  | DEPTNO | birthday   |
+=======+========+============+
Alice | 1      | 2024-04-21 |
+-------+--------+------------+
Bob   | 2      | 2025-08-21 |
+-------+--------+------------+
Jeff  | null   | 2025-04-22 |
+-------+--------+------------+
Adam  | 2      | null       |
+-------+--------+------------+
  
```ppl ignore
source=example
| stats count() as cnt by span(birthday, 1y) as year
```
  
Expected output:
  
```text
fetched rows / total rows = 3/3
+-----+------------+
| cnt | year       |
|-----+------------|
| 1   | 2024-01-01 |
| 2   | 2025-01-01 |
+-----+------------+
```
  
```ppl ignore
source=example
| stats count() as cnt by span(birthday, 1y) as year, DEPTNO
```
  
Expected output:
  
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
  
```ppl ignore
source=example
| stats bucket_nullable=false count() as cnt by span(birthday, 1y) as year, DEPTNO
```
  
Expected output:
  
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

This example demonstrates that if you omit the field parameter in the span function, it will automatically use the implicit `@timestamp` field.
  
```ppl ignore
source=big5
| stats count() by span(1month)
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+---------+---------------------+
| count() | span(1month)        |
|---------+---------------------|
| 1       | 2023-01-01 00:00:00 |
+---------+---------------------+
```
  