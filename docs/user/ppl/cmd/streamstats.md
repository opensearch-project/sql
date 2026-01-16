
# streamstats

The `streamstats` command calculates cumulative or rolling statistics as events that are processed in order. Unlike `stats` or `eventstats`, which operate on the entire dataset at once, `streamstats` processes events incrementally, making it suitable for time-series and sequence-based analysis.

Key features include support for the `window` (sliding window calculations) and `current` (whether to include the current event in calculations) parameters and specialized use cases such as identifying trends or detecting changes over sequences of events.  
  
## Comparing stats, eventstats, and streamstats

The `stats`, `eventstats`, and `streamstats` commands can all generate aggregations such as average, sum, and maximum. However, they differ in how they operate and the results they produce. The following table summarizes these differences.

| Aspect | `stats` | `eventstats` | `streamstats` |
| --- | --- | --- | --- |
| Transformation behavior | Transforms all events into an aggregated result table, losing original event structure | Adds aggregation results as new fields to the original events without removing the event structure | Adds cumulative (running) aggregation results to each event as it streams through the pipeline |
| Output format | Output contains only aggregated values. Original raw events are not preserved | Original events remain, with extra fields containing summary statistics | Original events remain, with extra fields containing running totals or cumulative statistics |
| Aggregation scope | Based on all events in the search (or groups defined by the `by` clause) | Based on all relevant events, then the result is added back to each event in the group | Calculations occur progressively as each event is processed; can be scoped by window |
| Use cases | When only aggregated results are needed (for example, counts, averages, sums) | When aggregated statistics are needed alongside original event data | When a running total or cumulative statistic is needed across event streams |  
  

## Syntax

The `streamstats` command has the following syntax:

```syntax
streamstats [bucket_nullable=bool] [current=<bool>] [window=<int>] [global=<bool>] [reset_before="("<eval-expression>")"] [reset_after="("<eval-expression>")"] <function>... [by-clause]
```

The following are examples of the `streamstats` command syntax:

```ppl ignore
source = table | streamstats avg(a)
source = table | streamstats current = false avg(a)
source = table | streamstats window = 5 sum(b)
source = table | streamstats current = false window = 2 max(a)
source = table | where a < 50 | streamstats count(c)
source = table | streamstats min(c), max(c) by b
source = table | streamstats count(c) as count_by by b | where count_by > 1000
source = table | streamstats dc(field) as distinct_count
source = table | streamstats distinct_count(category) by region
source = table | streamstats current=false window=2 global=false avg(a) by b
source = table | streamstats window=2 reset_before=a>31 avg(b)
source = table | streamstats current=false reset_after=a>31 avg(b) by c
```

## Parameters

The `streamstats` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<function>` | Required | An aggregation function or window function. |
| `bucket_nullable` | Optional | Controls whether to consider null buckets as a valid group in group-by aggregations. When `false`, does not treat null group-by values as a distinct group during aggregation. Default is the value of `plugins.ppl.syntax.legacy.preferred`. |
| `current` | Optional | Whether to include the current event in summary calculations. When `true`, includes the current event; when `false`, uses the field value from the previous event. Default is `true`. |
| `window` | Optional | The number of events to use when computing statistics. Default is `0` (all previous and current events are used). |
| `global` | Optional | Used only when `window` is specified. Determines whether to use a single window (`true`) or separate windows for each group defined by the `by` clause (`false`). When `false` and `window` is non-zero, a separate window is used for each group of values of the field specified in the `by` clause. Default is `true`. |
| `reset_before` | Optional | Resets all accumulated statistics before `streamstats` computes the running metrics for an event when the `eval-expression` evaluates to `true`. If used with `window`, the window is also reset. Syntax: `reset_before="(<eval-expression>)"`. Default is `false`. |
| `reset_after` | Optional | Resets all accumulated statistics after `streamstats` computes the running metrics for an event when the `eval-expression` evaluates to `true`. The expression can reference fields returned by `streamstats`. If used with `window`, the window is also reset. Syntax: `reset_after="(<eval-expression>)"`. Default is `false`. |
| `<by-clause>` | Optional | Fields and expressions for grouping, including scalar functions and aggregation functions. The `span` clause can be used to split specific fields into buckets by intervals. Syntax: `by [span-expression,] [field,]...` If not specified, all events are processed as a single group and running statistics are computed across the entire event stream. |
| `<span-expression>` | Optional | Splits a field into buckets by intervals (maximum of one). Syntax: `span(field_expr, interval_expr)`. By default, the interval uses the field's default unit. For date/time fields, aggregation results ignore null values. Examples: `span(age, 10)` creates 10-year age buckets, and `span(timestamp, 1h)` creates hourly buckets. Valid time units are millisecond (`ms`), second (`s`), minute (`m`), hour (`h`), day (`d`), week (`w`), month (`M`), quarter (`q`), year (`y`). |


## Aggregation functions  

The `streamstats` command supports the following aggregation functions:

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
  
For detailed documentation of each function, see [Aggregation Functions](../functions/aggregations.md).

## Example 1: Calculate the running average, sum, and count of a field by group  

The following query calculates the running average `age`, running sum of `age`, and running count of events for all accounts, grouped by `gender`:
  
```ppl
source=accounts
| streamstats avg(age) as running_avg, sum(age) as running_sum, count() as running_count by gender
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+-------------+---------------+
| account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | running_avg        | running_sum | running_count |
|----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+-------------+---------------|
| 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 32.0               | 32          | 1             |
| 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 34.0               | 68          | 2             |
| 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | 28.0               | 28          | 1             |
| 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 33.666666666666664 | 101         | 3             |
+----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+-------------+---------------+
```
  

## Example 2: Calculate the running maximum over a 2-row window

The following query calculates the running maximum `age` over a 2-row window, excluding the current event:
  
```ppl
source=state_country
| streamstats current=false window=2 max(age) as prev_max_age
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 8/8
+-------+---------+------------+-------+------+-----+--------------+
| name  | country | state      | month | year | age | prev_max_age |
|-------+---------+------------+-------+------+-----+--------------|
| Jake  | USA     | California | 4     | 2023 | 70  | null         |
| Hello | USA     | New York   | 4     | 2023 | 30  | 70           |
| John  | Canada  | Ontario    | 4     | 2023 | 25  | 70           |
| Jane  | Canada  | Quebec     | 4     | 2023 | 20  | 30           |
| Jim   | Canada  | B.C        | 4     | 2023 | 27  | 25           |
| Peter | Canada  | B.C        | 4     | 2023 | 57  | 27           |
| Rick  | Canada  | B.C        | 4     | 2023 | 70  | 57           |
| David | USA     | Washington | 4     | 2023 | 40  | 70           |
+-------+---------+------------+-------+------+-----+--------------+
```
  

## Example 3: Global compared to group-specific windows  

The `global` parameter takes the following values:

* `true`: A global window is applied across all rows, but the calculations inside the window still respect the `by` groups.
* `false`: The window itself is created per group, meaning each group receives an independent window. 
  
The following example uses a sample index containing the following data:

```text
+-------+---------+------------+-------+------+-----+
| name  | country | state      | month | year | age |

|-------+---------+------------+-------+------+-----+
| Jake  | USA     | California | 4     | 2023 | 70  |
| Hello | USA     | New York   | 4     | 2023 | 30  |
| John  | Canada  | Ontario    | 4     | 2023 | 25  |
| Jane  | Canada  | Quebec     | 4     | 2023 | 20  |
| Jim   | Canada  | B.C        | 4     | 2023 | 27  |
| Peter | Canada  | B.C        | 4     | 2023 | 57  |
| Rick  | Canada  | B.C        | 4     | 2023 | 70  |
| David | USA     | Washington | 4     | 2023 | 40  |

+-------+---------+------------+-------+------+-----+
```

The following examples calculate the running average of `age` across accounts by country, using a different `global` parameter.  

When `global=true`, the window slides across all rows in input order, but aggregation is still computed by `country`. The sliding window size is `2`:
  
```ppl
source=state_country
| streamstats window=2 global=true avg(age) as running_avg by country
```
  
As a result, `David` and `Rick` are included in the same sliding window when computing `running_avg` across all rows globally:
  
```text
fetched rows / total rows = 8/8
+-------+---------+------------+-------+------+-----+-------------+
| name  | country | state      | month | year | age | running_avg |
|-------+---------+------------+-------+------+-----+-------------|
| Jake  | USA     | California | 4     | 2023 | 70  | 70.0        |
| Hello | USA     | New York   | 4     | 2023 | 30  | 50.0        |
| John  | Canada  | Ontario    | 4     | 2023 | 25  | 25.0        |
| Jane  | Canada  | Quebec     | 4     | 2023 | 20  | 22.5        |
| Jim   | Canada  | B.C        | 4     | 2023 | 27  | 23.5        |
| Peter | Canada  | B.C        | 4     | 2023 | 57  | 42.0        |
| Rick  | Canada  | B.C        | 4     | 2023 | 70  | 63.5        |
| David | USA     | Washington | 4     | 2023 | 40  | 40.0        |
+-------+---------+------------+-------+------+-----+-------------+
```
  
In contrast, when `global=false`, each `by` group forms an independent stream and window:

```ppl
source=state_country
| streamstats window=2 global=false avg(age) as running_avg by country
```
  
`David` and `Hello` form a window for the `USA` group. As a result, for `David`, the `running_avg` is `35.0` instead of `40.0` in the previous case:
  
```text
fetched rows / total rows = 8/8
+-------+---------+------------+-------+------+-----+-------------+
| name  | country | state      | month | year | age | running_avg |
|-------+---------+------------+-------+------+-----+-------------|
| Jake  | USA     | California | 4     | 2023 | 70  | 70.0        |
| Hello | USA     | New York   | 4     | 2023 | 30  | 50.0        |
| John  | Canada  | Ontario    | 4     | 2023 | 25  | 25.0        |
| Jane  | Canada  | Quebec     | 4     | 2023 | 20  | 22.5        |
| Jim   | Canada  | B.C        | 4     | 2023 | 27  | 23.5        |
| Peter | Canada  | B.C        | 4     | 2023 | 57  | 42.0        |
| Rick  | Canada  | B.C        | 4     | 2023 | 70  | 63.5        |
| David | USA     | Washington | 4     | 2023 | 40  | 35.0        |
+-------+---------+------------+-------+------+-----+-------------+
```
  

## Example 4: Conditional statistics reset  

The following query calculates the running average of `age` across accounts by `country`, with resets applied:
  
```ppl
source=state_country
| streamstats current=false reset_before=age>34 reset_after=age<25 avg(age) as avg_age by country
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 8/8
+-------+---------+------------+-------+------+-----+---------+
| name  | country | state      | month | year | age | avg_age |
|-------+---------+------------+-------+------+-----+---------|
| Jake  | USA     | California | 4     | 2023 | 70  | null    |
| Hello | USA     | New York   | 4     | 2023 | 30  | 70.0    |
| John  | Canada  | Ontario    | 4     | 2023 | 25  | null    |
| Jane  | Canada  | Quebec     | 4     | 2023 | 20  | 25.0    |
| Jim   | Canada  | B.C        | 4     | 2023 | 27  | null    |
| Peter | Canada  | B.C        | 4     | 2023 | 57  | null    |
| Rick  | Canada  | B.C        | 4     | 2023 | 70  | null    |
| David | USA     | Washington | 4     | 2023 | 40  | null    |
+-------+---------+------------+-------+------+-----+---------+
```
  

## Example 5: Null bucket behavior

When `bucket_nullable=false`, null values are excluded from group-by aggregations:

```ppl
source=accounts
| streamstats bucket_nullable=false count() as cnt by employer
| fields account_number, firstname, employer, cnt
```
  
Rows in which the `by` field is `null` are excluded from aggregation, so the `cnt` for `Dale` is `null`:
  
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
  
When `bucket_nullable=true`, null values are treated as a valid group:

```ppl
source=accounts
| streamstats bucket_nullable=true count() as cnt by employer
| fields account_number, firstname, employer, cnt
```
  
As a result, the `cnt` for `Dale` is included and calculated normally:
  
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
  