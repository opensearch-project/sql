===========
streamstats
===========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
===========
The ``streamstats`` command is used to calculate cumulative or rolling statistics as events are processed in order. Unlike ``stats`` or ``eventstats`` which operate on the entire dataset at once, it computes values incrementally on a per-event basis, often respecting the order of events in the search results. It allows you to generate running totals, moving averages, and other statistics that evolve with the stream of events.

Key aspects of `streamstats`:

1. It computes statistics incrementally as each event is processed, making it suitable for time-series and sequence-based analysis.
2. Supports arguments such as window (for sliding window calculations) and current (to control whether the current event included in calculation).
3. Retains all original events and appends new fields containing the calculated statistics.
4. Particularly useful for calculating running totals, identifying trends, or detecting changes over sequences of events.

Difference between ``stats``, ``eventstats`` and ``streamstats``

All of these commands can be used to generate aggregations such as average, sum, and maximum, but they have some key differences in how they operate and what they produce:

* Transformation Behavior:
 * ``stats``: Transforms all events into an aggregated result table, losing original event structure.
 * ``eventstats``: Adds aggregation results as new fields to the original events without removing the event structure.
 * ``streamstats``: Adds cumulative (running) aggregation results to each event as they stream through the pipeline.
* Output Format:
 * ``stats``: Output contains only aggregated values. Original raw events are not preserved.
 * ``eventstats``: Original events remain, with extra fields containing summary statistics.
 * ``streamstats``: Original events remain, with extra fields containing running totals or cumulative statistics.
* Aggregation Scope:
 * ``stats``: Based on all events in the search (or groups defined by BY clause).
 * ``eventstats``: Based on all relevant events, then the result is added back to each event in the group.
 * ``streamstats``: Calculations occur progressively as each event is processed; can be scoped by window.
* Use Cases:
 * ``stats``: When only aggregated results are needed (e.g., counts, averages, sums).
 * ``eventstats``: When aggregated statistics are needed alongside original event data.
 * ``streamstats``: When a running total or cumulative statistic is needed across event streams.

Syntax
======
streamstats [current=<bool>] [window=<int>] [global=<bool>] [reset_before="("<eval-expression>")"] [reset_after="("<eval-expression>")"] <function>... [by-clause]

* function: mandatory. A aggregation function or window function.
* current: optional. If true, the search includes the given, or current, event in the summary calculations. If false, the search uses the field value from the previous event. Syntax: current=<boolean>. **Default:** true.
* window: optional. Specifies the number of events to use when computing the statistics. Syntax: window=<integer>. **Default:** 0, which means that all previous and current events are used.
* global: optional. Used only when the window argument is set. Defines whether to use a single window, global=true, or to use separate windows based on the by clause. If global=false and window is set to a non-zero value, a separate window is used for each group of values of the field specified in the by clause. Syntax: global=<boolean>. **Default:** true.
* reset_before: optional. Before streamstats calculates for an event, reset_before resets all accumulated statistics when the eval-expression evaluates to true. If used with window, the window is also reset. Syntax: reset_before="("<eval-expression>")". **Default:** false.
* reset_after: optional. After streamstats calculations for an event, reset_after resets all accumulated statistics when the eval-expression evaluates to true. This expression can reference fields returned by streamstats. If used with window, the window is also reset. Syntax: reset_after="("<eval-expression>")". **Default:** false.
* by-clause: optional. The by clause could be the fields and expressions like scalar functions and aggregation functions. Besides, the span clause can be used to split specific field into buckets in the same interval, the stats then does the aggregation by these span buckets. Syntax: by [span-expression,] [field,]... **Default:** If no <by-clause> is specified, all events are processed as a single group and running statistics are computed across the entire event stream.
* span-expression: optional, at most one. Splits field into buckets by intervals. Syntax: span(field_expr, interval_expr). For example, ``span(age, 10)`` creates 10-year age buckets, ``span(timestamp, 1h)`` creates hourly buckets.
  * Available time units:
    * millisecond (ms)
    * second (s)
    * minute (m, case sensitive)
    * hour (h)
    * day (d)
    * week (w)
    * month (M, case sensitive)
    * quarter (q)
    * year (y)

Aggregation Functions
=====================

The streamstats command supports the following aggregation functions:

* COUNT: Count of values
* SUM: Sum of numeric values
* AVG: Average of numeric values
* MAX: Maximum value
* MIN: Minimum value
* VAR_SAMP: Sample variance
* VAR_POP: Population variance
* STDDEV_SAMP: Sample standard deviation
* STDDEV_POP: Population standard deviation
* DISTINCT_COUNT/DC: Distinct count of values
* EARLIEST: Earliest value by timestamp
* LATEST: Latest value by timestamp

For detailed documentation of each function, see `Aggregation Functions <../functions/aggregation.rst>`_.

Usage
=====

Streamstats::

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


Example 1: Calculate the running average, sum, and count of a field by group
============================================================================

This example calculates the running average age, running sum of age, and running count of events for all the accounts, grouped by gender.

PPL query::

    os> source=accounts | streamstats avg(age) as running_avg, sum(age) as running_sum, count() as running_count by gender;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+-------------+---------------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | running_avg        | running_sum | running_count |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+-------------+---------------|
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 32.0               | 32          | 1             |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 34.0               | 68          | 2             |
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | 28.0               | 28          | 1             |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 33.666666666666664 | 101         | 3             |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+-------------+---------------+


Example 2: Running maximum age over a 2-row window
==================================================

This example calculates the running maximum age over a 2-row window, excluding the current event.

PPL query::

    os> source=state_country | streamstats current=false window=2 max(age) as prev_max_age
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


Example 3: Use the global argument to calculate running statistics
==================================================================

The global argument is only applicable when a window argument is set. It defines how the window is applied in relation to the grouping fields:

* global=true: a global window is applied across all rows, but the calculations inside the window still respect the by groups.
* global=false: the window itself is created per group, meaning each group gets its own independent window.

This example shows how to calculate the running average of age across accounts by country, using global argument.

original data::

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

* global=true: The window slides across all rows globally (following their input order), but inside each window, aggregation is still computed by country. So we process the data stream row by row to build the sliding window with size 2. We can see that David and Rick are in a window.
* global=false: Each by group (country) forms its own independent stream and window (size 2). So David and Hello are in one window for USA. This time we get running_avg 35 for David, rather than 40 when global is set true.

PPL query::

    os> source=state_country | streamstats window=2 global=true avg(age) as running_avg by country ;
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

    os> source=state_country | streamstats window=2 global=false avg(age) as running_avg by country ;
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


Example 4: Use the reset_before and reset_after arguments to reset statistics
=============================================================================

This example calculates the running average of age across accounts by country, with resets applied.

PPL query::

    os> source=state_country | streamstats current=false reset_before=age>34 reset_after=age<25 avg(age) as avg_age by country;
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