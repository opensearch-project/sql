==========
eventstats
==========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
===========
| The ``eventstats`` command enriches your event data with calculated summary statistics. It operates by analyzing specified fields within your events, computing various statistical measures, and then appending these results as new fields to each original event.

| Key aspects of `eventstats`:

1. It performs calculations across the entire result set or within defined groups.
2. The original events remain intact, with new fields added to contain the statistical results.
3. The command is particularly useful for comparative analysis, identifying outliers, or providing additional context to individual events.

| Difference between ``stats`` and ``eventstats``
The ``stats`` and ``eventstats`` commands are both used for calculating statistics, but they have some key differences in how they operate and what they produce:

* Output Format

     - ``stats``: Produces a summary table with only the calculated statistics.
     - ``eventstats``: Adds the calculated statistics as new fields to the existing events, preserving the original data.

* Event Retention

     - ``stats``: Reduces the result set to only the statistical summary, discarding individual events.
     - ``eventstats``: Retains all original events and adds new fields with the calculated statistics.

* Use Cases

     - ``stats``: Best for creating summary reports or dashboards. Often used as a final command to summarize results.
     - ``eventstats``: Useful when you need to enrich events with statistical context for further analysis or filtering. Can be used mid-search to add statistics that can be used in subsequent commands.


Syntax
======
eventstats <function>... [by-clause]

* function: mandatory. An aggregation function or window function.
* by-clause: optional. Groups results by specified fields or expressions. Syntax: by [span-expression,] [field,]... **Default:** aggregation over the entire result set.
* span-expression: optional, at most one. Splits field into buckets by intervals. Syntax: span(field_expr, interval_expr). For example, ``span(age, 10)`` creates 10-year age buckets, ``span(timestamp, 1h)`` creates hourly buckets.

    - Available time units

        - millisecond (ms)
        - second (s)
        - minute (m, case sensitive)
        - hour (h)
        - day (d)
        - week (w)
        - month (M, case sensitive)
        - quarter (q)
        - year (y)

Aggregation Functions
=====================

The eventstats command supports the following aggregation functions:

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

Eventstats::

    source = table | eventstats avg(a)
    source = table | where a < 50 | eventstats count(c)
    source = table | eventstats min(c), max(c) by b
    source = table | eventstats count(c) as count_by by b | where count_by > 1000
    source = table | eventstats dc(field) as distinct_count
    source = table | eventstats distinct_count(category) by region


Example 1: Calculate the average, sum and count of a field by group
===================================================================

This example shows calculating the average age, sum of age, and count of events for all accounts grouped by gender.

PPL query::

    os> source=accounts | fields account_number, gender, age | eventstats avg(age), sum(age), count() by gender | sort account_number;
    fetched rows / total rows = 4/4
    +----------------+--------+-----+--------------------+----------+---------+
    | account_number | gender | age | avg(age)           | sum(age) | count() |
    |----------------+--------+-----+--------------------+----------+---------|
    | 1              | M      | 32  | 33.666666666666664 | 101      | 3       |
    | 6              | M      | 36  | 33.666666666666664 | 101      | 3       |
    | 13             | F      | 28  | 28.0               | 28       | 1       |
    | 18             | M      | 33  | 33.666666666666664 | 101      | 3       |
    +----------------+--------+-----+--------------------+----------+---------+

Example 2: Calculate the count by a gender and span
===================================================

This example shows counting events by age intervals of 5 years, grouped by gender.

PPL query::

    os> source=accounts | fields account_number, gender, age | eventstats count() as cnt by span(age, 5) as age_span, gender | sort account_number;
    fetched rows / total rows = 4/4
    +----------------+--------+-----+-----+
    | account_number | gender | age | cnt |
    |----------------+--------+-----+-----|
    | 1              | M      | 32  | 2   |
    | 6              | M      | 36  | 1   |
    | 13             | F      | 28  | 1   |
    | 18             | M      | 33  | 2   |
    +----------------+--------+-----+-----+
