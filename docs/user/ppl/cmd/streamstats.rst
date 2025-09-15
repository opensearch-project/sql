=============
streamstats
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``streamstats`` command is used to calculate cumulative or rolling statistics as events are processed in order. Unlike ``stats`` or ``eventstats`` which operate on the entire dataset at once, it computes values incrementally on a per-event basis, often respecting the order of events in the search results. It allows you to generate running totals, moving averages, and other statistics that evolve with the stream of events.

| Key aspects of `streamstats`:

1. It computes statistics incrementally as each event is processed, making it suitable for time-series and sequence-based analysis.
2. Supports arguments such as window (for sliding window calculations) and current (to control whether the current event included in calculation).
3. Retains all original events and appends new fields containing the calculated statistics.
4. Particularly useful for calculating running totals, identifying trends, or detecting changes over sequences of events.

| Difference between ``stats``, ``eventstats`` and ``streamstats``
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

Version
=======
3.3.0


Syntax
======
streamstats [current=<bool>] [window=<int>] <function>... [by-clause]


* function: mandatory. A aggregation function or window function.

* current: optional.

 * Syntax: current=<boolean>
 * Description: If true, the search includes the given, or current, event in the summary calculations. If false, the search uses the field value from the previous event.
 * Default: true

* window: optional.

 * Syntax: window=<integer>
 * Description: Specifies the number of events to use when computing the statistics.
 * Default: 0, which means that all previous and current events are used.

* by-clause: optional.

 * Syntax: by [span-expression,] [field,]...
 * Description: The by clause could be the fields and expressions like scalar functions and aggregation functions. Besides, the span clause can be used to split specific field into buckets in the same interval, the stats then does the aggregation by these span buckets.
 * Default: If no <by-clause> is specified, all events are processed as a single group and running statistics are computed across the entire event stream.

* span-expression: optional, at most one.

 * Syntax: span(field_expr, interval_expr)
 * Description: The unit of the interval expression is the natural unit by default. If the field is a date and time type field, and the interval is in date/time units, you will need to specify the unit in the interval expression. For example, to split the field ``age`` into buckets by 10 years, it looks like ``span(age, 10)``. And here is another example of time span, the span to split a ``timestamp`` field into hourly intervals, it looks like ``span(timestamp, 1h)``.

* Available time unit:
+----------------------------+
| Span Interval Units        |
+============================+
| millisecond (ms)           |
+----------------------------+
| second (s)                 |
+----------------------------+
| minute (m, case sensitive) |
+----------------------------+
| hour (h)                   |
+----------------------------+
| day (d)                    |
+----------------------------+
| week (w)                   |
+----------------------------+
| month (M, case sensitive)  |
+----------------------------+
| quarter (q)                |
+----------------------------+
| year (y)                   |
+----------------------------+

Aggregation Functions
=====================
COUNT
-----

Description
>>>>>>>>>>>

Usage: Returns a count of the number of expr in the rows retrieved by a SELECT statement.

Example::

    PPL> source=accounts | streamstats count();
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+---------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | count() |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+---------|
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 1       |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 2       |
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | 3       |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 4       |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+---------+

SUM
---

Description
>>>>>>>>>>>

Usage: SUM(expr). Returns the sum of expr.

Example::

    PPL> source=accounts | streamstats sum(age) by gender;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+----------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | sum(age) |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+----------|
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | 28       |
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 32       |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 68       |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 101      |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+----------+

AVG
---

Description
>>>>>>>>>>>

Usage: AVG(expr). Returns the average value of expr.

Example::

    PPL> source=accounts | streamstats avg(age) by gender;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | avg(age)           |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------|
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | 28.0               |
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 32.0               |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 34.0               |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 33.666666666666664 |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+

MAX
---

Description
>>>>>>>>>>>

Usage: MAX(expr). Returns the maximum value of expr.

Example::

    PPL> source=accounts | streamstats max(age);
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+----------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | max(age) |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+----------|
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 32       |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 36       |
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | 36       |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 36       |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+----------+

MIN
---

Description
>>>>>>>>>>>

Usage: MIN(expr). Returns the minimum value of expr.

Example::

    PPL> source=accounts | streamstats min(age) by gender;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+----------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | min(age) |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+----------|
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | 28       |
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 32       |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 32       |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 32       |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+----------+


VAR_SAMP
--------

Description
>>>>>>>>>>>

Usage: VAR_SAMP(expr). Returns the sample variance of expr.

Example::

    PPL> source=accounts | streamstats var_samp(age);
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | var_samp(age)      |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------|
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | null               |
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 8                  |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 16                 |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 10.916666666666666 |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+


VAR_POP
-------

Description
>>>>>>>>>>>

Usage: VAR_POP(expr). Returns the population standard variance of expr.

Example::

    PPL> source=accounts | streamstats var_pop(age);
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | var_pop(age)       |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------|
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | 0                  |
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 4                  |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 10.666666666666666 |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 8.1875             |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+


STDDEV_SAMP
-----------

Description
>>>>>>>>>>>

Usage: STDDEV_SAMP(expr). Return the sample standard deviation of expr.

Example::

    PPL> source=accounts | streamstats stddev_samp(age);
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | stddev_samp(age)   |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------|
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | null               |
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 2.8284271247461903 |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 4                  |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 3.304037933599835  |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+


STDDEV_POP
----------

Description
>>>>>>>>>>>

Usage: STDDEV_POP(expr). Return the population standard deviation of expr.

Example::

    PPL> source=accounts | streamstats stddev_pop(age);
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | stddev_pop(age)    |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------|
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | 0                  |
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 2                  |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 3.265986323710904  |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 2.8613807855648994 |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+


DISTINCT_COUNT, DC
------------------

Description
>>>>>>>>>>>

Usage: DISTINCT_COUNT(expr), DC(expr). Returns the approximate number of distinct values using the HyperLogLog++ algorithm. Both functions are equivalent.

For details on algorithm accuracy and precision control, see the `OpenSearch Cardinality Aggregation documentation <https://docs.opensearch.org/latest/aggregations/metric/cardinality/#controlling-precision>`_.


Example::

    PPL> source=accounts | streamstats dc(state) as distinct_states, distinct_count(state) as dc_states_alt by gender;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+-----------------+-----------------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | distinct_states | dc_states_alt   |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+-----------------|-----------------|
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | 1               | 1               |
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 1               | 1               |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 2               | 2               |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 3               | 3               |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+-----------------+-----------------+


Configuration
=============
This command requires Calcite enabled.

Enable Calcite::

	>> curl -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_query/settings -d '{
	  "transient" : {
	    "plugins.calcite.enabled" : true
	  }
	}'

Result set::

    {
      "acknowledged": true,
      "persistent": {
        "plugins": {
          "calcite": {
            "enabled": "true"
          }
        }
      },
      "transient": {}
    }

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


Example 1: Calculate the running average, sum, and count of a field by group
==================================================================

This example calculates the running average age, running sum of age, and running count of events for all the accounts, grouped by gender.

PPL query::

    PPL> source=accounts | streamstats avg(age) as running_avg, sum(age) as running_sum, count() as running_count by gender;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+-------------+---------------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | running_avg        | running_sum | running_count |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+-------------+---------------|
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | 28.0               | 28          | 1             |
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 32.0               | 32          | 1             |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 34.0               | 68          | 2             |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 33.666666666666664 | 101         | 3             |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+--------------------+-------------+---------------+


Example 2: Running maximum temperature over a 2-row window
===================================================

This example calculates the running maximum temperature over a 2-row window, excluding the current event.

PPL query::

    PPL> source=test_temperature | streamstats current=false window=2 max(temperature) as prev_max_temp
    fetched rows / total rows = 5/5
    +----+------------+-------------+---------------+
    | id | timestamp  | temperature | prev_max_temp |
    |----+------------+-------------+---------------|
    | 1  | 2025-09-01 | 20          | null          |
    | 2  | 2025-09-02 | 25          | 20            |
    | 3  | 2025-09-03 | 22          | 25            |
    | 4  | 2025-09-04 | 30          | 25            |
    | 5  | 2025-09-05 | 28          | 30            |
    +----+------------+-------------+---------------+
