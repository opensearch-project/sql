=============
eventstats
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| (Experimental)
| (From 3.1.0)
| Using ``eventstats`` command to enriches your event data with calculated summary statistics. It operates by analyzing specified fields within your events, computing various statistical measures, and then appending these results as new fields to each original event.

| Key aspects of `eventstats`:

1. It performs calculations across the entire result set or within defined groups.
2. The original events remain intact, with new fields added to contain the statistical results.
3. The command is particularly useful for comparative analysis, identifying outliers, or providing additional context to individual events.

| Difference between ``stats`` and ``eventstats``
The ``stats`` and ``eventstats`` commands are both used for calculating statistics, but they have some key differences in how they operate and what they produce:

* Output Format:
 * ``stats``: Produces a summary table with only the calculated statistics.
 * ``eventstats``: Adds the calculated statistics as new fields to the existing events, preserving the original data.
* Event Retention:
 * ``stats``: Reduces the result set to only the statistical summary, discarding individual events.
 * ``eventstats``: Retains all original events and adds new fields with the calculated statistics.
* Use Cases:
 * ``stats``: Best for creating summary reports or dashboards. Often used as a final command to summarize results.
 * ``eventstats``: Useful when you need to enrich events with statistical context for further analysis or filtering. Can be used mid-search to add statistics that can be used in subsequent commands.


Version
=======
3.1.0


Syntax
======
eventstats [bucket_nullable=bool] <function>... [by-clause]


* function: mandatory. A aggregation function or window function.

* bucket_nullable: optional. Controls whether the eventstats command consider null buckets as a valid group in group-by aggregations. When set to ``false``, it will not treat null group-by values as a distinct group during aggregation. **Default:** Determined by ``plugins.ppl.syntax.legacy.preferred``.

 * When ``plugins.ppl.syntax.legacy.preferred=true``, ``bucket_nullable`` defaults to ``true``
 * When ``plugins.ppl.syntax.legacy.preferred=false``, ``bucket_nullable`` defaults to ``false``

* by-clause: optional.

 * Syntax: by [span-expression,] [field,]...
 * Description: The by clause could be the fields and expressions like scalar functions and aggregation functions. Besides, the span clause can be used to split specific field into buckets in the same interval, the stats then does the aggregation by these span buckets.
 * Default: If no <by-clause> is specified, the stats command returns only one row, which is the aggregation over the entire result set.

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

    os> source=accounts | fields account_number, gender, age | eventstats count() | sort account_number;
    fetched rows / total rows = 4/4
    +----------------+--------+-----+---------+
    | account_number | gender | age | count() |
    |----------------+--------+-----+---------|
    | 1              | M      | 32  | 4       |
    | 6              | M      | 36  | 4       |
    | 13             | F      | 28  | 4       |
    | 18             | M      | 33  | 4       |
    +----------------+--------+-----+---------+

SUM
---

Description
>>>>>>>>>>>

Usage: SUM(expr). Returns the sum of expr.

Example::

    os> source=accounts | fields account_number, gender, age | eventstats sum(age) by gender | sort account_number;
    fetched rows / total rows = 4/4
    +----------------+--------+-----+----------+
    | account_number | gender | age | sum(age) |
    |----------------+--------+-----+----------|
    | 1              | M      | 32  | 101      |
    | 6              | M      | 36  | 101      |
    | 13             | F      | 28  | 28       |
    | 18             | M      | 33  | 101      |
    +----------------+--------+-----+----------+

AVG
---

Description
>>>>>>>>>>>

Usage: AVG(expr). Returns the average value of expr.

Example::

    os> source=accounts | fields account_number, gender, age | eventstats avg(age) by gender | sort account_number;
    fetched rows / total rows = 4/4
    +----------------+--------+-----+--------------------+
    | account_number | gender | age | avg(age)           |
    |----------------+--------+-----+--------------------|
    | 1              | M      | 32  | 33.666666666666664 |
    | 6              | M      | 36  | 33.666666666666664 |
    | 13             | F      | 28  | 28.0               |
    | 18             | M      | 33  | 33.666666666666664 |
    +----------------+--------+-----+--------------------+

MAX
---

Description
>>>>>>>>>>>

Usage: MAX(expr). Returns the maximum value of expr.

Example::

    os> source=accounts | fields account_number, gender, age | eventstats max(age) | sort account_number;
    fetched rows / total rows = 4/4
    +----------------+--------+-----+----------+
    | account_number | gender | age | max(age) |
    |----------------+--------+-----+----------|
    | 1              | M      | 32  | 36       |
    | 6              | M      | 36  | 36       |
    | 13             | F      | 28  | 36       |
    | 18             | M      | 33  | 36       |
    +----------------+--------+-----+----------+

MIN
---

Description
>>>>>>>>>>>

Usage: MIN(expr). Returns the minimum value of expr.

Example::

    os> source=accounts | fields account_number, gender, age | eventstats min(age) by gender | sort account_number;
    fetched rows / total rows = 4/4
    +----------------+--------+-----+----------+
    | account_number | gender | age | min(age) |
    |----------------+--------+-----+----------|
    | 1              | M      | 32  | 32       |
    | 6              | M      | 36  | 32       |
    | 13             | F      | 28  | 28       |
    | 18             | M      | 33  | 32       |
    +----------------+--------+-----+----------+


VAR_SAMP
--------

Description
>>>>>>>>>>>

Usage: VAR_SAMP(expr). Returns the sample variance of expr.

Example::

    os> source=accounts | fields account_number, gender, age | eventstats var_samp(age) | sort account_number;
    fetched rows / total rows = 4/4
    +----------------+--------+-----+--------------------+
    | account_number | gender | age | var_samp(age)      |
    |----------------+--------+-----+--------------------|
    | 1              | M      | 32  | 10.916666666666666 |
    | 6              | M      | 36  | 10.916666666666666 |
    | 13             | F      | 28  | 10.916666666666666 |
    | 18             | M      | 33  | 10.916666666666666 |
    +----------------+--------+-----+--------------------+


VAR_POP
-------

Description
>>>>>>>>>>>

Usage: VAR_POP(expr). Returns the population standard variance of expr.

Example::

    os> source=accounts | fields account_number, gender, age | eventstats var_pop(age) | sort account_number;
    fetched rows / total rows = 4/4
    +----------------+--------+-----+--------------+
    | account_number | gender | age | var_pop(age) |
    |----------------+--------+-----+--------------|
    | 1              | M      | 32  | 8.1875       |
    | 6              | M      | 36  | 8.1875       |
    | 13             | F      | 28  | 8.1875       |
    | 18             | M      | 33  | 8.1875       |
    +----------------+--------+-----+--------------+

STDDEV_SAMP
-----------

Description
>>>>>>>>>>>

Usage: STDDEV_SAMP(expr). Return the sample standard deviation of expr.

Example::

    os> source=accounts | fields account_number, gender, age | eventstats stddev_samp(age) | sort account_number;
    fetched rows / total rows = 4/4
    +----------------+--------+-----+-------------------+
    | account_number | gender | age | stddev_samp(age)  |
    |----------------+--------+-----+-------------------|
    | 1              | M      | 32  | 3.304037933599835 |
    | 6              | M      | 36  | 3.304037933599835 |
    | 13             | F      | 28  | 3.304037933599835 |
    | 18             | M      | 33  | 3.304037933599835 |
    +----------------+--------+-----+-------------------+


STDDEV_POP
----------

Description
>>>>>>>>>>>

Usage: STDDEV_POP(expr). Return the population standard deviation of expr.

Example::

    os> source=accounts | fields account_number, gender, age | eventstats stddev_pop(age) | sort account_number;
    fetched rows / total rows = 4/4
    +----------------+--------+-----+--------------------+
    | account_number | gender | age | stddev_pop(age)    |
    |----------------+--------+-----+--------------------|
    | 1              | M      | 32  | 2.8613807855648994 |
    | 6              | M      | 36  | 2.8613807855648994 |
    | 13             | F      | 28  | 2.8613807855648994 |
    | 18             | M      | 33  | 2.8613807855648994 |
    +----------------+--------+-----+--------------------+


DISTINCT_COUNT, DC(Since 3.3)
------------------

Description
>>>>>>>>>>>

Usage: DISTINCT_COUNT(expr), DC(expr). Returns the approximate number of distinct values of expr using HyperLogLog++ algorithm. Both ``DISTINCT_COUNT`` and ``DC`` are equivalent and provide the same functionality.

Example::

    os> source=accounts | fields account_number, gender, state, age | eventstats dc(state) as distinct_states, distinct_count(state) as dc_states_alt by gender | sort account_number;
    fetched rows / total rows = 4/4
    +----------------+--------+-------+-----+-----------------+---------------+
    | account_number | gender | state | age | distinct_states | dc_states_alt |
    |----------------+--------+-------+-----+-----------------+---------------|
    | 1              | M      | IL    | 32  | 3               | 3             |
    | 6              | M      | TN    | 36  | 3               | 3             |
    | 13             | F      | VA    | 28  | 1               | 1             |
    | 18             | M      | MD    | 33  | 3               | 3             |
    +----------------+--------+-------+-----+-----------------+---------------+

EARLIEST (Since 3.3)
---------------------

Description
>>>>>>>>>>>

Usage: EARLIEST(field [, time_field]). Return the earliest value of a field based on timestamp ordering. This function enriches each event with the earliest value found within the specified grouping.

* field: mandatory. The field to return the earliest value for.
* time_field: optional. The field to use for time-based ordering. Defaults to @timestamp if not specified.

Note: This function requires Calcite to be enabled (see `Configuration`_ section above).

Example::

    os> source=events | fields @timestamp, host, message | eventstats earliest(message) by host | sort @timestamp;
    fetched rows / total rows = 8/8
    +---------------------+---------+----------------------+-------------------+
    | @timestamp          | host    | message              | earliest(message) |
    |---------------------+---------+----------------------+-------------------|
    | 2023-01-01 10:00:00 | server1 | Starting up          | Starting up       |
    | 2023-01-01 10:05:00 | server2 | Initializing         | Initializing      |
    | 2023-01-01 10:10:00 | server1 | Ready to serve       | Starting up       |
    | 2023-01-01 10:15:00 | server2 | Ready                | Initializing      |
    | 2023-01-01 10:20:00 | server1 | Processing requests  | Starting up       |
    | 2023-01-01 10:25:00 | server2 | Handling connections | Initializing      |
    | 2023-01-01 10:30:00 | server1 | Shutting down        | Starting up       |
    | 2023-01-01 10:35:00 | server2 | Maintenance mode     | Initializing      |
    +---------------------+---------+----------------------+-------------------+

Example with custom time field::

    os> source=events | fields event_time, status, category | eventstats earliest(status, event_time) by category | sort event_time;
    fetched rows / total rows = 8/8
    +---------------------+------------+----------+------------------------------+
    | event_time          | status     | category | earliest(status, event_time) |
    |---------------------+------------+----------+------------------------------|
    | 2023-01-01 09:55:00 | pending    | orders   | pending                      |
    | 2023-01-01 10:00:00 | active     | users    | active                       |
    | 2023-01-01 10:05:00 | processing | orders   | pending                      |
    | 2023-01-01 10:10:00 | inactive   | users    | active                       |
    | 2023-01-01 10:15:00 | completed  | orders   | pending                      |
    | 2023-01-01 10:20:00 | pending    | users    | active                       |
    | 2023-01-01 10:25:00 | cancelled  | orders   | pending                      |
    | 2023-01-01 10:30:00 | inactive   | users    | active                       |
    +---------------------+------------+----------+------------------------------+


LATEST (Since 3.3)
-------------------

Description
>>>>>>>>>>>

Usage: LATEST(field [, time_field]). Return the latest value of a field based on timestamp ordering. This function enriches each event with the latest value found within the specified grouping.

* field: mandatory. The field to return the latest value for.
* time_field: optional. The field to use for time-based ordering. Defaults to @timestamp if not specified.

Note: This function requires Calcite to be enabled (see `Configuration`_ section above).

Example::

    os> source=events | fields @timestamp, host, message | eventstats latest(message) by host | sort @timestamp;
    fetched rows / total rows = 8/8
    +---------------------+---------+----------------------+------------------+
    | @timestamp          | host    | message              | latest(message)  |
    |---------------------+---------+----------------------+------------------|
    | 2023-01-01 10:00:00 | server1 | Starting up          | Shutting down    |
    | 2023-01-01 10:05:00 | server2 | Initializing         | Maintenance mode |
    | 2023-01-01 10:10:00 | server1 | Ready to serve       | Shutting down    |
    | 2023-01-01 10:15:00 | server2 | Ready                | Maintenance mode |
    | 2023-01-01 10:20:00 | server1 | Processing requests  | Shutting down    |
    | 2023-01-01 10:25:00 | server2 | Handling connections | Maintenance mode |
    | 2023-01-01 10:30:00 | server1 | Shutting down        | Shutting down    |
    | 2023-01-01 10:35:00 | server2 | Maintenance mode     | Maintenance mode |
    +---------------------+---------+----------------------+------------------+

Example with custom time field::

    os> source=events | fields event_time, status message, category | eventstats latest(status, event_time) by category | sort event_time;
    fetched rows / total rows = 8/8
    +---------------------+------------+----------------------+----------+----------------------------+
    | event_time          | status     | message              | category | latest(status, event_time) |
    |---------------------+------------+----------------------+----------+----------------------------|
    | 2023-01-01 09:55:00 | pending    | Starting up          | orders   | cancelled                  |
    | 2023-01-01 10:00:00 | active     | Initializing         | users    | inactive                   |
    | 2023-01-01 10:05:00 | processing | Ready to serve       | orders   | cancelled                  |
    | 2023-01-01 10:10:00 | inactive   | Ready                | users    | inactive                   |
    | 2023-01-01 10:15:00 | completed  | Processing requests  | orders   | cancelled                  |
    | 2023-01-01 10:20:00 | pending    | Handling connections | users    | inactive                   |
    | 2023-01-01 10:25:00 | cancelled  | Shutting down        | orders   | cancelled                  |
    | 2023-01-01 10:30:00 | inactive   | Maintenance mode     | users    | inactive                   |
    +---------------------+------------+----------------------+----------+----------------------------+


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

Eventstats::

    source = table | eventstats avg(a)
    source = table | where a < 50 | eventstats count(c)
    source = table | eventstats min(c), max(c) by b
    source = table | eventstats count(c) as count_by by b | where count_by > 1000
    source = table | eventstats dc(field) as distinct_count
    source = table | eventstats distinct_count(category) by region


Example 1: Calculate the average, sum and count of a field by group
==================================================================

The example show calculate the average age, sum age and count of events of all the accounts group by gender.

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

The example gets the count of age by the interval of 10 years and group by gender.

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

Example 3: Null buckets handling
================================

PPL query::

    os> source=accounts | eventstats bucket_nullable=false count() as cnt by employer | fields account_number, firstname, employer, cnt | sort account_number;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------+------+
    | account_number | firstname | employer | cnt  |
    |----------------+-----------+----------+------|
    | 1              | Amber     | Pyrami   | 1    |
    | 6              | Hattie    | Netagy   | 1    |
    | 13             | Nanette   | Quility  | 1    |
    | 18             | Dale      | null     | null |
    +----------------+-----------+----------+------+

PPL query::

    os> source=accounts | eventstats bucket_nullable=true count() as cnt by employer | fields account_number, firstname, employer, cnt | sort account_number;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------+-----+
    | account_number | firstname | employer | cnt |
    |----------------+-----------+----------+-----|
    | 1              | Amber     | Pyrami   | 1   |
    | 6              | Hattie    | Netagy   | 1   |
    | 13             | Nanette   | Quility  | 1   |
    | 18             | Dale      | null     | 1   |
    +----------------+-----------+----------+-----+
