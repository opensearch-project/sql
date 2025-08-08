=============
timechart
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``timechart`` command creates a time-based visualization of aggregated data. It groups data by time intervals and optionally by a field, then applies an aggregation function to each group.

Version
=======
3.3.0

Syntax
============
timechart [span=<time_interval>] [limit=<number>] <aggregation_function> [by <field>]

* span: optional. Specifies the time interval for grouping data.
  * Default: 1m (1 minute)
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

* limit: optional. Specifies the maximum number of distinct values to display when using the "by" clause.
  * Default: 10
  * When there are more distinct values than the limit, the additional values are grouped into an "OTHER" category.

* aggregation_function: mandatory. The aggregation function to apply to each time bucket.
  * Currently, only a single aggregation function is supported.
  * Available functions: count(), avg(), sum(), min(), max()

* by: optional. Groups the results by the specified field in addition to time intervals.
  * If not specified, the aggregation is performed across all documents in each time interval.

Notes
============
* The ``timechart`` command requires a timestamp field named ``@timestamp`` in the data.
* The ``bins`` parameter is not implemented yet. Use ``span`` to control the time interval.
* Only a single aggregation function is supported in the current implementation.

Example 1: Count events by hour
==============================

This example counts events for each hour and groups them by host.

PPL query::

    source=events | timechart span=1h count() by host

Result::

    +---------------------+----------+----------+-------+-------+-------+--------+--------+--------+
    | $f2                 | cache-01 | cache-02 | db-01 | db-02 | lb-01 | web-01 | web-02 | web-03 |
    +---------------------+----------+----------+-------+-------+-------+--------+--------+--------+
    | 2024-07-01 00:00:00 | 1        | 1        | 1     | 1     | 1     | 6      | 5      | 5      |
    +---------------------+----------+----------+-------+-------+-------+--------+--------+--------+

Example 2: Count events by minute
================================

This example counts events for each minute and groups them by host.

PPL query::

    source=events | timechart span=1m count() by host

Result (partial)::

    +---------------------+----------+----------+-------+-------+-------+--------+--------+--------+
    | $f2                 | cache-01 | cache-02 | db-01 | db-02 | lb-01 | web-01 | web-02 | web-03 |
    +---------------------+----------+----------+-------+-------+-------+--------+--------+--------+
    | 2024-07-01 00:00:00 | null     | null     | null  | null  | null  | 1      | null   | null   |
    | 2024-07-01 00:01:00 | null     | null     | null  | null  | null  | null   | 1      | null   |
    | 2024-07-01 00:02:00 | null     | null     | null  | null  | null  | 1      | null   | null   |
    | ...                 | ...      | ...      | ...   | ...   | ...   | ...    | ...    | ...    |
    +---------------------+----------+----------+-------+-------+-------+--------+--------+--------+

Example 3: Calculate average CPU usage by minute
==============================================

This example calculates the average CPU usage for each minute without grouping by any field.

PPL query::

    source=events | timechart span=1m avg(cpu_usage)

Example 4: Count events by second and region
==========================================

This example counts events for each second and groups them by region.

PPL query::

    source=events | timechart span=1s count() by region

Result (partial)::

    +---------------------+----------+----------+---------+
    | $f2                 | eu-west  | us-east  | us-west |
    +---------------------+----------+----------+---------+
    | 2024-07-01 00:00:00 | null     | 1        | null    |
    | 2024-07-01 00:01:00 | null     | null     | 1       |
    | 2024-07-01 00:02:00 | null     | 1        | null    |
    | ...                 | ...      | ...      | ...     |
    +---------------------+----------+----------+---------+

Example 5: Using the limit parameter
==================================

When there are many distinct values in the "by" field, the timechart command will display the top values based on the limit parameter and group the rest into an "OTHER" category.

PPL query::

    source=events_many_hosts | timechart span=1h limit=5 avg(cpu_usage) by host

This query will display the top 5 hosts with the highest CPU usage values, and group the remaining hosts into an "OTHER" category.
