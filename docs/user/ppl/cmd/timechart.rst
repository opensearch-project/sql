=========
timechart
=========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
===========
| The ``timechart`` command creates a time-based aggregation of data. It groups data by time intervals and optionally by a field, then applies an aggregation function to each group. The results are returned in an unpivoted format with separate rows for each time-field combination.

Syntax
======

timechart [span=<time_interval>] [limit=<number>] [useother=<boolean>] <aggregation_function> [by <field>]

* span: optional. Specifies the time interval for grouping data. **Default:** 1m (1 minute).

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

* limit: optional. Specifies the maximum number of distinct values to display when using the "by" clause. **Default:** 10.

  * When there are more distinct values than the limit, the additional values are grouped into an "OTHER" category if useother is not set to false.
  * The "most distinct" values are determined by calculating the sum of the aggregation values across all time intervals for each distinct field value. The top N values with the highest sums are displayed individually, while the rest are grouped into the "OTHER" category.
  * Set to 0 to show all distinct values without any limit (when limit=0, useother is automatically set to false).
  * The parameters can be specified in any order before the aggregation function.
  * Only applies when using the "by" clause to group results.

* useother: optional. Controls whether to create an "OTHER" category for values beyond the limit. **Default:** true.

  * When set to false, only the top N values (based on limit) are shown without an "OTHER" column.
  * When set to true, values beyond the limit are grouped into an "OTHER" category.
  * Only applies when using the "by" clause and when there are more distinct values than the limit.

* aggregation_function: mandatory. The aggregation function to apply to each time bucket.

  * Currently, only a single aggregation function is supported.
  * Available functions: All aggregation functions supported by the :doc:`stats <stats>` command, as well as the timechart-specific aggregations listed below.

* by: optional. Groups the results by the specified field in addition to time intervals. If not specified, the aggregation is performed across all documents in each time interval.

PER_SECOND
----------

Usage: per_second(field) calculates the per-second rate for a numeric field within each time bucket.

The calculation formula is: `per_second(field) = sum(field) / span_in_seconds`, where `span_in_seconds` is the span interval in seconds.

Return type: DOUBLE

PER_MINUTE
----------

Usage: per_minute(field) calculates the per-minute rate for a numeric field within each time bucket.

The calculation formula is: `per_minute(field) = sum(field) * 60 / span_in_seconds`, where `span_in_seconds` is the span interval in seconds.

Return type: DOUBLE

PER_HOUR
--------

Usage: per_hour(field) calculates the per-hour rate for a numeric field within each time bucket.

The calculation formula is: `per_hour(field) = sum(field) * 3600 / span_in_seconds`, where `span_in_seconds` is the span interval in seconds.

Return type: DOUBLE

PER_DAY
-------

Usage: per_day(field) calculates the per-day rate for a numeric field within each time bucket.

The calculation formula is: `per_day(field) = sum(field) * 86400 / span_in_seconds`, where `span_in_seconds` is the span interval in seconds.

Return type: DOUBLE

Notes
=====

* The ``timechart`` command requires a timestamp field named ``@timestamp`` in the data.
* Results are returned in an unpivoted format with separate rows for each time-field combination that has data.
* Only combinations with actual data are included in the results - empty combinations are omitted rather than showing null or zero values.
* The "top N" values for the ``limit`` parameter are selected based on the sum of values across all time intervals for each distinct field value.
* When using the ``limit`` parameter, values beyond the limit are grouped into an "OTHER" category (unless ``useother=false``).
* Examples 6 and 7 use different datasets: Example 6 uses the ``events`` dataset with fewer hosts for simplicity, while Example 7 uses the ``events_many_hosts`` dataset with 11 distinct hosts.

* **Null values**: Documents with null values in the "by" field are treated as a separate category and appear as null in the results.

Example 1: Count events by hour
===============================

This example counts events for each hour and groups them by host.

PPL query::

    os> source=events | timechart span=1h count() by host
    fetched rows / total rows = 2/2
    +---------------------+---------+---------+
    | @timestamp          | host    | count() |
    |---------------------+---------+---------|
    | 2023-01-01 10:00:00 | server1 | 4       |
    | 2023-01-01 10:00:00 | server2 | 4       |
    +---------------------+---------+---------+

Example 2: Count events by minute
==========================================================

This example counts events for each minute and groups them by host.

PPL query::

    os> source=events | timechart span=1m count() by host
    fetched rows / total rows = 8/8
    +---------------------+---------+---------+
    | @timestamp          | host    | count() |
    |---------------------+---------+---------|
    | 2023-01-01 10:00:00 | server1 | 1       |
    | 2023-01-01 10:05:00 | server2 | 1       |
    | 2023-01-01 10:10:00 | server1 | 1       |
    | 2023-01-01 10:15:00 | server2 | 1       |
    | 2023-01-01 10:20:00 | server1 | 1       |
    | 2023-01-01 10:25:00 | server2 | 1       |
    | 2023-01-01 10:30:00 | server1 | 1       |
    | 2023-01-01 10:35:00 | server2 | 1       |
    +---------------------+---------+---------+

Example 3: Calculate average number of packets by minute
================================================

This example calculates the average packets for each minute without grouping by any field.

PPL query::

    os> source=events | timechart span=1m avg(packets)
    fetched rows / total rows = 8/8
    +---------------------+--------------+
    | @timestamp          | avg(packets) |
    |---------------------+--------------|
    | 2023-01-01 10:00:00 | 60.0         |
    | 2023-01-01 10:05:00 | 30.0         |
    | 2023-01-01 10:10:00 | 60.0         |
    | 2023-01-01 10:15:00 | 30.0         |
    | 2023-01-01 10:20:00 | 60.0         |
    | 2023-01-01 10:25:00 | 30.0         |
    | 2023-01-01 10:30:00 | 180.0        |
    | 2023-01-01 10:35:00 | 90.0         |
    +---------------------+--------------+

Example 4: Calculate average number of packets by every 20 minutes and status
===========================================================

This example calculates the average number of packets for every 20 minutes and groups them by status.

PPL query::

    os> source=events | timechart span=20m avg(packets) by status
    fetched rows / total rows = 8/8
    +---------------------+------------+--------------+
    | @timestamp          | status     | avg(packets) |
    |---------------------+------------+--------------|
    | 2023-01-01 10:00:00 | active     | 30.0         |
    | 2023-01-01 10:00:00 | inactive   | 30.0         |
    | 2023-01-01 10:00:00 | pending    | 60.0         |
    | 2023-01-01 10:00:00 | processing | 60.0         |
    | 2023-01-01 10:20:00 | cancelled  | 180.0        |
    | 2023-01-01 10:20:00 | completed  | 60.0         |
    | 2023-01-01 10:20:00 | inactive   | 90.0         |
    | 2023-01-01 10:20:00 | pending    | 30.0         |
    +---------------------+------------+--------------+

Example 5: Count events by hour and category
=====================================================================

This example counts events for each second and groups them by category

PPL query::

    os> source=events | timechart span=1h count() by category
    fetched rows / total rows = 2/2
    +---------------------+----------+---------+
    | @timestamp          | category | count() |
    |---------------------+----------+---------|
    | 2023-01-01 10:00:00 | orders   | 4       |
    | 2023-01-01 10:00:00 | users    | 4       |
    +---------------------+----------+---------+

Example 6: Using the limit parameter with count() function
==========================================================

When there are many distinct values in the "by" field, the timechart command will display the top values based on the limit parameter and group the rest into an "OTHER" category.
This query will display the top 2 hosts with the highest count values, and group the remaining hosts into an "OTHER" category.

PPL query::

    os> source=events | timechart span=1m limit=2 count() by host
    fetched rows / total rows = 8/8
    +---------------------+---------+---------+
    | @timestamp          | host    | count() |
    |---------------------+---------+---------|
    | 2023-01-01 10:00:00 | server1 | 1       |
    | 2023-01-01 10:05:00 | server2 | 1       |
    | 2023-01-01 10:10:00 | server1 | 1       |
    | 2023-01-01 10:15:00 | server2 | 1       |
    | 2023-01-01 10:20:00 | server1 | 1       |
    | 2023-01-01 10:25:00 | server2 | 1       |
    | 2023-01-01 10:30:00 | server1 | 1       |
    | 2023-01-01 10:35:00 | server2 | 1       |
    +---------------------+---------+---------+

Example 7: Using limit=0 with count() to show all values
========================================================

To display all distinct values without any limit, set limit=0:

PPL query::

    os> source=events_many_hosts | timechart span=1h limit=0 count() by host
    fetched rows / total rows = 11/11
    +---------------------+--------+---------+
    | @timestamp          | host   | count() |
    |---------------------+--------+---------|
    | 2024-07-01 00:00:00 | web-01 | 1       |
    | 2024-07-01 00:00:00 | web-02 | 1       |
    | 2024-07-01 00:00:00 | web-03 | 1       |
    | 2024-07-01 00:00:00 | web-04 | 1       |
    | 2024-07-01 00:00:00 | web-05 | 1       |
    | 2024-07-01 00:00:00 | web-06 | 1       |
    | 2024-07-01 00:00:00 | web-07 | 1       |
    | 2024-07-01 00:00:00 | web-08 | 1       |
    | 2024-07-01 00:00:00 | web-09 | 1       |
    | 2024-07-01 00:00:00 | web-10 | 1       |
    | 2024-07-01 00:00:00 | web-11 | 1       |
    +---------------------+--------+---------+

This shows all 11 hosts as separate rows without an "OTHER" category.

Example 8: Using useother=false with count() function
=====================================================

Limit to top 10 hosts without OTHER category (useother=false):

PPL query::

    os> source=events_many_hosts | timechart span=1h useother=false count() by host
    fetched rows / total rows = 10/10
    +---------------------+--------+---------+
    | @timestamp          | host   | count() |
    |---------------------+--------+---------|
    | 2024-07-01 00:00:00 | web-01 | 1       |
    | 2024-07-01 00:00:00 | web-02 | 1       |
    | 2024-07-01 00:00:00 | web-03 | 1       |
    | 2024-07-01 00:00:00 | web-04 | 1       |
    | 2024-07-01 00:00:00 | web-05 | 1       |
    | 2024-07-01 00:00:00 | web-06 | 1       |
    | 2024-07-01 00:00:00 | web-07 | 1       |
    | 2024-07-01 00:00:00 | web-08 | 1       |
    | 2024-07-01 00:00:00 | web-09 | 1       |
    | 2024-07-01 00:00:00 | web-10 | 1       |
    +---------------------+--------+---------+

Example 9: Using limit with useother parameter and avg() function
=================================================================

Limit to top 3 hosts with OTHER category (default useother=true):

PPL query::

    os> source=events_many_hosts | timechart span=1h limit=3 avg(cpu_usage) by host
    fetched rows / total rows = 4/4
    +---------------------+--------+----------------+
    | @timestamp          | host   | avg(cpu_usage) |
    |---------------------+--------+----------------|
    | 2024-07-01 00:00:00 | OTHER  | 41.3           |
    | 2024-07-01 00:00:00 | web-03 | 55.3           |
    | 2024-07-01 00:00:00 | web-07 | 48.6           |
    | 2024-07-01 00:00:00 | web-09 | 67.8           |
    +---------------------+--------+----------------+

Limit to top 3 hosts without OTHER category (useother=false):

PPL query::

    os> source=events_many_hosts | timechart span=1h limit=3 useother=false avg(cpu_usage) by host
    fetched rows / total rows = 3/3
    +---------------------+--------+----------------+
    | @timestamp          | host   | avg(cpu_usage) |
    |---------------------+--------+----------------|
    | 2024-07-01 00:00:00 | web-03 | 55.3           |
    | 2024-07-01 00:00:00 | web-07 | 48.6           |
    | 2024-07-01 00:00:00 | web-09 | 67.8           |
    +---------------------+--------+----------------+

Example 10: Handling null values in the "by" field
==================================================

This example shows how null values in the "by" field are treated as a separate category. The dataset events_null has 1 entry that does not have a host field.
It is put into a separate "NULL" category because the defaults for ``usenull`` and ``nullstr`` are ``true`` and ``"NULL"`` respectively.

PPL query::

    os> source=events_null | timechart span=1h count() by host
    fetched rows / total rows = 4/4
    +---------------------+--------+---------+
    | @timestamp          | host   | count() |
    |---------------------+--------+---------|
    | 2024-07-01 00:00:00 | NULL   | 1       |
    | 2024-07-01 00:00:00 | db-01  | 1       |
    | 2024-07-01 00:00:00 | web-01 | 2       |
    | 2024-07-01 00:00:00 | web-02 | 2       |
    +---------------------+--------+---------+

Example 11: Calculate packets per second rate
=============================================

This example calculates the per-second packet rate for network traffic data using the per_second() function.

PPL query::

    os> source=events | timechart span=30m per_second(packets) by host
    fetched rows / total rows = 4/4
    +---------------------+---------+---------------------+
    | @timestamp          | host    | per_second(packets) |
    |---------------------+---------+---------------------|
    | 2023-01-01 10:00:00 | server1 | 0.1                 |
    | 2023-01-01 10:00:00 | server2 | 0.05                |
    | 2023-01-01 10:30:00 | server1 | 0.1                 |
    | 2023-01-01 10:30:00 | server2 | 0.05                |
    +---------------------+---------+---------------------+

Limitations
===========
* Only a single aggregation function is supported per timechart command.
* The ``bins`` parameter and other bin options are not supported since the ``bin`` command is not implemented yet. Use the ``span`` parameter to control time intervals.

