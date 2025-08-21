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

.. code-block:: text

   timechart [span=<time_interval>] [limit=<number>] [useother=<boolean>] <aggregation_function> [by <field>]

**Parameters:**

* **span**: optional. Specifies the time interval for grouping data.
  
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

* **limit**: optional. Specifies the maximum number of distinct values to display when using the "by" clause.

  * Default: 10
  * When there are more distinct values than the limit, the additional values are grouped into an "OTHER" category.
  * The "most distinct" values are determined by calculating the sum of the aggregation values across all time intervals for each distinct field value. The top N values with the highest sums are displayed individually, while the rest are grouped into the "OTHER" category.
  * Set to 0 to show all distinct values without any limit.
  * The parameters can be specified in any order before the aggregation function.
  * Only applies when using the "by" clause to group results.

* **useother**: optional. Controls whether to create an "OTHER" category for values beyond the limit.

  * Default: true
  * When set to false, only the top N values (based on limit) are shown without an "OTHER" column.
  * When set to true, values beyond the limit are grouped into an "OTHER" category.
  * Only applies when using the "by" clause and when there are more distinct values than the limit.

* **aggregation_function**: mandatory. The aggregation function to apply to each time bucket.

  * Currently, only a single aggregation function is supported.
  * Available functions: All standard aggregation functions supported by stats are supported.

* **by**: optional. Groups the results by the specified field in addition to time intervals.

  * If not specified, the aggregation is performed across all documents in each time interval.

Notes
=====

* The ``timechart`` command requires a timestamp field named ``@timestamp`` in the data.
* For count() aggregation, when no data exists for a specific combination of timestamp and field value, ``0`` is displayed instead of ``null``.
* For other aggregation functions, when no data exists for a specific combination of timestamp and field value, ``null`` is displayed rather than ``0``. This preserves the distinction between "no data" and "zero value".
* The "top N" values for the ``limit`` parameter are selected based on the sum of values across all time intervals for each distinct field value.
* Examples 5 and 6 use different datasets: Example 5 uses the ``events`` dataset with fewer hosts for simplicity, while Example 6 uses the ``events_many_hosts`` dataset with 11 distinct hosts.

Limitations
============
* The ``timechart`` command must be the last command in the PPL query pipeline since the pivot formatting is applied as the final step.
* Only a single aggregation function is supported per timechart command.
* The ``bins`` parameter and other bin options are not supported since the ``bin`` command is not implemented yet. Use the ``span`` parameter to control time intervals.
* Cannot be combined with other commands that expect tabular data after timechart due to the pivot transformation.

Examples
========

Example 1: Count events by hour
===============================

This example counts events for each hour and groups them by host.

PPL query::

    PPL> source=events | timechart span=1h count() by host

Result::

    +---------------------+-------+--------+--------+
    | @timestamp          | db-01 | web-01 | web-02 |
    +---------------------+-------+--------+--------+
    | 2024-07-01 00:00:00 | 1     | 2      | 2      |
    +---------------------+-------+--------+--------+

Example 2: Count events by minute
=================================

This example counts events for each minute and groups them by host.

PPL query::

    PPL> source=events | timechart span=1m count() by host

Result::

    +---------------------+-------+--------+--------+
    | @timestamp          | db-01 | web-01 | web-02 |
    +---------------------+-------+--------+--------+
    | 2024-07-01 00:00:00 | 0     | 1      | 0      |
    | 2024-07-01 00:01:00 | 0     | 0      | 1      |
    | 2024-07-01 00:02:00 | 0     | 1      | 0      |
    | 2024-07-01 00:03:00 | 1     | 0      | 0      |
    | 2024-07-01 00:04:00 | 0     | 0      | 1      |
    +---------------------+-------+--------+--------+

Example 3: Calculate average CPU usage by minute
================================================

This example calculates the average CPU usage for each minute without grouping by any field.

PPL query::

    PPL> source=events | timechart span=1m avg(cpu_usage)

Result::

    +---------------------+--------+
    | @timestamp          | $f1    |
    +---------------------+--------+
    | 2024-07-01 00:00:00 | 45.2   |
    | 2024-07-01 00:01:00 | 38.7   |
    | 2024-07-01 00:02:00 | 55.3   |
    | 2024-07-01 00:03:00 | 42.1   |
    | 2024-07-01 00:04:00 | 41.8   |
    +---------------------+--------+

Example 4: Count events by second and region
============================================

This example counts events for each second and groups them by region.

PPL query::

    PPL> source=events | timechart span=1s count() by region

Result::

    +---------------------+---------+---------+---------+
    | @timestamp          | eu-west | us-east | us-west |
    +---------------------+---------+---------+---------+
    | 2024-07-01 00:00:00 | 0       | 1       | 0       |
    | 2024-07-01 00:01:00 | 0       | 0       | 1       |
    | 2024-07-01 00:02:00 | 0       | 1       | 0       |
    | 2024-07-01 00:03:00 | 1       | 0       | 0       |
    | 2024-07-01 00:04:00 | 0       | 0       | 1       |
    +---------------------+---------+---------+---------+

Example 5: Using the limit parameter
====================================

When there are many distinct values in the "by" field, the timechart command will display the top values based on the limit parameter and group the rest into an "OTHER" category.
This query will display the top 2 hosts with the highest average sum of CPU usage values, and group the remaining hosts into an "OTHER" category.
Example::

    PPL> source=events | timechart span=1m limit=2 avg(cpu_usage) by host

Result::

    +---------------------+--------+--------+-------+
    | @timestamp          | web-01 | web-02 | OTHER |
    +---------------------+--------+--------+-------+
    | 2024-07-01 00:00:00 | 45.2   | null   | null  |
    | 2024-07-01 00:01:00 | null   | 38.7   | null  |
    | 2024-07-01 00:02:00 | 55.3   | null   | null  |
    | 2024-07-01 00:03:00 | null   | null   | 42.1  |
    | 2024-07-01 00:04:00 | null   | 41.8   | null  |
    +---------------------+--------+--------+-------+

Example 6: Using limit=0 to show all values
===========================================

To display all distinct values without any limit, set limit=0:

PPL query::

    PPL> source=events_many_hosts | timechart span=1h limit=0 avg(cpu_usage) by host

Result::

    +---------------------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
    | @timestamp          | web-01 | web-02 | web-03 | web-04 | web-05 | web-06 | web-07 | web-08 | web-09 | web-10 | web-11 |
    +---------------------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
    | 2024-07-01 00:00:00 | 45.2   | 38.7   | 55.3   | 42.1   | 41.8   | 39.4   | 48.6   | 44.2   | 67.8   | 35.9   | 43.1   |
    +---------------------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+

This shows all 11 hosts as separate columns without an "OTHER" category.

Example 7: Using limit with useother parameter
==============================================

Limit to top 3 hosts with OTHER category (default useother=true):

PPL query::

    PPL> source=events_many_hosts | timechart span=1h limit=3 avg(cpu_usage) by host

Result::

    +---------------------+--------+--------+--------+-------+
    | @timestamp          | web-03 | web-07 | web-09 | OTHER |
    +---------------------+--------+--------+--------+-------+
    | 2024-07-01 00:00:00 | 55.3   | 48.6   | 67.8   | 330.4 |
    +---------------------+--------+--------+--------+-------+

Limit to top 3 hosts without OTHER category (useother=false):

PPL query::

    PPL> source=events_many_hosts | timechart span=1h limit=3 useother=false avg(cpu_usage) by host

Result::

    +---------------------+--------+--------+--------+
    | @timestamp          | web-03 | web-07 | web-09 |
    +---------------------+--------+--------+--------+
    | 2024-07-01 00:00:00 | 55.3   | 48.6   | 67.8   |
    +---------------------+--------+--------+--------+
