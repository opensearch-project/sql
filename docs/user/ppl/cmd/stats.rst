=============
stats
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Using ``stats`` command to calculate the aggregation from search result.

The following table catalogs the aggregation functions and also indicates how the NULL/MISSING values is handled:

+----------+-------------+-------------+
| Function | NULL        | MISSING     |
+----------+-------------+-------------+
| COUNT    | Not counted | Not counted |
+----------+-------------+-------------+
| SUM      | Ignore      | Ignore      |
+----------+-------------+-------------+
| AVG      | Ignore      | Ignore      |
+----------+-------------+-------------+
| MAX      | Ignore      | Ignore      |
+----------+-------------+-------------+
| MIN      | Ignore      | Ignore      |
+----------+-------------+-------------+


Syntax
============
stats <aggregation>... [by-clause]...


* aggregation: mandatory. A aggregation function. The argument of aggregation must be field.
* by-clause: optional. The one or more fields to group the results by. **Default**: If no <by-clause> is specified, the stats command returns only one row, which is the aggregation over the entire result set.


Aggregation Functions
=====================

COUNT
-----

Description
>>>>>>>>>>>

Usage: Returns a count of the number of expr in the rows retrieved by a SELECT statement.

Example::

    os> source=accounts | stats count();
    fetched rows / total rows = 1/1
    +-----------+
    | count()   |
    |-----------|
    | 4         |
    +-----------+

SUM
---

Description
>>>>>>>>>>>

Usage: SUM(expr). Returns the sum of expr.

Example::

    os> source=accounts | stats sum(age) by gender;
    fetched rows / total rows = 2/2
    +------------+----------+
    | sum(age)   | gender   |
    |------------+----------|
    | 28         | F        |
    | 101        | M        |
    +------------+----------+

AVG
---

Description
>>>>>>>>>>>

Usage: AVG(expr). Returns the average value of expr.

Example::

    os> source=accounts | stats avg(age) by gender;
    fetched rows / total rows = 2/2
    +--------------------+----------+
    | avg(age)           | gender   |
    |--------------------+----------|
    | 28.0               | F        |
    | 33.666666666666664 | M        |
    +--------------------+----------+

MAX
---

Description
>>>>>>>>>>>

Usage: MAX(expr). Returns the maximum value of expr.

Example::

    os> source=accounts | stats max(age);
    fetched rows / total rows = 1/1
    +------------+
    | max(age)   |
    |------------|
    | 36         |
    +------------+

MIN
---

Description
>>>>>>>>>>>

Usage: MIN(expr). Returns the minimum value of expr.

Example::

    os> source=accounts | stats min(age);
    fetched rows / total rows = 1/1
    +------------+
    | min(age)   |
    |------------|
    | 28         |
    +------------+

VAR_SAMP
--------

Description
>>>>>>>>>>>

Usage: VAR_SAMP(expr). Returns the sample variance of expr.

Example::

    os> source=accounts | stats var_samp(age);
    fetched rows / total rows = 1/1
    +--------------------+
    | var_samp(age)      |
    |--------------------|
    | 10.916666666666666 |
    +--------------------+

VAR_POP
-------

Description
>>>>>>>>>>>

Usage: VAR_POP(expr). Returns the population standard variance of expr.

Example::

    os> source=accounts | stats var_pop(age);
    fetched rows / total rows = 1/1
    +----------------+
    | var_pop(age)   |
    |----------------|
    | 8.1875         |
    +----------------+

STDDEV_SAMP
-----------

Description
>>>>>>>>>>>

Usage: STDDEV_SAMP(expr). Return the sample standard deviation of expr.

Example::

    os> source=accounts | stats stddev_samp(age);
    fetched rows / total rows = 1/1
    +--------------------+
    | stddev_samp(age)   |
    |--------------------|
    | 3.304037933599835  |
    +--------------------+

STDDEV_POP
----------

Description
>>>>>>>>>>>

Usage: STDDEV_POP(expr). Return the population standard deviation of expr.

Example::

    os> source=accounts | stats stddev_pop(age);
    fetched rows / total rows = 1/1
    +--------------------+
    | stddev_pop(age)    |
    |--------------------|
    | 2.8613807855648994 |
    +--------------------+

Example 1: Calculate the count of events
========================================

The example show calculate the count of events in the accounts.

PPL query::

    os> source=accounts | stats count();
    fetched rows / total rows = 1/1
    +-----------+
    | count()   |
    |-----------|
    | 4         |
    +-----------+


Example 2: Calculate the average of a field
===========================================

The example show calculate the average age of all the accounts.

PPL query::

    os> source=accounts | stats avg(age);
    fetched rows / total rows = 1/1
    +------------+
    | avg(age)   |
    |------------|
    | 32.25      |
    +------------+


Example 3: Calculate the average of a field by group
====================================================

The example show calculate the average age of all the accounts group by gender.

PPL query::

    os> source=accounts | stats avg(age) by gender;
    fetched rows / total rows = 2/2
    +--------------------+----------+
    | avg(age)           | gender   |
    |--------------------+----------|
    | 28.0               | F        |
    | 33.666666666666664 | M        |
    +--------------------+----------+


Example 4: Calculate the average, sum and count of a field by group
===================================================================

The example show calculate the average age, sum age and count of events of all the accounts group by gender.

PPL query::

    os> source=accounts | stats avg(age), sum(age), count() by gender;
    fetched rows / total rows = 2/2
    +--------------------+------------+-----------+----------+
    | avg(age)           | sum(age)   | count()   | gender   |
    |--------------------+------------+-----------+----------|
    | 28.0               | 28         | 1         | F        |
    | 33.666666666666664 | 101        | 3         | M        |
    +--------------------+------------+-----------+----------+

Example 5: Calculate the maximum of a field
===========================================

The example calculates the max age of all the accounts.

PPL query::

    os> source=accounts | stats max(age);
    fetched rows / total rows = 1/1
    +------------+
    | max(age)   |
    |------------|
    | 36         |
    +------------+

Example 6: Calculate the maximum and minimum of a field by group
================================================================

The example calculates the max and min age values of all the accounts group by gender.

PPL query::

    os> source=accounts | stats max(age), min(age) by gender;
    fetched rows / total rows = 2/2
    +------------+------------+----------+
    | max(age)   | min(age)   | gender   |
    |------------+------------+----------|
    | 28         | 28         | F        |
    | 36         | 32         | M        |
    +------------+------------+----------+

Example 7: Calculate the distinct count of a field
==================================================

To get the count of distinct values of a field, you can use ``DISTINCT_COUNT`` (or ``DC``) function instead of ``COUNT``. The example calculates both the count and the distinct count of gender field of all the accounts.

PPL query::

    os> source=accounts | stats count(gender), distinct_count(gender);
    fetched rows / total rows = 1/1
    +-----------------+--------------------------+
    | count(gender)   | distinct_count(gender)   |
    |-----------------+--------------------------|
    | 4               | 2                        |
    +-----------------+--------------------------+

