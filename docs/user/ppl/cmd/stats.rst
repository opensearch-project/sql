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

The following table dataSources the aggregation functions and also indicates how the NULL/MISSING values is handled:

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
stats <aggregation>... [by-clause]


* aggregation: mandatory. A aggregation function. The argument of aggregation must be field.

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

    os> source=accounts | stats count();
    fetched rows / total rows = 1/1
    +---------+
    | count() |
    |---------|
    | 4       |
    +---------+

SUM
---

Description
>>>>>>>>>>>

Usage: SUM(expr). Returns the sum of expr.

Example::

    os> source=accounts | stats sum(age) by gender;
    fetched rows / total rows = 2/2
    +----------+--------+
    | sum(age) | gender |
    |----------+--------|
    | 28       | F      |
    | 101      | M      |
    +----------+--------+

AVG
---

Description
>>>>>>>>>>>

Usage: AVG(expr). Returns the average value of expr.

Example::

    os> source=accounts | stats avg(age) by gender;
    fetched rows / total rows = 2/2
    +--------------------+--------+
    | avg(age)           | gender |
    |--------------------+--------|
    | 28.0               | F      |
    | 33.666666666666664 | M      |
    +--------------------+--------+

MAX
---

Description
>>>>>>>>>>>

Usage: MAX(expr). Returns the maximum value of expr.

Example::

    os> source=accounts | stats max(age);
    fetched rows / total rows = 1/1
    +----------+
    | max(age) |
    |----------|
    | 36       |
    +----------+

MIN
---

Description
>>>>>>>>>>>

Usage: MIN(expr). Returns the minimum value of expr.

Example::

    os> source=accounts | stats min(age);
    fetched rows / total rows = 1/1
    +----------+
    | min(age) |
    |----------|
    | 28       |
    +----------+

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
    +--------------+
    | var_pop(age) |
    |--------------|
    | 8.1875       |
    +--------------+

STDDEV_SAMP
-----------

Description
>>>>>>>>>>>

Usage: STDDEV_SAMP(expr). Return the sample standard deviation of expr.

Example::

    os> source=accounts | stats stddev_samp(age);
    fetched rows / total rows = 1/1
    +-------------------+
    | stddev_samp(age)  |
    |-------------------|
    | 3.304037933599835 |
    +-------------------+

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

DISTINCT_COUNT_APPROX
----------

Description
>>>>>>>>>>>

Usage: DISTINCT_COUNT_APPROX(expr). Return the approximate distinct count value of the expr, using the hyperloglog++ algorithm.

Example::

    PPL> source=accounts | stats distinct_count_approx(gender);
    fetched rows / total rows = 1/1
    +-------------------------------+
    | distinct_count_approx(gender) |
    |-------------------------------|
    | 2                             |
    +-------------------------------+

TAKE
----------

Description
>>>>>>>>>>>

Usage: TAKE(field [, size]). Return original values of a field. It does not guarantee on the order of values.

* field: mandatory. The field must be a text field.
* size: optional integer. The number of values should be returned. Default is 10.

Example::

    os> source=accounts | stats take(firstname);
    fetched rows / total rows = 1/1
    +-----------------------------+
    | take(firstname)             |
    |-----------------------------|
    | [Amber,Hattie,Nanette,Dale] |
    +-----------------------------+

PERCENTILE or PERCENTILE_APPROX
-------------------------------

Description
>>>>>>>>>>>

Usage: PERCENTILE(expr, percent) or PERCENTILE_APPROX(expr, percent). Return the approximate percentile value of expr at the specified percentage.

* percent: The number must be a constant between 0 and 100.

Example::

    os> source=accounts | stats percentile(age, 90) by gender;
    fetched rows / total rows = 2/2
    +---------------------+--------+
    | percentile(age, 90) | gender |
    |---------------------+--------|
    | 28                  | F      |
    | 36                  | M      |
    +---------------------+--------+

Example 1: Calculate the count of events
========================================

The example show calculate the count of events in the accounts.

PPL query::

    os> source=accounts | stats count();
    fetched rows / total rows = 1/1
    +---------+
    | count() |
    |---------|
    | 4       |
    +---------+


Example 2: Calculate the average of a field
===========================================

The example show calculate the average age of all the accounts.

PPL query::

    os> source=accounts | stats avg(age);
    fetched rows / total rows = 1/1
    +----------+
    | avg(age) |
    |----------|
    | 32.25    |
    +----------+


Example 3: Calculate the average of a field by group
====================================================

The example show calculate the average age of all the accounts group by gender.

PPL query::

    os> source=accounts | stats avg(age) by gender;
    fetched rows / total rows = 2/2
    +--------------------+--------+
    | avg(age)           | gender |
    |--------------------+--------|
    | 28.0               | F      |
    | 33.666666666666664 | M      |
    +--------------------+--------+


Example 4: Calculate the average, sum and count of a field by group
===================================================================

The example show calculate the average age, sum age and count of events of all the accounts group by gender.

PPL query::

    os> source=accounts | stats avg(age), sum(age), count() by gender;
    fetched rows / total rows = 2/2
    +--------------------+----------+---------+--------+
    | avg(age)           | sum(age) | count() | gender |
    |--------------------+----------+---------+--------|
    | 28.0               | 28       | 1       | F      |
    | 33.666666666666664 | 101      | 3       | M      |
    +--------------------+----------+---------+--------+

Example 5: Calculate the maximum of a field
===========================================

The example calculates the max age of all the accounts.

PPL query::

    os> source=accounts | stats max(age);
    fetched rows / total rows = 1/1
    +----------+
    | max(age) |
    |----------|
    | 36       |
    +----------+

Example 6: Calculate the maximum and minimum of a field by group
================================================================

The example calculates the max and min age values of all the accounts group by gender.

PPL query::

    os> source=accounts | stats max(age), min(age) by gender;
    fetched rows / total rows = 2/2
    +----------+----------+--------+
    | max(age) | min(age) | gender |
    |----------+----------+--------|
    | 28       | 28       | F      |
    | 36       | 32       | M      |
    +----------+----------+--------+

Example 7: Calculate the distinct count of a field
==================================================

To get the count of distinct values of a field, you can use ``DISTINCT_COUNT`` (or ``DC``) function instead of ``COUNT``. The example calculates both the count and the distinct count of gender field of all the accounts.

PPL query::

    os> source=accounts | stats count(gender), distinct_count(gender);
    fetched rows / total rows = 1/1
    +---------------+------------------------+
    | count(gender) | distinct_count(gender) |
    |---------------+------------------------|
    | 4             | 2                      |
    +---------------+------------------------+

Example 8: Calculate the count by a span
========================================

The example gets the count of age by the interval of 10 years.

PPL query::

    os> source=accounts | stats count(age) by span(age, 10) as age_span
    fetched rows / total rows = 2/2
    +------------+----------+
    | count(age) | age_span |
    |------------+----------|
    | 1          | 20       |
    | 3          | 30       |
    +------------+----------+

Example 9: Calculate the count by a gender and span
===================================================

The example gets the count of age by the interval of 10 years and group by gender.

PPL query::

    os> source=accounts | stats count() as cnt by span(age, 5) as age_span, gender
    fetched rows / total rows = 3/3
    +-----+----------+--------+
    | cnt | age_span | gender |
    |-----+----------+--------|
    | 1   | 25       | F      |
    | 2   | 30       | M      |
    | 1   | 35       | M      |
    +-----+----------+--------+

Span will always be the first grouping key whatever order you specify.

PPL query::

    os> source=accounts | stats count() as cnt by gender, span(age, 5) as age_span
    fetched rows / total rows = 3/3
    +-----+----------+--------+
    | cnt | age_span | gender |
    |-----+----------+--------|
    | 1   | 25       | F      |
    | 2   | 30       | M      |
    | 1   | 35       | M      |
    +-----+----------+--------+

Example 10: Calculate the count and get email list by a gender and span
=======================================================================

The example gets the count of age by the interval of 10 years and group by gender, additionally for each row get a list of at most 5 emails.

PPL query::

    os> source=accounts | stats count() as cnt, take(email, 5) by span(age, 5) as age_span, gender
    fetched rows / total rows = 3/3
    +-----+--------------------------------------------+----------+--------+
    | cnt | take(email, 5)                             | age_span | gender |
    |-----+--------------------------------------------+----------+--------|
    | 1   | []                                         | 25       | F      |
    | 2   | [amberduke@pyrami.com,daleadams@boink.com] | 30       | M      |
    | 1   | [hattiebond@netagy.com]                    | 35       | M      |
    +-----+--------------------------------------------+----------+--------+

Example 11: Calculate the percentile of a field
===============================================

The example show calculate the percentile 90th age of all the accounts.

PPL query::

    os> source=accounts | stats percentile(age, 90);
    fetched rows / total rows = 1/1
    +---------------------+
    | percentile(age, 90) |
    |---------------------|
    | 36                  |
    +---------------------+


Example 12: Calculate the percentile of a field by group
========================================================

The example show calculate the percentile 90th age of all the accounts group by gender.

PPL query::

    os> source=accounts | stats percentile(age, 90) by gender;
    fetched rows / total rows = 2/2
    +---------------------+--------+
    | percentile(age, 90) | gender |
    |---------------------+--------|
    | 28                  | F      |
    | 36                  | M      |
    +---------------------+--------+

Example 13: Calculate the percentile by a gender and span
=========================================================

The example gets the percentile 90th age by the interval of 10 years and group by gender.

PPL query::

    os> source=accounts | stats percentile(age, 90) as p90 by span(age, 10) as age_span, gender
    fetched rows / total rows = 2/2
    +-----+----------+--------+
    | p90 | age_span | gender |
    |-----+----------+--------|
    | 28  | 20       | F      |
    | 36  | 30       | M      |
    +-----+----------+--------+

