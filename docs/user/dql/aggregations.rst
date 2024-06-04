===========
Aggregation
===========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Introduction
============

Aggregate functions operate on sets of values. They are often used with a GROUP BY clause to group values into subsets.


GROUP BY Clause
===============

Description
-----------

The GROUP BY expression could be

1. Identifier
2. Ordinal
3. Expression

Identifier
----------

The group by expression could be identifier::

    os> SELECT gender, sum(age) FROM accounts GROUP BY gender;
    fetched rows / total rows = 2/2
    +----------+------------+
    | gender   | sum(age)   |
    |----------+------------|
    | F        | 28         |
    | M        | 101        |
    +----------+------------+


Ordinal
-------

The group by expression could be ordinal::

    os> SELECT gender, sum(age) FROM accounts GROUP BY 1;
    fetched rows / total rows = 2/2
    +----------+------------+
    | gender   | sum(age)   |
    |----------+------------|
    | F        | 28         |
    | M        | 101        |
    +----------+------------+


Expression
----------

The group by expression could be expression::

    os> SELECT abs(account_number), sum(age) FROM accounts GROUP BY abs(account_number);
    fetched rows / total rows = 4/4
    +-----------------------+------------+
    | abs(account_number)   | sum(age)   |
    |-----------------------+------------|
    | 1                     | 32         |
    | 13                    | 28         |
    | 18                    | 33         |
    | 6                     | 36         |
    +-----------------------+------------+


Aggregation
===========

Description
-----------

1. The aggregation could be used select.
2. The aggregation could be used as arguments of expression.
3. The aggregation could has expression as arguments.

Aggregation in Select
---------------------

The aggregation could be used select::

    os> SELECT gender, sum(age) FROM accounts GROUP BY gender;
    fetched rows / total rows = 2/2
    +----------+------------+
    | gender   | sum(age)   |
    |----------+------------|
    | F        | 28         |
    | M        | 101        |
    +----------+------------+

Expression over Aggregation
---------------------------

The aggregation could be used as arguments of expression::

    os> SELECT gender, sum(age) * 2 as sum2 FROM accounts GROUP BY gender;
    fetched rows / total rows = 2/2
    +----------+--------+
    | gender   | sum2   |
    |----------+--------|
    | F        | 56     |
    | M        | 202    |
    +----------+--------+

Expression as argument of Aggregation
-------------------------------------

The aggregation could has expression as arguments::

    os> SELECT gender, sum(age * 2) as sum2 FROM accounts GROUP BY gender;
    fetched rows / total rows = 2/2
    +----------+--------+
    | gender   | sum2   |
    |----------+--------|
    | F        | 56     |
    | M        | 202    |
    +----------+--------+

COUNT Aggregations
------------------

Besides regular identifiers, ``COUNT`` aggregate function also accepts arguments such as ``*`` or literals like ``1``. The meaning of these different forms are as follows:

1. ``COUNT(field)`` will count only if given field (or expression) is not null or missing in the input rows.
2. ``COUNT(*)`` will count the number of all its input rows.
3. ``COUNT(1)`` is same as ``COUNT(*)`` because any non-null literal will count.

Aggregation Functions
=====================

COUNT
-----

Description
>>>>>>>>>>>

Usage: Returns a count of the number of expr in the rows retrieved by a SELECT statement.

Example::

    os> SELECT gender, count(*) as countV FROM accounts GROUP BY gender;
    fetched rows / total rows = 2/2
    +----------+----------+
    | gender   | countV   |
    |----------+----------|
    | F        | 1        |
    | M        | 3        |
    +----------+----------+

SUM
---

Description
>>>>>>>>>>>

Usage: SUM(expr). Returns the sum of `expr`. `expr` could be of any of the numeric data types.

Example::

    os> SELECT gender, sum(age) as sumV FROM accounts GROUP BY gender;
    fetched rows / total rows = 2/2
    +----------+--------+
    | gender   | sumV   |
    |----------+--------|
    | F        | 28     |
    | M        | 101    |
    +----------+--------+

AVG
---

Description
>>>>>>>>>>>

Usage: AVG(expr). Returns the average value of `expr`. `expr` can be any numeric or datetime data type. Datetime aggregation is performed with milliseconds precision.

Example::

    os> SELECT gender, avg(age) as avgV FROM accounts GROUP BY gender;
    fetched rows / total rows = 2/2
    +----------+--------------------+
    | gender   | avgV               |
    |----------+--------------------|
    | F        | 28.0               |
    | M        | 33.666666666666664 |
    +----------+--------------------+

MAX
---

Description
>>>>>>>>>>>

Usage: MAX(expr). Returns the maximum value of `expr`. `expr` can be any numeric or datetime data type. Datetime aggregation is performed with milliseconds precision.

Example::

    os> SELECT max(age) as maxV FROM accounts;
    fetched rows / total rows = 1/1
    +--------+
    | maxV   |
    |--------|
    | 36     |
    +--------+

MIN
---

Description
>>>>>>>>>>>

Usage: MIN(expr). Returns the minimum value of `expr`. `expr` can be any numeric or datetime data type. Datetime aggregation is performed with milliseconds precision.

Example::

    os> SELECT min(age) as minV FROM accounts;
    fetched rows / total rows = 1/1
    +--------+
    | minV   |
    |--------|
    | 28     |
    +--------+

VAR_POP
-------

Description
>>>>>>>>>>>

Usage: VAR_POP(expr). Returns the population standard variance of expr.

Example::

    os> SELECT var_pop(age) as varV FROM accounts;
    fetched rows / total rows = 1/1
    +--------+
    | varV   |
    |--------|
    | 8.1875 |
    +--------+

VAR_SAMP
--------

Description
>>>>>>>>>>>

Usage: VAR_SAMP(expr). Returns the sample variance of expr.

Example::

    os> SELECT var_samp(age) as varV FROM accounts;
    fetched rows / total rows = 1/1
    +--------------------+
    | varV               |
    |--------------------|
    | 10.916666666666666 |
    +--------------------+

VARIANCE
--------

Description
>>>>>>>>>>>

Usage: VARIANCE(expr). Returns the population standard variance of expr. VARIANCE() is a synonym VAR_POP() function.

Example::

    os> SELECT variance(age) as varV FROM accounts;
    fetched rows / total rows = 1/1
    +--------+
    | varV   |
    |--------|
    | 8.1875 |
    +--------+

STDDEV_POP
----------

Description
>>>>>>>>>>>

Usage: STDDEV_POP(expr). Returns the population standard deviation of expr.

Example::

    os> SELECT stddev_pop(age) as stddevV FROM accounts;
    fetched rows / total rows = 1/1
    +--------------------+
    | stddevV            |
    |--------------------|
    | 2.8613807855648994 |
    +--------------------+

STDDEV_SAMP
-----------

Description
>>>>>>>>>>>

Usage: STDDEV_SAMP(expr). Returns the sample standard deviation of expr.

Example::

    os> SELECT stddev_samp(age) as stddevV FROM accounts;
    fetched rows / total rows = 1/1
    +-------------------+
    | stddevV           |
    |-------------------|
    | 3.304037933599835 |
    +-------------------+

STD
---

Description
>>>>>>>>>>>

Usage: STD(expr). Returns the population standard deviation of expr. STD() is a synonym STDDEV_POP() function.

Example::

    os> SELECT stddev_pop(age) as stddevV FROM accounts;
    fetched rows / total rows = 1/1
    +--------------------+
    | stddevV            |
    |--------------------|
    | 2.8613807855648994 |
    +--------------------+

STDDEV
------

Description
>>>>>>>>>>>

Usage: STDDEV(expr). Returns the population standard deviation of expr. STDDEV() is a synonym STDDEV_POP() function.

Example::

    os> SELECT stddev(age) as stddevV FROM accounts;
    fetched rows / total rows = 1/1
    +--------------------+
    | stddevV            |
    |--------------------|
    | 2.8613807855648994 |
    +--------------------+

DISTINCT COUNT Aggregation
--------------------------

To get the count of distinct values of a field, you can add a keyword ``DISTINCT`` before the field in the count aggregation. Example::

    os> SELECT COUNT(DISTINCT gender), COUNT(gender) FROM accounts;
    fetched rows / total rows = 1/1
    +--------------------------+-----------------+
    | COUNT(DISTINCT gender)   | COUNT(gender)   |
    |--------------------------+-----------------|
    | 2                        | 4               |
    +--------------------------+-----------------+

PERCENTILE or PERCENTILE_APPROX
-------------------------------

Description
>>>>>>>>>>>

Usage: PERCENTILE(expr, percent) or PERCENTILE_APPROX(expr, percent). Returns the approximate percentile value of `expr` at the specified percentage. `percent` must be a constant between 0 and 100.

Example::

    os> SELECT gender, percentile(age, 90) as p90 FROM accounts GROUP BY gender;
    fetched rows / total rows = 2/2
    +----------+-------+
    | gender   | p90   |
    |----------+-------|
    | F        | 28    |
    | M        | 36    |
    +----------+-------+

HAVING Clause
=============

Description
-----------

A ``HAVING`` clause can serve as aggregation filter that filters out aggregated values satisfy the condition expression given.

HAVING with GROUP BY
--------------------

Aggregate expressions or its alias defined in ``SELECT`` clause can be used in ``HAVING`` condition.

1. It's recommended to use non-aggregate expression in ``WHERE`` although it's allowed to do this in ``HAVING`` clause.
2. The aggregation in ``HAVING`` clause is not necessarily same as that on select list. As extension to SQL standard, it's also not restricted to involve identifiers only on group by list.

Here is an example for typical use of ``HAVING`` clause::

    os> SELECT
    ...  gender, sum(age)
    ... FROM accounts
    ... GROUP BY gender
    ... HAVING sum(age) > 100;
    fetched rows / total rows = 1/1
    +----------+------------+
    | gender   | sum(age)   |
    |----------+------------|
    | M        | 101        |
    +----------+------------+

Here is another example for using alias in ``HAVING`` condition. Note that if an identifier is ambiguous, for example present both as a select alias and an index field, preference is alias. This means the identifier will be replaced by expression aliased in ``SELECT`` clause::

    os> SELECT
    ...  gender, sum(age) AS s
    ... FROM accounts
    ... GROUP BY gender
    ... HAVING s > 100;
    fetched rows / total rows = 1/1
    +----------+-----+
    | gender   | s   |
    |----------+-----|
    | M        | 101 |
    +----------+-----+

HAVING without GROUP BY
-----------------------

Additionally, a ``HAVING`` clause can work without ``GROUP BY`` clause. This is useful because aggregation is not allowed to be present in ``WHERE`` clause::

    os> SELECT
    ...  'Total of age > 100'
    ... FROM accounts
    ... HAVING sum(age) > 100;
    fetched rows / total rows = 1/1
    +------------------------+
    | 'Total of age > 100'   |
    |------------------------|
    | Total of age > 100     |
    +------------------------+


FILTER Clause
=============

Description
-----------

A ``FILTER`` clause can set specific condition for the current aggregation bucket, following the syntax ``aggregation_function(expr) FILTER(WHERE condition_expr)``. If a filter is specified, then only the input rows for which the condition in the filter clause evaluates to true are fed to the aggregate function; other rows are discarded. The aggregation with filter clause can be use in ``SELECT`` clause only.

FILTER with GROUP BY
--------------------

The group by aggregation with ``FILTER`` clause can set different conditions for each aggregation bucket. Here is an example to use ``FILTER`` in group by aggregation::

    os> SELECT avg(age) FILTER(WHERE balance > 10000) AS filtered, gender FROM accounts GROUP BY gender
    fetched rows / total rows = 2/2
    +------------+----------+
    | filtered   | gender   |
    |------------+----------|
    | 28.0       | F        |
    | 32.0       | M        |
    +------------+----------+

FILTER without GROUP BY
-----------------------

The ``FILTER`` clause can be used in aggregation functions without GROUP BY as well. For example::

    os> SELECT
    ...   count(*) AS unfiltered,
    ...   count(*) FILTER(WHERE age > 34) AS filtered
    ... FROM accounts
    fetched rows / total rows = 1/1
    +--------------+------------+
    | unfiltered   | filtered   |
    |--------------+------------|
    | 4            | 1          |
    +--------------+------------+

Distinct count aggregate with FILTER
------------------------------------

The ``FILTER`` clause is also used in distinct count to do the filtering before count the distinct values of specific field. For example::

    os> SELECT COUNT(DISTINCT firstname) FILTER(WHERE age > 30) AS distinct_count FROM accounts
    fetched rows / total rows = 1/1
    +------------------+
    | distinct_count   |
    |------------------|
    | 3                |
    +------------------+

