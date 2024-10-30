=============
trendline
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Use the ``trendline`` command to calculate the moving average on one or more fields in a search result.


Syntax
============
trendline <average-type>(<number-of-samples>, <source-field>) AS <target-field> [" " <average-type>(<number-of-samples>, <source-field>) AS <target-field> ]...

* average-type: mandatory. The moving average computation. Can be ``sma`` (simple moving average) currently.
* number-of-samples: mandatory. The number of samples to use in the average calculation. Must be a positive non-zero integer.
* source-field: mandatory. The field to compute the average on.
* target-field: mandatory. The field name to report the computation under.


Example 1: Calculate the moving average on one field.
=====================================================

The example shows how to calculate the moving average on one field.

PPL query::

    os> source=accounts | trendline sma(2, account_number) as an | fields an;
    fetched rows / total rows = 4/4
    +------+
    | an   |
    |------|
    | null |
    | 3.5  |
    | 9.5  |
    | 15.5 |
    +------+


Example 2: Calculate the moving average on multiple fields.
===========================================================

The example shows how to calculate the moving average on multiple fields.

PPL query::

    os> source=accounts | trendline sma(2, account_number) as an sma(2, age) as age_trend | fields an, age_trend ;
    fetched rows / total rows = 4/4
    +------+-----------+
    | an   | age_trend |
    |------+-----------|
    | null | null      |
    | 3.5  | 34.0      |
    | 9.5  | 32.0      |
    | 15.5 | 30.5      |
    +------+-----------+

