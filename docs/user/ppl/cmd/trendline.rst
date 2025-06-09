=============
trendline
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Using ``trendline`` command to calculate moving averages of fields.

Syntax
============
`TRENDLINE [sort <[+|-] sort-field>] SMA(number-of-datapoints, field) [AS alias] [SMA(number-of-datapoints, field) [AS alias]]...`

* [+|-]: optional. The plus [+] stands for ascending order and NULL/MISSING first and a minus [-] stands for descending order and NULL/MISSING last. **Default:** ascending order and NULL/MISSING first.
* sort-field: mandatory when sorting is used. The field used to sort.
* number-of-datapoints: mandatory. The number of datapoints to calculate the moving average (must be greater than zero).
* field: mandatory. The name of the field the moving average should be calculated for.
* alias: optional. The name of the resulting column containing the moving average (defaults to the field name with "_trendline").

At the moment only the Simple Moving Average (SMA) type is supported.

It is calculated like

    f[i]: The value of field 'f' in the i-th data-point
    n: The number of data-points in the moving window (period)
    t: The current time index

    SMA(t) = (1/n) * Σ(f[i]), where i = t-n+1 to t

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

Example 4: Calculate the moving average on one field without specifying an alias.
=================================================================================

The example shows how to calculate the moving average on one field.

PPL query::

    os> source=accounts | trendline sma(2, account_number)  | fields account_number_trendline;
    fetched rows / total rows = 4/4
    +--------------------------+
    | account_number_trendline |
    |--------------------------|
    | null                     |
    | 3.5                      |
    | 9.5                      |
    | 15.5                     |
    +--------------------------+

