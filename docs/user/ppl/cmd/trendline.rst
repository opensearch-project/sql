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
`TRENDLINE [sort <[+|-] sort-field>] <trendline-type>(number-of-datapoints, field) [AS alias] [<trendline-type>(number-of-datapoints, field) [AS alias]]...`

* [+|-]: optional. The plus [+] stands for ascending order and NULL/MISSING first and a minus [-] stands for descending order and NULL/MISSING last. **Default:** ascending order and NULL/MISSING first.
* sort-field: mandatory when sorting is used. The field used to sort.
* trendline-type: mandatory. The type of algorithm being used for the calculation, only SMA (simple moving average) or WMA (weighted moving average) are supported at the moment.
* number-of-datapoints: mandatory. The number of datapoints to calculate the moving average (must be greater than zero).
* field: mandatory. The name of the field the moving average should be calculated for.
* alias: optional. The name of the resulting column containing the moving average (defaults to the field name with "_<trendline-type>_trendline").

In the case of Simple Moving Average - SMA, result will be calculated as per the below formula.

    f[i]: The value of field 'f' in the i-th data-point
    n: The number of data-points in the moving window (period)
    t: The current time index

    SMA(t) = (1/n) * Σ(f[i]), where i = t-n+1 to t

Example 1: Calculate the simple moving average on one field.
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


Example 2: Calculate the simple moving average on multiple fields.
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

Example 3: Calculate the simple moving average on one field without specifying an alias.
=================================================================================

The example shows how to calculate the moving average on one field without specifying an alias.

PPL query::

    os> source=accounts | trendline sma(2, account_number)  | fields account_number_sma_trendline;
    fetched rows / total rows = 4/4
    +--------------------------+
    | account_number_trendline |
    |--------------------------|
    | null                     |
    | 3.5                      |
    | 9.5                      |
    | 15.5                     |
    +--------------------------+



In the case of Weighted Moving Average - WMA, result will be calculated as per the below formula.

    f[i]: The value of field 'f' in the i-th data point
    n: The number of data points in the moving window (period)
    t: The current time index
    w[i]: The weight of the i-th data point, increasing by one per step to prioritize recent points.

    WMA(t) = ( Σ from i=t−n+1 to t of (w[i] * f[i]) ) / ( Σ from i=t−n+1 to t of w[i] )

Example 1: Calculate the weighted moving average on one field.
=====================================================

The example shows how to calculate the weighted moving average on one field.

PPL query::

    os> source=accounts | trendline wma(2, account_number) as an | fields an;
    fetched rows / total rows = 4/4
    +--------------------+
    | an                 |
    |--------------------|
    | null               |
    | 4.333333333333333  |
    | 10.666666666666666 |
    | 16.333333333333332 |
    +--------------------+

Example 2: Calculate the weighted moving average on multiple fields.
===========================================================

The example shows how to calculate the weighted moving average on multiple fields.

PPL query::

    os> source=accounts | trendline wma(2, account_number) as an sma(2, age) as age_trend | fields an, age_trend ;
    fetched rows / total rows = 4/4
    +--------------------+-----------+
    | an                 | age_trend |
    |--------------------+-----------|
    | null               | null      |
    | 4.333333333333333  | 34.0      |
    | 10.666666666666666 | 32.0      |
    | 16.333333333333332 | 30.5      |
    +--------------------+-----------+


Example 3: Calculate the weighted moving average on one field without specifying an alias.
=================================================================================

The example shows how to calculate the weighted moving average on one field without specifying an alias.

PPL query::

    os> source=accounts | trendline wma(2, account_number) | fields account_number_wma_trendline;
    fetched rows / total rows = 4/4
    +--------------------------+
    | account_number_trendline |
    |--------------------------|
    | null                     |
    | 4.333333333333333        |
    | 10.666666666666666       |
    | 16.333333333333332       |
    +--------------------------+

