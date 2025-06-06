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
`TRENDLINE [sort <[+|-] sort-field>] [SMA|WMA](number-of-datapoints, field) [AS alias] [[SMA|WMA](number-of-datapoints, field) [AS alias]]...`

* [+|-]: optional. The plus [+] stands for ascending order and NULL/MISSING first and a minus [-] stands for descending order and NULL/MISSING last. **Default:** ascending order and NULL/MISSING first.
* sort-field: mandatory when sorting is used. The field used to sort.
* number-of-datapoints: mandatory. The number of datapoints to calculate the moving average (must be greater than zero).
* field: mandatory. The name of the field the moving average should be calculated for.
* alias: optional. The name of the resulting column containing the moving average (defaults to the field name with "_trendline").

Starting with version 3.1.0, two trendline algorithms are supported, aka Simple Moving Average (SMA) amd Weighted Moving Average (WMA).

Suppose:

* f[i]: The value of field 'f' in the i-th data-point
* n: The number of data-points in the moving window (period)
* t: The current time index

SMA is calculated like

    SMA(t) = (1/n) * Σ(f[i]), where i = t-n+1 to t

WMA places more weights on recent values compared to equal-weighted SMA algorithm

    WMA(t) = (1/(1 + 2 + ... + n)) * (1 * f(i-n+1) + 2 * f(i-n+2) + ... + n * f(i))


Example 1: Calculate the simple moving average on one field.
=====================================================

The example shows how to calculate the simple moving average on one field.

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

The example shows how to calculate the simple moving average on multiple fields.

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

The example shows how to calculate the simple moving average on one field.

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

Example 4: Calculate the weighted moving average on one field.
=================================================================================

Version
-------
3.1.0

Configuration
-------------
wma algorithm requires Calcite enabled.

Enable Calcite:

    >> curl -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_query/settings -d '{
      "persistent" : {
        "plugins.calcite.enabled" : true
      }
    }'

The example shows how to calculate the weighted moving average on one field.

PPL query::

    PPL> source=accounts | trendline wma(2, account_number)  | fields account_number_trendline;
    fetched rows / total rows = 4/4
    +--------------------------+
    | account_number_trendline |
    |--------------------------|
    | null                     |
    | 4.333333333333333        |
    | 10.666666666666666       |
    | 16.333333333333332       |
    +--------------------------+

Limitation
==========
Starting with version 3.1.0, the ``trendline`` command requires all values in the specified ``field`` to be non-null. Any null values present in the calculation field will be automatically excluded from the command's output.