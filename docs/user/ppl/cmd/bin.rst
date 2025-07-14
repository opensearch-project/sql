=============
bin
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``bin`` command groups numeric values into buckets of equal intervals, making it useful for creating histograms and analyzing data distribution. It takes a numeric field and generates a new field with values that represent the lower bound of each bucket.

Syntax
============
bin <field> [span=<interval>] [bins=<count>] [start=<value>] [end=<value>] [AS <alias>]

* field: mandatory. The numeric field to bin.
* span: optional. The interval size for each bin. Cannot be used with bins parameter.
* bins: optional. The number of equal-width bins to create. Cannot be used with span parameter.
* start: optional. The starting value for binning range. If not specified, uses the minimum field value.
* end: optional. The ending value for binning range. If not specified, uses the maximum field value.
* alias: optional. Custom name for the binned field. **Default:** <field>_bin

Parameters
============

span Parameter
--------------
Specifies the width of each bin interval. The bin value will be calculated as ``floor(field / span) * span``.

bins Parameter
--------------
Automatically calculates the span based on the data range divided by the specified number of bins. The span is calculated as ``(max_value - min_value) / bins``.

start and end Parameters
-------------------------
Define the range for binning. Values outside this range will be binned to the nearest boundary.

alias Parameter
---------------
Provides a custom name for the new binned field. If not specified, the field name will be appended with "_bin".

Example 1: Basic binning with span
===================================

The example shows binning account balances into $5000 intervals.

PPL query::

    os> source=accounts | bin balance span=5000 | fields account_number, balance, balance_bin | head 5;
    fetched rows / total rows = 5/5
    +----------------+---------+-------------+
    | account_number | balance | balance_bin |
    |----------------+---------+-------------|
    | 1              | 39225   | 35000       |
    | 6              | 5686    | 5000        |
    | 13             | 32838   | 30000       |
    | 18             | 4180    | 0           |
    | 20             | 16418   | 15000       |
    +----------------+---------+-------------+

Example 2: Binning with custom alias
=====================================

The example shows binning with a custom field name.

PPL query::

    os> source=accounts | bin balance span=10000 AS balance_range | fields account_number, balance, balance_range | head 3;
    fetched rows / total rows = 3/3
    +----------------+---------+---------------+
    | account_number | balance | balance_range |
    |----------------+---------+---------------|
    | 1              | 39225   | 30000         |
    | 6              | 5686    | 0             |
    | 13             | 32838   | 30000         |
    +----------------+---------+---------------+

Example 3: Binning with bins parameter
=======================================

The example shows creating 5 equal-width bins for age field.

PPL query::

    os> source=accounts | bin age bins=5 | fields account_number, age, age_bin | head 3;
    fetched rows / total rows = 3/3
    +----------------+-----+---------+
    | account_number | age | age_bin |
    |----------------+-----+---------|
    | 1              | 32  | 30.2    |
    | 6              | 36  | 36.8    |
    | 13             | 28  | 28.0    |
    +----------------+-----+---------+

Example 4: Binning with stats aggregation
==========================================

The example shows using bin command with stats to create a histogram.

PPL query::

    os> source=accounts | bin balance span=10000 AS balance_bucket | stats count() by balance_bucket | sort balance_bucket;
    fetched rows / total rows = 5/5
    +---------+----------------+
    | count() | balance_bucket |
    |---------+----------------|
    | 2       | 0              |
    | 1       | 10000          |
    | 1       | 30000          |
    | 2       | 40000          |
    | 1       | 50000          |
    +---------+----------------+

Example 5: Binning with decimal span
=====================================

The example shows binning with decimal interval values.

PPL query::

    os> source=accounts | bin balance span=7500.5 AS balance_group | fields account_number, balance, balance_group | head 3;
    fetched rows / total rows = 3/3
    +----------------+---------+---------------+
    | account_number | balance | balance_group |
    |----------------+---------+---------------|
    | 1              | 39225   | 37502.5       |
    | 6              | 5686    | 0.0           |
    | 13             | 32838   | 30002.0       |
    +----------------+---------+---------------+

Example 6: Default binning behavior
====================================

The example shows bin command without parameters (uses span=1 by default).

PPL query::

    os> source=accounts | bin age | fields account_number, age, age_bin | head 3;
    fetched rows / total rows = 3/3
    +----------------+-----+---------+
    | account_number | age | age_bin |
    |----------------+-----+---------|
    | 1              | 32  | 32      |
    | 6              | 36  | 36      |
    | 13             | 28  | 28      |
    +----------------+-----+---------+

Example 7: Binning with range specification
============================================

The example shows binning with start and end parameters to focus on a specific range.

PPL query::

    os> source=accounts | bin balance span=5000 start=10000 end=50000 AS balance_range | fields account_number, balance, balance_range | head 4;
    fetched rows / total rows = 4/4
    +----------------+---------+---------------+
    | account_number | balance | balance_range |
    |----------------+---------+---------------|
    | 1              | 39225   | 35000         |
    | 13             | 32838   | 30000         |
    | 20             | 16418   | 15000         |
    | 25             | 40540   | 40000         |
    +----------------+---------+---------------+

Best Practices
==============

Choosing Bin Parameters
------------------------
* Use ``span`` when you know the desired interval size (e.g., $1000 for financial data)
* Use ``bins`` when you want a specific number of buckets for visualization
* Consider your data range when choosing span values to avoid too many or too few bins

Performance Considerations
--------------------------
* Binning is performed during query execution and may impact performance on large datasets
* Consider using appropriate span sizes to balance detail and performance
* Use with ``stats`` command for efficient histogram generation

Common Use Cases
================
* **Histograms**: Combine with ``stats count()`` to create frequency distributions
* **Time-based Analysis**: Bin timestamp fields for time-series analysis
* **Data Categorization**: Group continuous values into discrete categories
* **Outlier Detection**: Identify unusual value distributions

Relationship to span() Function
================================
The ``bin`` command is similar to using the ``span()`` function in stats aggregations, but with key differences:

* ``bin`` creates a new field that can be used in subsequent commands
* ``span()`` is used within stats aggregations for grouping
* ``bin`` supports the ``bins`` parameter for automatic span calculation
* ``bin`` allows more flexible field naming with aliases

Comparison::

    # Using bin command
    source=accounts | bin balance span=5000 | stats count() by balance_bin
    
    # Using span() function  
    source=accounts | stats count() by span(balance, 5000)

Both approaches create similar results, but ``bin`` provides more flexibility for complex queries where the binned field needs to be used in multiple places.

Limitations
===========
* The ``span`` and ``bins`` parameters are mutually exclusive
* Only numeric fields can be binned
* The ``start`` and ``end`` parameters are currently not fully implemented
* Requires Calcite engine (not supported in legacy engine)