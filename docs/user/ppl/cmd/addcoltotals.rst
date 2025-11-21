==========
AddColTotals
==========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Description
===========

The ``addcoltotals`` command computes the sum of each column and add a summary event at the end to show the total of each column. This command works the same way ``addtotals`` command works with row=false and col=true option. This is useful for creating summary reports with subtotals or grand totals.
The ``addcoltotals`` command only sums numeric fields (integers, floats, doubles). Non-numeric fields in the field list are ignored even if its specified in field-list or in the case of no field-list specified.

Syntax
======

``addcoltotals [field-list] [label=<string>] [labelfield=<field>]  ``

* ``field-list``: Optional. Comma-separated list of numeric fields to sum. If not specified, all numeric fields are summed.
* ``labelfield=<field>``: Optional. Field name to place the label. If it  specifies a non-existing field, adds the field and shows label at the summary event row at this field. This is applicable when col=true.
* ``label=<string>``: Optional. Custom text for the totals row labelfield's label. Default is "Total".  This is applicable when col=true.


Example 1: Basic Example
=========================

The example shows placing the label in an existing field.

PPL query::

    os> source=accounts | fields firstname, balance | head 3 | addcoltotals labelfield='firstname';
    fetched rows / total rows = 4/4
    +-----------+---------+
    | firstname | balance |
    |-----------+---------|
    | Amber     | 39225   |
    | Hattie    | 5686    |
    | Nanette   | 32838   |
    | Total     | 77749   |
    +-----------+---------+


Example 2: Adding column totals and adding a summary event with label specified.
=================================================================================

The example shows adding totals after a stats command where final summary event label is 'Sum' and row=true value was used by default when not specified. It also added new field
specified by labelfield as it did not match existing field.


PPL query::

    os> source=accounts | stats count() by gender | addcoltotals `count()` label='Sum' labelfield='Total';
    fetched rows / total rows = 3/3
    +---------+--------+-------+
    | count() | gender | Total |
    |---------+--------+-------|
    | 1       | F      | null  |
    | 3       | M      | null  |
    | 4       | null   | Sum   |
    +---------+--------+-------+

Example 3: With all options
============================

The example shows using addcoltotals with all options set.

PPL query::

    os> source=accounts | where age > 30 | stats avg(balance) as avg_balance, count() as count by state | head 3 | addcoltotals avg_balance, count  label='Sum' labelfield='Column Total';
    fetched rows / total rows = 4/4
    +-------------+-------+-------+--------------+
    | avg_balance | count | state | Column Total |
    |-------------+-------+-------+--------------|
    | 39225.0     | 1     | IL    | null         |
    | 4180.0      | 1     | MD    | null         |
    | 5686.0      | 1     | TN    | null         |
    | 49091.0     | 3     | null  | Sum          |
    +-------------+-------+-------+--------------+





