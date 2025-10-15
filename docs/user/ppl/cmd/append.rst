=========
append
=========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``append`` command appends the result of a sub-search and attaches it as additional rows to the bottom of the input search results (The main search).
The command aligns columns with the same field names and types. For different column fields between the main search and sub-search, NULL values are filled in the respective rows.

Syntax
============
append <sub-search>

* sub-search: mandatory. Executes PPL commands as a secondary search.

Example 1: Append rows from a count aggregation to existing search result
=========================================================================

This example appends rows from "count by gender" to "sum by gender, state".

PPL query::

    os> source=accounts | stats sum(age) by gender, state | sort -`sum(age)` | head 5 | append [ source=accounts | stats count(age) by gender ];
    fetched rows / total rows = 6/6
    +----------+--------+-------+------------+
    | sum(age) | gender | state | count(age) |
    |----------+--------+-------+------------|
    | 36       | M      | TN    | null       |
    | 33       | M      | MD    | null       |
    | 32       | M      | IL    | null       |
    | 28       | F      | VA    | null       |
    | null     | F      | null  | 1          |
    | null     | M      | null  | 3          |
    +----------+--------+-------+------------+

Example 2: Append rows with merged column names
===============================================

This example appends rows from "sum by gender" to "sum by gender, state" with merged column of same field name and type.

PPL query::

    os> source=accounts | stats sum(age) as sum by gender, state | sort -sum | head 5 | append [ source=accounts | stats sum(age) as sum by gender ];
    fetched rows / total rows = 6/6
    +-----+--------+-------+
    | sum | gender | state |
    |-----+--------+-------|
    | 36  | M      | TN    |
    | 33  | M      | MD    |
    | 32  | M      | IL    |
    | 28  | F      | VA    |
    | 28  | F      | null  |
    | 101 | M      | null  |
    +-----+--------+-------+

Example 3: Append rows with column type conflict
================================================

This example shows how column type conflicts are handled when appending results. Same name columns with different types will generate two different columns in appended result.

PPL query::

    os> source=accounts | stats sum(age) as sum by gender, state | sort -sum | head 5 | append [ source=accounts | stats sum(age) as sum by gender | eval sum = cast(sum as double) ];
    fetched rows / total rows = 6/6
    +------+--------+-------+-------+
    | sum  | gender | state | sum0  |
    |------+--------+-------+-------|
    | 36   | M      | TN    | null  |
    | 33   | M      | MD    | null  |
    | 32   | M      | IL    | null  |
    | 28   | F      | VA    | null  |
    | null | F      | null  | 28.0  |
    | null | M      | null  | 101.0 |
    +------+--------+-------+-------+

