=========
append
=========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Using ``append`` command to append the result of a sub-search and attach it as additional rows to the bottom of the input search results (The main search).
The command aligns columns with the same field names and types. For different column fields between the main search and sub-search, NULL values are filled in the respective rows.

Version
=======
3.3.0

Syntax
============
append <sub-search>

* sub-search: mandatory. Executes PPL commands as a secondary search.

Limitations
===========

* **Schema Compatibility**: When fields with the same name exist between the main search and sub-search but have incompatible types, the query will fail with an error. To avoid type conflicts, ensure that fields with the same name have the same data type, or use different field names (e.g., by renaming with ``eval`` or using ``fields`` to select non-conflicting columns).

Example 1: Append rows from a count aggregation to existing search result
===============================================================

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
====================================================================================

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

