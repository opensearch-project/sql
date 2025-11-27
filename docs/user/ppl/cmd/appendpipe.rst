=========
appendpipe
=========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``appendpipe`` command appends the result of the subpipeline to the search results. Unlike a subsearch, the subpipeline is not run first.The subpipeline is run when the search reaches the appendpipe command.
The command aligns columns with the same field names and types. For different column fields between the main search and sub-search, NULL values are filled in the respective rows.

Syntax
============
appendpipe [<subpipeline>]

* subpipeline: mandatory. A list of commands that are applied to the search results from the commands that occur in the search before the ``appendpipe`` command.

Example 1: Append rows from a total count to existing search result
====================================================================================

This example appends rows from "total by gender" to "sum by gender, state" with merged column of same field name and type.

PPL query::

    os> source=accounts | stats sum(age) as part by gender, state | sort -part | head 5 | appendpipe [ stats sum(part) as total by gender ];
    fetched rows / total rows = 6/6
    +------+--------+-------+-------+
    | part | gender | state | total |
    |------+--------+-------+-------|
    | 36   | M      | TN    | null  |
    | 33   | M      | MD    | null  |
    | 32   | M      | IL    | null  |
    | 28   | F      | VA    | null  |
    | null | F      | null  | 28    |
    | null | M      | null  | 101   |
    +------+--------+-------+-------+



Example 2: Append rows with merged column names
===============================================================

This example appends rows from "count by gender" to "sum by gender, state".

PPL query::

    os> source=accounts | stats sum(age) as total by gender, state | sort -total | head 5 | appendpipe [ stats sum(total) as total by gender ];
    fetched rows / total rows = 6/6
    +----------+--------+-------+
    | total    | gender | state |
    |----------+--------+-------|
    | 36       | M      | TN    |
    | 33       | M      | MD    |
    | 32       | M      | IL    |
    | 28       | F      | VA    |
    | 28       | F      | null  |
    | 101      | M      | null  |
    +----------+--------+-------+

Limitations
===========

* **Schema Compatibility**: Same as command ``append``, when fields with the same name exist between the main search and sub-search but have incompatible types, the query will fail with an error. To avoid type conflicts, ensure that fields with the same name have the same data type, or use different field names (e.g., by renaming with ``eval`` or using ``fields`` to select non-conflicting columns).
