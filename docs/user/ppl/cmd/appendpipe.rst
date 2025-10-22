=========
appendpipe
=========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Using ``appendpipe`` command to appends the result of the subpipeline to the search results. Unlike a subsearch, the subpipeline is not run first.The subpipeline is run when the search reaches the appendpipe command.
The appendpipe command is used to append the output of transforming commands, such as chart, timechart, stats, and top.
The command aligns columns with the same field names and types. For different column fields between the main search and sub-search, NULL values are filled in the respective rows.

Version
=======
3.3.0

Syntax
============
appendpipe [<subpipeline>]

* subpipeline: mandatory. A list of commands that are applied to the search results from the commands that occur in the search before the ``appendpipe`` command.

Example 1: Append rows from a total count to existing search result
====================================================================================

This example appends rows from "total by gender" to "sum by gender, state" with merged column of same field name and type.

PPL query::

    os> source=accounts | stats sum(age) as part by gender, state | sort -sum | head 5 | appendpipe [ stats sum(part) as total by gender ];
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

    os> source=accounts | stats sum(age) as total by gender, state | sort -`sum(age)` | head 5 | appendpipe [ stats sum(total) as total by gender ];
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

Example 3: Append rows with column type conflict
=============================================

This example shows how column type conflicts are handled when appending results. Same name columns with different types will generate two different columns in appended result.

PPL query::

    os> source=accounts | stats sum(age) as total by gender, state | sort -`sum(age)` | head 5 | appendpipe [ stats sum(total) as total by gender | eval state = 123 ];
    fetched rows / total rows = 6/6
    +------+--------+-------+--------+
    | sum  | gender | state | state0 |
    |------+--------+-------+--------|
    | 36   | M      | TN    | null   |
    | 33   | M      | MD    | null   |
    | 32   | M      | IL    | null   |
    | 28   | F      | VA    | null   |
    | 28   | F      | null  | 123    |
    | 101  | M      | null  | 123    |
    +------+--------+-------+--------+

