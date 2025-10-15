=========
appendcol
=========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``appendcol`` command appends the result of a sub-search and attaches it alongside with the input search results (The main search).

Syntax
============
appendcol [override=<boolean>] <sub-search>

* override=<boolean>: optional. Boolean field to specify should result from main-result be overwritten in the case of column name conflict. **Default:** false.
* sub-search: mandatory. Executes PPL commands as a secondary search. The sub-search uses the same data specified in the source clause of the main search results as its input.

Example 1: Append a count aggregation to existing search result
===============================================================

This example appends "count by gender" to "sum by gender, state".

PPL query::

    PPL> source=accounts | stats sum(age) by gender, state | appendcol [ stats count(age) by gender ] | head 10;
    fetched rows / total rows = 10/10
    +--------+-------+----------+------------+
    | gender | state | sum(age) | count(age) |
    |--------+-------+----------+------------|
    | F      | AK    | 317      | 493        |
    | F      | AL    | 397      | 507        |
    | F      | AR    | 229      | NULL       |
    | F      | AZ    | 238      | NULL       |
    | F      | CA    | 282      | NULL       |
    | F      | CO    | 217      | NULL       |
    | F      | CT    | 147      | NULL       |
    | F      | DC    | 358      | NULL       |
    | F      | DE    | 101      | NULL       |
    | F      | FL    | 310      | NULL       |
    +--------+-------+----------+------------+

Example 2: Append a count aggregation to existing search result with override option
====================================================================================

This example appends "count by gender" to "sum by gender, state" with override option.

PPL query::

    PPL> source=accounts | stats sum(age) by gender, state | appendcol override=true [ stats count(age) by gender ] | head 10;
    fetched rows / total rows = 10/10
    +--------+-------+----------+------------+
    | gender | state | sum(age) | count(age) |
    |--------+-------+----------+------------|
    | F      | AK    | 317      | 493        |
    | M      | AL    | 397      | 507        |
    | F      | AR    | 229      | NULL       |
    | F      | AZ    | 238      | NULL       |
    | F      | CA    | 282      | NULL       |
    | F      | CO    | 217      | NULL       |
    | F      | CT    | 147      | NULL       |
    | F      | DC    | 358      | NULL       |
    | F      | DE    | 101      | NULL       |
    | F      | FL    | 310      | NULL       |
    +--------+-------+----------+------------+

Example 3: Append multiple sub-search results
=============================================

This example shows how to chain multiple appendcol commands to add columns from different sub-searches.

PPL query::

    PPL> source=employees | fields name, dept, age | appendcol [ stats avg(age) as avg_age ] | appendcol [ stats max(age) as max_age ];
    fetched rows / total rows = 9/9
    +------+-------------+-----+------------------+---------+
    | name | dept        | age | avg_age          | max_age |
    |------+-------------+-----+------------------+---------|
    | Lisa | Sales       | 35  | 31.2222222222222 | 38      |
    | Fred | Engineering | 28  | NULL             | NULL    |
    | Paul | Engineering | 23  | NULL             | NULL    |
    | Evan | Sales       | 38  | NULL             | NULL    |
    | Chloe| Engineering | 25  | NULL             | NULL    |
    | Tom  | Engineering | 33  | NULL             | NULL    |
    | Alex | Sales       | 33  | NULL             | NULL    |
    | Jane | Marketing   | 28  | NULL             | NULL    |
    | Jeff | Marketing   | 38  | NULL             | NULL    |
    +------+-------------+-----+------------------+---------+

Example 4: Override case of column name conflict
================================================

This example demonstrates the override option when column names conflict between main search and sub-search.

PPL query::

    PPL> source=employees | stats avg(age) as agg by dept | appendcol override=true [ stats max(age) as agg by dept ];
    fetched rows / total rows = 3/3
    +-----+-------------+
    | agg | dept        |
    |-----+-------------|
    | 38  | Sales       |
    | 38  | Engineering |
    | 38  | Marketing   |
    +-----+-------------+

