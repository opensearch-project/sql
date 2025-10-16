=======
replace
=======

.. rubric:: Table of contents

.. contents::
 :local:
 :depth: 2


Description
===========
The ``replace`` replaces text in one or more fields in the search result.


Syntax
======
replace '<pattern>' WITH '<replacement>' [, '<pattern>' WITH '<replacement>']... IN <field-name>[, <field-name>]...

* pattern: mandatory. The text pattern you want to replace. Currently supports only plain text literals (no wildcards or regular expressions).
* replacement: mandatory. The text you want to replace with.
* field-name: mandatory. One or more field names where the replacement should occur.

Example 1: Replace text in one field
====================================

This example shows replacing text in one field.

PPL query::

 os> source=accounts | replace "IL" WITH "Illinois" IN state | fields state;
 fetched rows / total rows = 4/4
 +----------+
 | state    |
 |----------|
 | Illinois |
 | TN       |
 | VA       |
 | MD       |
 +----------+


Example 2: Replace text in multiple fields
==========================================

This example shows replacing text in multiple fields.

PPL query::

 os> source=accounts | replace "IL" WITH "Illinois" IN state, address | fields state, address;
 fetched rows / total rows = 4/4
 +----------+----------------------+
 | state    | address              |
 |----------+----------------------|
 | Illinois | 880 Holmes Lane      |
 | TN       | 671 Bristol Street   |
 | VA       | 789 Madison Street   |
 | MD       | 467 Hutchinson Court |
 +----------+----------------------+


Example 3: Replace with other commands in a pipeline
====================================================

This example shows using replace with other commands in a query pipeline.

PPL query::

 os> source=accounts | replace "IL" WITH "Illinois" IN state | where age > 30 | fields state, age;
 fetched rows / total rows = 3/3
 +----------+-----+
 | state    | age |
 |----------+-----|
 | Illinois | 32  |
 | TN       | 36  |
 | MD       | 33  |
 +----------+-----+

Example 4: Replace with multiple pattern/replacement pairs
==========================================================

This example shows using multiple pattern/replacement pairs in a single replace command. The replacements are applied sequentially.

PPL query::

 os> source=accounts | replace "IL" WITH "Illinois", "TN" WITH "Tennessee" IN state | fields state;
 fetched rows / total rows = 4/4
 +-----------+
 | state     |
 |-----------|
 | Illinois  |
 | Tennessee |
 | VA        |
 | MD        |
 +-----------+

Example 5: Pattern matching with LIKE and replace
=================================================

Since replace command only supports plain string literals, you can use LIKE command with replace for pattern matching needs.

PPL query::

 os> source=accounts | where LIKE(address, '%Holmes%') | replace "Holmes" WITH "HOLMES" IN address | fields address, state, gender, age, city;
 fetched rows / total rows = 1/1
 +-----------------+-------+--------+-----+--------+
 | address         | state | gender | age | city   |
 |-----------------+-------+--------+-----+--------|
 | 880 HOLMES Lane | IL    | M      | 32  | Brogan |
 +-----------------+-------+--------+-----+--------+


Limitations
===========
* Only supports plain text literals for pattern matching. Wildcards and regular expressions are not supported.
* Pattern and replacement values must be string literals.
* The replace command modifies the specified fields in-place.