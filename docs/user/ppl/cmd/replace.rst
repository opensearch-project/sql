=============
replace
=============

.. rubric:: Table of contents

.. contents::
 :local:
 :depth: 2


Description
============
Using ``replace`` command to replace text in one or more fields in the search result.

Note: This command is only available when Calcite engine is enabled.


Syntax
============
replace '<pattern>' WITH '<replacement>' IN <field-name>[, <field-name>]...


Parameters
==========
* **pattern**: mandatory. The text pattern you want to replace. Currently supports only plain text literals (no wildcards or regular expressions).
* **replacement**: mandatory. The text you want to replace with.
* **field-name**: mandatory. One or more field names where the replacement should occur.


Examples
========

Example 1: Replace text in one field
------------------------------------

The example shows replacing text in one field.

PPL query::

 os> source=accounts | replace "IL" WITH "Illinois" IN state | fields state, new_state;
 fetched rows / total rows = 4/4
 +-------+-----------+
 | state | new_state |
 |-------+-----------|
 | IL    | Illinois  |
 | TN    | TN        |
 | VA    | VA        |
 | MD    | MD        |
 +-------+-----------+


Example 2: Replace text in multiple fields
------------------------------------

The example shows replacing text in multiple fields.

PPL query::

 os> source=accounts | replace "IL" WITH "Illinois" IN state, address | fields state, address, new_state, new_address;
 fetched rows / total rows = 4/4
 +-------+----------------------+-----------+----------------------+
 | state | address              | new_state | new_address          |
 |-------+----------------------+-----------+----------------------|
 | IL    | 880 Holmes Lane      | Illinois  | 880 Holmes Lane      |
 | TN    | 671 Bristol Street   | TN        | 671 Bristol Street   |
 | VA    | 789 Madison Street   | VA        | 789 Madison Street   |
 | MD    | 467 Hutchinson Court | MD        | 467 Hutchinson Court |
 +-------+----------------------+-----------+----------------------+


Example 3: Replace with IN clause and other commands
------------------------------------

The example shows using replace with other commands.

PPL query::

 os> source=accounts | replace "IL" WITH "Illinois" IN state | where age > 30 | fields state, age, new_state;
 fetched rows / total rows = 3/3
 +-------+-----+-----------+
 | state | age | new_state |
 |-------+-----+-----------|
 | IL    | 32  | Illinois  |
 | TN    | 36  | TN        |
 | MD    | 33  | MD        |
 +-------+-----+-----------+

Example 4: Pattern matching with LIKE and replace
------------------------------------

Since replace command only supports plain string literals, you can use LIKE command with replace for pattern matching needs.

PPL query::

 os> source=accounts | where LIKE(address, '%Holmes%') | replace "Holmes" WITH "HOLMES" IN address | fields address, state, gender, age, city, new_address;
 fetched rows / total rows = 1/1
 +-----------------+-------+--------+-----+--------+-----------------+
 | address         | state | gender | age | city   | new_address     |
 |-----------------+-------+--------+-----+--------+-----------------|
 | 880 Holmes Lane | IL    | M      | 32  | Brogan | 880 HOLMES Lane |
 +-----------------+-------+--------+-----+--------+-----------------+


Limitations
===========
* Only supports plain text literals for pattern matching. Wildcards and regular expressions are not supported.
* Pattern and replacement values must be string literals.
* For each field specified in the IN clause, a new field is created with prefix *new_* containing the replaced text. The original fields remain unchanged.
* If a field with *new_* prefix already exists (e.g., 'new_country'), a number will be appended to create a unique field name (e.g., 'new_country0').