=============
replace
=============

.. rubric:: Table of contents

.. contents::
 :local:
 :depth: 2


Description
============
Using ``replace`` command to replace text in one or more fields in the search result. Supports both literal string replacement and wildcard pattern matching.

Note: This command is only available when Calcite engine is enabled.


Syntax
============
replace '<pattern>' WITH '<replacement>' [, '<pattern>' WITH '<replacement>']... IN <field-name>[, <field-name>]...


Parameters
==========
* **pattern**: mandatory. The text pattern you want to replace. Supports:

  - Plain text literals for exact matching
  - Wildcard patterns using ``*`` (asterisk) to match zero or more characters

* **replacement**: mandatory. The text you want to replace with. When using wildcards:

  - Can contain ``*`` to substitute captured wildcard portions
  - Must have the same number of wildcards as the pattern, or zero wildcards
  - Wildcards in replacement are substituted with values captured from the pattern match

* **field-name**: mandatory. One or more field names where the replacement should occur.


Examples
========

Example 1: Replace text in one field
------------------------------------

The example shows replacing text in one field.

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
------------------------------------

The example shows replacing text in multiple fields.

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
------------------------------------

The example shows using replace with other commands in a query pipeline.

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
------------------------------------

The example shows using multiple pattern/replacement pairs in a single replace command. The replacements are applied sequentially.

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
------------------------------------

Since replace command only supports plain string literals, you can use LIKE command with replace for pattern matching needs.

PPL query::

 os> source=accounts | where LIKE(address, '%Holmes%') | replace "Holmes" WITH "HOLMES" IN address | fields address, state, gender, age, city;
 fetched rows / total rows = 1/1
 +-----------------+-------+--------+-----+--------+
 | address         | state | gender | age | city   |
 |-----------------+-------+--------+-----+--------|
 | 880 HOLMES Lane | IL    | M      | 32  | Brogan |
 +-----------------+-------+--------+-----+--------+


Wildcard Pattern Matching
==========================

The replace command supports wildcard patterns using ``*`` (asterisk) to match zero or more characters. This provides flexible pattern matching for text transformation.

Example 6: Wildcard suffix match
---------------------------------

Replace values that end with a specific pattern. The wildcard ``*`` matches any prefix.

PPL query::

 os> source=accounts | replace "*IL" WITH "Illinois" IN state | fields state;
 fetched rows / total rows = 4/4
 +----------+
 | state    |
 |----------|
 | Illinois |
 | TN       |
 | VA       |
 | MD       |
 +----------+


Example 7: Wildcard prefix match
---------------------------------

Replace values that start with a specific pattern. The wildcard ``*`` matches any suffix.

PPL query::

 os> source=accounts | replace "IL*" WITH "Illinois" IN state | fields state;
 fetched rows / total rows = 4/4
 +----------+
 | state    |
 |----------|
 | Illinois |
 | TN       |
 | VA       |
 | MD       |
 +----------+


Example 8: Wildcard capture and substitution
---------------------------------------------

Use wildcards in both pattern and replacement to capture and reuse matched portions. The number of wildcards must match in pattern and replacement.

PPL query::

 os> source=accounts | replace "* Lane" WITH "Lane *" IN address | fields address;
 fetched rows / total rows = 4/4
 +----------------------+
 | address              |
 |----------------------|
 | Lane 880 Holmes      |
 | 671 Bristol Street   |
 | 789 Madison Street   |
 | 467 Hutchinson Court |
 +----------------------+


Example 9: Multiple wildcards for pattern transformation
---------------------------------------------------------

Use multiple wildcards to transform patterns. Each wildcard in the replacement substitutes the corresponding captured value.

PPL query::

 os> source=accounts | replace "* *" WITH "*_*" IN address | fields address;
 fetched rows / total rows = 4/4
 +----------------------+
 | address              |
 |----------------------|
 | 880_Holmes Lane      |
 | 671_Bristol Street   |
 | 789_Madison Street   |
 | 467_Hutchinson Court |
 +----------------------+


Example 10: Wildcard with zero wildcards in replacement
--------------------------------------------------------

When replacement has zero wildcards, all matching values are replaced with the literal replacement string.

PPL query::

 os> source=accounts | replace "*IL*" WITH "Illinois" IN state | fields state;
 fetched rows / total rows = 4/4
 +----------+
 | state    |
 |----------|
 | Illinois |
 | TN       |
 | VA       |
 | MD       |
 +----------+


Wildcard Rules
==============

When using wildcards in the replace command:

* **Wildcard character**: Use ``*`` to match zero or more characters
* **Symmetry requirement**: The replacement must have the same number of wildcards as the pattern, OR zero wildcards
* **Substitution order**: Wildcards in replacement are substituted left-to-right with values captured from pattern
* **No match behavior**: If pattern doesn't match, the original value is returned unchanged
* **Case sensitivity**: Wildcard matching is case-sensitive

**Valid wildcard pairs:**

* Pattern: ``"*ada"`` (1 wildcard), Replacement: ``"CA"`` (0 wildcards) ✓
* Pattern: ``"* localhost"`` (1 wildcard), Replacement: ``"localhost *"`` (1 wildcard) ✓
* Pattern: ``"* - *"`` (2 wildcards), Replacement: ``"*_*"`` (2 wildcards) ✓

**Invalid wildcard pair:**

* Pattern: ``"* - *"`` (2 wildcards), Replacement: ``"*"`` (1 wildcard) ✗ (mismatch error)


Limitations
===========
* Pattern and replacement values must be string literals.
* The replace command modifies the specified fields in-place.
* Wildcard matching is case-sensitive.
* Regular expressions are not supported (only simple wildcard patterns with ``*``).
* Literal asterisk characters (``*``) cannot be matched or replaced when using wildcard patterns. To replace literal asterisks in your data, use non-wildcard patterns (do not include ``*`` in the pattern string).