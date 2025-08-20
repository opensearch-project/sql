=============
rename
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Using ``rename`` command to rename one or more fields in the search result.


Syntax
============
rename <source-field> AS <target-field>["," <source-field> AS <target-field>]...

* source-field: mandatory. The name of the field you want to rename. Supports wildcard patterns using ``*``.
* target-field: mandatory. The name you want to rename to. Must have same number of wildcards as the source.

**Notes:** 

* Literal asterisk (*) characters in field names cannot be replaced as asterisk is used for wildcard matching.
* Wildcards are only supported when the Calcite query engine is enabled.


Example 1: Rename one field
===========================

The example show rename one field.

PPL query::

    os> source=accounts | rename account_number as an | fields an;
    fetched rows / total rows = 4/4
    +----+
    | an |
    |----|
    | 1  |
    | 6  |
    | 13 |
    | 18 |
    +----+


Example 2: Rename multiple fields
=================================

The example show rename multiple fields.

PPL query::

    os> source=accounts | rename account_number as an, employer as emp | fields an, emp;
    fetched rows / total rows = 4/4
    +----+---------+
    | an | emp     |
    |----+---------|
    | 1  | Pyrami  |
    | 6  | Netagy  |
    | 13 | Quility |
    | 18 | null    |
    +----+---------+


Example 3: Rename with wildcards
=================================

The example shows renaming multiple fields using wildcard patterns. (Requires Calcite query engine)

PPL query::

    PPL> source=accounts | rename *name as *_name | fields first_name, last_name;
    fetched rows / total rows = 4/4
    +------------+-----------+
    | first_name | last_name |
    |------------+-----------|
    | Amber      | Duke      |
    | Hattie     | Bond      |
    | Nanette    | Bates     |
    | Dale       | Adams     |
    +------------+-----------+


Example 4: Rename with multiple wildcard patterns
==================================================

The example shows renaming multiple fields using multiple wildcard patterns. (Requires Calcite query engine)

PPL query::

    PPL> source=accounts | rename *name as *_name, *_number as *number | fields first_name, last_name, accountnumber;
    fetched rows / total rows = 4/4
    +------------+-----------+---------------+
    | first_name | last_name | accountnumber |
    |------------+-----------+---------------|
    | Amber      | Duke      | 1             |
    | Hattie     | Bond      | 6             |
    | Nanette    | Bates     | 13            |
    | Dale       | Adams     | 18            |
    +------------+-----------+---------------+

Limitation
==========
The ``rename`` command is not rewritten to OpenSearch DSL, it is only executed on the coordination node.
