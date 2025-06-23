=============
rare
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Using ``rare`` command to find the least common tuple of values of all fields in the field list.

**Note**: A maximum of 10 results is returned for each distinct tuple of values of the group-by fields.

Syntax
============
rare <field-list> [by-clause]

* field-list: mandatory. comma-delimited list of field names.
* by-clause: optional. one or more fields to group the results by.


Example 1: Find the least common values in a field
===========================================

The example finds least common gender of all the accounts.

PPL query::

    os> source=accounts | rare gender;
    fetched rows / total rows = 2/2
    +--------+
    | gender |
    |--------|
    | F      |
    | M      |
    +--------+


Example 2: Find the least common values organized by gender
====================================================

The example finds least common age of all the accounts group by gender.

PPL query::

    os> source=accounts | rare age by gender;
    fetched rows / total rows = 4/4
    +--------+-----+
    | gender | age |
    |--------+-----|
    | F      | 28  |
    | M      | 32  |
    | M      | 33  |
    | M      | 36  |
    +--------+-----+

Limitation
==========
The ``rare`` command is not rewritten to OpenSearch DSL, it is only executed on the coordination node.
