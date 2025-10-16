=============
rare
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
===========
| The ``rare`` command finds the least common tuple of values of all fields in the field list.

| **Note**: A maximum of 10 results is returned for each distinct tuple of values of the group-by fields.

Syntax
======
rare [rare-options] <field-list> [by-clause]

* field-list: mandatory. Comma-delimited list of field names.
* by-clause: optional. One or more fields to group the results by.
* rare-options: optional. Options for the rare command. Supported syntax is [countfield=<string>] [showcount=<bool>].
* showcount=<bool>: optional. Whether to create a field in output that represent a count of the tuple of values. **Default:** ``true``.
* countfield=<string>: optional. The name of the field that contains count. **Default:** ``'count'``.


Example 1: Find the least common values in a field
==================================================

This example shows how to find the least common gender of all the accounts.

PPL query::

    os> source=accounts | rare showcount=false gender;
    fetched rows / total rows = 2/2
    +--------+
    | gender |
    |--------|
    | F      |
    | M      |
    +--------+


Example 2: Find the least common values organized by gender
===========================================================

This example shows how to find the least common age of all the accounts grouped by gender.

PPL query::

    os> source=accounts | rare showcount=false age by gender;
    fetched rows / total rows = 4/4
    +--------+-----+
    | gender | age |
    |--------+-----|
    | F      | 28  |
    | M      | 32  |
    | M      | 33  |
    | M      | 36  |
    +--------+-----+

Example 3: Rare command
=======================

This example shows how to find the least common gender of all the accounts.

PPL query::

    PPL> source=accounts | rare gender;
    fetched row
    +--------+-------+
    | gender | count |
    |--------+-------|
    | F      | 1     |
    | M      | 3     |
    +--------+-------+


Example 4: Specify the count field option
=========================================

This example shows how to specify the count field.

PPL query::

    PPL> source=accounts | rare countfield='cnt' gender;
    fetched row
    +--------+-----+
    | gender | cnt |
    |--------+-----|
    | F      | 1   |
    | M      | 3   |
    +--------+-----+

Limitations
===========
| The ``rare`` command is not rewritten to OpenSearch DSL, it is only executed on the coordination node.
