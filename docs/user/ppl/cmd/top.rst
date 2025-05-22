=============
top
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Using ``top`` command to find the most common tuple of values of all fields in the field list.


Syntax
============
top [N] <field-list> [by-clause]

top [N] <field-list> [top-options] [by-clause] ``(available from 3.1.0+)``

* N: number of results to return. **Default**: 10
* field-list: mandatory. comma-delimited list of field names.
* by-clause: optional. one or more fields to group the results by.
* top-options: optional. options for the top command. Supported syntax is [countfield=<string>] [showcount=<bool>].
* showcount=<bool>: optional. whether to create a field in output that represent a count of the tuple of values. Default value is ``true``.
* countfield=<string>: optional. the name of the field that contains count. Default value is ``'count'``.


Example 1: Find the most common values in a field
===========================================

The example finds most common gender of all the accounts.

PPL query::

    os> source=accounts | top gender;
    fetched rows / total rows = 2/2
    +--------+
    | gender |
    |--------|
    | M      |
    | F      |
    +--------+

Example 2: Find the most common values in a field
===========================================

The example finds most common gender of all the accounts.

PPL query::

    os> source=accounts | top 1 gender;
    fetched rows / total rows = 1/1
    +--------+
    | gender |
    |--------|
    | M      |
    +--------+

Example 2: Find the most common values organized by gender
====================================================

The example finds most common age of all the accounts group by gender.

PPL query::

    os> source=accounts | top 1 age by gender;
    fetched rows / total rows = 2/2
    +--------+-----+
    | gender | age |
    |--------+-----|
    | F      | 28  |
    | M      | 32  |
    +--------+-----+

Example 3: Top command with Calcite enabled
===========================================

The example finds most common gender of all the accounts when ``plugins.calcite.enabled`` is true.

PPL query::

    PPL> source=accounts | top gender;
    fetched row
    +--------+-------+
    | gender | count |
    |--------+-------|
    | M      | 3     |
    | F      | 1     |
    +--------+-------+


Example 4: Specify the count field option
=========================================

The example specifies the count field when ``plugins.calcite.enabled`` is true.

PPL query::

    PPL> source=accounts | top countfield='cnt' gender;
    fetched row
    +--------+-----+
    | gender | cnt |
    |--------+-----|
    | M      | 3   |
    | F      | 1   |
    +--------+-----+

Limitation
==========
The ``top`` command is not rewritten to OpenSearch DSL, it is only executed on the coordination node.
