====
eval
====

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
===========
The ``eval`` command evaluates the expression and appends the result to the search result.

Syntax
======
eval <field>=<expression> ["," <field>=<expression> ]...

* field: mandatory. If the field name does not exist, a new field is added. If the field name already exists, it will be overridden.
* expression: mandatory. Any expression supported by the system.

Example 1: Create a new field
=============================

This example shows creating a new field doubleAge for each document. The new doubleAge field is the result of multiplying age by 2.

PPL query::

    os> source=accounts | eval doubleAge = age * 2 | fields age, doubleAge ;
    fetched rows / total rows = 4/4
    +-----+-----------+
    | age | doubleAge |
    |-----+-----------|
    | 32  | 64        |
    | 36  | 72        |
    | 28  | 56        |
    | 33  | 66        |
    +-----+-----------+


Example 2: Override an existing field
=====================================

This example shows overriding the existing age field by adding 1 to it.

PPL query::

    os> source=accounts | eval age = age + 1 | fields age ;
    fetched rows / total rows = 4/4
    +-----+
    | age |
    |-----|
    | 33  |
    | 37  |
    | 29  |
    | 34  |
    +-----+

Example 3: Create a new field with field defined in eval
========================================================

This example shows creating a new field ddAge using a field defined in the same eval command. The new field ddAge is the result of multiplying doubleAge by 2, where doubleAge is defined in the same eval command.

PPL query::

    os> source=accounts | eval doubleAge = age * 2, ddAge = doubleAge * 2 | fields age, doubleAge, ddAge ;
    fetched rows / total rows = 4/4
    +-----+-----------+-------+
    | age | doubleAge | ddAge |
    |-----+-----------+-------|
    | 32  | 64        | 128   |
    | 36  | 72        | 144   |
    | 28  | 56        | 112   |
    | 33  | 66        | 132   |
    +-----+-----------+-------+

Example 4: String concatenation
===============================

This example shows using the + operator for string concatenation. You can concatenate string literals and field values.

PPL query::

    source=accounts | eval greeting = 'Hello ' + firstname | fields firstname, greeting

Expected result::

    +---------------+---------------------+
    | firstname     | greeting            |
    |---------------+---------------------|
    | Amber JOHnny  | Hello Amber JOHnny  |
    | Hattie        | Hello Hattie        |
    | Nanette       | Hello Nanette       |
    | Dale          | Hello Dale          |
    +---------------+---------------------+

Example 5: Multiple string concatenation with type casting
==========================================================

This example shows multiple concatenations with type casting from numeric to string.

PPL query::

    source=accounts | eval full_info = 'Name: ' + firstname + ', Age: ' + CAST(age AS STRING) | fields firstname, age, full_info

Expected result::

    +---------------+-----+-------------------------------+
    | firstname     | age | full_info                     |
    |---------------+-----+-------------------------------|
    | Amber JOHnny  | 32  | Name: Amber JOHnny, Age: 32   |
    | Hattie        | 36  | Name: Hattie, Age: 36         |
    | Nanette       | 28  | Name: Nanette, Age: 28        |
    | Dale          | 33  | Name: Dale, Age: 33           |
    +---------------+-----+-------------------------------+

Limitations
===========
The ``eval`` command is not rewritten to OpenSearch DSL, it is only executed on the coordination node.
