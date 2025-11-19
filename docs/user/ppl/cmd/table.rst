=====
table
=====

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
===========
The ``table`` command is an alias for the `fields <fields.rst>`_ command and provides the same field selection capabilities. It allows you to keep or remove fields from the search result using enhanced syntax options.

Syntax
======
table [+|-] <field-list>

* [+|-]: optional. If the plus (+) is used, only the fields specified in the field list will be kept. If the minus (-) is used, all the fields specified in the field list will be removed. **Default:** +.
* field-list: mandatory. Comma-delimited or space-delimited list of fields to keep or remove. Supports wildcard patterns.

Example 1: Basic table command usage
====================================

This example shows basic field selection using the table command.

PPL query::

    os> source=accounts | table firstname lastname age;
    fetched rows / total rows = 4/4
    +-----------+----------+-----+
    | firstname | lastname | age |
    |-----------+----------+-----|
    | Amber     | Duke     | 32  |
    | Hattie    | Bond     | 36  |
    | Nanette   | Bates    | 28  |
    | Dale      | Adams    | 33  |
    +-----------+----------+-----+


See Also
========
- `fields <fields.rst>`_ - Alias command with identical functionality