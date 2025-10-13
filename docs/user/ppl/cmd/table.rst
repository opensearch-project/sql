=============
table
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
The ``table`` command is an alias for the `fields <fields.rst>`_ command and provides the same field selection capabilities. It allows you to keep or remove fields from the search result using enhanced syntax options.

Note: The ``table`` command requires the Calcite to be enabled. All enhanced field features are available through this command. For detailed examples and documentation of all enhanced features, see the `fields command documentation <fields.rst>`_.

Syntax
============
table [+|-] <field-list>

* index: optional. if the plus (+) is used, only the fields specified in the field list will be keep. if the minus (-) is used, all the fields specified in the field list will be removed. **Default** +
* field list: mandatory. Fields can be specified using various enhanced syntax options.

Example 1: Basic table command usage
-------------------------------------

The ``table`` command works identically to the ``fields`` command. This example shows basic field selection.

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

Enhanced Features
=================

The ``table`` command supports all enhanced features available in the ``fields`` command, including:

- Space-delimited syntax
- Wildcard pattern matching (prefix, suffix, contains)
- Mixed delimiters
- Field deduplication
- Full wildcard selection
- Wildcard exclusion

Requirements
============
- **Calcite Engine**: The ``table`` command requires the Calcite engine to be enabled
- **Feature Parity**: All enhanced features available in ``fields`` are also available in ``table``
- **Error Handling**: Attempting to use the ``table`` command without Calcite will result in an ``UnsupportedOperationException``