=================================
mvexpand
=================================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``mvexpand`` command expands each value in a multivalue (array) field into a separate row,
| similar to Splunk's ``mvexpand`` command. For each document, every element in the specified
| array field is returned as a new row.

Syntax
======
``mvexpand <field> [limit=<int>]``

* ``field``: The multivalue (array) field to expand. (Required)
* ``limit``: Maximum number of values per document to expand. (Optional)

Notes about these doctests
--------------------------
- The tests below target a single, deterministic document by using ``where case='<name>'`` so the doctests are stable.
- The test index name used in these examples is ``mvexpand_logs``.

Example 1: Basic Expansion (single document)
-------------------------------------------
Input document (case "basic") contains three tag values.

PPL query / expected output::

    os> source=mvexpand_logs | where case='basic' | mvexpand tags | fields tags.value
    fetched rows / total rows = 3/3
    +------------+
    | tags.value |
    |------------|
    | error      |
    | warning    |
    | info       |
    +------------+

Example 2: Expansion with Limit
-------------------------------
Input document (case "ids") contains an array of integers; expand and apply limit.

PPL query / expected output::

    os> source=mvexpand_logs | where case='ids' | mvexpand ids limit=3 | fields ids.value
    fetched rows / total rows = 3/3
    +-----------+
    | ids.value |
    |-----------|
    | 1         |
    | 2         |
    | 3         |
    +-----------+

Example 3: Empty and Null Arrays
--------------------------------
Empty array (case "empty")::

    os> source=mvexpand_logs | where case='empty' | mvexpand tags | fields tags.value
    fetched rows / total rows = 0/0
    +------------+
    | tags.value |
    |------------|
    +------------+

Null array (case "null")::

    os> source=mvexpand_logs | where case='null' | mvexpand tags | fields tags.value
    fetched rows / total rows = 0/0
    +------------+
    | tags.value |
    |------------|
    +------------+

Example 4: Single-value array (case "single")
---------------------------------------------
Single-element array should expand to one row.

PPL query / expected output::

    os> source=mvexpand_logs | where case='single' | mvexpand tags | fields tags.value
    fetched rows / total rows = 1/1
    +------------+
    | tags.value |
    |------------|
    | error      |
    +------------+

Example 5: Missing Field
------------------------
If the field is missing in the document (case "missing"), no rows are produced.

PPL query / expected output::

    os> source=mvexpand_logs | where case='missing' | mvexpand tags | fields tags.value
    fetched rows / total rows = 0/0
    +------------+
    | tags.value |
    |------------|
    +------------+