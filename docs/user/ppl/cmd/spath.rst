=============
spath
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The `spath` command allows extracting fields from structured text data. It currently allows selecting from JSON data with JSON paths.

If the inner data is unable to be extracted (malformed data, missing keys), `"null"` is returned. `spath` always returns string values, except if the input field is `null`.

Version
=======
3.3.0

Syntax
============
spath input=<field> [output=<field>] [path=]<path>


* input: mandatory. The field to scan for JSON data.
* output: optional. The destination field that the data will be loaded to. Defaults to the value of `path`.
* path: mandatory. The path of the data to load for the object. For more information on path syntax, see `json_extract <../functions/json.rst#json_extract>`_.

Note
=====
The `spath` command currently does not support pushdown behavior for extraction. It will be slow on large datasets. It's generally better to index fields needed for filtering directly instead of using `spath` to filter nested fields.

Example 1: Simple Field Extraction
==================================

The simplest spath is to extract a single field. This extracts `n` from the `doc` field of type `text`.

PPL query::

    os> source=structured | spath input=doc_n n | fields doc_n n;
    fetched rows / total rows = 3/3
    +----------+---+
    | doc_n    | n |
    |----------+---|
    | {"n": 1} | 1 |
    | {"n": 2} | 2 |
    | {"n": 3} | 3 |
    +----------+---+

Example 2: Lists & Nesting
============================

These queries demonstrate more JSON path uses, like traversing nested fields and extracting list elements.

PPL query::

    os> source=structured | spath input=doc_list output=first_element list{0} | spath input=doc_list output=all_elements list{} | spath input=doc_list output=nested nest_out.nest_in | fields doc_list first_element all_elements nested;
    fetched rows / total rows = 3/3
    +------------------------------------------------------+---------------+--------------+--------+
    | doc_list                                             | first_element | all_elements | nested |
    |------------------------------------------------------+---------------+--------------+--------|
    | {"list": [1, 2, 3, 4], "nest_out": {"nest_in": "a"}} | 1             | [1,2,3,4]    | a      |
    | {"list": [], "nest_out": {"nest_in": "a"}}           | null          | []           | a      |
    | {"list": [5, 6], "nest_out": {"nest_in": "a"}}       | 5             | [5,6]        | a      |
    +------------------------------------------------------+---------------+--------------+--------+

Example 3: Sum of inner elements
============================

The example shows extracting an inner field and doing statistics on it, using the docs from example 1. It also demonstrates that `spath` always returns strings for inner types.

PPL query::

    os> source=structured | spath input=doc_n n | eval n=cast(n as int) | stats sum(n) | fields `sum(n)`;
    fetched rows / total rows = 1/1
    +--------+
    | sum(n) |
    |--------|
    | 6      |
    +--------+

Example 4: Field traversal
============================

SPath can also traverse plain documents, in which case it acts similarly to renaming.

PPL query::

    os> source=structured | spath input=obj_field path=field | fields obj_field field;
    fetched rows / total rows = 3/3
    +----------------+-------+
    | obj_field      | field |
    |----------------+-------|
    | {'field': 'a'} | a     |
    | {'field': 'b'} | b     |
    | {'field': 'c'} | c     |
    +----------------+-------+
