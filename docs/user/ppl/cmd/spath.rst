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

Version
=======
3.3.0

Syntax
============
spath input=<field> [output=<field>] [path=<path>]


* input: mandatory. The field to scan for JSON data.
* output: optional. The destination field that the data will be loaded to. Defaults to the value of `path` when path is specified.
* path: optional. The path of the data to load for the object. For more information on path syntax, see `json_extract <../functions/json.rst#json_extract>`_.
  * When path param is omitted, all fields from the JSON are extracted as separate fields.

Note
=====
The `spath` command currently does not support pushdown behavior for extraction. It will be slow on large datasets. It's generally better to index fields needed for filtering directly instead of using `spath` to filter nested fields.

Example 1: Simple Field Extraction
==================================

The simplest spath is to extract a single field. This extracts `n` from the `doc` field of type `text`.

PPL query::

    os> source=nested_json | spath input=doc id;
    fetched rows / total rows = 3/3
    +------------------------------------------------------------------------------------------------------------+----+
    | doc                                                                                                        | id |
    |------------------------------------------------------------------------------------------------------------+----|
    | {"id": 1, "name": "John", "age": 30, "list": [1, 2, 3, 4], "extras": [{"nest_in": "a"}, {"nest_in": "b"}]} | 1  |
    | {"id": 2, "name": "Jane", "list": [], "extras": [{"nest_in": "a"}]}                                        | 2  |
    | {"id": 3, "list": [5, 6], "extras": []}                                                                    | 3  |
    +------------------------------------------------------------------------------------------------------------+----+

Example 2: Lists & Nesting
============================

These queries demonstrate more JSON path uses, like traversing nested fields and extracting list elements.

PPL query::

    os> source=nested_json | spath input=doc output=first_element list{0} | spath input=doc output=all_elements list{} | spath input=doc output=nested extras{0}.nest_in;
    fetched rows / total rows = 3/3
    +------------------------------------------------------------------------------------------------------------+---------------+--------------+--------+
    | doc                                                                                                        | first_element | all_elements | nested |
    |------------------------------------------------------------------------------------------------------------+---------------+--------------+--------|
    | {"id": 1, "name": "John", "age": 30, "list": [1, 2, 3, 4], "extras": [{"nest_in": "a"}, {"nest_in": "b"}]} | 1             | [1,2,3,4]    | a      |
    | {"id": 2, "name": "Jane", "list": [], "extras": [{"nest_in": "a"}]}                                        | null          | []           | a      |
    | {"id": 3, "list": [5, 6], "extras": []}                                                                    | 5             | [5,6]        | null   |
    +------------------------------------------------------------------------------------------------------------+---------------+--------------+--------+

Example 3: Sum of inner elements
============================

The example shows extracting an inner field and doing statistics on it, using the docs from example 1. It also demonstrates that `spath` always returns strings for inner types.

PPL query::

    os> source=nested_json | spath input=doc id | eval id=cast(id as int) | stats sum(id);
    fetched rows / total rows = 1/1
    +---------+
    | sum(id) |
    |---------|
    | 6       |
    +---------+

Example 4: Dynamic Column Extraction
====================================

When the path parameter is omitted, `spath` extracts all fields from the JSON as dynamic columns. This is useful when you want to access multiple fields from JSON data without specifying individual paths.

PPL query::

    os> source=nested_json | spath input=doc;
    fetched rows / total rows = 3/3
    +------------------------------------------------------------------------------------------------------------+------+------------------+----+-------------------+------+
    | doc                                                                                                        | age  | extras{}.nest_in | id | list{}            | name |
    |------------------------------------------------------------------------------------------------------------+------+------------------+----+-------------------+------|
    | {"id": 1, "name": "John", "age": 30, "list": [1, 2, 3, 4], "extras": [{"nest_in": "a"}, {"nest_in": "b"}]} | 30   | ["a","b"]        | 1  | ["1","2","3","4"] | John |
    | {"id": 2, "name": "Jane", "list": [], "extras": [{"nest_in": "a"}]}                                        | null | a                | 2  | null              | Jane |
    | {"id": 3, "list": [5, 6], "extras": []}                                                                    | null | null             | 3  | ["5","6"]         | null |
    +------------------------------------------------------------------------------------------------------------+------+------------------+----+-------------------+------+

This approach allows you to reference any field that exists in the JSON data structure, making it particularly useful for semi-structured data where different documents may contain different fields.
