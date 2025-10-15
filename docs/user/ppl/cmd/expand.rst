=============
expand
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``expand`` command transforms a single document with a nested array field into multiple documents—each containing one element from the array. All other fields in the original document are duplicated across the resulting documents.

| Key aspects of ``expand``:

1. It generates one row per element in the specified array field.
2. The specified array field is converted into individual rows.
3. If an alias is provided, the expanded values appear under the alias instead of the original field name.
4. If the specified field is an empty array, the row is retained with the expanded field set to null.

Syntax
======

expand <field> [as alias]

* field: mandatory. The field to be expanded (exploded). Currently only nested arrays are supported.
* alias: optional. The name to use instead of the original field name.


Example 1: Expand address field with an alias
=============================================

Given a dataset ``migration`` with the following data:

.. code-block::

   {"name":"abbas","age":24,"address":[{"city":"New york city","state":"NY","moveInDate":{"dateAndTime":"19840412T090742.000Z"}}]}
   {"name":"chen","age":32,"address":[{"city":"Miami","state":"Florida","moveInDate":{"dateAndTime":"19010811T040333.000Z"}},{"city":"los angeles","state":"CA","moveInDate":{"dateAndTime":"20230503T080742.000Z"}}]}

The following query expand the address field and rename it to addr:

PPL query::

    PPL> source=migration | expand address as addr;
    fetched rows / total rows = 3/3
    +-------+-----+-------------------------------------------------------------------------------------------+
    | name  | age | addr                                                                                      |
    |-------+-----+-------------------------------------------------------------------------------------------|
    | abbas | 24  | {"city":"New york city","state":"NY","moveInDate":{"dateAndTime":"19840412T090742.000Z"}} |
    | chen  | 32  | {"city":"Miami","state":"Florida","moveInDate":{"dateAndTime":"19010811T040333.000Z"}}    |
    | chen  | 32  | {"city":"los angeles","state":"CA","moveInDate":{"dateAndTime":"20230503T080742.000Z"}}   |
    +-------+-----+-------------------------------------------------------------------------------------------+

Limitations
============

* The ``expand`` command currently only supports nested arrays. Primitive
  fields storing arrays are not supported. E.g. a string field storing an array
  of strings cannot be expanded with the current implementation.
