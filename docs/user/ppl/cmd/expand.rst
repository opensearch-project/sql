=============
expand
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
     (From 3.1.0)

Using ``expand`` command to flatten an array of nested type.

The expand command produces a row for each element in the specified array or map field, where:

* Array elements become individual rows.
* When an alias is provided, the exploded values are represented under the alias instead of the original field name.
* In case where the expanded field is an empty array, the row will be kept the leaving the expanded field as null.



Syntax
======

expand <field> [As alias]

* field: The field to be expanded (exploded). Currently only nested arrays are supported.
* alias: (Optional) The name to use instead of the original field name.


Example: expand address field with an alias
===========================================

Given a dataset ``mirgration`` with the following data:

.. code-block::

   {"name":"abbas","age":24,"address":[{"city":"New york city","state":"NY","moveInDate":{"dateAndTime":"19840412T090742.000Z"}}]}
   {"name":"chen","age":32,"address":[{"city":"Miami","state":"Florida","moveInDate":{"dateAndTime":"19010811T040333.000Z"}},{"city":"los angeles","state":"CA","moveInDate":{"dateAndTime":"20230503T080742.000Z"}}]}

The following query expand the address field and rename it to addr:

PPL query::

    PPL> source=mirgration | expand address as addr;
    fetched rows / total rows = 3/3
    +-------+-----+-------------------------------------------------------------------------------------------+
    | name  | age | addr                                                                                      |
    |-------+-----+-------------------------------------------------------------------------------------------|
    | abbas | 24  | {"city":"New york city","state":"NY","moveInDate":{"dateAndTime":"19840412T090742.000Z"}} |
    | chen  | 32  | {"city":"Miami","state":"Florida","moveInDate":{"dateAndTime":"19010811T040333.000Z"}}    |
    | chen  | 32  | {"city":"los angeles","state":"CA","moveInDate":{"dateAndTime":"20230503T080742.000Z"}}   |
    +-------+-----+-------------------------------------------------------------------------------------------+
