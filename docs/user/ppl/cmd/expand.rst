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

Use the ``expand`` command on a nested array field to transform a single
document into multiple documents—each containing one element from the array.
All other fields in the original document are duplicated across the resulting
documents.

The expand command generates one row per element in the specified array field:

* The specified array field is converted into individual rows.
* If an alias is provided, the expanded values appear under the alias instead
  of the original field name.
* If the specified field is an empty array, the row is retained with the
  expanded field set to null.

Version
=======
Since 3.1.0

Syntax
======

expand <field> [as alias]

* field: The field to be expanded (exploded). Currently only nested arrays are
   supported.
* alias: (Optional) The name to use instead of the original field name.


Example: expand address field with an alias
===========================================

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
  fields storing arrays are not currently supported. E.g. a string field
  storing an array of strings cannot be expanded with the current
  implementation.
* The ``expand`` command is only available since 3.1.0.
