PPL ``expand`` command
=======================

Description
-----------

Using ``expand`` command to flatten an array of nested type.

Syntax
------

``expand <field> [As alias]``

- *field*: The field to be expanded (exploded). Currently only nested arrays are supported.
- *alias*: (Optional) The name to use instead of the original field name.

Usage Guidelines
----------------

The expand command produces a row for each element in the specified array or map field, where:

- Array elements become individual rows.
- When an alias is provided, the exploded values are represented under the alias instead of the original field name.
- This can be used in combination with other commands, such as ``stats``, ``eval``, and ``parse`` to manipulate or extract data post-expansion.

Examples
--------

Given a dataset ``move`` with the following data:
```
{"name":"abbas","age":24,"address":[{"city":"New york city","state":"NY","moveInDate":{"dateAndTime":"19840412T090742.000Z"}}]}
{"name":"chen","age":32,"address":[{"city":"Miami","state":"Florida","moveInDate":{"dateAndTime":"19010811T040333.000Z"}},{"city":"los angeles","state":"CA","moveInDate":{"dateAndTime":"20230503T080742.000Z"}}]}
```
The following query expand the address field and rename it to addr:

PPL query::

    PPL> source=move | expand address as addr;
    fetched rows / total rows = 3/3
    +-------+-----+-------------------------------------------------------------------------------------------+
    | name  | age | addr                                                                                      |
    |-------+-----+-------------------------------------------------------------------------------------------|
    | abbas | 24  | {"city":"New york city","state":"NY","moveInDate":{"dateAndTime":"19840412T090742.000Z"}} |
    | chen  | 32  | {"city":"Miami","state":"Florida","moveInDate":{"dateAndTime":"19010811T040333.000Z"}}    |
    | chen  | 32  | {"city":"los angeles","state":"CA","moveInDate":{"dateAndTime":"20230503T080742.000Z"}}   |
    +-------+-----+-------------------------------------------------------------------------------------------+
