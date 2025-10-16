========
fillnull
========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
===========
| The ``fillnull`` command fills null values with the provided value in one or more fields in the search result.


Syntax
======

| fillnull with <replacement> [in <field-list>]
| fillnull using <field> = <replacement> [, <field> = <replacement>]
| fillnull value=<replacement> [<field-list>]

* replacement: mandatory. The value used to replace null values.
* field-list: optional. List of fields to apply the replacement to. Can be comma-delimited (with ``with`` or ``using`` syntax) or space-delimited (with ``value=`` syntax). **Default:** all fields.
* field: mandatory when using ``using`` syntax. Individual field name to assign a specific replacement value.

* **Syntax variations:**
    * ``with <replacement> in <field-list>`` - Apply same value to specified fields
    * ``using <field>=<replacement>, ...`` - Apply different values to different fields
    * ``value=<replacement> [<field-list>]`` - Alternative syntax with optional space-delimited field list

Example 1: Replace null values with a specified value on one field
==================================================================

This example shows replacing null values in the email field with '<not found>'.

PPL query::

    os> source=accounts | fields email, employer | fillnull with '<not found>' in email;
    fetched rows / total rows = 4/4
    +-----------------------+----------+
    | email                 | employer |
    |-----------------------+----------|
    | amberduke@pyrami.com  | Pyrami   |
    | hattiebond@netagy.com | Netagy   |
    | <not found>           | Quility  |
    | daleadams@boink.com   | null     |
    +-----------------------+----------+

Example 2: Replace null values with a specified value on multiple fields
========================================================================

This example shows replacing null values in both email and employer fields with the same replacement value '<not found>'.

PPL query::

    os> source=accounts | fields email, employer | fillnull with '<not found>' in email, employer;
    fetched rows / total rows = 4/4
    +-----------------------+-------------+
    | email                 | employer    |
    |-----------------------+-------------|
    | amberduke@pyrami.com  | Pyrami      |
    | hattiebond@netagy.com | Netagy      |
    | <not found>           | Quility     |
    | daleadams@boink.com   | <not found> |
    +-----------------------+-------------+

Example 3: Replace null values with a specified value on all fields
===================================================================

This example shows replacing null values in all fields when no field list is specified.

PPL query::

    PPL> source=accounts | fields email, employer | fillnull with '<not found>';
    fetched rows / total rows = 4/4
    +-----------------------+-------------+
    | email                 | employer    |
    |-----------------------+-------------|
    | amberduke@pyrami.com  | Pyrami      |
    | hattiebond@netagy.com | Netagy      |
    | <not found>           | Quility     |
    | daleadams@boink.com   | <not found> |
    +-----------------------+-------------+

Example 4: Replace null values with multiple specified values on multiple fields
================================================================================

This example shows using different replacement values for different fields using the 'using' syntax.

PPL query::

    os> source=accounts | fields email, employer | fillnull using email = '<not found>', employer = '<no employer>';
    fetched rows / total rows = 4/4
    +-----------------------+---------------+
    | email                 | employer      |
    |-----------------------+---------------|
    | amberduke@pyrami.com  | Pyrami        |
    | hattiebond@netagy.com | Netagy        |
    | <not found>           | Quility       |
    | daleadams@boink.com   | <no employer> |
    +-----------------------+---------------+


Example 5: Replace null with specified value on specific fields (value= syntax)
===============================================================================

This example shows using the alternative 'value=' syntax to replace null values in specific fields.

PPL query::

    os> source=accounts | fields email, employer | fillnull value="<not found>" email employer;
    fetched rows / total rows = 4/4
    +-----------------------+-------------+
    | email                 | employer    |
    |-----------------------+-------------|
    | amberduke@pyrami.com  | Pyrami      |
    | hattiebond@netagy.com | Netagy      |
    | <not found>           | Quility     |
    | daleadams@boink.com   | <not found> |
    +-----------------------+-------------+

Example 6: Replace null with specified value on all fields (value= syntax)
==========================================================================

When no field list is specified, the replacement applies to all fields in the result.

PPL query::

    os> source=accounts | fields email, employer | fillnull value='<not found>';
    fetched rows / total rows = 4/4
    +-----------------------+-------------+
    | email                 | employer    |
    |-----------------------+-------------|
    | amberduke@pyrami.com  | Pyrami      |
    | hattiebond@netagy.com | Netagy      |
    | <not found>           | Quility     |
    | daleadams@boink.com   | <not found> |
    +-----------------------+-------------+

Limitations
===========
* The ``fillnull`` command is not rewritten to OpenSearch DSL, it is only executed on the coordination node.
* When applying the same value to all fields without specifying field names, all fields must be the same type. For mixed types, use separate fillnull commands or explicitly specify fields.
* The replacement value type must match ALL field types in the field list. When applying the same value to multiple fields, all fields must be the same type (all strings or all numeric).

  **Example:**

  .. code-block:: sql

     # This FAILS - same value for mixed-type fields
     source=accounts | fillnull value=0 firstname, age
     # ERROR: fillnull failed: replacement value type INTEGER is not compatible with field 'firstname' (type: VARCHAR). The replacement value type must match the field type.

