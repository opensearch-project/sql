=============
fillnull
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
Using ``fillnull`` command to fill null with provided value in one or more fields in the search result.


Syntax
============

fillnull with <replacement> [in <field-list>]

fillnull using <field> = <replacement> [, <field> = <replacement>]

fillnull value=<replacement> [<field-list>]

**Parameters:**

* replacement: **Mandatory**. The value used to replace `null`s.

* field-list: Optional. Comma-delimited (when using ``with`` or ``using``) or space-delimited (when using ``value=``) list of fields. The `null` values in the field will be replaced with the values from the replacement. From 3.1.0, when ``plugins.calcite.enabled`` is true, if no field specified, the replacement is applied to all fields.

**Syntax Variations:**

* ``with <replacement> in <field-list>`` - Apply same value to specified fields
* ``using <field>=<replacement>, ...`` - Apply different values to different fields
* ``value=<replacement> [<field-list>]`` - (Since 3.4) Alternative syntax with optional space-delimited field list

Example 1: replace null values with a specified value on one field
==================================================================

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

Example 2: replace null values with a specified value on multiple fields
========================================================================

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

Example 3: replace null values with a specified value on all fields
===================================================================

This example only works when Calcite enabled.

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

Example 4: replace null values with multiple specified values on multiple fields
================================================================================

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


Example 5: replace null with specified value on specific fields (value= syntax)
===============================================================================

Since 3.4.

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

Example 6: replace null with specified value on all fields (value= syntax)
==========================================================================

Since 3.4. This example only works when Calcite enabled. When no field list is specified, the replacement applies to all fields in the result.

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

**Note:** When applying the same value to all fields without specifying field names, all fields must be the same type. For mixed types, use separate fillnull commands or explicitly specify fields.

Limitations
===========
* The ``fillnull`` command is not rewritten to OpenSearch DSL, it is only executed on the coordination node.
* Before 3.1.0, at least one field is required.
* **Type Restrictions**:

  The replacement value type must match ALL field types in the field list. When applying the same value to multiple fields, all fields must be the same type (all strings or all numeric).

  **Example:**

  .. code-block:: sql

     # This FAILS - same value for mixed-type fields
     source=accounts | fillnull value=0 firstname, age
     # ERROR: fillnull failed: replacement value type INTEGER is not compatible with field 'firstname' (type: VARCHAR). The replacement value type must match the field type.

