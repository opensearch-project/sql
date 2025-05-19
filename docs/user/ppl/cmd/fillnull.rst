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

* replacement: mandatory. The value used to replace `null`s.
* field-list: optional. The comma-delimited field list. The `null` values in the field will be replaced with the values from the replacement. In v2, at least one field is required. In v3, if no field specified, the replacement is applied to all fields.

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


Limitation
==========
* The ``fillnull`` command is not rewritten to OpenSearch DSL, it is only executed on the coordination node.
* In v2, at least one field is required.
