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
`fillnull [with <null-replacement> in <nullable-field>["," <nullable-field>]] | [using <source-field> = <null-replacement> [","<source-field> = <null-replacement>]]`

* null-replacement: mandatory. The value used to replace `null`s.
* nullable-field: mandatory. Field reference. The `null` values in the field referred to by the property will be replaced with the values from the null-replacement.

Example 1: fillnull one field
======================================================================

The example show fillnull one field.

PPL query::

    os> source=accounts | fields email, employer | fillnull with '<not found>' in email ;
    fetched rows / total rows = 4/4
    +-----------------------+----------+
    | email                 | employer |
    |-----------------------+----------|
    | amberduke@pyrami.com  | Pyrami   |
    | hattiebond@netagy.com | Netagy   |
    | <not found>           | Quility  |
    | daleadams@boink.com   | null     |
    +-----------------------+----------+

Example 2: fillnull applied to multiple fields
========================================================================

The example show fillnull applied to multiple fields.

PPL query::

    os> source=accounts | fields email, employer | fillnull using email = '<not found>', employer = '<no employer>' ;
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
The ``fillnull`` command is not rewritten to OpenSearch DSL, it is only executed on the coordination node.