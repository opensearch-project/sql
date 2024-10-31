=============
fillnull
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``fillnull`` command replaces null values for one or more fields.


Syntax
============
fillnull "with" <expression> <field> ["," <field> ]...

* field: mandatory. Name of an existing field that was piped into ``fillnull``. Null values for all specified fields are replaced with the value of expression.
* expression: mandatory. Any expression support by the system. The expression value type must match the type of field.

fillnull "using" <field> "=" <expression> ["," <field> "=" <expression> ]...

* field: mandatory. Name of an existing field that was piped into ``fillnull``.
* expression: mandatory. Any expression support by the system. The expression value type must match the type of field.

Example 1: Replace null values with the same value for multiple fields
======================================================================

The example show to replace null values for email and host with "<not found>".

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

Example 2: Replace null values for multiple fields with different values
========================================================================

The example show to replace null values for email with "<not found>" and null values for host with "<no host>".

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