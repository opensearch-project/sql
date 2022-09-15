=============
patterns
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``patterns`` command extracts log patterns from a text field and appends the results to the search result.


Syntax
============
patterns [new_field=<new-field-name>] [pattern=<pattern>] <field>

* new-field-name: optional string. The name of the new field for extracted patterns, default is ``patterns_field``. If the name already exists, it will replace the original field.
* pattern: optional string. The regex pattern of characters that should be filtered out from the text field. If absent, the default pattern is alphanumeric characters (``[a-zA-Z\d]``).
* field: mandatory. The field must be a text field.

Example 1: Create the new field
===============================

The example shows how to use ``punct`` method to extract punctuations in ``email`` for each document. Parsing a null field will return an empty string.

PPL query::

    os> source=accounts | patterns email | fields email, patterns_field ;
    fetched rows / total rows = 4/4
    +-----------------------+------------------+
    | email                 | patterns_field   |
    |-----------------------+------------------|
    | amberduke@pyrami.com  | @.               |
    | hattiebond@netagy.com | @.               |
    | null                  |                  |
    | daleadams@boink.com   | @.               |
    +-----------------------+------------------+

Example 2: Extract log patterns
===============================

The example shows how to extract punctuations from a raw log field using the default patterns.

PPL query::

    os> source=accounts | eval raw='145.128.75.121 - - [29/Aug/2022:13:26:44 -0700] \"GET /deliverables HTTP/2.0\" 501 2721' | patterns raw | head 1 | fields raw, patterns_field ;
    fetched rows / total rows = 1/1
    +-----------------------------------------------------------------------------------------+-------------------------------+
    | raw                                                                                     | patterns_field                |
    |-----------------------------------------------------------------------------------------+-------------------------------|
    | 145.128.75.121 - - [29/Aug/2022:13:26:44 -0700] \"GET /deliverables HTTP/2.0\" 501 2721 | ... - - [//::: -] \" / /.\"   |
    +-----------------------------------------------------------------------------------------+-------------------------------+

Example 3: Extract log patterns with custom regex pattern
=========================================================

The example shows how to extract punctuations from a raw log field using user defined patterns.

PPL query::

    os> source=accounts | eval raw='145.128.75.121 - - [29/Aug/2022:13:26:44 -0700] \"GET /deliverables HTTP/2.0\" 501 2721' | patterns new_field='no_numbers' pattern='[0-9]' raw | head 1 | fields raw, no_numbers ;
    fetched rows / total rows = 1/1
    +-----------------------------------------------------------------------------------------+-----------------------------------------------------+
    | raw                                                                                     | no_numbers                                          |
    |-----------------------------------------------------------------------------------------+-----------------------------------------------------|
    | 145.128.75.121 - - [29/Aug/2022:13:26:44 -0700] \"GET /deliverables HTTP/2.0\" 501 2721 | ... - - [/Aug/::: -] \"GET /deliverables HTTP/.\"   |
    +-----------------------------------------------------------------------------------------+-----------------------------------------------------+

Limitation
==========

The patterns command has the same limitations as the parse command, see `parse limitations <./parse.rst#Limitations>`_ for details.
