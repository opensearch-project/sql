=============
parse
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
patterns [method=<method>] [new_field=<new-field-name>] [pattern=<pattern>] <field>

* method: optional. Possible values are ``punct``, ``regex``, default value is ``punct``.
* new-field-name: optional string. The name of the new field for extracted patterns, default is ``patterns_field``. If the name already exists, it will replace the original field.
* pattern: if ``method`` is ``regex``, ``pattern`` defines the regex pattern of characters that should be filtered out from the text field.
* field: mandatory. The field must be a text field.

Patterns Methods
================

punct
-----

Remove alphanumeric characters (``[a-zA-Z\d]``) in the text field of each document to form a new field.

regex
-----

Remove characters defined by ``pattern`` in the text field of each document to form a new field.

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


Example 2: Override the existing field
======================================

The example shows how to override the existing ``address`` field with street number removed using a custom regex filter.

PPL query::

    os> source=accounts | patterns method=regex new_field='address' pattern='\d+ ' address | fields address ;
    fetched rows / total rows = 4/4
    +------------------+
    | address          |
    |------------------|
    | Holmes Lane      |
    | Bristol Street   |
    | Madison Street   |
    | Hutchinson Court |
    +------------------+

Example 3: Using punct to extract log patterns
==============================================

The example shows how to extract punctuations from a raw log field using the ``punct`` method.

PPL query::

    os> source=accounts | eval raw='145.128.75.121 - - [29/Aug/2022:13:26:44 -0700] \"GET /deliverables HTTP/2.0\" 501 2721' | patterns raw | head 1 | fields raw, patterns_field ;
    fetched rows / total rows = 1/1
    +-----------------------------------------------------------------------------------------+-------------------------------+
    | raw                                                                                     | patterns_field                |
    |-----------------------------------------------------------------------------------------+-------------------------------|
    | 145.128.75.121 - - [29/Aug/2022:13:26:44 -0700] \"GET /deliverables HTTP/2.0\" 501 2721 | ... - - [//::: -] \" / /.\"   |
    +-----------------------------------------------------------------------------------------+-------------------------------+

Limitation
==========

The patterns command has the same limitations as the parse command, see `parse limitations <./parse.rst#Limitations>`_ for details.
