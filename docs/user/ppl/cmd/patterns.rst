=============
patterns
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``patterns`` command extracts log patterns from a text field and appends the results to the search result. Grouping logs by their patterns makes it easier to aggregate stats from large volumes of log data for analysis and troubleshooting.


Syntax
============
patterns [new_field=<new-field-name>] [pattern=<pattern>] <field>

* new-field-name: optional string. The name of the new field for extracted patterns, default is ``patterns_field``. If the name already exists, it will replace the original field.
* pattern: optional string. The regex pattern of characters that should be filtered out from the text field. If absent, the default pattern is alphanumeric characters (``[a-zA-Z\d]``).
* field: mandatory. The field must be a text field.

Example 1: Create the new field
===============================

The example shows how to use extract punctuations in ``email`` for each document. Parsing a null field will return an empty string.

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

    os> source=apache | patterns message | fields message, patterns_field ;
    fetched rows / total rows = 4/4
    +-----------------------------------------------------------------------------------------------------------------------------+---------------------------------+
    | message                                                                                                                     | patterns_field                  |
    |-----------------------------------------------------------------------------------------------------------------------------+---------------------------------|
    | 177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927                        | ... -  [//::: -] " /-/ /."      |
    | 127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722 | ... -  [//::: -] " //// /."     |
    | 118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439                      | ... - - [//::: -] " //--- /."   |
    | 210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481                                             | ... - - [//::: -] " / /."       |
    +-----------------------------------------------------------------------------------------------------------------------------+---------------------------------+

Example 3: Extract log patterns with custom regex pattern
=========================================================

The example shows how to extract punctuations from a raw log field using user defined patterns.

PPL query::

    os> source=apache | patterns new_field='no_numbers' pattern='[0-9]' message | fields message, no_numbers ;
    fetched rows / total rows = 4/4
    +-----------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------+
    | message                                                                                                                     | no_numbers                                                                           |
    |-----------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------|
    | 177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927                        | ... - upton [/Sep/::: -] "HEAD /e-business/mindshare HTTP/."                         |
    | 127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722 | ... - pouros [/Sep/::: -] "GET /architectures/convergence/niches/mindshare HTTP/."   |
    | 118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439                      | ... - - [/Sep/::: -] "PATCH /strategize/out-of-the-box HTTP/."                       |
    | 210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481                                             | ... - - [/Sep/::: -] "POST /users HTTP/."                                            |
    +-----------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------+

Limitation
==========

The patterns command has the same limitations as the parse command, see `parse limitations <./parse.rst#Limitations>`_ for details.
