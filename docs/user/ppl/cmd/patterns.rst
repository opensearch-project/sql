=============
patterns
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
* The ``patterns`` command extracts log patterns from a text field and appends the results to the search result. Grouping logs by their patterns makes it easier to aggregate stats from large volumes of log data for analysis and troubleshooting.
* ``patterns`` command now allows users to select different log parsing algorithms to get high log pattern grouping accuracy. Two pattern methods are supported, aka ``simple_pattern`` and ``brain``.
* ``simple_pattern`` algorithm is basically a regex parsing method vs ``brain`` algorithm is an automatic log grouping algorithm with high grouping accuracy and keeps semantic meaning.

Syntax
============
patterns [new_field=<new-field-name>] (algorithm parameters...) <field> <pattern_method>

Simple Pattern
============
patterns [new_field=<new-field-name>] [pattern=<pattern>] <field>

or

patterns [new_field=<new-field-name>] [pattern=<pattern>] <field> SIMPLE_PATTERN

* new-field-name: optional string. The name of the new field for extracted patterns, default is ``patterns_field``. If the name already exists, it will replace the original field.
* pattern: optional string. The regex pattern of characters that should be filtered out from the text field. If absent, the default pattern is alphanumeric characters (``[a-zA-Z\d]``).
* field: mandatory. The field must be a text field.
* SIMPLE_PATTERN: Specify pattern method to be simple_pattern. By default, it's simple_pattern if the setting ``plugins.ppl.default.pattern.method`` is not specified.

Brain
============
patterns [new_field=<new-field-name>] [variable_count_threshold=<variable_count_threshold>] [frequency_threshold_percentage=<frequency_threshold_percentage>] <field> BRAIN

* new-field-name: optional string. The name of the new field for extracted patterns, default is ``patterns_field``. If the name already exists, it will replace the original field.
* variable_count_threshold: optional integer. Number of tokens in the group per position as variable threshold in case of word tokens appear rarely.
* frequency_threshold_percentage: optional double. To select longest word combination frequency, it needs a lower bound of frequency. The representative frequency of longest word combination should be >= highest token frequency of log * threshold percentage
* field: mandatory. The field must be a text field.
* BRAIN: Specify pattern method to be brain. By default, it's simple_pattern if the setting ``plugins.ppl.default.pattern.method`` is not specified.

Change default pattern method
============
To override default pattern method, users can run following command

.. code-block::

  PUT _cluster/settings
  {
    "transient": {
      "plugins.ppl.default.pattern.method": "BRAIN"
    }
  }

Simple Pattern Example 1: Create the new field
===============================

The example shows how to use extract punctuations in ``email`` for each document. Parsing a null field will return an empty string.

PPL query::

    os> source=accounts | patterns email SIMPLE_PATTERN | fields email, patterns_field ;
    fetched rows / total rows = 4/4
    +-----------------------+------------------+
    | email                 | patterns_field   |
    |-----------------------+------------------|
    | amberduke@pyrami.com  | @.               |
    | hattiebond@netagy.com | @.               |
    | null                  |                  |
    | daleadams@boink.com   | @.               |
    +-----------------------+------------------+

Simple Pattern Example 2: Extract log patterns
===============================

The example shows how to extract punctuations from a raw log field using the default patterns.

PPL query::

    os> source=apache | patterns message SIMPLE_PATTERN | fields message, patterns_field ;
    fetched rows / total rows = 4/4
    +-----------------------------------------------------------------------------------------------------------------------------+---------------------------------+
    | message                                                                                                                     | patterns_field                  |
    |-----------------------------------------------------------------------------------------------------------------------------+---------------------------------|
    | 177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927                        | ... -  [//::: -] " /-/ /."      |
    | 127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722 | ... -  [//::: -] " //// /."     |
    | 118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439                      | ... - - [//::: -] " //--- /."   |
    | 210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481                                             | ... - - [//::: -] " / /."       |
    +-----------------------------------------------------------------------------------------------------------------------------+---------------------------------+

Simple Pattern Example 3: Extract log patterns with custom regex pattern
=========================================================

The example shows how to extract punctuations from a raw log field using user defined patterns.

PPL query::

    os> source=apache | patterns new_field='no_numbers' pattern='[0-9]' message SIMPLE_PATTERN | fields message, no_numbers ;
    fetched rows / total rows = 4/4
    +-----------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------+
    | message                                                                                                                     | no_numbers                                                                           |
    |-----------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------|
    | 177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927                        | ... - upton [/Sep/::: -] "HEAD /e-business/mindshare HTTP/."                         |
    | 127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722 | ... - pouros [/Sep/::: -] "GET /architectures/convergence/niches/mindshare HTTP/."   |
    | 118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439                      | ... - - [/Sep/::: -] "PATCH /strategize/out-of-the-box HTTP/."                       |
    | 210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481                                             | ... - - [/Sep/::: -] "POST /users HTTP/."                                            |
    +-----------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------+

Brain Example 1: Extract log patterns
===============================

The example shows how to extract semantic meaningful log patterns from a raw log field using the brain algorithm. The default variable count threshold is 5.

PPL query::

    os> source=apache | patterns message BRAIN | fields message, patterns_field ;
    fetched rows / total rows = 4/4
    +-----------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------+
    | message                                                                                                                     | patterns_field                                                                                                   |
    |-----------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------|
    | 177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927                        | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] "HEAD /e-business/mindshare HTTP/<*><*>" 404 <*>                      |
    | 127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722 | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] "GET /architectures/convergence/niches/mindshare HTTP/<*><*>" 100 <*> |
    | 118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439                      | <*IP*> - - [<*>/Sep/<*>:<*>:<*>:<*> <*>] "PATCH /strategize/out-of-the-box HTTP/<*><*>" 401 <*>                  |
    | 210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481                                             | <*IP*> - - [<*>/Sep/<*>:<*>:<*>:<*> <*>] "POST /users HTTP/<*><*>" 301 <*>                                       |
    +-----------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------+

Brain Example 2: Extract log patterns with custom parameters
===============================

The example shows how to extract semantic meaningful log patterns from a raw log field using defined parameter of brain algorithm.

PPL query::

    os> source=apache | patterns variable_count_threshold=2 message BRAIN | fields message, patterns_field ;
    fetched rows / total rows = 4/4
    +-----------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------+
    | message                                                                                                                     | patterns_field                                                          |
    |-----------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------|
    | 177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927                        | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] <*> <*> HTTP/<*><*>" <*> <*> |
    | 127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722 | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] <*> <*> HTTP/<*><*>" <*> <*> |
    | 118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439                      | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] <*> <*> HTTP/<*><*>" <*> <*> |
    | 210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481                                             | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] <*> <*> HTTP/<*><*>" <*> <*> |
    +-----------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------+


Limitations
==========

- Patterns command is not pushed down to OpenSearch data node for now. It will only group log patterns on log messages returned to coordinator node.
