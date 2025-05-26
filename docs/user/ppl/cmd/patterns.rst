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
(From 3.1.0)

* ``patterns`` command supports two modes, aka ``label`` and ``aggregation``. ``label`` mode is similar to previous 3.0.0 output. ``aggregation`` mode returns aggregated results on target field.
* V2 Engine engine still have the same output in ``label`` mode as before. In ``aggregation`` mode, it returns aggregated pattern count on labeled pattern as well as sample logs (sample count is configurable) per pattern.
* Calcite engine's ``label`` mode not only labels pattern of text but also labels variable tokens in map. In ``aggregation`` mode, it will also output labeled pattern as well as variable tokens per pattern.

Syntax
============
patterns <field> [by byClause...] [pattern_method=SIMPLE_PATTERN | BRAIN] [pattern_mode=LABEL | AGGREGATION] [pattern_max_sample_count=integer] [pattern_buffer_limit=integer] [new_field=<new-field-name>] (algorithm parameters...)

* field: mandatory. It's the target text(string) field to be analyzed.
* byClause: optional. The log groups to be labeled or aggregated. It could be fields and scalar functions.
* pattern_method: optional. Algorithms to be chosen. Default is SIMPLE_PATTERN. The other option is BRAIN.
* pattern_mode: optional. label mode or aggregation mode. Default is label mode.
* pattern_max_sample_count: optional. The max sample logs to be returned per pattern in aggregation mode.
* pattern_buffer_limit: optional. This is a special safeguard parameter for BRAIN algorithm to limit internal temporary buffer to hold processed logs.
* new_field: The alias of the new field of the pattern. Default is "patterns_field".
* algorithm parameters: optional. List of algorithm parameters. SIMPLE_PATTERN can specify regex by setting "pattern" parameter. BRAIN can specify variable_count_threshold(integer>0) and frequency_threshold_percentage(0.0f ~ 1.0f) parameters to tune variable token detection effect.

Simple Pattern
============
patterns <field> [by byClause...] [pattern_mode=LABEL | AGGREGATION] [pattern_max_sample_count=integer] [new_field=<new-field-name>] [pattern=<pattern>]

or

patterns <field> [by byClause...] pattern_method=SIMPLE_PATTERN [pattern_mode=LABEL | AGGREGATION] [pattern_max_sample_count=integer] [new_field=<new-field-name>] [pattern=<pattern>]

* field: mandatory. The field must be a text field.
* byClause: optional. The log groups to be labeled or aggregated. It could be fields and scalar functions.
* pattern_method: optional. Specify pattern method to be simple_pattern. By default, it's simple_pattern if the setting ``plugins.ppl.default.pattern.method`` is not specified.
* pattern_mode: optional. label mode or aggregation mode. Default is label mode.
* pattern_max_sample_count: optional. The max sample logs to be returned per pattern in aggregation mode.
* new-field-name: optional string. The name of the new field for extracted patterns, default is ``patterns_field``. If the name already exists, it will replace the original field.
* pattern: optional string. The regex pattern of characters that should be filtered out from the text field. If absent, the default pattern is alphanumeric characters (``[a-zA-Z\d]``).

Brain
============
patterns <field> pattern_method=BRAIN [new_field=<new-field-name>] [variable_count_threshold=<variable_count_threshold>] [frequency_threshold_percentage=<frequency_threshold_percentage>]

* field: mandatory. The field must be a text field.
* byClause: optional. The log groups to be labeled or aggregated. It could be fields and scalar functions.
* pattern_method: optional. Specify pattern method to be brain. By default, it's simple_pattern if the setting ``plugins.ppl.default.pattern.method`` is not specified.
* pattern_mode: optional. label mode or aggregation mode. Default is label mode.
* pattern_max_sample_count: optional. The max sample logs to be returned per pattern in aggregation mode.
* pattern_buffer_limit: optional. This is a special safeguard parameter for BRAIN algorithm to limit internal temporary buffer to hold processed logs.
* new-field-name: optional string. The name of the new field for extracted patterns, default is ``patterns_field``. If the name already exists, it will replace the original field.
* variable_count_threshold: optional integer. Number of tokens in the group per position as variable threshold in case of word tokens appear rarely.
* frequency_threshold_percentage: optional double. To select longest word combination frequency, it needs a lower bound of frequency. The representative frequency of longest word combination should be >= highest token frequency of log * threshold percentage

Change default pattern method
============
To override default pattern parameters, users can run following command

.. code-block::

  PUT _cluster/settings
  {
    "persistent": {
      "plugins.ppl.default.pattern.method": "BRAIN",
      "plugins.ppl.default.pattern.mode": "AGGREGATION",
      "plugins.ppl.default.pattern.max.sample.count": 5,
      "plugins.ppl.default.pattern.buffer.limit": 50000
    }
  }

Simple Pattern Example 1: Create the new field
===============================

The example shows how to use extract punctuations in ``email`` for each document. Parsing a null field will return an empty string.

PPL query::

    os> source=accounts | patterns email pattern_method=SIMPLE_PATTERN | fields email, patterns_field ;
    fetched rows / total rows = 4/4
    +-----------------------+----------------+
    | email                 | patterns_field |
    |-----------------------+----------------|
    | amberduke@pyrami.com  | @.             |
    | hattiebond@netagy.com | @.             |
    | null                  |                |
    | daleadams@boink.com   | @.             |
    +-----------------------+----------------+

Simple Pattern Example 2: Extract log patterns
===============================

The example shows how to extract punctuations from a raw log field using the default patterns.

PPL query::

    os> source=apache | patterns message pattern_method=SIMPLE_PATTERN | fields message, patterns_field ;
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

    os> source=apache | patterns message pattern_method=SIMPLE_PATTERN new_field='no_numbers' pattern='[0-9]' | fields message, no_numbers ;
    fetched rows / total rows = 4/4
    +-----------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------+
    | message                                                                                                                     | no_numbers                                                                           |
    |-----------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------|
    | 177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927                        | ... - upton [/Sep/::: -] "HEAD /e-business/mindshare HTTP/."                         |
    | 127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722 | ... - pouros [/Sep/::: -] "GET /architectures/convergence/niches/mindshare HTTP/."   |
    | 118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439                      | ... - - [/Sep/::: -] "PATCH /strategize/out-of-the-box HTTP/."                       |
    | 210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481                                             | ... - - [/Sep/::: -] "POST /users HTTP/."                                            |
    +-----------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------+

Simple Pattern Example 4: Return log patterns aggregation result
=========================================================

The example shows how to get aggregated results from a raw log field.

PPL query::

    os> source=apache | patterns message pattern_method=SIMPLE_PATTERN pattern_mode=AGGREGATION | fields patterns_field, pattern_count, sample_logs ;
    fetched rows / total rows = 4/4
    +---------------------------------+---------------+-------------------------------------------------------------------------------------------------------------------------------+
    | patterns_field                  | pattern_count | sample_logs                                                                                                                   |
    |---------------------------------+---------------+-------------------------------------------------------------------------------------------------------------------------------|
    | ... -  [//::: -] " /-/ /."      | 1             | [177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927]                        |
    | ... -  [//::: -] " //// /."     | 1             | [127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722] |
    | ... - - [//::: -] " / /."       | 1             | [210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481]                                             |
    | ... - - [//::: -] " //--- /."   | 1             | [118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439]                      |
    +---------------------------------+---------------+-------------------------------------------------------------------------------------------------------------------------------+

Brain Example 1: Extract log patterns
===============================

The example shows how to extract semantic meaningful log patterns from a raw log field using the brain algorithm. The default variable count threshold is 5.

PPL query::

    os> source=apache | patterns message pattern_method=BRAIN | fields message, patterns_field ;
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

    os> source=apache | patterns message pattern_method=BRAIN variable_count_threshold=2 | fields message, patterns_field ;
    fetched rows / total rows = 4/4
    +-----------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------+
    | message                                                                                                                     | patterns_field                                                          |
    |-----------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------|
    | 177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927                        | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] <*> <*> HTTP/<*><*>" <*> <*> |
    | 127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722 | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] <*> <*> HTTP/<*><*>" <*> <*> |
    | 118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439                      | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] <*> <*> HTTP/<*><*>" <*> <*> |
    | 210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481                                             | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] <*> <*> HTTP/<*><*>" <*> <*> |
    +-----------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------+

Brain Example 3: Return log patterns aggregation result
===============================

get aggregated results from a raw log field for brain algorithm.

PPL query::

    os> source=apache | patterns message pattern_method=BRAIN pattern_mode=AGGREGATION variable_count_threshold=2 | fields patterns_field, pattern_count, sample_logs ;
    fetched rows / total rows = 1/1
    +-------------------------------------------------------------------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | patterns_field                                                          | pattern_count | sample_logs                                                                                                                                                                                                                                                                                                                                                                                                               |
    |-------------------------------------------------------------------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
    | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] <*> <*> HTTP/<*><*>" <*> <*> | 4             | [177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927,127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722,118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439,210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481] |
    +-------------------------------------------------------------------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Limitations
==========

- Patterns command is not pushed down to OpenSearch data node for now. It will only group log patterns on log messages returned to coordinator node.
