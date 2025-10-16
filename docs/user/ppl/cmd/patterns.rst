=============
patterns
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
===========
| The ``patterns`` command extracts log patterns from a text field and appends the results to the search result. Grouping logs by their patterns makes it easier to aggregate stats from large volumes of log data for analysis and troubleshooting.

| ``patterns`` command allows users to select different log parsing algorithms to get high log pattern grouping accuracy. Two pattern methods are supported: ``simple_pattern`` and ``brain``.

| ``simple_pattern`` algorithm is basically a regex parsing method vs ``brain`` algorithm is an automatic log grouping algorithm with high grouping accuracy and keeps semantic meaning.

| ``patterns`` command supports two modes: ``label`` and ``aggregation``. ``label`` mode returns individual pattern labels. ``aggregation`` mode returns aggregated results on target field.

| Calcite engine by default labels the variables with '<*>' placeholder. If ``show_numbered_token`` option is turned on, Calcite engine's ``label`` mode not only labels pattern of text but also labels variable tokens in map. In ``aggregation`` mode, it will also output labeled pattern as well as variable tokens per pattern. The variable placeholder is in the format of '<token%d>' instead of '<*>'.

Syntax
======
patterns <field> [by byClause...] [method=simple_pattern | brain] [mode=label | aggregation] [max_sample_count=integer] [buffer_limit=integer] [show_numbered_token=boolean] [new_field=<new-field-name>] (algorithm parameters...)

* field: mandatory. The text field to analyze for patterns.
* byClause: optional. Fields or scalar functions used to group logs for labeling/aggregation.
* method: optional. Algorithm choice: ``simple_pattern`` or ``brain``. **Default:** ``simple_pattern``.
* mode: optional. Output mode: ``label`` or ``aggregation``. **Default:** ``label``.
* max_sample_count: optional. Max sample logs returned per pattern in aggregation mode. **Default:** 10.
* buffer_limit: optional. Safeguard parameter for ``brain`` algorithm to limit internal temporary buffer size (min: 50,000). **Default:** 100,000.
* show_numbered_token: optional. The flag to turn on numbered token output format. **Default:** false.
* new_field: optional. Alias of the output pattern field. **Default:** "patterns_field".
* algorithm parameters: optional. Algorithm-specific tuning:

  - ``simple_pattern``: Define regex via "pattern".
  - ``brain``: Adjust sensitivity with variable_count_threshold and frequency_threshold_percentage.

    - ``variable_count_threshold``: optional integer. Words are split by space. Algorithm counts how many distinct words are at specific position in initial log groups. Adjusting this threshold can determine the sensitivity of constant words. **Default:** 5.
    - ``frequency_threshold_percentage``: optional double. Brain's log pattern is selected based on longest word combination. This sets the lower bound of frequency to ignore low frequency words. **Default:** 0.3.

Change the default pattern method
=================================
To override default pattern parameters, users can run following command

.. code-block::

  PUT _cluster/settings
  {
    "persistent": {
      "plugins.ppl.pattern.method": "brain",
      "plugins.ppl.pattern.mode": "aggregation",
      "plugins.ppl.pattern.max.sample.count": 5,
      "plugins.ppl.pattern.buffer.limit": 50000,
      "plugins.ppl.pattern.show.numbered.token": true
    }
  }

Simple Pattern Example 1: Create the new field
==============================================

This example shows how to extract patterns in ``email`` for each document. Parsing a null field will return an empty string.

PPL query::

    os> source=accounts | patterns email method=simple_pattern | fields email, patterns_field ;
    fetched rows / total rows = 4/4
    +-----------------------+----------------+
    | email                 | patterns_field |
    |-----------------------+----------------|
    | amberduke@pyrami.com  | <*>@<*>.<*>    |
    | hattiebond@netagy.com | <*>@<*>.<*>    |
    | null                  |                |
    | daleadams@boink.com   | <*>@<*>.<*>    |
    +-----------------------+----------------+

Simple Pattern Example 2: Extract log patterns
==============================================

This example shows how to extract patterns from a raw log field using the default patterns.

PPL query::

    os> source=apache | patterns message method=simple_pattern | fields message, patterns_field ;
    fetched rows / total rows = 4/4
    +-----------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------+
    | message                                                                                                                     | patterns_field                                                                                    |
    |-----------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------|
    | 177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927                        | <*>.<*>.<*>.<*> - <*> [<*>/<*>/<*>:<*>:<*>:<*> -<*>] "<*> /<*>-<*>/<*> <*>/<*>.<*>" <*> <*>       |
    | 127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722 | <*>.<*>.<*>.<*> - <*> [<*>/<*>/<*>:<*>:<*>:<*> -<*>] "<*> /<*>/<*>/<*>/<*> <*>/<*>.<*>" <*> <*>   |
    | 118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439                      | <*>.<*>.<*>.<*> - - [<*>/<*>/<*>:<*>:<*>:<*> -<*>] "<*> /<*>/<*>-<*>-<*>-<*> <*>/<*>.<*>" <*> <*> |
    | 210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481                                             | <*>.<*>.<*>.<*> - - [<*>/<*>/<*>:<*>:<*>:<*> -<*>] "<*> /<*> <*>/<*>.<*>" <*> <*>                 |
    +-----------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------+

Simple Pattern Example 3: Extract log patterns with custom regex pattern
========================================================================

This example shows how to extract patterns from a raw log field using user defined patterns.

PPL query::

    os> source=apache | patterns message method=simple_pattern new_field='no_numbers' pattern='[0-9]' | fields message, no_numbers ;
    fetched rows / total rows = 4/4
    +-----------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | message                                                                                                                     | no_numbers                                                                                                                                                                                                |
    |-----------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
    | 177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927                        | <*><*><*>.<*><*>.<*>.<*><*> - upton<*><*><*><*> [<*><*>/Sep/<*><*><*><*>:<*><*>:<*><*>:<*><*> -<*><*><*><*>] "HEAD /e-business/mindshare HTTP/<*>.<*>" <*><*><*> <*><*><*><*><*>                          |
    | 127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722 | <*><*><*>.<*><*>.<*><*><*>.<*> - pouros<*><*><*><*> [<*><*>/Sep/<*><*><*><*>:<*><*>:<*><*>:<*><*> -<*><*><*><*>] "GET /architectures/convergence/niches/mindshare HTTP/<*>.<*>" <*><*><*> <*><*><*><*><*> |
    | 118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439                      | <*><*><*>.<*><*><*>.<*><*><*>.<*><*><*> - - [<*><*>/Sep/<*><*><*><*>:<*><*>:<*><*>:<*><*> -<*><*><*><*>] "PATCH /strategize/out-of-the-box HTTP/<*>.<*>" <*><*><*> <*><*><*><*><*>                        |
    | 210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481                                             | <*><*><*>.<*><*><*>.<*><*>.<*><*><*> - - [<*><*>/Sep/<*><*><*><*>:<*><*>:<*><*>:<*><*> -<*><*><*><*>] "POST /users HTTP/<*>.<*>" <*><*><*> <*><*><*><*>                                                   |
    +-----------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Simple Pattern Example 4: Return log patterns aggregation result
================================================================

This example shows how to get aggregated results from a raw log field.

PPL query::

    os> source=apache | patterns message method=simple_pattern mode=aggregation | fields patterns_field, pattern_count, sample_logs ;
    fetched rows / total rows = 4/4
    +---------------------------------------------------------------------------------------------------+---------------+-------------------------------------------------------------------------------------------------------------------------------+
    | patterns_field                                                                                    | pattern_count | sample_logs                                                                                                                   |
    |---------------------------------------------------------------------------------------------------+---------------+-------------------------------------------------------------------------------------------------------------------------------|
    | <*>.<*>.<*>.<*> - - [<*>/<*>/<*>:<*>:<*>:<*> -<*>] "<*> /<*> <*>/<*>.<*>" <*> <*>                 | 1             | [210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481]                                             |
    | <*>.<*>.<*>.<*> - - [<*>/<*>/<*>:<*>:<*>:<*> -<*>] "<*> /<*>/<*>-<*>-<*>-<*> <*>/<*>.<*>" <*> <*> | 1             | [118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439]                      |
    | <*>.<*>.<*>.<*> - <*> [<*>/<*>/<*>:<*>:<*>:<*> -<*>] "<*> /<*>-<*>/<*> <*>/<*>.<*>" <*> <*>       | 1             | [177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927]                        |
    | <*>.<*>.<*>.<*> - <*> [<*>/<*>/<*>:<*>:<*>:<*> -<*>] "<*> /<*>/<*>/<*>/<*> <*>/<*>.<*>" <*> <*>   | 1             | [127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722] |
    +---------------------------------------------------------------------------------------------------+---------------+-------------------------------------------------------------------------------------------------------------------------------+

Simple Pattern Example 5: Return log patterns aggregation result with detected variable tokens
==============================================================================================

This example shows how to get aggregated results with detected variable tokens.

Configuration
-------------
With  option ``show_numbered_token`` enabled, the output can detect numbered variable tokens from the pattern field.

PPL query::

    os> source=apache | patterns message method=simple_pattern mode=aggregation show_numbered_token=true | fields patterns_field, pattern_count, tokens | head 1 ;
    fetched rows / total rows = 1/1
    +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | patterns_field                                                                                                                                                                       | pattern_count | tokens                                                                                                                                                                                                                                                                                                                                                                                            |
    |--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
    | <token1>.<token2>.<token3>.<token4> - - [<token5>/<token6>/<token7>:<token8>:<token9>:<token10> -<token11>] "<token12> /<token13> <token14>/<token15>.<token16>" <token17> <token18> | 1             | {'<token14>': ['HTTP'], '<token13>': ['users'], '<token16>': ['1'], '<token15>': ['1'], '<token18>': ['9481'], '<token17>': ['301'], '<token5>': ['28'], '<token4>': ['104'], '<token7>': ['2022'], '<token6>': ['Sep'], '<token9>': ['15'], '<token8>': ['10'], '<token10>': ['57'], '<token1>': ['210'], '<token12>': ['POST'], '<token3>': ['15'], '<token11>': ['0700'], '<token2>': ['204']} |
    +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Brain Example 1: Extract log patterns
=====================================

This example shows how to extract semantic meaningful log patterns from a raw log field using the brain algorithm. The default variable count threshold is 5.

PPL query::

    os> source=apache | patterns message method=brain | fields message, patterns_field ;
    fetched rows / total rows = 4/4
    +-----------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------+
    | message                                                                                                                     | patterns_field                                                                                                |
    |-----------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------|
    | 177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927                        | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] "HEAD /e-business/mindshare HTTP/<*>" 404 <*>                      |
    | 127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722 | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] "GET /architectures/convergence/niches/mindshare HTTP/<*>" 100 <*> |
    | 118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439                      | <*IP*> - - [<*>/Sep/<*>:<*>:<*>:<*> <*>] "PATCH /strategize/out-of-the-box HTTP/<*>" 401 <*>                  |
    | 210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481                                             | <*IP*> - - [<*>/Sep/<*>:<*>:<*>:<*> <*>] "POST /users HTTP/<*>" 301 <*>                                       |
    +-----------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------+

Brain Example 2: Extract log patterns with custom parameters
============================================================

This example shows how to extract semantic meaningful log patterns from a raw log field using custom parameters of the brain algorithm.

PPL query::

    os> source=apache | patterns message method=brain variable_count_threshold=2 | fields message, patterns_field ;
    fetched rows / total rows = 4/4
    +-----------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------+
    | message                                                                                                                     | patterns_field                                                       |
    |-----------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------|
    | 177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927                        | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] <*> <*> HTTP/<*>" <*> <*> |
    | 127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722 | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] <*> <*> HTTP/<*>" <*> <*> |
    | 118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439                      | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] <*> <*> HTTP/<*>" <*> <*> |
    | 210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481                                             | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] <*> <*> HTTP/<*>" <*> <*> |
    +-----------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------+

Brain Example 3: Return log patterns aggregation result
=======================================================

This example shows how to get aggregated results from a raw log field using the brain algorithm.

PPL query::

    os> source=apache | patterns message method=brain mode=aggregation variable_count_threshold=2 | fields patterns_field, pattern_count, sample_logs ;
    fetched rows / total rows = 1/1
    +----------------------------------------------------------------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | patterns_field                                                       | pattern_count | sample_logs                                                                                                                                                                                                                                                                                                                                                                                                               |
    |----------------------------------------------------------------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
    | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] <*> <*> HTTP/<*>" <*> <*> | 4             | [177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927,127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722,118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439,210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481] |
    +----------------------------------------------------------------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Brain Example 4: Return log patterns aggregation result with detected variable tokens
=====================================================================================

This example shows how to get aggregated results with detected variable tokens using the brain algorithm.

Configuration
-------------
With option ``show_numbered_token`` enabled, the output can detect numbered variable tokens from the pattern field.

PPL query::

    os> source=apache | patterns message method=brain mode=aggregation show_numbered_token=true variable_count_threshold=2 | fields patterns_field, pattern_count, tokens ;
    fetched rows / total rows = 1/1
    +----------------------------------------------------------------------------------------------------------------------------------------+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | patterns_field                                                                                                                         | pattern_count | tokens                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
    |----------------------------------------------------------------------------------------------------------------------------------------+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
    | <token1> - <token2> [<token3>/Sep/<token4>:<token5>:<token6>:<token7> <token8>] <token9> <token10> HTTP/<token11>" <token12> <token13> | 4             | {'<token13>': ['19927', '28722', '27439', '9481'], '<token5>': ['10', '10', '10', '10'], '<token4>': ['2022', '2022', '2022', '2022'], '<token7>': ['57', '57', '57', '57'], '<token6>': ['15', '15', '15', '15'], '<token9>': ['"HEAD', '"GET', '"PATCH', '"POST'], '<token8>': ['-0700', '-0700', '-0700', '-0700'], '<token10>': ['/e-business/mindshare', '/architectures/convergence/niches/mindshare', '/strategize/out-of-the-box', '/users'], '<token1>': ['177.95.8.74', '127.45.152.6', '118.223.210.10... |
    +----------------------------------------------------------------------------------------------------------------------------------------+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Limitations
===========

- Patterns command is not pushed down to OpenSearch data node for now. It will only group log patterns on log messages returned to coordinator node.
