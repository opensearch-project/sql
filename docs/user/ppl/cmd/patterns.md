
# patterns

The `patterns` command extracts log patterns from a text field and appends the results to the search results. Grouping logs by pattern simplifies aggregating statistics from large volumes of log data for analysis and troubleshooting. You can choose from the following log parsing methods to achieve high pattern-grouping accuracy:

* `simple_pattern`: A parsing method that uses [Java regular expressions](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html).
* `brain`: An automatic log-grouping method that provides high grouping accuracy while preserving semantic meaning.

The `patterns` command supports the following modes:

* `label`: Returns individual pattern labels.
* `aggregation`: Returns aggregated results for the target field.

The command identifies variable parts of log messages (such as timestamps, numbers, IP addresses, and unique identifiers) and replaces them with `<*>` placeholders to create reusable patterns. For example, email addresses like `amberduke@pyrami.com` and `hattiebond@netagy.com` are replaced with the pattern `<*>@<*>.<*>`.

> **Note**: The `patterns` command is not executed on OpenSearch data nodes. It only groups log patterns from log messages that have been returned to the coordinator node.

## Syntax

The `patterns` command supports the following syntax options.

### Simple pattern method syntax

The `patterns` command with a `simple_pattern` method has the following syntax:

```syntax
patterns <field> [by <byClause>] [method=simple_pattern] [mode=label | aggregation] [max_sample_count=integer] [show_numbered_token=boolean] [new_field=<new-field-name>] [pattern=<regex-pattern>]
```

### Brain method syntax

The `patterns` command with a `brain` method has the following syntax:

```syntax
patterns <field> [by <byClause>] [method=brain] [mode=label | aggregation] [max_sample_count=integer] [buffer_limit=integer] [show_numbered_token=boolean] [new_field=<new-field-name>] [variable_count_threshold=integer] [frequency_threshold_percentage=decimal]
```

## Parameters

The `patterns` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field>` | Required | The text field that is analyzed to extract log patterns. |
| `<byClause>` | Optional | The fields or scalar functions used to group logs before labeling or aggregation. |
| `method` | Optional | The pattern extraction method to use. Valid values are `simple_pattern` and `brain`. Default is `simple_pattern`. |
| `mode` | Optional | The output mode of the command. Valid values are `label` and `aggregation`. Default is `label`. |
| `max_sample_count` | Optional | The maximum number of sample log entries returned per pattern in `aggregation` mode. Default is `10`. |
| `buffer_limit` | Optional | A safeguard setting for the `brain` method that limits the size of its internal temporary buffer. Minimum is `50000`. Default is `100000`. |
| `show_numbered_token` | Optional | Enables numbered token placeholders in the output instead of the default wildcard token. See [Placeholder behavior](#placeholder-behavior). Default is `false`. |
| `<new_field>` | Optional | An alias for the output field that contains the extracted pattern. Default is `patterns_field`. |

The `simple_pattern` method accepts the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<pattern>` | Optional | A custom [Java regular expression](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html) pattern that identifies characters or sequences to replace with `<*>` placeholders. When not specified, the method uses a default pattern that automatically removes alphanumeric characters and replaces variable parts with `<*>` placeholders while preserving structural elements. |

The `brain` method accepts the following parameters.

| Parameter | Required/Optional | Description | 
| --- | --- | --- | 
| `variable_count_threshold` | Optional | Controls the algorithm sensitivity to detecting constant words by counting distinct words at specific positions in the initial log groups. Default is `5`. |
| `frequency_threshold_percentage` | Optional | Sets the minimum word frequency percentage threshold. Words with frequencies below this value are ignored. The `brain` algorithm selects log patterns based on the longest word combination. Default is `0.3`. |

## Placeholder behavior

By default, the Apache Calcite engine labels variables using the `<*>` placeholder. If the `show_numbered_token` option is enabled, the Calcite engine's `label` mode not only labels the text pattern but also assigns numbered placeholders to variable tokens. In `aggregation` mode, it outputs both the labeled pattern and the variable tokens for each pattern. In this case, variable placeholders use the format `<token%d>` instead of `<*>`.

## Changing the default pattern method

To override default pattern parameters, run the following command:

```bash ignore
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
```

## Enabling UDAF pushdown for patterns aggregation

When using the `patterns` command with `mode=aggregation` and `method=brain`, the aggregation can optionally be pushed down to OpenSearch as a scripted metric aggregation for parallel execution across data nodes. This can improve performance for large datasets but uses scripted metric aggregations which lack circuit breaker protection.

By default, UDAF pushdown is **disabled**. To enable it, run the following command:

```bash ignore
PUT _cluster/settings
{
  "persistent": {
    "plugins.calcite.udaf_pushdown.enabled": true
  }
}
```

> **Warning**: Enabling UDAF pushdown executes user-defined aggregation functions as scripted metric aggregations on OpenSearch data nodes. This bypasses certain memory circuit breakers and may cause out-of-memory errors on nodes when processing very large datasets. Use with caution and monitor cluster resource usage.

When UDAF pushdown is disabled (the default), the pattern aggregation runs locally on the coordinator node after fetching the data from OpenSearch.

## Simple pattern examples

The following are examples of using the `simple_pattern` method.

### Example 1: Create a new field

The following query extracts patterns from the `email` field for each document. If the `email` field is `null`, the command returns an empty string:
  
```ppl
source=accounts
| patterns email method=simple_pattern
| fields email, patterns_field
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------------------+----------------+
| email                 | patterns_field |
|-----------------------+----------------|
| amberduke@pyrami.com  | <*>@<*>.<*>    |
| hattiebond@netagy.com | <*>@<*>.<*>    |
| null                  |                |
| daleadams@boink.com   | <*>@<*>.<*>    |
+-----------------------+----------------+
```
  

### Example 2: Extract log patterns

The following query extracts default patterns from a raw log field:
  
```ppl
source=apache
| patterns message method=simple_pattern
| fields message, patterns_field
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------+
| message                                                                                                                     | patterns_field                                                                                    |
|-----------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------|
| 177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927                        | <*>.<*>.<*>.<*> - <*> [<*>/<*>/<*>:<*>:<*>:<*> -<*>] "<*> /<*>-<*>/<*> <*>/<*>.<*>" <*> <*>       |
| 127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722 | <*>.<*>.<*>.<*> - <*> [<*>/<*>/<*>:<*>:<*>:<*> -<*>] "<*> /<*>/<*>/<*>/<*> <*>/<*>.<*>" <*> <*>   |
| 118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439                      | <*>.<*>.<*>.<*> - - [<*>/<*>/<*>:<*>:<*>:<*> -<*>] "<*> /<*>/<*>-<*>-<*>-<*> <*>/<*>.<*>" <*> <*> |
| 210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481                                             | <*>.<*>.<*>.<*> - - [<*>/<*>/<*>:<*>:<*>:<*> -<*>] "<*> /<*> <*>/<*>.<*>" <*> <*>                 |
+-----------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------+
```
  

### Example 3: Extract log patterns using a custom regex pattern

The following query extracts patterns from a raw log field using a custom pattern:
  
```ppl
source=apache
| patterns message method=simple_pattern new_field='no_numbers' pattern='[0-9]'
| fields message, no_numbers
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| message                                                                                                                     | no_numbers                                                                                                                                                                                                |
|-----------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927                        | <*><*><*>.<*><*>.<*>.<*><*> - upton<*><*><*><*> [<*><*>/Sep/<*><*><*><*>:<*><*>:<*><*>:<*><*> -<*><*><*><*>] "HEAD /e-business/mindshare HTTP/<*>.<*>" <*><*><*> <*><*><*><*><*>                          |
| 127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722 | <*><*><*>.<*><*>.<*><*><*>.<*> - pouros<*><*><*><*> [<*><*>/Sep/<*><*><*><*>:<*><*>:<*><*>:<*><*> -<*><*><*><*>] "GET /architectures/convergence/niches/mindshare HTTP/<*>.<*>" <*><*><*> <*><*><*><*><*> |
| 118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439                      | <*><*><*>.<*><*><*>.<*><*><*>.<*><*><*> - - [<*><*>/Sep/<*><*><*><*>:<*><*>:<*><*>:<*><*> -<*><*><*><*>] "PATCH /strategize/out-of-the-box HTTP/<*>.<*>" <*><*><*> <*><*><*><*><*>                        |
| 210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481                                             | <*><*><*>.<*><*><*>.<*><*>.<*><*><*> - - [<*><*>/Sep/<*><*><*><*>:<*><*>:<*><*>:<*><*> -<*><*><*><*>] "POST /users HTTP/<*>.<*>" <*><*><*> <*><*><*><*>                                                   |
+-----------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```
  

### Example 4: Return a log pattern aggregation result

The following query aggregates patterns extracted from a raw log field:
  
```ppl
source=apache
| patterns message method=simple_pattern mode=aggregation
| fields patterns_field, pattern_count, sample_logs
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+---------------------------------------------------------------------------------------------------+---------------+-------------------------------------------------------------------------------------------------------------------------------+
| patterns_field                                                                                    | pattern_count | sample_logs                                                                                                                   |
|---------------------------------------------------------------------------------------------------+---------------+-------------------------------------------------------------------------------------------------------------------------------|
| <*>.<*>.<*>.<*> - - [<*>/<*>/<*>:<*>:<*>:<*> -<*>] "<*> /<*> <*>/<*>.<*>" <*> <*>                 | 1             | [210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481]                                             |
| <*>.<*>.<*>.<*> - - [<*>/<*>/<*>:<*>:<*>:<*> -<*>] "<*> /<*>/<*>-<*>-<*>-<*> <*>/<*>.<*>" <*> <*> | 1             | [118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439]                      |
| <*>.<*>.<*>.<*> - <*> [<*>/<*>/<*>:<*>:<*>:<*> -<*>] "<*> /<*>-<*>/<*> <*>/<*>.<*>" <*> <*>       | 1             | [177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927]                        |
| <*>.<*>.<*>.<*> - <*> [<*>/<*>/<*>:<*>:<*>:<*> -<*>] "<*> /<*>/<*>/<*>/<*> <*>/<*>.<*>" <*> <*>   | 1             | [127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722] |
+---------------------------------------------------------------------------------------------------+---------------+-------------------------------------------------------------------------------------------------------------------------------+
```
  

### Example 5: Return aggregated log patterns with detected variable tokens

The following query returns aggregated results with detected variable tokens. When the `show_numbered_token` option is enabled, the pattern output uses numbered placeholders (for example, `<token1>`, `<token2>`) and returns a mapping of each placeholder to the values that it represents:

  
```ppl
source=apache
| patterns message method=simple_pattern mode=aggregation show_numbered_token=true
| fields patterns_field, pattern_count, tokens
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| patterns_field                                                                                                                                                                       | pattern_count | tokens                                                                                                                                                                                                                                                                                                                                                                                            |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| <token1>.<token2>.<token3>.<token4> - - [<token5>/<token6>/<token7>:<token8>:<token9>:<token10> -<token11>] "<token12> /<token13> <token14>/<token15>.<token16>" <token17> <token18> | 1             | {'<token14>': ['HTTP'], '<token13>': ['users'], '<token16>': ['1'], '<token15>': ['1'], '<token18>': ['9481'], '<token17>': ['301'], '<token5>': ['28'], '<token4>': ['104'], '<token7>': ['2022'], '<token6>': ['Sep'], '<token9>': ['15'], '<token8>': ['10'], '<token10>': ['57'], '<token1>': ['210'], '<token12>': ['POST'], '<token3>': ['15'], '<token11>': ['0700'], '<token2>': ['204']} |
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```


## Brain pattern examples

The following are examples of using the `brain` method.

### Example 1: Extract log patterns

The following query extracts semantically meaningful log patterns from a raw log field using the `brain` algorithm. This query uses the default `variable_count_threshold` value of `5`:
  
```ppl
source=apache
| patterns message method=brain
| fields message, patterns_field
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------+
| message                                                                                                                     | patterns_field                                                                                                |
|-----------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------|
| 177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927                        | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] "HEAD /e-business/mindshare HTTP/<*>" 404 <*>                      |
| 127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722 | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] "GET /architectures/convergence/niches/mindshare HTTP/<*>" 100 <*> |
| 118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439                      | <*IP*> - - [<*>/Sep/<*>:<*>:<*>:<*> <*>] "PATCH /strategize/out-of-the-box HTTP/<*>" 401 <*>                  |
| 210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481                                             | <*IP*> - - [<*>/Sep/<*>:<*>:<*>:<*> <*>] "POST /users HTTP/<*>" 301 <*>                                       |
+-----------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------+
```
  

### Example 2: Extract log patterns using custom parameters

The following query extracts semantically meaningful log patterns from a raw log field using custom parameters of the `brain` algorithm:
  
```ppl
source=apache
| patterns message method=brain variable_count_threshold=2
| fields message, patterns_field
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------+
| message                                                                                                                     | patterns_field                                                       |
|-----------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------|
| 177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927                        | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] <*> <*> HTTP/<*>" <*> <*> |
| 127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722 | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] <*> <*> HTTP/<*>" <*> <*> |
| 118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439                      | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] <*> <*> HTTP/<*>" <*> <*> |
| 210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481                                             | <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] <*> <*> HTTP/<*>" <*> <*> |
+-----------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------+
```
  

### Example 3: Return a log pattern aggregation result

The following query aggregates patterns extracted from a raw log field using the `brain` algorithm:
  
```ppl
source=apache
| patterns message method=brain mode=aggregation variable_count_threshold=2
| fields patterns_field, pattern_count, sample_logs
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------------------------------------------------------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| patterns_field                                                       | pattern_count | sample_logs                                                                                                                                                                                                                                                                                                                                                                                                               |
|----------------------------------------------------------------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| <*IP*> - <*> [<*>/Sep/<*>:<*>:<*>:<*> <*>] <*> <*> HTTP/<*>" <*> <*> | 4             | [177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] "HEAD /e-business/mindshare HTTP/1.0" 404 19927,127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] "GET /architectures/convergence/niches/mindshare HTTP/1.0" 100 28722,118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] "PATCH /strategize/out-of-the-box HTTP/1.0" 401 27439,210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] "POST /users HTTP/1.1" 301 9481] |
+----------------------------------------------------------------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```
  

### Example 4: Return aggregated log patterns with detected variable tokens

The following query returns aggregated results with detected variable tokens using the `brain` method. When the `show_numbered_token` option is enabled, the pattern output uses numbered placeholders (for example, `<token1>`, `<token2>`) and returns a mapping of each placeholder to the values that it represents:
  
```ppl
source=apache
| patterns message method=brain mode=aggregation show_numbered_token=true variable_count_threshold=2
| fields patterns_field, pattern_count, tokens
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------------------------------------------------------------------------------------------------------------------------------+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| patterns_field                                                                                                                         | pattern_count | tokens                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
|----------------------------------------------------------------------------------------------------------------------------------------+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| <token1> - <token2> [<token3>/Sep/<token4>:<token5>:<token6>:<token7> <token8>] <token9> <token10> HTTP/<token11>" <token12> <token13> | 4             | {'<token13>': ['19927', '28722', '27439', '9481'], '<token5>': ['10', '10', '10', '10'], '<token4>': ['2022', '2022', '2022', '2022'], '<token7>': ['57', '57', '57', '57'], '<token6>': ['15', '15', '15', '15'], '<token9>': ['"HEAD', '"GET', '"PATCH', '"POST'], '<token8>': ['-0700', '-0700', '-0700', '-0700'], '<token10>': ['/e-business/mindshare', '/architectures/convergence/niches/mindshare', '/strategize/out-of-the-box', '/users'], '<token1>': ['177.95.8.74', '127.45.152.6', '118.223.210.10... |
+----------------------------------------------------------------------------------------------------------------------------------------+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```
  

  