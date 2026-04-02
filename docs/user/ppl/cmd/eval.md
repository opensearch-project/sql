
# eval

The `eval` command evaluates the specified expression and appends the result of the evaluation to the search results.

> **Note**: The `eval` command is not rewritten to [query domain-specific language (DSL)](https://docs.opensearch.org/latest/query-dsl/). It is only executed on the coordinating node.

## Syntax

The `eval` command has the following syntax:

```syntax
eval <field>=<expression> ["," <field>=<expression> ]...
```

## Parameters

The `eval` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field>` | Required | The name of the field to create or update. If the field does not exist, a new field is added. If it already exists, its value is overwritten. |
| `<expression>` | Required | The expression to evaluate. |  
  

## Example 1: Classify logs by severity tier  

The following query creates an `is_critical` field that classifies each log as critical or non-critical based on severity, useful for building alert rules:
  
```ppl
source=otellogs
| eval is_critical = IF(severityNumber >= 17, 'yes', 'no')
| dedup severityText
| sort severityNumber
| fields severityText, is_critical
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 5/5
+--------------+-------------+
| severityText | is_critical |
|--------------+-------------|
| DEBUG        | no          |
| INFO         | no          |
| WARN         | no          |
| ERROR        | yes         |
| FATAL        | yes         |
+--------------+-------------+
```
  

## Example 2: Find untraced errors  

The following query creates two boolean fields to identify error logs and whether they have distributed tracing context. Untraced errors are harder to debug because you can't follow the request across services:
  
```ppl
source=otellogs
| eval is_error = severityNumber >= 17, is_traced = LENGTH(traceId) > 0
| where is_error = true
| sort `resource.attributes.service.name`
| fields `resource.attributes.service.name`, is_error, is_traced
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 7/7
+----------------------------------+----------+-----------+
| resource.attributes.service.name | is_error | is_traced |
|----------------------------------+----------+-----------|
| checkout                         | True     | True      |
| checkout                         | True     | False     |
| frontend-proxy                   | True     | True      |
| payment                          | True     | True      |
| payment                          | True     | False     |
| product-catalog                  | True     | False     |
| recommendation                   | True     | True      |
+----------------------------------+----------+-----------+
```
  

## Example 3: Build a standardized log line  

The following query prepends the severity level to the log body, creating a standardized format for export or alerting:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'FATAL')
| eval formatted = '[' + severityText + '] ' + body
| sort severityNumber, `resource.attributes.service.name`
| fields formatted
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+------------------------------------------------------------------------------------------------------+
| formatted                                                                                            |
|------------------------------------------------------------------------------------------------------|
| [ERROR] NullPointerException in CheckoutService.placeOrder at line 142                               |
| [ERROR] Kafka producer delivery failed: message too large for topic order-events (max 1048576 bytes) |
| [ERROR] HTTP POST /api/checkout 503 Service Unavailable - upstream connect error                     |
+------------------------------------------------------------------------------------------------------+
```
  
