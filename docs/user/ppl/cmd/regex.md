
# regex

The `regex` command filters search results by matching field values against a regular expression pattern. Only documents in which the specified field matches the pattern are included in the results.

## Syntax

The `regex` command has the following syntax:

```syntax
regex <field> = <pattern>
regex <field> != <pattern>
```

The following operators are supported:

* `=` -- Positive matching (include matches)
* `!=` -- Negative matching (exclude matches)

The `regex` command uses Java's built-in regular expression engine, which supports:

* **Standard regex features**: Character classes, quantifiers, anchors.  
* **Named capture groups**: `(?<name>pattern)` syntax.  
* **Lookahead/lookbehind**: `(?=...)` and `(?<=...)` assertions.  
* **Inline flags**: Case-insensitive `(?i)`, multiline `(?m)`, dotall `(?s)`, and other modes.  

## Parameters

The `regex` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field>` | Required | The field name to match against. |
| `<pattern>` | Required | The regular expression pattern to match. Supports [Java regular expression syntax](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html). |

## Example 1: Find logs matching a pattern  

The following query finds all logs mentioning connection timeouts, useful for diagnosing network issues:
  
```ppl
source=otellogs
| regex body=".*timeout.*|.*Timeout.*"
| fields severityText, `resource.attributes.service.name`, body
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------+----------------------------------+---------------------------------------------------------------------+
| severityText | resource.attributes.service.name | body                                                                |
|--------------+----------------------------------+---------------------------------------------------------------------|
| ERROR        | payment-service                  | Payment failed: connection timeout to payment gateway after 30000ms |
+--------------+----------------------------------+---------------------------------------------------------------------+
```
  

## Example 2: Exclude logs matching a pattern  

The following query finds all errors except those related to timeouts, helping you focus on other failure types:
  
```ppl
source=otellogs
| where severityText = 'ERROR'
| regex body!=".*timeout.*"
| sort `resource.attributes.service.name`
| fields severityText, `resource.attributes.service.name`, body
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+--------------+----------------------------------+----------------------------------------------------------------------------------------------+
| severityText | resource.attributes.service.name | body                                                                                         |
|--------------+----------------------------------+----------------------------------------------------------------------------------------------|
| ERROR        | api-gateway                      | HTTP POST /api/checkout 503 Service Unavailable - upstream connect error                     |
| ERROR        | auth-service                     | Failed to authenticate user U400: invalid credentials from 203.0.113.50                      |
| ERROR        | cart-service                     | Kafka producer delivery failed: message too large for topic order-events (max 1048576 bytes) |
| ERROR        | user-service                     | NullPointerException in UserService.getProfile at line 142                                   |
+--------------+----------------------------------+----------------------------------------------------------------------------------------------+
```
  

## Example 3: Filter by service name pattern  

The following query finds logs from all services with "service" in their name, excluding infrastructure components like monitors and controllers:
  
```ppl
source=otellogs
| where severityText = 'FATAL'
| regex `resource.attributes.service.name`=".*-service$"
| fields severityText, `resource.attributes.service.name`, body
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+--------------+----------------------------------+---------------------------------------------------------------------------------+
| severityText | resource.attributes.service.name | body                                                                            |
|--------------+----------------------------------+---------------------------------------------------------------------------------|
| FATAL        | payment-service                  | Out of memory: Java heap space - shutting down pod payment-service-7d4b8c-xk2q9 |
| FATAL        | inventory-service                | Database primary node unreachable: connection refused to db-primary-01:5432     |
+--------------+----------------------------------+---------------------------------------------------------------------------------+
```
  
