
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

The following query finds error logs mentioning connection timeouts:
  
```ppl
source=otellogs
| where severityText = 'ERROR'
| regex body=".*timeout.*"
| fields severityText, `resource.attributes.service.name`, body
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------+----------------------------------+---------------------------------------------------------------------+
| severityText | resource.attributes.service.name | body                                                                |
|--------------+----------------------------------+---------------------------------------------------------------------|
| ERROR        | payment                          | Payment failed: connection timeout to payment gateway after 30000ms |
+--------------+----------------------------------+---------------------------------------------------------------------+
```
  

## Example 2: Exclude logs matching a pattern  

The following query finds all errors except those related to timeouts:
  
```ppl
source=otellogs
| where severityText = 'ERROR'
| regex body!=".*timeout.*"
| fields severityText, `resource.attributes.service.name`, body
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+--------------+----------------------------------+--------------------------------------------------------------------------+
| severityText | resource.attributes.service.name | body                                                                     |
|--------------+----------------------------------+--------------------------------------------------------------------------|
| ERROR        | checkout                         | NullPointerException in CheckoutService.placeOrder at line 142           |
| ERROR        | payment                          | Out of memory: Java heap space - shutting down pod payment-6f8d4b-ht7q3  |
| ERROR        | frontend-proxy                   | HTTP POST /api/checkout 503 Service Unavailable - upstream connect error |
+--------------+----------------------------------+--------------------------------------------------------------------------+
```
  

## Example 3: Filter by service name pattern  

The following query finds warning logs from services whose names end with "catalog":
  
```ppl
source=otellogs
| where severityText = 'WARN'
| regex `resource.attributes.service.name`=".*catalog$"
| fields severityText, `resource.attributes.service.name`, body
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+--------------+----------------------------------+----------------------------------------------------------------------------------------+
| severityText | resource.attributes.service.name | body                                                                                   |
|--------------+----------------------------------+----------------------------------------------------------------------------------------|
| WARN         | product-catalog                  | Slow query detected: SELECT * FROM products WHERE category = 'electronics' took 3200ms |
| WARN         | product-catalog                  | Connection pool 80% utilized on database replica db-replica-02                         |
+--------------+----------------------------------+----------------------------------------------------------------------------------------+
```
  
