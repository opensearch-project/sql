
# fields

The `fields` command specifies the fields that should be included in or excluded from the search results.

## Syntax

The `fields` command has the following syntax:

```syntax
fields [+|-] <field-list>
```

## Parameters

The `fields` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field-list>` | Required | A comma-delimited or space-delimited list of fields to keep or remove. Supports wildcard patterns. |
| `[+|-]` | Optional | If the plus sign (`+`) is used, only the fields specified in the `field-list` are included. If the minus sign (`-`) is used, all fields specified in the `field-list` are excluded. Default is `+`. |
  

## Example 1: Select the fields you need for triage

The following query selects the key fields an SRE needs when investigating an incident -- severity, service name, and the log message:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'FATAL')
| sort severityNumber, `resource.attributes.service.name`
| fields severityText, `resource.attributes.service.name`, body
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+--------------+----------------------------------+----------------------------------------------------------------------------------------------+
| severityText | resource.attributes.service.name | body                                                                                         |
|--------------+----------------------------------+----------------------------------------------------------------------------------------------|
| ERROR        | checkout                         | NullPointerException in CheckoutService.placeOrder at line 142                               |
| ERROR        | checkout                         | Kafka producer delivery failed: message too large for topic order-events (max 1048576 bytes) |
| ERROR        | frontend-proxy                   | HTTP POST /api/checkout 503 Service Unavailable - upstream connect error                     |
+--------------+----------------------------------+----------------------------------------------------------------------------------------------+
```
  

## Example 2: Remove specified fields from the search results 

The following query removes the `body` field from the results, keeping the output compact:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'FATAL')
| sort severityNumber, `resource.attributes.service.name`
| fields severityText, `resource.attributes.service.name`, body
| fields - body
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+--------------+----------------------------------+
| severityText | resource.attributes.service.name |
|--------------+----------------------------------|
| ERROR        | checkout                         |
| ERROR        | checkout                         |
| ERROR        | frontend-proxy                   |
+--------------+----------------------------------+
```
  

## Example 3: Space-delimited field selection  

Fields can be specified using spaces instead of commas, providing a more concise syntax:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'FATAL')
| sort severityNumber, `resource.attributes.service.name`
| fields severityText severityNumber body
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+--------------+----------------+----------------------------------------------------------------------------------------------+
| severityText | severityNumber | body                                                                                         |
|--------------+----------------+----------------------------------------------------------------------------------------------|
| ERROR        | 17             | NullPointerException in CheckoutService.placeOrder at line 142                               |
| ERROR        | 17             | Kafka producer delivery failed: message too large for topic order-events (max 1048576 bytes) |
| ERROR        | 17             | HTTP POST /api/checkout 503 Service Unavailable - upstream connect error                     |
+--------------+----------------+----------------------------------------------------------------------------------------------+
```
  

## Example 4: Prefix wildcard pattern  

The following query selects all fields starting with `severity`, capturing both the text label and numeric level:
  
```ppl
source=otellogs
| where severityText = 'ERROR'
| sort `resource.attributes.service.name`
| fields severity*
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+--------------+----------------+
| severityText | severityNumber |
|--------------+----------------|
| ERROR        | 17             |
| ERROR        | 17             |
| ERROR        | 17             |
+--------------+----------------+
```
  

## Example 5: Suffix wildcard pattern  

The following query selects fields ending with `Id`, useful for retrieving trace correlation identifiers:
  
```ppl
source=otellogs
| where LENGTH(traceId) > 0
| fields *Id
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+----------+------------------+
| spanId   | traceId          |
|----------+------------------|
| span0001 | abcd1234efgh5678 |
| span0002 | abcd1234efgh5678 |
| span0003 | abcd1234efgh5678 |
+----------+------------------+
```
  

## Example 6: Wildcard pattern matching  

The following query selects fields containing `severity` using a `contains` wildcard:
  
```ppl
source=otellogs
| where severityText = 'FATAL'
| fields *severity*
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------+----------------+
| severityText | severityNumber |
|--------------+----------------|
| FATAL        | 21             |
+--------------+----------------+
```
  

## Example 7: Mixed delimiter syntax with automatic field deduplication  

The following query combines spaces and commas for flexible field specification. It also demonstrates automatic deduplication: `severityText` is explicitly listed and also matches the `severity*` wildcard, but it appears only once in the output:
  
```ppl
source=otellogs
| where LENGTH(traceId) > 0
| fields severityText, severity* *Id
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+--------------+----------------+----------+------------------+
| severityText | severityNumber | spanId   | traceId          |
|--------------+----------------+----------+------------------|
| INFO         | 9              | span0001 | abcd1234efgh5678 |
| INFO         | 9              | span0002 | abcd1234efgh5678 |
| WARN         | 13             | span0003 | abcd1234efgh5678 |
+--------------+----------------+----------+------------------+
```

## Example 8: Full wildcard selection  

The following query selects all available fields using `` `*` ``. This expression selects all fields defined in the index schema, including fields that may contain null values:
  
```ppl
source=otellogs
| where severityText = 'FATAL'
| fields `*`
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------+---------+---------------------+----------------------+--------------+...+-------+...+----------------+---------------------+-------------------------------------------------------------------------+
| spanId | traceId | @timestamp          | instrumentationScope | severityText |...| flags |...| severityNumber | time                | body                                                                    |
|--------+---------+---------------------+----------------------+--------------+...+-------+...+----------------+---------------------+-------------------------------------------------------------------------|
|        |         | 2024-02-01 09:17:00 | {}                   | FATAL        |...| 0     |...| 21             | 2024-02-01 09:17:00 | Out of memory: Java heap space - shutting down pod payment-6f8d4b-ht7q3 |
+--------+---------+---------------------+----------------------+--------------+...+-------+...+----------------+---------------------+-------------------------------------------------------------------------+
```

## Example 9: Wildcard exclusion  

The following query removes trace identifier fields using wildcard patterns containing the minus (`-`) operator:
  
```ppl
source=otellogs
| where severityText = 'ERROR'
| sort `resource.attributes.service.name`
| fields - *Id
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------+----------------------+--------------+...+-------+...+----------------+---------------------+----------------------------------------------------------------+
| @timestamp          | instrumentationScope | severityText |...| flags |...| severityNumber | time                | body                                                           |
|---------------------+----------------------+--------------+...+-------+...+----------------+---------------------+----------------------------------------------------------------|
| 2024-02-01 09:15:00 | {}                   | ERROR        |...| 0     |...| 17             | 2024-02-01 09:15:00 | NullPointerException in CheckoutService.placeOrder at line 142 |
+---------------------+----------------------+--------------+...+-------+...+----------------+---------------------+----------------------------------------------------------------+
```
  

## Related documentation 

- [`table`](table.md) -- An alias command with identical functionality  
