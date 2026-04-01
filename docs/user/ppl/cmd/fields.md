
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
| ERROR        | api-gateway                      | HTTP POST /api/checkout 503 Service Unavailable - upstream connect error                     |
| ERROR        | auth-service                     | Failed to authenticate user U400: invalid credentials from 203.0.113.50                      |
| ERROR        | cart-service                     | Kafka producer delivery failed: message too large for topic order-events (max 1048576 bytes) |
+--------------+----------------------------------+----------------------------------------------------------------------------------------------+
```
  

## Example 2: Remove noisy fields from results 

The following query removes the raw `body` field after extracting what you need, keeping the output clean:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'FATAL')
| sort severityNumber, `resource.attributes.service.name`
| fields severityText, `resource.attributes.service.name`, severityNumber, body
| fields - body, severityNumber
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+--------------+----------------------------------+
| severityText | resource.attributes.service.name |
|--------------+----------------------------------|
| ERROR        | api-gateway                      |
| ERROR        | auth-service                     |
| ERROR        | cart-service                     |
+--------------+----------------------------------+
```
  

## Example 3: Select all severity-related fields with a prefix wildcard

When you're not sure of the exact field names, use wildcards to grab all fields starting with a common prefix. This selects both `severityText` and `severityNumber`:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'FATAL')
| sort severityNumber, `resource.attributes.service.name`
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
  

## Example 4: Select trace correlation fields with a suffix wildcard

The following query grabs all fields ending with `Id`, useful for pulling trace correlation identifiers when debugging distributed requests:
  
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
  

## Example 5: Combine explicit fields with wildcards

The following query selects specific fields alongside wildcard-matched fields. This grabs the severity text plus all trace identifiers in one query:
  
```ppl
source=otellogs
| where LENGTH(traceId) > 0
| fields severityText, *Id
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+--------------+----------+------------------+
| severityText | spanId   | traceId          |
|--------------+----------+------------------|
| INFO         | span0001 | abcd1234efgh5678 |
| INFO         | span0002 | abcd1234efgh5678 |
| WARN         | span0003 | abcd1234efgh5678 |
+--------------+----------+------------------+
```
  

## Example 6: Remove trace fields with wildcard exclusion

The following query strips all identifier fields from the output, useful when you want the log content without the tracing metadata:
  
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
+---------------------+----------------------+--------------+...+-------+...+----------------+---------------------+--------------------------------------------------------------------------+
| @timestamp          | instrumentationScope | severityText |...| flags |...| severityNumber | time                | body                                                                     |
|---------------------+----------------------+--------------+...+-------+...+----------------+---------------------+--------------------------------------------------------------------------|
| 2024-02-01 09:20:00 | null                 | ERROR        |...| 0     |...| 17             | 2024-02-01 09:20:00 | HTTP POST /api/checkout 503 Service Unavailable - upstream connect error |
+---------------------+----------------------+--------------+...+-------+...+----------------+---------------------+--------------------------------------------------------------------------+
```

## Example 7: Select all fields  

The following query selects all fields defined in the index schema using `` `*` ``. Fields with null values are included in the result set:
  
```ppl
source=otellogs
| where severityText = 'FATAL'
| fields `*`
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------+---------+---------------------+----------------------+--------------+...+-------+...+----------------+---------------------+---------------------------------------------------------------------------------+
| spanId | traceId | @timestamp          | instrumentationScope | severityText |...| flags |...| severityNumber | time                | body                                                                            |
|--------+---------+---------------------+----------------------+--------------+...+-------+...+----------------+---------------------+---------------------------------------------------------------------------------|
|        |         | 2024-02-01 09:17:00 | null                 | FATAL        |...| 0     |...| 21             | 2024-02-01 09:17:00 | Out of memory: Java heap space - shutting down pod payment-service-7d4b8c-xk2q9 |
+--------+---------+---------------------+----------------------+--------------+...+-------+...+----------------+---------------------+---------------------------------------------------------------------------------+
```
  

## Related documentation 

- [`table`](table.md) -- An alias command with identical functionality  
