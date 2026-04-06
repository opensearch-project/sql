
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

The following query selects specific fields from the search results:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'WARN')
| sort severityNumber, `resource.attributes.service.name`
| fields severityText, `resource.attributes.service.name`, body
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+--------------+----------------------------------+-------------------------------------------------------------------------------------------+
| severityText | resource.attributes.service.name | body                                                                                      |
|--------------+----------------------------------+-------------------------------------------------------------------------------------------|
| WARN         | frontend-proxy                   | SSL certificate for api.example.com expires in 14 days                                    |
| WARN         | frontend-proxy                   | Rate limit threshold reached: 450/500 requests per minute for API key ending in ...abc789 |
| WARN         | product-catalog                  | Slow query detected: SELECT * FROM products WHERE category = 'electronics' took 3200ms    |
+--------------+----------------------------------+-------------------------------------------------------------------------------------------+
```
  

## Example 2: Remove noisy fields from results 

The following query removes the raw `body` field after extracting what you need, keeping the output clean:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'WARN')
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
| WARN         | frontend-proxy                   |
| WARN         | frontend-proxy                   |
| WARN         | product-catalog                  |
+--------------+----------------------------------+
```
  

## Example 3: Select all severity-related fields with a prefix wildcard

When you're not sure of the exact field names, use wildcards to grab all fields starting with a common prefix. This selects both `severityText` and `severityNumber`:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'WARN')
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
| WARN         | 13             |
| WARN         | 13             |
| WARN         | 13             |
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
+---------------------+----------------------+--------------+---------------------------------------------------------------------------------------------------------------------------+-------+------------+------------------------+----------------+---------------------+----------------------------------------------------------------+
| @timestamp          | instrumentationScope | severityText | resource                                                                                                                  | flags | attributes | droppedAttributesCount | severityNumber | time                | body                                                           |
|---------------------+----------------------+--------------+---------------------------------------------------------------------------------------------------------------------------+-------+------------+------------------------+----------------+---------------------+----------------------------------------------------------------|
| 2024-02-01 09:15:00 | {}                   | ERROR        | {'attributes': {'service': {'name': 'checkout'}, 'host': {'name': 'checkout-8b4c2d-jp5r7'}}, 'droppedAttributesCount': 0} | 0     | {}         | 0                      | 17             | 2024-02-01 09:15:00 | NullPointerException in CheckoutService.placeOrder at line 142 |
+---------------------+----------------------+--------------+---------------------------------------------------------------------------------------------------------------------------+-------+------------+------------------------+----------------+---------------------+----------------------------------------------------------------+
```

## Example 7: Field deduplication

The following query automatically prevents duplicate columns when wildcards expand to already specified fields:

```ppl
source=otellogs
| fields severityText, severity*
| head 3
```

The query returns the following results. Even though `severityText` is explicitly specified and also matches `severity*`, it appears only once because of automatic deduplication:

```text
fetched rows / total rows = 3/3
+--------------+----------------+
| severityText | severityNumber |
|--------------+----------------|
| INFO         | 9              |
| INFO         | 9              |
| WARN         | 13             |
+--------------+----------------+
```

## Example 8: Select all fields  

The following query selects all fields defined in the index schema using `` `*` ``. Fields with null values are included in the result set:
  
```ppl
source=otellogs
| where severityText = 'WARN'
| fields `*`
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------+------------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------------+--------------+--------------------------------------------------------------------------------------------------------------------------------------+-------+------------+------------------------+----------------+---------------------+----------------------------------------------------------------------------------------+
| spanId   | traceId          | @timestamp          | instrumentationScope                                                                                                                      | severityText | resource                                                                                                                             | flags | attributes | droppedAttributesCount | severityNumber | time                | body                                                                                   |
|----------+------------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------------+--------------+--------------------------------------------------------------------------------------------------------------------------------------+-------+------------+------------------------+----------------+---------------------+----------------------------------------------------------------------------------------|
| span0003 | abcd1234efgh5678 | 2024-02-01 09:12:00 | {'name': 'go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc', 'droppedAttributesCount': 0, 'version': '0.49.0'} | WARN         | {'attributes': {'service': {'name': 'product-catalog'}, 'host': {'name': 'productcatalog-7c9d-zn4p2'}}, 'droppedAttributesCount': 0} | 0     | {}         | 0                      | 13             | 2024-02-01 09:12:00 | Slow query detected: SELECT * FROM products WHERE category = 'electronics' took 3200ms |
+----------+------------------+---------------------+-------------------------------------------------------------------------------------------------------------------------------------------+--------------+--------------------------------------------------------------------------------------------------------------------------------------+-------+------------+------------------------+----------------+---------------------+----------------------------------------------------------------------------------------+
```
  

## Related documentation 

- [`table`](table.md) -- An alias command with identical functionality  
