
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
  

## Example 2: Remove specified fields from the search results 

The following query removes the `body` field from the results, keeping the output compact:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'WARN')
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
| WARN         | frontend-proxy                   |
| WARN         | frontend-proxy                   |
| WARN         | product-catalog                  |
+--------------+----------------------------------+
```
  

## Example 3: Space-delimited field selection  

Fields can be specified using spaces instead of commas, providing a more concise syntax:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'WARN')
| sort severityNumber, `resource.attributes.service.name`
| fields severityText severityNumber body
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+--------------+----------------+-------------------------------------------------------------------------------------------+
| severityText | severityNumber | body                                                                                      |
|--------------+----------------+-------------------------------------------------------------------------------------------|
| WARN         | 13             | SSL certificate for api.example.com expires in 14 days                                    |
| WARN         | 13             | Rate limit threshold reached: 450/500 requests per minute for API key ending in ...abc789 |
| WARN         | 13             | Slow query detected: SELECT * FROM products WHERE category = 'electronics' took 3200ms    |
+--------------+----------------+-------------------------------------------------------------------------------------------+
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
| where severityText = 'WARN'
| fields *severity*
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------+----------------+
| severityText | severityNumber |
|--------------+----------------|
| WARN         | 13             |
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
