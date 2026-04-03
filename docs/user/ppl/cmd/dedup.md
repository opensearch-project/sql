
# dedup

The `dedup` command removes duplicate documents defined by specified fields from the search result.

## Syntax

The `dedup` command has the following syntax:

```syntax
dedup [int] <field-list> [keepempty=<bool>] [consecutive=<bool>]
```

## Parameters

The `dedup` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field-list>` | Required | A comma-delimited list of fields to use for deduplication. At least one field is required. |
| `<int>` | Optional | The number of duplicate documents to retain for each combination. Must be greater than `0`. Default is `1`. |
| `keepempty` | Optional | When set to `true`, keeps documents in which any field in the field list has a `NULL` value or is missing. Default is `false`. |
| `consecutive` | Optional | When set to `true`, removes only consecutive duplicate documents. Default is `false`. Requires the legacy SQL engine (`plugins.calcite.enabled=false`). |
  

## Example 1: Remove duplicates based on a single field  

The following query deduplicates by service name to get one sample error per service, giving you a quick view of what's failing across your system:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'WARN')
| dedup `resource.attributes.service.name`
| sort `resource.attributes.service.name`
| fields `resource.attributes.service.name`, severityText, body
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 5/5
+----------------------------------+--------------+----------------------------------------------------------------------------------------+
| resource.attributes.service.name | severityText | body                                                                                   |
|----------------------------------+--------------+----------------------------------------------------------------------------------------|
| checkout                         | ERROR        | NullPointerException in CheckoutService.placeOrder at line 142                         |
| frontend-proxy                   | ERROR        | HTTP POST /api/checkout 503 Service Unavailable - upstream connect error               |
| payment                          | ERROR        | Payment failed: connection timeout to payment gateway after 30000ms                    |
| product-catalog                  | WARN         | Slow query detected: SELECT * FROM products WHERE category = 'electronics' took 3200ms |
| recommendation                   | ERROR        | Failed to process recommendation request: invalid product ID from 203.0.113.50         |
+----------------------------------+--------------+----------------------------------------------------------------------------------------+
```
  

## Example 2: Retain multiple duplicate documents  

The following query keeps up to two logs per severity level, giving you a broader sample of each level to understand the variety of issues:
  
```ppl
source=otellogs
| dedup 2 severityText
| sort severityNumber
| fields severityText, severityNumber
| head 6
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 6/6
+--------------+----------------+
| severityText | severityNumber |
|--------------+----------------|
| DEBUG        | 5              |
| DEBUG        | 5              |
| INFO         | 9              |
| INFO         | 9              |
| WARN         | 13             |
| WARN         | 13             |
+--------------+----------------+
```
  

## Example 3: Handle documents with empty field values  

The following query deduplicates by instrumentation scope name to see which OTel SDKs are reporting. By default, records with null values are dropped:
  
```ppl
source=otellogs
| dedup instrumentationScope.name
| fields instrumentationScope.name
| sort instrumentationScope.name
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-----------------------------------------------------------------------------+
| instrumentationScope.name                                                   |
|-----------------------------------------------------------------------------|
| @opentelemetry/instrumentation-http                                         |
| Microsoft.Extensions.Hosting                                                |
| go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc |
+-----------------------------------------------------------------------------+
```
  
The following query deduplicates while ignoring documents with empty values in the specified field:
  
```ppl
source=otellogs
| dedup instrumentationScope.name
| fields instrumentationScope.name
| sort instrumentationScope.name
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-----------------------------------------------------------------------------+
| instrumentationScope.name                                                   |
|-----------------------------------------------------------------------------|
| @opentelemetry/instrumentation-http                                         |
| Microsoft.Extensions.Hosting                                                |
| go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc |
+-----------------------------------------------------------------------------+
```
  

## Example 4: Deduplicate consecutive documents  

The following query removes duplicate consecutive documents. When logs are sorted by severity, this shows the transitions between severity levels, helping you see the pattern of escalation:
  
```ppl
source=otellogs
| sort severityNumber, `resource.attributes.service.name`
| dedup severityText consecutive=true
| fields severityText, `resource.attributes.service.name`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+--------------+----------------------------------+
| severityText | resource.attributes.service.name |
|--------------+----------------------------------|
| DEBUG        | cart                             |
| INFO         | cart                             |
| WARN         | frontend-proxy                   |
| ERROR        | checkout                         |
+--------------+----------------------------------+
```
  
