
# head

The `head` command returns the first N lines from a search result.

> **Note**: The `head` command is not rewritten to [query domain-specific language (DSL)](https://docs.opensearch.org/latest/query-dsl/index/). It is only executed on the coordinating node.

## Syntax

The `head` command has the following syntax:

```syntax
head [<size>] [from <offset>]
```

## fetched rows / total rows = 1/1
+------------+
| total_logs |
|------------|
| 20         |
+------------+arameters

The `head` command supports the following parameters.

| fetched rows / total rows = 1/1
+--------------+
| avg_severity |
|--------------|
| 12.4         |
+--------------+arameter | Required/Optional | Description |
| --- | --- | --- |
| `<size>` | Optional | The number of results to return. Must be an integer. Default is `10`. |
| `<offset>` | Optional | The number of results to skip (used with the `from` keyword). Must be an integer. Default is `0`. |
  

## Example 1: Retrieve the first set of results using the default size 

The following query retrieves the most recent errors, limited to the default 10 results. This is a common first step when investigating an incident:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'FATAL')
| sort - severityNumber, `resource.attributes.service.name`
| fields severityText, `resource.attributes.service.name`, body
| head
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 7/7
+--------------+----------------------------------+----------------------------------------------------------------------------------------------+
| severityText | resource.attributes.service.name | body                                                                                         |
|--------------+----------------------------------+----------------------------------------------------------------------------------------------|
| FATAL        | payment                          | Out of memory: Java heap space - shutting down pod payment-6f8d4b-ht7q3                      |
| FATAL        | product-catalog                  | Database primary node unreachable: connection refused to db-primary-01:5432                  |
| ERROR        | checkout                         | NullPointerException in CheckoutService.placeOrder at line 142                               |
| ERROR        | checkout                         | Kafka producer delivery failed: message too large for topic order-events (max 1048576 bytes) |
| ERROR        | frontend-proxy                   | HTTP POST /api/checkout 503 Service Unavailable - upstream connect error                     |
| ERROR        | payment                          | Payment failed: connection timeout to payment gateway after 30000ms                          |
| ERROR        | recommendation                   | Failed to process recommendation request: invalid product ID from 203.0.113.50               |
+--------------+----------------------------------+----------------------------------------------------------------------------------------------+
```
  

## Example 2: Retrieve a specified number of results  

The following query grabs just the top 3 most critical log entries for a quick severity check:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'FATAL')
| sort - severityNumber, `resource.attributes.service.name`
| fields severityText, `resource.attributes.service.name`, body
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+--------------+----------------------------------+-----------------------------------------------------------------------------+
| severityText | resource.attributes.service.name | body                                                                        |
|--------------+----------------------------------+-----------------------------------------------------------------------------|
| FATAL        | payment                          | Out of memory: Java heap space - shutting down pod payment-6f8d4b-ht7q3     |
| FATAL        | product-catalog                  | Database primary node unreachable: connection refused to db-primary-01:5432 |
| ERROR        | checkout                         | NullPointerException in CheckoutService.placeOrder at line 142              |
+--------------+----------------------------------+-----------------------------------------------------------------------------+
```
  

## Example 3: Retrieve the first N results after an offset M

The following query skips the 2 most critical entries and returns the next 3, useful for paging through results after reviewing the top issues:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'FATAL')
| sort - severityNumber, `resource.attributes.service.name`
| fields severityText, `resource.attributes.service.name`, body
| head 3 from 2
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
  

