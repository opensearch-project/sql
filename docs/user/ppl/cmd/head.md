
# head

The `head` command returns the first N lines from a search result.

> **Note**: The `head` command is not rewritten to [query domain-specific language (DSL)](https://docs.opensearch.org/latest/query-dsl/index/). It is only executed on the coordinating node.

## Syntax

The `head` command has the following syntax:

```syntax
head [<size>] [from <offset>]
```

## Parameters

The `head` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<size>` | Optional | The number of results to return. Must be an integer. Default is `10`. |
| `<offset>` | Optional | The number of results to skip (used with the `from` keyword). Must be an integer. Default is `0`. |
  

## Example 1: Retrieve the first set of results using the default size 

The following query retrieves the most recent errors, limited to the default 10 results. This is a common first step when investigating an incident:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'WARN')
| sort - severityNumber, `resource.attributes.service.name`
| fields severityText, `resource.attributes.service.name`, body
| head
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 10/10
+--------------+----------------------------------+----------------------------------------------------------------------------------------------+
| severityText | resource.attributes.service.name | body                                                                                         |
|--------------+----------------------------------+----------------------------------------------------------------------------------------------|
| ERROR        | checkout                         | NullPointerException in CheckoutService.placeOrder at line 142                               |
| ERROR        | checkout                         | Kafka producer delivery failed: message too large for topic order-events (max 1048576 bytes) |
| ERROR        | frontend-proxy                   | [2024-02-01T09:20:00.456Z] "POST /api/checkout HTTP/1.1" 503 - 0 30000 checkout-8d4f7b-mk2p9 |
| ERROR        | payment                          | Payment failed: connection timeout to payment gateway after 30000ms                          |
| ERROR        | payment                          | Out of memory: Java heap space - shutting down pod payment-6f8d4b-ht7q3                      |
| ERROR        | product-catalog                  | Database primary node unreachable: connection refused to db-primary-01:5432                  |
| ERROR        | recommendation                   | Failed to process recommendation request: invalid product ID from 203.0.113.50               |
| WARN         | frontend-proxy                   | SSL certificate for api.example.com expires in 14 days                                       |
| WARN         | frontend-proxy                   | Rate limit threshold reached: 450/500 requests per minute for API key ending in ...abc789    |
| WARN         | product-catalog                  | Slow query detected: SELECT * FROM products WHERE category = 'electronics' took 3200ms       |
+--------------+----------------------------------+----------------------------------------------------------------------------------------------+
```
  

## Example 2: Retrieve a specified number of results  

The following query grabs just the top 3 most critical log entries for a quick severity check:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'WARN')
| sort - severityNumber, `resource.attributes.service.name`
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
| ERROR        | frontend-proxy                   | [2024-02-01T09:20:00.456Z] "POST /api/checkout HTTP/1.1" 503 - 0 30000 checkout-8d4f7b-mk2p9 |
+--------------+----------------------------------+----------------------------------------------------------------------------------------------+
```
  

## Example 3: Retrieve the first N results after an offset M

The following query skips the 2 most critical entries and returns the next 3, useful for paging through results after reviewing the top issues:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'WARN')
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
| ERROR        | frontend-proxy                   | [2024-02-01T09:20:00.456Z] "POST /api/checkout HTTP/1.1" 503 - 0 30000 checkout-8d4f7b-mk2p9 |
| ERROR        | payment                          | Payment failed: connection timeout to payment gateway after 30000ms                          |
| ERROR        | payment                          | Out of memory: Java heap space - shutting down pod payment-6f8d4b-ht7q3                      |
+--------------+----------------------------------+----------------------------------------------------------------------------------------------+
```
  

