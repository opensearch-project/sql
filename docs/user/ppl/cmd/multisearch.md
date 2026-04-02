
# multisearch


The `multisearch` command runs multiple subsearches and merges their results. It allows you to combine data from different queries on the same or different sources. You can optionally apply subsequent processing, such as aggregation or sorting, to the combined results. Each subsearch can have different filtering criteria, data transformations, and field selections. 

Multisearch is particularly useful for comparative analysis, union operations, and creating comprehensive datasets from multiple search criteria. The command supports timestamp-based result interleaving when working with time-series data.

Use multisearch for:

* **Comparative analysis**: Compare metrics across different segments, regions, or time periods.
* **Success rate monitoring**: Calculate success rates by comparing successful to total operations.
* **Multi-source data combination**: Merge data from different indexes or apply different filters to the same source.
* **A/B testing analysis**: Combine results from different test groups for comparison.
* **Time-series data merging**: Interleave events from multiple sources based on timestamps.
 
  

## Syntax

The `multisearch` command has the following syntax:

```syntax
multisearch <subsearch1> <subsearch2> [<subsearch3> ...]
```

The following are examples of the `multisearch` command syntax:

```syntax
| multisearch [search source=table | where condition1] [search source=table | where condition2]
| multisearch [search source=index1 | fields field1, field2] [search source=index2 | fields field1, field2]
| multisearch [search source=table | where status="success"] [search source=table | where status="error"]
```

## Parameters

The `multisearch` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<subsearchN>` | Required | At least two subsearches are required. Each subsearch must be enclosed in square brackets and start with the `search` keyword (`[search source=index | <commands>]`). All PPL commands are supported within subsearches. |
| `<result-processing>` | Optional | Commands applied to the merged results after the multisearch operation (for example, `stats`, `sort`, or `head`). |  

## Example 1: Compare errors with debug logs

This example merges error logs with debug logs side by side. This is useful when investigating whether debug-level logs from the same services provide clues about the root cause of errors:
  
```ppl
| multisearch [search source=otellogs
| where severityText = 'ERROR'
| eval env = 'errors'
| fields env, `resource.attributes.service.name`, body] [search source=otellogs
| where severityText = 'DEBUG'
| eval env = 'debug'
| fields env, `resource.attributes.service.name`, body]
| sort env, `resource.attributes.service.name`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 8/8
+--------+----------------------------------+----------------------------------------------------------------------------------------------+
| env    | resource.attributes.service.name | body                                                                                         |
|--------+----------------------------------+----------------------------------------------------------------------------------------------|
| debug  | cart                             | Cache miss for key user:session:U200 in Valkey cluster                                       |
| debug  | cart                             | Valkey SETEX user:session:U300 3600 - session refreshed                                      |
| debug  | product-catalog                  | gRPC call /ProductCatalogService/GetProduct completed in 12ms                                |
| errors | checkout                         | NullPointerException in CheckoutService.placeOrder at line 142                               |
| errors | checkout                         | Kafka producer delivery failed: message too large for topic order-events (max 1048576 bytes) |
| errors | frontend-proxy                   | HTTP POST /api/checkout 503 Service Unavailable - upstream connect error                     |
| errors | payment                          | Payment failed: connection timeout to payment gateway after 30000ms                          |
| errors | recommendation                   | Failed to process recommendation request: invalid product ID from 203.0.113.50               |
+--------+----------------------------------+----------------------------------------------------------------------------------------------+
```
  

## Example 2: Segmenting logs by severity tier

This example separates critical and non-critical logs for comparative analysis:
  
```ppl
| multisearch [search source=otellogs
| where severityNumber >= 17
| eval tier = "critical"
| fields severityText, severityNumber, tier] [search source=otellogs
| where severityNumber < 17 AND severityNumber >= 13
| eval tier = "warning"
| fields severityText, severityNumber, tier]
| sort - severityNumber
| head 5
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 5/5
+--------------+----------------+----------+
| severityText | severityNumber | tier     |
|--------------+----------------+----------|
| FATAL        | 21             | critical |
| FATAL        | 21             | critical |
| ERROR        | 17             | critical |
| ERROR        | 17             | critical |
| ERROR        | 17             | critical |
+--------------+----------------+----------+
```
  

## Example 3: Merging time-series data from multiple sources

This example demonstrates how to combine time-series data from different sources while maintaining chronological order. The results are automatically sorted by timestamp to create a unified timeline:
  
```ppl
| multisearch [search source=time_data
| where category IN ("A", "B")] [search source=time_data2
| where category IN ("E", "F")]
| fields @timestamp, category, value, timestamp
| head 5
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 5/5
+---------------------+----------+-------+---------------------+
| @timestamp          | category | value | timestamp           |
|---------------------+----------+-------+---------------------|
| 2025-08-01 04:00:00 | E        | 2001  | 2025-08-01 04:00:00 |
| 2025-08-01 03:47:41 | A        | 8762  | 2025-08-01 03:47:41 |
| 2025-08-01 02:30:00 | F        | 2002  | 2025-08-01 02:30:00 |
| 2025-08-01 01:14:11 | B        | 9015  | 2025-08-01 01:14:11 |
| 2025-08-01 01:00:00 | E        | 2003  | 2025-08-01 01:00:00 |
+---------------------+----------+-------+---------------------+
```
  

## Example 4: Handling missing fields across subsearches

This example demonstrates how `multisearch` handles schema differences when subsearches return different fields. When one subsearch includes a field that others don't have, missing values are automatically filled with null values:
  
```ppl
| multisearch [search source=otellogs
| where severityText = 'ERROR'
| eval needs_page = "yes"
| fields severityText, `resource.attributes.service.name`, needs_page] [search source=otellogs
| where severityText = 'WARN'
| fields severityText, `resource.attributes.service.name`]
| sort `resource.attributes.service.name`
| head 5
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 5/5
+--------------+----------------------------------+------------+
| severityText | resource.attributes.service.name | needs_page |
|--------------+----------------------------------+------------|
| ERROR        | checkout                         | yes        |
| ERROR        | checkout                         | yes        |
| ERROR        | frontend-proxy                   | yes        |
| WARN         | frontend-proxy                   | null       |
| WARN         | frontend-proxy                   | null       |
+--------------+----------------------------------+------------+
```
  

## Limitations

The `multisearch` command has the following limitations:

* At least two subsearches must be specified.
* When fields with the same name exist across subsearches but have incompatible types, the system automatically resolves conflicts by renaming the conflicting fields. The first occurrence retains the original name, while subsequent conflicting fields are renamed using a numeric suffix (for example, `age` becomes `age0`, `age1`, and so on). This ensures that all data is preserved while maintaining schema consistency.  