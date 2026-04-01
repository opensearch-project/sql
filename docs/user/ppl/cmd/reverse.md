
# reverse

The `reverse` command reverses the display order of the search results. It returns the same results but in the opposite order.

> **Note**: The `reverse` command processes the entire dataset. If applied directly to millions of records, it consumes significant coordinating node memory resources. Only apply the `reverse` command to smaller datasets, typically after aggregation operations.

## Syntax

The `reverse` command has the following syntax:

```syntax
reverse
```

The `reverse` command takes no parameters.

## Example 1: Basic reverse operation  

The following query retrieves the top 4 errors sorted by service name, then reverses the order:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'FATAL')
| sort severityNumber, `resource.attributes.service.name`
| fields severityText, `resource.attributes.service.name`
| head 4
| reverse
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+--------------+----------------------------------+
| severityText | resource.attributes.service.name |
|--------------+----------------------------------|
| ERROR        | payment-service                  |
| ERROR        | cart-service                     |
| ERROR        | auth-service                     |
| ERROR        | api-gateway                      |
+--------------+----------------------------------+
```
  

## Example 2: Use the reverse and sort commands

The following query reverses results after sorting by severity in ascending order, effectively showing the most critical issues first:
  
```ppl
source=otellogs
| dedup severityText
| sort severityNumber
| fields severityText, severityNumber
| reverse
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 5/5
+--------------+----------------+
| severityText | severityNumber |
|--------------+----------------|
| FATAL        | 21             |
| ERROR        | 17             |
| WARN         | 13             |
| INFO         | 9              |
| DEBUG        | 5              |
+--------------+----------------+
```
  

## Example 3: Reverse aggregation results  

The following query reverses the order of aggregated error counts by service, showing the least active services first:
  
```ppl
source=otellogs
| stats count() as log_count by severityText
| sort - log_count
| reverse
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 5/5
+-----------+--------------+
| log_count | severityText |
|-----------+--------------|
| 2         | FATAL        |
| 3         | DEBUG        |
| 4         | WARN         |
| 5         | ERROR        |
| 6         | INFO         |
+-----------+--------------+
```
  
