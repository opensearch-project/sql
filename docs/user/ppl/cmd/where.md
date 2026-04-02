
# where

The `where` command filters the search results. It only returns results that match the specified conditions.

## Syntax

The `where` command has the following syntax:

```syntax
where <boolean-expression>
```

## Parameters

The `where` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<boolean-expression>` | Required | The condition used to filter the results. Only rows in which this condition evaluates to `true` are returned. |

## Example 1: Filter by severity level

The following query finds all log entries with a severity level above `WARN` (severityNumber > 13), helping you quickly identify errors and critical issues across your services:

```ppl
source=otellogs
| where severityNumber > 13
| sort severityNumber, `resource.attributes.service.name`
| fields severityText, severityNumber, `resource.attributes.service.name`
```

The query returns the following results:

```text
fetched rows / total rows = 7/7
+--------------+----------------+----------------------------------+
| severityText | severityNumber | resource.attributes.service.name |
|--------------+----------------+----------------------------------|
| ERROR        | 17             | checkout                         |
| ERROR        | 17             | checkout                         |
| ERROR        | 17             | frontend-proxy                   |
| ERROR        | 17             | payment                          |
| ERROR        | 17             | recommendation                   |
| FATAL        | 21             | payment                          |
| FATAL        | 21             | product-catalog                  |
+--------------+----------------+----------------------------------+
```

## Example 2: Filter using combined criteria

The following query narrows down errors to a specific service during an incident investigation, combining severity and service name conditions with `AND`:

```ppl
source=otellogs
| where severityNumber >= 17 AND `resource.attributes.service.name` = 'payment-service'
| sort severityNumber
| fields severityText, severityNumber, `resource.attributes.service.name`
```

The query returns the following results:

```text
fetched rows / total rows = 0/0
+--------------+----------------+----------------------------------+
| severityText | severityNumber | resource.attributes.service.name |
|--------------+----------------+----------------------------------|
+--------------+----------------+----------------------------------+
```


## Example 3: Filter with multiple possible values

The following query retrieves all error and fatal logs to get a full picture of failures during an outage, using `OR` to match either condition:

```ppl
source=otellogs
| where severityText = 'FATAL' or severityText = 'ERROR'
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
  

## Example 4: Filter by text patterns 

The `LIKE` operator enables pattern matching on string fields using wildcards.

### Matching with a prefix pattern

The following query uses a percent sign (`%`) to find all services in the `cart` domain, useful when investigating issues across a team's microservices:

```ppl
source=otellogs
| where LIKE(`resource.attributes.service.name`, 'cart-%')
| sort severityNumber
| fields severityText, `resource.attributes.service.name`, body
```

### Matching a service name pattern

The following query finds all logs from the auth team's services to investigate authentication issues:

```ppl
source=otellogs
| where LIKE(`resource.attributes.service.name`, 'auth-%')
| sort severityNumber
| fields severityText, `resource.attributes.service.name`
```

The query returns the following results:

```text
fetched rows / total rows = 0/0
+--------------+----------------------------------+
| severityText | resource.attributes.service.name |
|--------------+----------------------------------|
+--------------+----------------------------------+
```

## Example 5: Filter by excluding specific values  

The following query uses a `NOT` operator to exclude routine informational and debug logs, focusing on warnings, errors, and fatal issues that need attention:
  
```ppl
source=otellogs
| where NOT severityText IN ('INFO', 'DEBUG')
| sort severityNumber, `resource.attributes.service.name`
| fields severityText, `resource.attributes.service.name`, body
| head 4
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+--------------+----------------------------------+-------------------------------------------------------------------------------------------+
| severityText | resource.attributes.service.name | body                                                                                      |
|--------------+----------------------------------+-------------------------------------------------------------------------------------------|
| WARN         | frontend-proxy                   | SSL certificate for api.example.com expires in 14 days                                    |
| WARN         | frontend-proxy                   | Rate limit threshold reached: 450/500 requests per minute for API key ending in ...abc789 |
| WARN         | product-catalog                  | Slow query detected: SELECT * FROM products WHERE category = 'electronics' took 3200ms    |
| WARN         | product-catalog                  | Connection pool 80% utilized on database replica db-replica-02                            |
+--------------+----------------------------------+-------------------------------------------------------------------------------------------+
```
  

## Example 6: Filter using value lists  

The following query uses an `IN` operator to match multiple severity levels at once, retrieving all critical failures for incident response:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'FATAL')
| sort severityNumber, `resource.attributes.service.name`
| fields severityText, `resource.attributes.service.name`, body
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 7/7
+--------------+----------------------------------+----------------------------------------------------------------------------------------------+
| severityText | resource.attributes.service.name | body                                                                                         |
|--------------+----------------------------------+----------------------------------------------------------------------------------------------|
| ERROR        | checkout                         | NullPointerException in CheckoutService.placeOrder at line 142                               |
| ERROR        | checkout                         | Kafka producer delivery failed: message too large for topic order-events (max 1048576 bytes) |
| ERROR        | frontend-proxy                   | HTTP POST /api/checkout 503 Service Unavailable - upstream connect error                     |
| ERROR        | payment                          | Payment failed: connection timeout to payment gateway after 30000ms                          |
| ERROR        | recommendation                   | Failed to process recommendation request: invalid product ID from 203.0.113.50               |
| FATAL        | payment                          | Out of memory: Java heap space - shutting down pod payment-6f8d4b-ht7q3                      |
| FATAL        | product-catalog                  | Database primary node unreachable: connection refused to db-primary-01:5432                  |
+--------------+----------------------------------+----------------------------------------------------------------------------------------------+
```
  

## Example 7: Filter records with missing data  

The following query finds logs that have instrumentation scope metadata, which helps identify which services are properly instrumented with OpenTelemetry:
  
```ppl
source=otellogs
| where NOT ISNULL(instrumentationScope.name)
| fields severityText, instrumentationScope.name
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+--------------+---------------------------+
| severityText | instrumentationScope.name |
|--------------+---------------------------|
| INFO         | opentelemetry-js          |
| INFO         | opentelemetry-dotnet      |
| WARN         | opentelemetry-go          |
| ERROR        | opentelemetry-js          |
+--------------+---------------------------+
```
  

## Example 8: Filter using grouped conditions  

The following query investigates a specific service's critical failures by combining severity conditions with a service filter, using parentheses to control evaluation order:
  
```ppl
source=otellogs
| where (severityText = 'ERROR' OR severityText = 'FATAL') AND `resource.attributes.service.name` = 'payment-service'
| sort severityNumber
| fields severityText, `resource.attributes.service.name`, body
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 0/0
+--------------+----------------------------------+------+
| severityText | resource.attributes.service.name | body |
|--------------+----------------------------------+------|
+--------------+----------------------------------+------+
```
  
