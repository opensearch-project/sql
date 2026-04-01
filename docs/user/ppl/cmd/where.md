
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
| ERROR        | 17             | api-gateway                      |
| ERROR        | 17             | auth-service                     |
| ERROR        | 17             | cart-service                     |
| ERROR        | 17             | payment-service                  |
| ERROR        | 17             | user-service                     |
| FATAL        | 21             | inventory-service                |
| FATAL        | 21             | payment-service                  |
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
fetched rows / total rows = 2/2
+--------------+----------------+----------------------------------+
| severityText | severityNumber | resource.attributes.service.name |
|--------------+----------------+----------------------------------|
| ERROR        | 17             | payment-service                  |
| FATAL        | 21             | payment-service                  |
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
| ERROR        | api-gateway                      | HTTP POST /api/checkout 503 Service Unavailable - upstream connect error                     |
| ERROR        | auth-service                     | Failed to authenticate user U400: invalid credentials from 203.0.113.50                      |
| ERROR        | cart-service                     | Kafka producer delivery failed: message too large for topic order-events (max 1048576 bytes) |
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
fetched rows / total rows = 4/4
+--------------+----------------------------------+
| severityText | resource.attributes.service.name |
|--------------+----------------------------------|
| DEBUG        | auth-service                     |
| DEBUG        | auth-service                     |
| INFO         | auth-service                     |
| ERROR        | auth-service                     |
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
| WARN         | api-gateway                      | Rate limit threshold reached: 450/500 requests per minute for API key ending in ...abc789 |
| WARN         | cert-monitor                     | SSL certificate for api.example.com expires in 14 days                                    |
| WARN         | inventory-service                | Slow query detected: SELECT * FROM inventory WHERE stock < 10 took 3200ms                 |
| WARN         | inventory-service                | Connection pool 80% utilized on database replica db-replica-02                            |
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
| ERROR        | api-gateway                      | HTTP POST /api/checkout 503 Service Unavailable - upstream connect error                     |
| ERROR        | auth-service                     | Failed to authenticate user U400: invalid credentials from 203.0.113.50                      |
| ERROR        | cart-service                     | Kafka producer delivery failed: message too large for topic order-events (max 1048576 bytes) |
| ERROR        | payment-service                  | Payment failed: connection timeout to payment gateway after 30000ms                          |
| ERROR        | user-service                     | NullPointerException in UserService.getProfile at line 142                                   |
| FATAL        | inventory-service                | Database primary node unreachable: connection refused to db-primary-01:5432                  |
| FATAL        | payment-service                  | Out of memory: Java heap space - shutting down pod payment-service-7d4b8c-xk2q9              |
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
| INFO         | opentelemetry-java        |
| INFO         | opentelemetry-java        |
| WARN         | opentelemetry-python      |
| ERROR        | opentelemetry-java        |
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
fetched rows / total rows = 2/2
+--------------+----------------------------------+---------------------------------------------------------------------------------+
| severityText | resource.attributes.service.name | body                                                                            |
|--------------+----------------------------------+---------------------------------------------------------------------------------|
| ERROR        | payment-service                  | Payment failed: connection timeout to payment gateway after 30000ms             |
| FATAL        | payment-service                  | Out of memory: Java heap space - shutting down pod payment-service-7d4b8c-xk2q9 |
+--------------+----------------------------------+---------------------------------------------------------------------------------+
```
  
