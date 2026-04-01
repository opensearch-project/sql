
# eventstats

The `eventstats` command enriches your event data with calculated summary statistics. It analyzes the specified fields within your events, computes various statistical measures, and then appends these results as new fields to each original event.

The `eventstats` command operates in the following way:

1. It performs calculations across the entire search results or within defined groups.
2. The original events remain intact, with new fields added to contain the statistical results.
3. The command is particularly useful for comparative analysis, identifying outliers, and providing additional context to individual events.

## Comparing stats and eventstats

For a comprehensive comparison of `stats`, `eventstats`, and `streamstats` commands, including their differences in transformation behavior, output format, aggregation scope, and use cases, see [Comparing stats, eventstats, and streamstats](streamstats.md/#comparing-stats-eventstats-and-streamstats).

## Syntax

The `eventstats` command has the following syntax:

```syntax
eventstats [bucket_nullable=bool] <function>... [by-clause]
```

The following are examples of the `eventstats` command syntax:

```syntax
source = table | eventstats avg(a)
source = table | where a < 50 | eventstats count(c)
source = table | eventstats min(c), max(c) by b
source = table | eventstats count(c) as count_by by b | where count_by > 1000
source = table | eventstats dc(field) as distinct_count
source = table | eventstats distinct_count(category) by region
```

## Parameters

The `eventstats` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<function>` | Required | An aggregation function or window function. |
| `bucket_nullable` | Optional | Controls whether the `eventstats` command considers `null` buckets as a valid group in group-by aggregations. When set to `false`, it does not treat `null` group-by values as a distinct group during aggregation. Default is determined by `plugins.ppl.syntax.legacy.preferred`. |
| `<by-clause>` | Optional | Groups results by specified fields or expressions. Syntax: `by [span-expression,] [field,]...` Default is aggregating over the entire search results. |
| `<span-expression>` | Optional | Splits a field into buckets by intervals (at most one). Syntax: `span(field_expr, interval_expr)`. For example, `span(age, 10)` creates 10-year age buckets, while `span(timestamp, 1h)` creates hourly buckets. |

### Time units

The following time units are available for span expressions:

* Milliseconds (`ms`)
* Seconds (`s`)
* Minutes (`m`, case sensitive)
* Hours (`h`)
* Days (`d`)
* Weeks (`w`)
* Months (`M`, case sensitive)
* Quarters (`q`)
* Years (`y`)  

## Aggregation functions

The `eventstats` command supports the following aggregation functions:

* `COUNT` -- Count of values
* `SUM` -- Sum of numeric values
* `AVG` -- Average of numeric values
* `MAX` -- Maximum value
* `MIN` -- Minimum value
* `VAR_SAMP` -- Sample variance
* `VAR_POP` -- Population variance
* `STDDEV_SAMP` -- Sample standard deviation
* `STDDEV_POP` -- Population standard deviation
* `DISTINCT_COUNT`/`DC` -- Distinct count of values
* `EARLIEST` -- Earliest value by timestamp
* `LATEST` -- Latest value by timestamp  

For detailed documentation of each function, see [Functions](../functions/aggregations.md).  

## Example 1: Enrich logs with per-service counts  

The following query adds the total log count for each service to every log entry, letting you see how active each service is alongside individual log details:
  
```ppl
source=otellogs
| eventstats count() as service_total by `resource.attributes.service.name`
| where severityText = 'ERROR'
| sort `resource.attributes.service.name`
| fields severityText, `resource.attributes.service.name`, service_total, body
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+--------------+----------------------------------+---------------+----------------------------------------------------------------------------------------------+
| severityText | resource.attributes.service.name | service_total | body                                                                                         |
|--------------+----------------------------------+---------------+----------------------------------------------------------------------------------------------|
| ERROR        | api-gateway                      | 2             | HTTP POST /api/checkout 503 Service Unavailable - upstream connect error                     |
| ERROR        | auth-service                     | 4             | Failed to authenticate user U400: invalid credentials from 203.0.113.50                      |
| ERROR        | cart-service                     | 3             | Kafka producer delivery failed: message too large for topic order-events (max 1048576 bytes) |
+--------------+----------------------------------+---------------+----------------------------------------------------------------------------------------------+
```
  

## Example 2: Calculate severity statistics by group  

The following query adds the average and max severity per service to each log entry. This lets you compare individual log severity against the service's overall severity profile:
  
```ppl
source=otellogs
| where severityText = 'ERROR'
| eventstats avg(severityNumber) as avg_sev, count() as error_count by `resource.attributes.service.name`
| sort `resource.attributes.service.name`
| fields `resource.attributes.service.name`, severityNumber, avg_sev, error_count
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 5/5
+----------------------------------+----------------+---------+-------------+
| resource.attributes.service.name | severityNumber | avg_sev | error_count |
|----------------------------------+----------------+---------+-------------|
| api-gateway                      | 17             | 17.0    | 1           |
| auth-service                     | 17             | 17.0    | 1           |
| cart-service                     | 17             | 17.0    | 1           |
| payment-service                  | 17             | 17.0    | 1           |
| user-service                     | 17             | 17.0    | 1           |
+----------------------------------+----------------+---------+-------------+
```
  

## Example 3: Null bucket handling

The following query uses `bucket_nullable=false` to exclude logs with null namespace from the group-by aggregation. Logs from services without a namespace get `null` for the count:

```ppl
source=otellogs
| eventstats bucket_nullable=false count() as scope_count by instrumentationScope.name
| where severityText = 'ERROR'
| sort `resource.attributes.service.name`
| fields `resource.attributes.service.name`, `instrumentationScope.name`, scope_count
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 5/5
+----------------------------------+---------------------------+-------------+
| resource.attributes.service.name | instrumentationScope.name | scope_count |
|----------------------------------+---------------------------+-------------|
| api-gateway                      | null                      | null        |
| auth-service                     | null                      | null        |
| cart-service                     | null                      | null        |
| payment-service                  | opentelemetry-java        | 3           |
| user-service                     | null                      | null        |
+----------------------------------+---------------------------+-------------+
```
