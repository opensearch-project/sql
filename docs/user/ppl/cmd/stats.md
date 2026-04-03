
# stats

The `stats` command calculates aggregations on the search results.

## Comparing stats, eventstats, and streamstats

For a comprehensive comparison of `stats`, `eventstats`, and `streamstats` commands, including their differences in transformation behavior, output format, aggregation scope, and use cases, see [Comparing stats, eventstats, and streamstats](streamstats.md/#comparing-stats-eventstats-and-streamstats).

## Syntax

The `stats` command has the following syntax:

```syntax
stats [bucket_nullable=bool] <aggregation>... [by-clause]
```

## fetched rows / total rows = 1/1
+-----+
| p90 |
|-----|
| 21  |
+-----+arameters

The `stats` command supports the following parameters.

| fetched rows / total rows = 5/5
+--------------------------------+--------------+
| percentile(severityNumber, 90) | severityText |
|--------------------------------+--------------|
| 5                              | DEBUG        |
| 17                             | ERROR        |
| 21                             | ERROR        |
| 9                              | INFO         |
| 13                             | WARN         |
+--------------------------------+--------------+arameter | Required/Optional | Description |
| --- | --- | --- |
| `<aggregation>` | Required | An aggregation function. |
| `<by-clause>` | Optional | Groups results by specified fields or expressions. Syntax: `by [span-expression,] [field,]...` If no `by-clause` is specified, the stats command returns only one row, which is the aggregation over the entire search results. |
| `bucket_nullable` | Optional | Controls whether to include `null` buckets in group-by aggregations. When `false`, ignores records in which the `group-by` field is null, resulting in faster performance. Default is the value of `plugins.ppl.syntax.legacy.preferred`. |
| `<span-expression>` | Optional | Splits a field into buckets by intervals (maximum of one). Syntax: `span(field_expr, interval_expr)`. By default, the interval uses the field's default unit. For date/time fields, aggregation results ignore null values. Examples: `span(age, 10)` creates 10-year age buckets, and `span(timestamp, 1h)` creates hourly buckets. Valid time units are millisecond (`ms`), second (`s`), minute (`m`), hour (`h`), day (`d`), week (`w`), month (`M`), quarter (`q`), year (`y`). |

## Aggregation functions  

The `stats` command supports the following aggregation functions:

* `COUNT`/`C` -- Count of values
* `SUM` -- Sum of numeric values
* `AVG` -- Average of numeric values
* `MAX` -- Maximum value
* `MIN` -- Minimum value
* `VAR_SAMfetched rows / total rows = 5/5
+-----+-----------+--------------+
| p90 | sev_range | severityText |
|-----+-----------+--------------|
| 5   | 0         | DEBUG        |
| 9   | 0         | INFO         |
| 17  | 10        | ERROR        |
| 13  | 10        | WARN         |
| 21  | 20        | ERROR        |
+-----+-----------+--------------+` -- Sample variance
* `VAR_fetched rows / total rows = 1/1
+-----------------------------------------------------------------------------------------------------------------+
| all_levels                                                                                                      |
|-----------------------------------------------------------------------------------------------------------------|
| [INFO,INFO,WARN,ERROR,DEBUG,ERROR,INFO,ERROR,WARN,INFO,ERROR,DEBUG,WARN,INFO,ERROR,ERROR,INFO,WARN,DEBUG,ERROR] |
+-----------------------------------------------------------------------------------------------------------------+Ofetched rows / total rows = 3/3
+-----+-----------------------------------------------------------------------------+
| cnt | instrumentationScope.name                                                   |
|-----+-----------------------------------------------------------------------------|
| 2   | @opentelemetry/instrumentation-http                                         |
| 1   | Microsoft.Extensions.Hosting                                                |
| 1   | go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc |
+-----+-----------------------------------------------------------------------------+` -- fetched rows / total rows = 1/1
+-------------------------+
| severity_levels         |
|-------------------------|
| [DEBUG,ERROR,INFO,WARN] |
+-------------------------+opulation variance
* `STDDEV_SAMfetched rows / total rows = 4/4
+-----+-----------+--------------+
| cnt | sev_range | severityText |
|-----+-----------+--------------|
| 3   | 0         | DEBUG        |
| 6   | 0         | INFO         |
| 7   | 10        | ERROR        |
| 4   | 10        | WARN         |
+-----+-----------+--------------+` -- Sample standard deviation
* `STDDEV_fetched rows / total rows = 5/5
+--------------------------------------------------+--------------+
| services                                         | severityText |
|--------------------------------------------------+--------------|
| [cart,product-catalog,cart]                      | DEBUG        |
| [payment,checkout,frontend-proxy]                | ERROR        |
| [payment,product-catalog]                        | ERROR        |
| [frontend,cart,frontend]                         | INFO         |
| [product-catalog,product-catalog,frontend-proxy] | WARN         |
+--------------------------------------------------+--------------+Ofetched rows / total rows = 1/1
+-----+
| p90 |
|-----|
| 21  |
+-----+` -- fetched rows / total rows = 5/5
+--------------------------------+--------------+
| percentile(severityNumber, 90) | severityText |
|--------------------------------+--------------|
| 5                              | DEBUG        |
| 17                             | ERROR        |
| 21                             | ERROR        |
| 9                              | INFO         |
| 13                             | WARN         |
+--------------------------------+--------------+opulation standard deviation
* `DISTINCT_COUNT_Afetched rows / total rows = 5/5
+-----+-----------+--------------+
| p90 | sev_range | severityText |
|-----+-----------+--------------|
| 5   | 0         | DEBUG        |
| 9   | 0         | INFO         |
| 17  | 10        | ERROR        |
| 13  | 10        | WARN         |
| 21  | 20        | ERROR        |
+-----+-----------+--------------+fetched rows / total rows = 1/1
+-----------------------------------------------------------------------------------------------------------------+
| all_levels                                                                                                      |
|-----------------------------------------------------------------------------------------------------------------|
| [INFO,INFO,WARN,ERROR,DEBUG,ERROR,INFO,ERROR,WARN,INFO,ERROR,DEBUG,WARN,INFO,ERROR,ERROR,INFO,WARN,DEBUG,ERROR] |
+-----------------------------------------------------------------------------------------------------------------+ROX` -- Approximate distinct count
* `TAKE` -- List of original values
* `fetched rows / total rows = 3/3
+-----+-----------------------------------------------------------------------------+
| cnt | instrumentationScope.name                                                   |
|-----+-----------------------------------------------------------------------------|
| 2   | @opentelemetry/instrumentation-http                                         |
| 1   | Microsoft.Extensions.Hosting                                                |
| 1   | go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc |
+-----+-----------------------------------------------------------------------------+ERCENTILE`/`fetched rows / total rows = 1/1
+-------------------------+
| severity_levels         |
|-------------------------|
| [DEBUG,ERROR,INFO,WARN] |
+-------------------------+ERCENTILE_Afetched rows / total rows = 4/4
+-----+-----------+--------------+
| cnt | sev_range | severityText |
|-----+-----------+--------------|
| 3   | 0         | DEBUG        |
| 6   | 0         | INFO         |
| 7   | 10        | ERROR        |
| 4   | 10        | WARN         |
+-----+-----------+--------------+fetched rows / total rows = 5/5
+--------------------------------------------------+--------------+
| services                                         | severityText |
|--------------------------------------------------+--------------|
| [cart,product-catalog,cart]                      | DEBUG        |
| [payment,checkout,frontend-proxy]                | ERROR        |
| [payment,product-catalog]                        | ERROR        |
| [frontend,cart,frontend]                         | INFO         |
| [product-catalog,product-catalog,frontend-proxy] | WARN         |
+--------------------------------------------------+--------------+ROX` -- fetched rows / total rows = 1/1
+-----+
| p90 |
|-----|
| 21  |
+-----+ercentile calculations
* `fetched rows / total rows = 5/5
+--------------------------------+--------------+
| percentile(severityNumber, 90) | severityText |
|--------------------------------+--------------|
| 5                              | DEBUG        |
| 17                             | ERROR        |
| 21                             | ERROR        |
| 9                              | INFO         |
| 13                             | WARN         |
+--------------------------------+--------------+ERC<percent>`/`fetched rows / total rows = 5/5
+-----+-----------+--------------+
| p90 | sev_range | severityText |
|-----+-----------+--------------|
| 5   | 0         | DEBUG        |
| 9   | 0         | INFO         |
| 17  | 10        | ERROR        |
| 13  | 10        | WARN         |
| 21  | 20        | ERROR        |
+-----+-----------+--------------+<percent>` -- fetched rows / total rows = 1/1
+-----------------------------------------------------------------------------------------------------------------+
| all_levels                                                                                                      |
|-----------------------------------------------------------------------------------------------------------------|
| [INFO,INFO,WARN,ERROR,DEBUG,ERROR,INFO,ERROR,WARN,INFO,ERROR,DEBUG,WARN,INFO,ERROR,ERROR,INFO,WARN,DEBUG,ERROR] |
+-----------------------------------------------------------------------------------------------------------------+ercentile shortcut functions
* `MEDIAN` -- 50th percentile
* `EARLIEST` -- Earliest value by timestamp
* `LATEST` -- Latest value by timestamp
* `FIRST` -- First non-null value
* `LAST` -- Last non-null value
* `LIST` -- Collect all values into array
* `VALUES` -- Collect unique values into sorted array  
  
For detailed documentation of each function, see [Aggregation Functions](../functions/aggregations.md).

## Example 1: Calculate the count of events  

The following query counts the total number of log entries, a basic health check for log ingestion:
  
```ppl
source=otellogs
| stats count() as total_logs
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+------------+
| total_logs |
|------------|
| 20         |
+------------+
```
  

## Example 2: Calculate the average of a field  

The following query calculates the average severity number across all logs. A rising average over time may indicate increasing system instability:
  
```ppl
source=otellogs
| stats avg(severityNumber) as avg_severity
```
  
The query returns the following results:

```text
fetched rows / total rows = 1/1
+--------------+
| avg_severity |
|--------------|
| 12.0         |
+--------------+
```
  

## Example 3: Calculate the count by group  

The following query counts logs by severity level, giving you a breakdown of your system's health at a glance:
  
```ppl
source=otellogs
| stats count() as log_count by severityText
| sort - log_count
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------+--------------+
| log_count | severityText |
|-----------+--------------|
| 7         | ERROR        |
| 6         | INFO         |
| 4         | WARN         |
| 3         | DEBUG        |
+-----------+--------------+
```
  

## Example 4: Calculate multiple aggregations by group  

The following query calculates the total log count and severity range per service, helping you identify which services are most active and most problematic:
  
```ppl
source=otellogs
| stats count() as total, min(severityNumber) as min_sev, max(severityNumber) as max_sev by `resource.attributes.service.name`
| sort - total
| head 5
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 5/5
+-------+---------+---------+----------------------------------+
| total | min_sev | max_sev | resource.attributes.service.name |
|-------+---------+---------+----------------------------------|
| 4     | 9       | 9       | frontend                         |
| 4     | 5       | 17      | product-catalog                  |
| 3     | 5       | 9       | cart                             |
| 3     | 9       | 17      | checkout                         |
| 3     | 13      | 17      | frontend-proxy                   |
+-------+---------+---------+----------------------------------+
```
  

## Example 5: Calculate the count by a span  

The following query groups logs into severity buckets of 10, showing the distribution across low (0-9), medium (10-19), and high (20+) severity ranges:
  
```ppl
source=otellogs
| stats count() as log_count by span(severityNumber, 10)
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+-----------+-------------------------+
| log_count | span(severityNumber,10) |
|-----------+-------------------------|
| 9         | 0                       |
| 11        | 10                      |
+-----------+-------------------------+
```
  

## Example 6: Calculate the count by a field and span  

The following query counts logs by severity level within severity number ranges, showing how severity text maps to numeric ranges:
  
```ppl
source=otellogs
| stats count() as cnt by span(severityNumber, 10) as sev_range, severityText
| sort sev_range
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----+-----------+--------------+
| cnt | sev_range | severityText |
|-----+-----------+--------------|
| 3   | 0         | DEBUG        |
| 6   | 0         | INFO         |
| 7   | 10        | ERROR        |
| 4   | 10        | WARN         |
+-----+-----------+--------------+
```
  

## Example 7: Calculate the distinct count of a field  

The following query counts the total and distinct number of services reporting logs, useful for verifying all expected services are reporting:
  
```ppl
source=otellogs
| stats count(`resource.attributes.service.name`) as total_entries, distinct_count(`resource.attributes.service.name`) as unique_services
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------+-----------------+
| total_entries | unique_services |
|---------------+-----------------|
| 20            | 7               |
+---------------+-----------------+
```
  

## Example 8: Collect unique values using VALUES by group  

The following query collects the unique service names for each severity level, useful for quickly seeing which services are affected at each level:
  
```ppl
source=otellogs
| stats values(`resource.attributes.service.name`) as services by severityText
| sort severityText
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+------------------------------------------------------------------+--------------+
| services                                                         | severityText |
|------------------------------------------------------------------+--------------|
| [cart,product-catalog]                                           | DEBUG        |
| [checkout,frontend-proxy,payment,product-catalog,recommendation] | ERROR        |
| [cart,checkout,frontend]                                         | INFO         |
| [frontend-proxy,product-catalog]                                 | WARN         |
+------------------------------------------------------------------+--------------+
```
  

## Example 9: Calculate the percentile of a field  

The following query calculates the 90th percentile of severity numbers, helping you understand the severity distribution:
  
```ppl
source=otellogs
| stats percentile(severityNumber, 90) as p90_severity
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------+
| p90_severity |
|--------------|
| 17           |
+--------------+
```
  

## Example 10: Collect unique values using VALUES  

The following query collects all unique severity levels present in the logs:
  
```ppl
source=otellogs
| stats values(severityText) as severity_levels
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------------+
| severity_levels         |
|-------------------------|
| [DEBUG,ERROR,INFO,WARN] |
+-------------------------+
```
  

## Example 11: Ignore a null bucket

The following query excludes null values from grouping by setting `bucket_nullable=false`, useful when you only want to see services that have a defined namespace:

```ppl
source=otellogs
| stats bucket_nullable=false count() as cnt by instrumentationScope.name
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-----+-----------------------------------------------------------------------------+
| cnt | instrumentationScope.name                                                   |
|-----+-----------------------------------------------------------------------------|
| 2   | @opentelemetry/instrumentation-http                                         |
| 1   | Microsoft.Extensions.Hosting                                                |
| 1   | go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc |
+-----+-----------------------------------------------------------------------------+
```
  

## Example 12: Date span grouping with null handling  

The following example uses this sample index data:

```text
+-------+--------+------------+
| Name  | DEfetched rows / total rows = 3/3
+-----+---------------------------+
| cnt | instrumentationScope.name |
|-----+---------------------------|
| 1   | opentelemetry-dotnet      |
| 1   | opentelemetry-go          |
| 2   | opentelemetry-js          |
+-----+---------------------------+TNO | birthday   |
|-------+--------+------------|
| Alice | 1      | 2024-04-21 |
| Bob   | 2      | 2025-08-21 |
| Jeff  | null   | 2025-04-22 |
| Adam  | 2      | null       |
+-------+--------+------------+
```

The following query groups data by yearly spans of the `birthday` field, automatically excluding null values:

```ppl ignore
source=example
| stats count() as cnt by span(birthday, 1y) as year
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-----+------------+
| cnt | year       |
|-----+------------|
| 1   | 2024-01-01 |
| 2   | 2025-01-01 |
+-----+------------+
```

Group by both yearly spans and department number (by default, null `DEfetched rows / total rows = 1/1
+-------------------------------+
| severity_levels               |
|-------------------------------|
| [DEBUG,ERROR,ERROR,INFO,WARN] |
+-------------------------------+TNO` values are included in the results):

```ppl ignore
source=example
| stats count() as cnt by span(birthday, 1y) as year, DEfetched rows / total rows = 5/5
+-----+-----------+--------------+
| cnt | sev_range | severityText |
|-----+-----------+--------------|
| 3   | 0         | DEBUG        |
| 6   | 0         | INFO         |
| 5   | 10        | ERROR        |
| 4   | 10        | WARN         |
| 2   | 20        | ERROR        |
+-----+-----------+--------------+TNO
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-----+------------+--------+
| cnt | year       | DEfetched rows / total rows = 5/5
+--------------------------------------------------+--------------+
| services                                         | severityText |
|--------------------------------------------------+--------------|
| [cart,product-catalog,cart]                      | DEBUG        |
| [payment,checkout,frontend-proxy]                | ERROR        |
| [payment,product-catalog]                        | ERROR        |
| [frontend,cart,frontend]                         | INFO         |
| [product-catalog,product-catalog,frontend-proxy] | WARN         |
+--------------------------------------------------+--------------+TNO |
|-----+------------+--------|
| 1   | 2024-01-01 | 1      |
| 1   | 2025-01-01 | 2      |
| 1   | 2025-01-01 | null   |
+-----+------------+--------+
```

Use `bucket_nullable=false` to exclude null `DEfetched rows / total rows = 1/1
+-----+
| p90 |
|-----|
| 21  |
+-----+TNO` values from the grouping:

```ppl ignore
source=example
| stats bucket_nullable=false count() as cnt by span(birthday, 1y) as year, DEfetched rows / total rows = 5/5
+--------------------------------+--------------+
| percentile(severityNumber, 90) | severityText |
|--------------------------------+--------------|
| 5                              | DEBUG        |
| 17                             | ERROR        |
| 21                             | ERROR        |
| 9                              | INFO         |
| 13                             | WARN         |
+--------------------------------+--------------+TNO
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-----+------------+--------+
| cnt | year       | DEfetched rows / total rows = 5/5
+-----+-----------+--------------+
| p90 | sev_range | severityText |
|-----+-----------+--------------|
| 5   | 0         | DEBUG        |
| 9   | 0         | INFO         |
| 17  | 10        | ERROR        |
| 13  | 10        | WARN         |
| 21  | 20        | ERROR        |
+-----+-----------+--------------+TNO |
|-----+------------+--------|
| 1   | 2024-01-01 | 1      |
| 1   | 2025-01-01 | 2      |
+-----+------------+--------+
```
  

## Example 13: Calculate the count by the implicit @timestamp field  

If you omit the `field` parameter in the `span` function, it automatically uses the implicit `@timestamp` field:
  
```ppl ignore
source=big5
| stats count() by span(1month)
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------+---------------------+
| count() | span(1month)        |
|---------+---------------------|
| 1       | 2023-01-01 00:00:00 |
+---------+---------------------+
```

## Limitations

The following limitations apply to the `stats` command.

### Bucket aggregation results may be approximate for high-cardinality fields

In OpenSearch, `doc_count` values for a `terms` bucket aggregation can be approximate. Thus, any aggregations (such as `sum` or `avg`) performed on those buckets may also be approximate.

For example, the following query retrieves the top 10 URLs:

```ppl ignore
source=hits
| stats bucket_nullable=false count() as c by URL
| sort - c
| head 10
```

This query is translated into a `terms` aggregation in OpenSearch with `"order": { "_count": "desc" }`. For fields with high cardinality, some buckets may be discarded, so the results may only be approximate.

### Sorting by doc_count in ascending order may produce inaccurate results

When retrieving the least frequent terms for high-cardinality fields, results may be inaccurate. Shard-level aggregations can miss globally rare terms or misrepresent their frequency, causing errors in the overall results.

For example, the following query retrieves the 10 least frequent URLs:

```ppl ignore
source=hits
| stats bucket_nullable=false count() as c by URL
| sort + c
| head 10
```

A globally rare term might not appear as rare on every shard or could be entirely absent from some shard results. Conversely, a term that is infrequent on one shard might be common on another. In both cases, shard-level approximations can cause rare terms to be missed, leading to inaccurate overall results.
