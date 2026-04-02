
# timechart

The `timechart` command creates a time-based aggregation of data. It groups data by time intervals and, optionally, by a field, and then applies an aggregation function to each group. The results are returned in an unpivoted format, with separate rows for each time-field combination.

## Syntax

The `timechart` command has the following syntax:

```syntax
timechart [timefield=<field_name>] [span=<time_interval>] [limit=<number>] [useother=<boolean>] [usenull=<boolean>] [nullstr=<string>] <aggregation_function> [by <field>]
```

## Parameters

The `timechart` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `timefield` | Optional | The field to use for time-based grouping. Must be a timestamp field. Default is `@timestamp`. |
| `span` | Optional | Specifies the time interval for grouping data. Default is `1m` (1 minute). For a complete list of supported time units, see [Time units](#time-units). |
| `limit` | Optional | Specifies the maximum number of distinct values to display when using the `by` clause. Default is `10`. When there are more distinct values than the limit, additional values are grouped into an `OTHER` category if `useother` is not set to `false`. The "most distinct" values are determined by calculating the sum of aggregation values across all time intervals. Set to `0` to show all distinct values without any limit (when `limit=0`, `useother` is automatically set to `false`). Only applies when using the `by` clause. |
| `useother` | Optional | Controls whether to create an `OTHER` category for values beyond the `limit`. When set to `false`, only the top N values (based on `limit`) are shown without an `OTHER` category. When set to `true`, values beyond the `limit` are grouped into an `OTHER` category. This parameter only applies when using the `by` clause and when there are more values than the `limit`. Default is `true`. |
| `usenull` | Optional | Controls whether to group documents that have null values in the `by` field into a separate `NULL` category. When `usenull=false`, documents with null values in the `by` field are excluded from the results. When `usenull=true`, documents with null values in the `by` field are grouped into a separate `NULL` category. Default is `true`. |
| `nullstr` | Optional | Specifies the category name for documents that have null values in the `by` field. This parameter only applies when `usenull` is `true`. Default is `"NULL"`. |
| `<aggregation_function>` | Required | The aggregation function to apply to each time bucket. Only a single aggregation function is supported. Available functions: All aggregation functions supported by the [stats](stats.md) command as well as the timechart-specific aggregations. |
| `by` | Optional | Groups the results by the specified field in addition to time intervals. If not specified, the aggregation is performed across all documents in each time interval. |

## Notes

The following considerations apply when using the `timechart` command:

* The `timechart` command requires a timestamp field in the data. By default, it uses the `@timestamp` field, but you can specify a different field using the `timefield` parameter.  
* Results are returned in an unpivoted format with separate rows for each time-field combination that has data.  
* Only combinations with actual data are included in the results---empty combinations are omitted rather than showing null or zero values.  
* The top N values for the `limit` parameter are selected based on the sum of values across all time intervals for each distinct field value.  
* When using the `limit` parameter, values beyond the limit are grouped into an `OTHER` category (unless `useother=false`).   
* Documents with null values in the `by` field are treated as a separate category and appear as null in the results.  

### Time units

The following time units are available for the `span` parameter:

* Milliseconds (`ms`)
* Seconds (`s`)
* Minutes (`m`, case sensitive)
* Hours (`h`)
* Days (`d`)
* Weeks (`w`)
* Months (`M`, case sensitive)
* Quarters (`q`)
* Years (`y`)

## Timechart-specific aggregation functions

The `timechart` command provides specialized rate-based aggregation functions that calculate values per unit of time.

### per_second

**Usage**: `per_second(field)` calculates the per-second rate for a numeric field within each time bucket.

**Calculation formula**: `per_second(field) = sum(field) / span_in_seconds`, where `span_in_seconds` is the span interval in seconds.

**Return type**: DOUBLE

### per_minute

**Usage**: `per_minute(field)` calculates the per-minute rate for a numeric field within each time bucket.

**Calculation formula**: `per_minute(field) = sum(field) * 60 / span_in_seconds`, where `span_in_seconds` is the span interval in seconds.

**Return type**: DOUBLE

### per_hour

**Usage**: `per_hour(field)` calculates the per-hour rate for a numeric field within each time bucket.

**Calculation formula**: `per_hour(field) = sum(field) * 3600 / span_in_seconds`, where `span_in_seconds` is the span interval in seconds.

**Return type**: DOUBLE

### per_day

**Usage**: `per_day(field)` calculates the per-day rate for a numeric field within each time bucket.

**Calculation formula**: `per_day(field) = sum(field) * 86400 / span_in_seconds`, where `span_in_seconds` is the span interval in seconds.

**Return type**: DOUBLE
  
## Example 1: Log volume per 5 minutes

The following query counts all log events in 5-minute windows to monitor overall system activity:

```ppl
source=otellogs
| timechart timefield=@timestamp span=5m count()
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+---------------------+---------+
| @timestamp          | count() |
|---------------------+---------|
| 2024-02-01 09:10:00 | 5       |
| 2024-02-01 09:15:00 | 5       |
| 2024-02-01 09:20:00 | 5       |
| 2024-02-01 09:25:00 | 5       |
+---------------------+---------+
```
  

## Example 2: Error rate over time by service

The following query counts only error logs per service in 10-minute windows to track service health:

```ppl
source=otellogs
| where severityText = 'ERROR'
| timechart timefield=@timestamp span=10m count() by `resource.attributes.service.name`
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+---------------------+----------------------------------+---------+
| @timestamp          | resource.attributes.service.name | count() |
|---------------------+----------------------------------+---------|
| 2024-02-01 09:10:00 | checkout                         | 1       |
| 2024-02-01 09:10:00 | payment                          | 1       |
| 2024-02-01 09:20:00 | checkout                         | 1       |
| 2024-02-01 09:20:00 | frontend-proxy                   | 1       |
| 2024-02-01 09:20:00 | recommendation                   | 1       |
+---------------------+----------------------------------+---------+
```
  

## Example 3: Top 3 services with the rest grouped as OTHER

The following query limits the breakdown to the top 3 services by log volume, grouping remaining services into an OTHER category:

```ppl
source=otellogs
| timechart timefield=@timestamp span=15m limit=3 count() by `resource.attributes.service.name`
```

The query returns the following results:

```text
fetched rows / total rows = 8/8
+---------------------+----------------------------------+---------+
| @timestamp          | resource.attributes.service.name | count() |
|---------------------+----------------------------------+---------|
| 2024-02-01 09:00:00 | OTHER                            | 1       |
| 2024-02-01 09:00:00 | cart                             | 2       |
| 2024-02-01 09:00:00 | frontend                         | 1       |
| 2024-02-01 09:00:00 | product-catalog                  | 1       |
| 2024-02-01 09:15:00 | OTHER                            | 8       |
| 2024-02-01 09:15:00 | cart                             | 1       |
| 2024-02-01 09:15:00 | frontend                         | 3       |
| 2024-02-01 09:15:00 | product-catalog                  | 3       |
+---------------------+----------------------------------+---------+
```
  

## Example 4: Exclude the OTHER category

The following query shows only the top 2 services without an OTHER bucket by setting useother=false:

```ppl
source=otellogs
| timechart timefield=@timestamp span=30m limit=2 useother=false count() by `resource.attributes.service.name`
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+---------------------+----------------------------------+---------+
| @timestamp          | resource.attributes.service.name | count() |
|---------------------+----------------------------------+---------|
| 2024-02-01 09:00:00 | frontend                         | 4       |
| 2024-02-01 09:00:00 | product-catalog                  | 4       |
+---------------------+----------------------------------+---------+
```
  

## Example 5: Per-second error rate by severity

The following query uses the per_second rate function to normalize error counts across different time windows, grouped by severity level:

```ppl
source=otellogs
| where severityNumber >= 13
| timechart timefield=@timestamp span=2m per_second(severityNumber) by severityText
```

The query returns the following results:

```text
fetched rows / total rows = 11/11
+---------------------+--------------+----------------------------+
| @timestamp          | severityText | per_second(severityNumber) |
|---------------------+--------------+----------------------------|
| 2024-02-01 09:12:00 | ERROR        | 0.14166666666666666        |
| 2024-02-01 09:12:00 | WARN         | 0.10833333333333334        |
| 2024-02-01 09:14:00 | ERROR        | 0.14166666666666666        |
| 2024-02-01 09:16:00 | FATAL        | 0.175                      |
| 2024-02-01 09:18:00 | WARN         | 0.10833333333333334        |
| 2024-02-01 09:20:00 | ERROR        | 0.14166666666666666        |
| 2024-02-01 09:22:00 | WARN         | 0.10833333333333334        |
| 2024-02-01 09:24:00 | ERROR        | 0.14166666666666666        |
| 2024-02-01 09:24:00 | FATAL        | 0.175                      |
| 2024-02-01 09:26:00 | WARN         | 0.10833333333333334        |
| 2024-02-01 09:28:00 | ERROR        | 0.14166666666666666        |
+---------------------+--------------+----------------------------+
```
  
## Example 6: Distinct service count over time

The following query tracks how many unique services are actively logging per hour, useful for detecting service outages:

```ppl
source=otellogs
| timechart timefield=@timestamp span=1h distinct_count(`resource.attributes.service.name`)
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+---------------------+----------------------------------------------------+
| @timestamp          | distinct_count(`resource.attributes.service.name`) |
|---------------------+----------------------------------------------------|
| 2024-02-01 09:00:00 | 7                                                  |
+---------------------+----------------------------------------------------+
```
  

## Example 7: Use limit=0 with count() to show all values  

This example uses the `events_many_hosts` dataset, which contains 11 distinct hosts.

To display all distinct values without applying any limit, set `limit=0`:
  
```ppl
source=events_many_hosts
| timechart span=1h limit=0 count() by host
```
  
All 11 hosts are returned as separate rows without an `OTHER` category:
  
```text
fetched rows / total rows = 11/11
+---------------------+--------+---------+
| @timestamp          | host   | count() |
|---------------------+--------+---------|
| 2024-07-01 00:00:00 | web-01 | 1       |
| 2024-07-01 00:00:00 | web-02 | 1       |
| 2024-07-01 00:00:00 | web-03 | 1       |
| 2024-07-01 00:00:00 | web-04 | 1       |
| 2024-07-01 00:00:00 | web-05 | 1       |
| 2024-07-01 00:00:00 | web-06 | 1       |
| 2024-07-01 00:00:00 | web-07 | 1       |
| 2024-07-01 00:00:00 | web-08 | 1       |
| 2024-07-01 00:00:00 | web-09 | 1       |
| 2024-07-01 00:00:00 | web-10 | 1       |
| 2024-07-01 00:00:00 | web-11 | 1       |
+---------------------+--------+---------+
```

## Example 8: Use useother=false with the count() function  

The following query limits the results to the top 10 hosts without creating an `OTHER` category by setting `useother=false`:
  
```ppl
source=events_many_hosts
| timechart span=1h useother=false count() by host
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 10/10
+---------------------+--------+---------+
| @timestamp          | host   | count() |
|---------------------+--------+---------|
| 2024-07-01 00:00:00 | web-01 | 1       |
| 2024-07-01 00:00:00 | web-02 | 1       |
| 2024-07-01 00:00:00 | web-03 | 1       |
| 2024-07-01 00:00:00 | web-04 | 1       |
| 2024-07-01 00:00:00 | web-05 | 1       |
| 2024-07-01 00:00:00 | web-06 | 1       |
| 2024-07-01 00:00:00 | web-07 | 1       |
| 2024-07-01 00:00:00 | web-08 | 1       |
| 2024-07-01 00:00:00 | web-09 | 1       |
| 2024-07-01 00:00:00 | web-10 | 1       |
+---------------------+--------+---------+
```
  

## Example 9: Use the limit parameter with the useother parameter and the avg() function  

The following query displays the top 3 hosts based on average `cpu_usage` per hour. All remaining hosts are grouped into an `OTHER` category (by default, `useother=true`):
  
```ppl
source=events_many_hosts
| timechart span=1h limit=3 avg(cpu_usage) by host
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+---------------------+--------+----------------+
| @timestamp          | host   | avg(cpu_usage) |
|---------------------+--------+----------------|
| 2024-07-01 00:00:00 | OTHER  | 41.3           |
| 2024-07-01 00:00:00 | web-03 | 55.3           |
| 2024-07-01 00:00:00 | web-07 | 48.6           |
| 2024-07-01 00:00:00 | web-09 | 67.8           |
+---------------------+--------+----------------+
```
  
The following query displays the top 3 hosts based on average `cpu_usage` per hour without creating an `OTHER` category by setting `useother=false`:

```ppl
source=events_many_hosts
| timechart span=1h limit=3 useother=false avg(cpu_usage) by host
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+---------------------+--------+----------------+
| @timestamp          | host   | avg(cpu_usage) |
|---------------------+--------+----------------|
| 2024-07-01 00:00:00 | web-03 | 55.3           |
| 2024-07-01 00:00:00 | web-07 | 48.6           |
| 2024-07-01 00:00:00 | web-09 | 67.8           |
+---------------------+--------+----------------+
```
  

## Example 10: Handling null values in the by field

The following query demonstrates how null values in the `by` field are treated as a separate category:

```ppl
source=events_null
| timechart span=1h count() by host
```
  
The `events_null` dataset contains one entry without a `host` value. Because the default settings are `usenull=true` and `nullstr="NULL"`, this entry is grouped into a separate `NULL` category:
  
```text
fetched rows / total rows = 4/4
+---------------------+--------+---------+
| @timestamp          | host   | count() |
|---------------------+--------+---------|
| 2024-07-01 00:00:00 | NULL   | 1       |
| 2024-07-01 00:00:00 | db-01  | 1       |
| 2024-07-01 00:00:00 | web-01 | 2       |
| 2024-07-01 00:00:00 | web-02 | 2       |
+---------------------+--------+---------+
```
  

## Example 11: Calculate the per-second packet rate  

The following query calculates the per-second packet rate for network traffic data using the `per_second()` function:
  
```ppl
source=events
| timechart span=30m per_second(packets) by host
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+---------------------+---------+---------------------+
| @timestamp          | host    | per_second(packets) |
|---------------------+---------+---------------------|
| 2023-01-01 10:00:00 | server1 | 0.1                 |
| 2023-01-01 10:00:00 | server2 | 0.05                |
| 2023-01-01 10:30:00 | server1 | 0.1                 |
| 2023-01-01 10:30:00 | server2 | 0.05                |
+---------------------+---------+---------------------+
```
  

## Limitations

The `timechart` command has the following limitations:

* Only a single aggregation function is supported per `timechart` command.
* The `bins` parameter and other `bin` options are not supported. To control the time intervals, use the `span` parameter.  