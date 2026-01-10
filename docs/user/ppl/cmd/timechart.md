
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
  
## Example 1: Count events by hour  

The following query counts events in each hourly interval and groups the results by `host`:
  
```ppl
source=events
| timechart span=1h count() by host
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+---------------------+---------+---------+
| @timestamp          | host    | count() |
|---------------------+---------+---------|
| 2023-01-01 10:00:00 | server1 | 4       |
| 2023-01-01 10:00:00 | server2 | 4       |
+---------------------+---------+---------+
```
  

## Example 2: Count events by minute  

The following query counts events in each 1-minute interval and groups the results by `host`:
  
```ppl
source=events
| timechart span=1m count() by host
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 8/8
+---------------------+---------+---------+
| @timestamp          | host    | count() |
|---------------------+---------+---------|
| 2023-01-01 10:00:00 | server1 | 1       |
| 2023-01-01 10:05:00 | server2 | 1       |
| 2023-01-01 10:10:00 | server1 | 1       |
| 2023-01-01 10:15:00 | server2 | 1       |
| 2023-01-01 10:20:00 | server1 | 1       |
| 2023-01-01 10:25:00 | server2 | 1       |
| 2023-01-01 10:30:00 | server1 | 1       |
| 2023-01-01 10:35:00 | server2 | 1       |
+---------------------+---------+---------+
```
  

## Example 3: Calculate the average number of packets per minute  

The following query calculates the average number of packets per minute without grouping by any additional field:
  
```ppl
source=events
| timechart span=1m avg(packets)
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 8/8
+---------------------+--------------+
| @timestamp          | avg(packets) |
|---------------------+--------------|
| 2023-01-01 10:00:00 | 60.0         |
| 2023-01-01 10:05:00 | 30.0         |
| 2023-01-01 10:10:00 | 60.0         |
| 2023-01-01 10:15:00 | 30.0         |
| 2023-01-01 10:20:00 | 60.0         |
| 2023-01-01 10:25:00 | 30.0         |
| 2023-01-01 10:30:00 | 180.0        |
| 2023-01-01 10:35:00 | 90.0         |
+---------------------+--------------+
```
  

## Example 4: Calculate the average number of packets per 20 minutes and status  

The following query calculates the average number of packets in each 20-minute interval and groups the results by `status`:
  
```ppl
source=events
| timechart span=20m avg(packets) by status
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 8/8
+---------------------+------------+--------------+
| @timestamp          | status     | avg(packets) |
|---------------------+------------+--------------|
| 2023-01-01 10:00:00 | active     | 30.0         |
| 2023-01-01 10:00:00 | inactive   | 30.0         |
| 2023-01-01 10:00:00 | pending    | 60.0         |
| 2023-01-01 10:00:00 | processing | 60.0         |
| 2023-01-01 10:20:00 | cancelled  | 180.0        |
| 2023-01-01 10:20:00 | completed  | 60.0         |
| 2023-01-01 10:20:00 | inactive   | 90.0         |
| 2023-01-01 10:20:00 | pending    | 30.0         |
+---------------------+------------+--------------+
```
  

## Example 5: Count events by hour and category  

The following query counts events in each 1-second interval and groups the results by `category`:
  
```ppl
source=events
| timechart span=1h count() by category
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+---------------------+----------+---------+
| @timestamp          | category | count() |
|---------------------+----------+---------|
| 2023-01-01 10:00:00 | orders   | 4       |
| 2023-01-01 10:00:00 | users    | 4       |
+---------------------+----------+---------+
```
  
## Example 6: Using the limit parameter with count()

This example uses the `events` dataset with fewer hosts for simplicity.

When there are many distinct values in the `by` field, the `timechart` command displays only the top values according to the `limit` parameter and groups the remaining values into an `OTHER` category.

The following query displays the top `2` hosts with the highest event counts and groups all remaining hosts into an `OTHER` category:

```ppl
source=events
| timechart span=1m limit=2 count() by host
```

The query returns the following results:
  
```text
fetched rows / total rows = 8/8
+---------------------+---------+---------+
| @timestamp          | host    | count() |
|---------------------+---------+---------|
| 2023-01-01 10:00:00 | server1 | 1       |
| 2023-01-01 10:05:00 | server2 | 1       |
| 2023-01-01 10:10:00 | server1 | 1       |
| 2023-01-01 10:15:00 | server2 | 1       |
| 2023-01-01 10:20:00 | server1 | 1       |
| 2023-01-01 10:25:00 | server2 | 1       |
| 2023-01-01 10:30:00 | server1 | 1       |
| 2023-01-01 10:35:00 | server2 | 1       |
+---------------------+---------+---------+
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