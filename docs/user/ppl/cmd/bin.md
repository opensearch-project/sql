
# bin

The `bin` command groups numeric values into buckets of equal intervals, which is useful for creating histograms and analyzing data distribution. It accepts a numeric or time-based field and generates a new field containing values that represent the lower bound of each bucket.

## Syntax

The `bin` command has the following syntax:

```syntax
bin <field> [span=<interval>] [minspan=<interval>] [bins=<count>] [aligntime=(earliest | latest | <time-specifier>)] [start=<value>] [end=<value>]
```

## Parameters

The `bin` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field>` | Required | The field to group into buckets. Accepts numeric or time-based fields. |
| `span` | Optional | The interval size for each bin. Cannot be used with the `bins` or `minspan` parameters. Supports numeric, logarithmic (`log10`, `2log10`), and time intervals. See [Time units](#time-units).|
| `minspan` | Optional | The minimum interval size for automatic span calculation. Cannot be used with the `span` or `bins` parameters. |
| `bins` | Optional | The maximum number of equal-width bins to create. Must be between `2` and `50000` (inclusive). Cannot be used with the `span` or `minspan` parameters. See [The bins parameter for timestamp fields](#the-bins-parameter-for-timestamp-fields).|
| `aligntime` | Optional | Align the bin times for time-based fields. Valid only for time-based discretization. Valid values are `earliest`, `latest`, or a specific time. See [Align options](#align-time-options).|
| `start` | Optional | The starting value of the interval range. Default is the minimum value of the field. |
| `end` | Optional | The ending value of the interval range. Default is the maximum value of the field. |

### The bins parameter for timestamp fields

The `bins` parameter for timestamp fields has the following requirements:

- **Pushdown must be enabled**: Enable pushdown by setting `plugins.calcite.pushdown.enabled` to `true` (enabled by default). If pushdown is disabled, use the `span` parameter instead (for example, `bin @timestamp span=5m`).
- **The timestamp field must be used as an aggregation bucket**: The binned timestamp field must be included in a `stats` aggregation (for example, `source=events | bin @timestamp bins=3 | stats count() by @timestamp`). Using `bins` on timestamp fields outside of aggregation buckets is not supported.


### Time units

The following time units are available for the `span` parameter:

* Microseconds (`us`)
* Milliseconds (`ms`)
* Centiseconds (`cs`)
* Deciseconds (`ds`)
* Seconds (`s`, `sec`, `secs`, `second`, or `seconds`)
* Minutes (`m`, `min`, `mins`, `minute`, or `minutes`)
* Hours (`h`, `hr`, `hrs`, `hour`, or `hours`)
* Days (`d`, `day`, or `days`)
* Months (`M`, `mon`, `month`, or `months`)

### Align time options

The following options are available for the `aligntime` parameter:

* `earliest` -- Align bins to the earliest timestamp in the data.
* `latest` -- Align bins to the latest timestamp in the data.
* `<time-specifier>` -- Align bins to a specific epoch time value or time modifier expression.
  
### Parameter behavior

When multiple parameters are specified, the priority order is: `span` > `minspan` > `bins` > `start`/`end` > default.

### Special parameter types

The `bin` command has the following special handling for certain parameter types:

* Logarithmic spans (for example, `log10` or `2log10`) create logarithmic bin boundaries instead of linear ones.
* Daily or monthly spans automatically align to calendar boundaries and return date strings (`YYYY-MM-DD`) instead of timestamps.
* The `aligntime` parameter applies only to time spans shorter than a day (excluding daily or monthly spans).
* The `start` and `end` parameters expand the range (they never reduce it) and affect bin width calculations.

## Example 1: Basic numeric span  

```ppl
source=accounts
| bin age span=10
| fields age, account_number
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-------+----------------+
| age   | account_number |
|-------+----------------|
| 30-40 | 1              |
| 30-40 | 6              |
| 20-30 | 13             |
+-------+----------------+
```
  

## Example 2: Large numeric span  

```ppl
source=accounts
| bin balance span=25000
| fields balance
| head 2
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+-------------+
| balance     |
|-------------|
| 25000-50000 |
| 0-25000     |
+-------------+
```
  

## Example 3: Logarithmic span (log10)  

```ppl
source=accounts
| bin balance span=log10
| fields balance
| head 2
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+------------------+
| balance          |
|------------------|
| 10000.0-100000.0 |
| 1000.0-10000.0   |
+------------------+
```
  

## Example 4: Logarithmic span with coefficient  

```ppl
source=accounts
| bin balance span=2log10
| fields balance
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+------------------+
| balance          |
|------------------|
| 20000.0-200000.0 |
| 2000.0-20000.0   |
| 20000.0-200000.0 |
+------------------+
```
  

## Example 5: Basic bins parameter  

```ppl
source=time_test
| bin value bins=5
| fields value
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+------------+
| value      |
|------------|
| 8000-9000  |
| 7000-8000  |
| 9000-10000 |
+------------+
```
  

## Example 6: Low bin count  

```ppl
source=accounts
| bin age bins=2
| fields age
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------+
| age   |
|-------|
| 30-40 |
+-------+
```
  

## Example 7: High bin count  

```ppl
source=accounts
| bin age bins=21
| fields age, account_number
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-------+----------------+
| age   | account_number |
|-------+----------------|
| 32-33 | 1              |
| 36-37 | 6              |
| 28-29 | 13             |
+-------+----------------+
```
  

## Example 8: Basic minspan  

```ppl
source=accounts
| bin age minspan=5
| fields age, account_number
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-------+----------------+
| age   | account_number |
|-------+----------------|
| 30-40 | 1              |
| 30-40 | 6              |
| 20-30 | 13             |
+-------+----------------+
```
  

## Example 9: Large minspan  

```ppl
source=accounts
| bin age minspan=101
| fields age
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------+
| age    |
|--------|
| 0-1000 |
+--------+
```
  

## Example 10: Start and end range  

```ppl
source=accounts
| bin age start=0 end=101
| fields age
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------+
| age   |
|-------|
| 0-100 |
+-------+
```
  

## Example 11: Large end range  

```ppl
source=accounts
| bin balance start=0 end=100001
| fields balance
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------+
| balance  |
|----------|
| 0-100000 |
+----------+
```
  

## Example 12: Span with start/end  

```ppl
source=accounts
| bin age span=1 start=25 end=35
| fields age
| head 6
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-------+
| age   |
|-------|
| 32-33 |
| 36-37 |
| 28-29 |
| 33-34 |
+-------+
```
  

## Example 13: Hour span  

```ppl
source=time_test
| bin @timestamp span=1h
| fields @timestamp, value
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+---------------------+-------+
| @timestamp          | value |
|---------------------+-------|
| 2025-07-28 00:00:00 | 8945  |
| 2025-07-28 01:00:00 | 7623  |
| 2025-07-28 02:00:00 | 9187  |
+---------------------+-------+
```
  

## Example 14: Minute span  

```ppl
source=time_test
| bin @timestamp span=45minute
| fields @timestamp, value
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+---------------------+-------+
| @timestamp          | value |
|---------------------+-------|
| 2025-07-28 00:00:00 | 8945  |
| 2025-07-28 01:30:00 | 7623  |
| 2025-07-28 02:15:00 | 9187  |
+---------------------+-------+
```
  

## Example 15: Second span  

```ppl
source=time_test
| bin @timestamp span=30seconds
| fields @timestamp, value
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+---------------------+-------+
| @timestamp          | value |
|---------------------+-------|
| 2025-07-28 00:15:30 | 8945  |
| 2025-07-28 01:42:00 | 7623  |
| 2025-07-28 02:28:30 | 9187  |
+---------------------+-------+
```
  

## Example 16: Daily span  

```ppl
source=time_test
| bin @timestamp span=7day
| fields @timestamp, value
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+---------------------+-------+
| @timestamp          | value |
|---------------------+-------|
| 2025-07-24 00:00:00 | 8945  |
| 2025-07-24 00:00:00 | 7623  |
| 2025-07-24 00:00:00 | 9187  |
+---------------------+-------+
```
  

## Example 17: Aligntime with time modifier  

```ppl
source=time_test
| bin @timestamp span=2h aligntime='@d+3h'
| fields @timestamp, value
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+---------------------+-------+
| @timestamp          | value |
|---------------------+-------|
| 2025-07-27 23:00:00 | 8945  |
| 2025-07-28 01:00:00 | 7623  |
| 2025-07-28 01:00:00 | 9187  |
+---------------------+-------+
```
  

## Example 18: Aligntime with epoch timestamp  

```ppl
source=time_test
| bin @timestamp span=2h aligntime=1500000000
| fields @timestamp, value
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+---------------------+-------+
| @timestamp          | value |
|---------------------+-------|
| 2025-07-27 22:40:00 | 8945  |
| 2025-07-28 00:40:00 | 7623  |
| 2025-07-28 00:40:00 | 9187  |
+---------------------+-------+
```
  

## Example 19: Default behavior (no parameters)  

```ppl
source=accounts
| bin age
| fields age, account_number
| head 3
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-----------+----------------+
| age       | account_number |
|-----------+----------------|
| 32.0-33.0 | 1              |
| 36.0-37.0 | 6              |
| 28.0-29.0 | 13             |
+-----------+----------------+
```
  

## Example 20: Binning with string fields  

<!-- TODO: Enable after fixing https://github.com/opensearch-project/sql/issues/4973 -->
```ppl ignore
source=accounts
| eval age_str = CAST(age AS STRING)
| bin age_str bins=3
| stats count() by age_str
| sort age_str
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+---------+---------+
| count() | age_str |
|---------+---------|
| 1       | 20-30   |
| 3       | 30-40   |
+---------+---------+
```
