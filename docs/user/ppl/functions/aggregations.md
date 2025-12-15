# Aggregation Functions  

## Description  

Aggregation functions perform calculations across multiple rows to return a single result value. These functions are used with `stats`, `eventstats` and `streamstats` commands to analyze and summarize data.
The following table shows how NULL/MISSING values are handled by aggregation functions:
  
| Function | NULL | MISSING |
| --- | --- | --- |
| COUNT | Not counted | Not counted |
| SUM | Ignore | Ignore |
| AVG | Ignore | Ignore |
| MAX | Ignore | Ignore |
| MIN | Ignore | Ignore |
| FIRST | Ignore | Ignore |
| LAST | Ignore | Ignore |
| LIST | Ignore | Ignore |
| VALUES | Ignore | Ignore |
  
## Functions  

### COUNT  

#### Description  

Usage: Returns a count of the number of expr in the rows retrieved. The `C()` function, `c`, and `count` can be used as abbreviations for `COUNT()`. To perform a filtered counting, wrap the condition to satisfy in an `eval` expression.
### Example
  
```ppl
source=accounts
| stats count(), c(), count, c
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+---------+-----+-------+---+
| count() | c() | count | c |
|---------+-----+-------+---|
| 4       | 4   | 4     | 4 |
+---------+-----+-------+---+
```
  
Example of filtered counting
  
```ppl
source=accounts
| stats count(eval(age > 30)) as mature_users
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------------+
| mature_users |
|--------------|
| 3            |
+--------------+
```
  
### SUM  

#### Description  

Usage: `SUM(expr)`. Returns the sum of expr.
### Example
  
```ppl
source=accounts
| stats sum(age) by gender
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+----------+--------+
| sum(age) | gender |
|----------+--------|
| 28       | F      |
| 101      | M      |
+----------+--------+
```
  
### AVG  

#### Description  

Usage: `AVG(expr)`. Returns the average value of expr.
### Example
  
```ppl
source=accounts
| stats avg(age) by gender
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+--------------------+--------+
| avg(age)           | gender |
|--------------------+--------|
| 28.0               | F      |
| 33.666666666666664 | M      |
+--------------------+--------+
```
  
### MAX  

#### Description  

Usage: `MAX(expr)`. Returns the maximum value of expr.
For non-numeric fields, values are sorted lexicographically.
### Example
  
```ppl
source=accounts
| stats max(age)
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------+
| max(age) |
|----------|
| 36       |
+----------+
```
  
Example with text field
  
```ppl
source=accounts
| stats max(firstname)
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------------+
| max(firstname) |
|----------------|
| Nanette        |
+----------------+
```
  
### MIN  

#### Description  

Usage: `MIN(expr)`. Returns the minimum value of expr.
For non-numeric fields, values are sorted lexicographically.
### Example
  
```ppl
source=accounts
| stats min(age)
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------+
| min(age) |
|----------|
| 28       |
+----------+
```
  
Example with text field
  
```ppl
source=accounts
| stats min(firstname)
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------------+
| min(firstname) |
|----------------|
| Amber          |
+----------------+
```
  
### VAR_SAMP  

#### Description  

Usage: `VAR_SAMP(expr)`. Returns the sample variance of expr.
### Example
  
```ppl
source=accounts
| stats var_samp(age)
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------------------+
| var_samp(age)      |
|--------------------|
| 10.916666666666666 |
+--------------------+
```
  
### VAR_POP  

#### Description  

Usage: `VAR_POP(expr)`. Returns the population standard variance of expr.
### Example
  
```ppl
source=accounts
| stats var_pop(age)
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------------+
| var_pop(age) |
|--------------|
| 8.1875       |
+--------------+
```
  
### STDDEV_SAMP  

#### Description  

Usage: `STDDEV_SAMP(expr)`. Return the sample standard deviation of expr.
### Example
  
```ppl
source=accounts
| stats stddev_samp(age)
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-------------------+
| stddev_samp(age)  |
|-------------------|
| 3.304037933599835 |
+-------------------+
```
  
### STDDEV_POP  

#### Description  

Usage: `STDDEV_POP(expr)`. Return the population standard deviation of expr.
### Example
  
```ppl
source=accounts
| stats stddev_pop(age)
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------------------+
| stddev_pop(age)    |
|--------------------|
| 2.8613807855648994 |
+--------------------+
```
  
### DISTINCT_COUNT, DC  

#### Description  

Usage: `DISTINCT_COUNT(expr)`, `DC(expr)`. Returns the approximate number of distinct values using the HyperLogLog++ algorithm. Both functions are equivalent.
For details on algorithm accuracy and precision control, see the [OpenSearch Cardinality Aggregation documentation](https://docs.opensearch.org/latest/aggregations/metric/cardinality/#controlling-precision).
### Example
  
```ppl
source=accounts
| stats dc(state) as distinct_states, distinct_count(state) as dc_states_alt by gender
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+-----------------+---------------+--------+
| distinct_states | dc_states_alt | gender |
|-----------------+---------------+--------|
| 1               | 1             | F      |
| 3               | 3             | M      |
+-----------------+---------------+--------+
```
  
### DISTINCT_COUNT_APPROX  

#### Description  

Usage: `DISTINCT_COUNT_APPROX(expr)`. Return the approximate distinct count value of the expr, using the hyperloglog++ algorithm.
### Example
  
```ppl
source=accounts
| stats distinct_count_approx(gender)
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-------------------------------+
| distinct_count_approx(gender) |
|-------------------------------|
| 2                             |
+-------------------------------+
```
  
### EARLIEST  

#### Description  

Usage: `EARLIEST(field [, time_field])`. Return the earliest value of a field based on timestamp ordering.
* `field`: mandatory. The field to return the earliest value for.  
* `time_field`: optional. The field to use for time-based ordering. Defaults to @timestamp if not specified.  
  
### Example
  
```ppl
source=events
| stats earliest(message) by host
| sort host
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+-------------------+---------+
| earliest(message) | host    |
|-------------------+---------|
| Starting up       | server1 |
| Initializing      | server2 |
+-------------------+---------+
```
  
Example with custom time field
  
```ppl
source=events
| stats earliest(status, event_time) by category
| sort category
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+------------------------------+----------+
| earliest(status, event_time) | category |
|------------------------------+----------|
| pending                      | orders   |
| active                       | users    |
+------------------------------+----------+
```
  
### LATEST  

#### Description  

Usage: `LATEST(field [, time_field])`. Return the latest value of a field based on timestamp ordering.
* `field`: mandatory. The field to return the latest value for.  
* `time_field`: optional. The field to use for time-based ordering. Defaults to @timestamp if not specified.  
  
### Example
  
```ppl
source=events
| stats latest(message) by host
| sort host
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+------------------+---------+
| latest(message)  | host    |
|------------------+---------|
| Shutting down    | server1 |
| Maintenance mode | server2 |
+------------------+---------+
```
  
Example with custom time field
  
```ppl
source=events
| stats latest(status, event_time) by category
| sort category
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+----------------------------+----------+
| latest(status, event_time) | category |
|----------------------------+----------|
| cancelled                  | orders   |
| inactive                   | users    |
+----------------------------+----------+
```
  
### TAKE  

#### Description  

Usage: `TAKE(field [, size])`. Return original values of a field. It does not guarantee on the order of values.
* `field`: mandatory. The field must be a text field.  
* `size`: optional integer. The number of values should be returned. Default is 10.  
  
### Example
  
```ppl
source=accounts
| stats take(firstname)
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----------------------------+
| take(firstname)             |
|-----------------------------|
| [Amber,Hattie,Nanette,Dale] |
+-----------------------------+
```
  
### PERCENTILE or PERCENTILE_APPROX  

#### Description  

Usage: `PERCENTILE(expr, percent)` or `PERCENTILE_APPROX(expr, percent)`. Return the approximate percentile value of expr at the specified percentage.
* `percent`: The number must be a constant between 0 and 100.  
  
Note: From 3.1.0, the percentile implementation is switched to MergingDigest from AVLTreeDigest. Ref [issue link](https://github.com/opensearch-project/OpenSearch/issues/18122).
### Example
  
```ppl
source=accounts
| stats percentile(age, 90) by gender
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+---------------------+--------+
| percentile(age, 90) | gender |
|---------------------+--------|
| 28                  | F      |
| 36                  | M      |
+---------------------+--------+
```
  
#### Percentile Shortcut Functions  

For convenience, OpenSearch PPL provides shortcut functions for common percentiles:
- `PERC<percent>(expr)` - Equivalent to `PERCENTILE(expr, <percent>)`  
- `P<percent>(expr)` - Equivalent to `PERCENTILE(expr, <percent>)`  
  
Both integer and decimal percentiles from 0 to 100 are supported (e.g., `PERC95`, `P99.5`).
  
```ppl
source=accounts 
| stats perc99.5(age);
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+---------------+
| perc99.5(age) |
|---------------|
| 36            |
+---------------+
```
  
```ppl
source=accounts 
| stats p50(age);
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------+
| p50(age) |
|----------|
| 33       |
+----------+
```
  
### MEDIAN  

#### Description  

Usage: `MEDIAN(expr)`. Returns the median (50th percentile) value of `expr`. This is equivalent to `PERCENTILE(expr, 50)`.
### Example
  
```ppl
source=accounts
| stats median(age)
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-------------+
| median(age) |
|-------------|
| 33          |
+-------------+
```
  
### FIRST  

#### Description  

Usage: `FIRST(field)`. Return the first non-null value of a field based on natural document order. Returns NULL if no records exist, or if all records have NULL values for the field.
* `field`: mandatory. The field to return the first value for.  
  
### Example
  
```ppl
source=accounts
| stats first(firstname) by gender
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+------------------+--------+
| first(firstname) | gender |
|------------------+--------|
| Nanette          | F      |
| Amber            | M      |
+------------------+--------+
```
  
### LAST  

#### Description  

Usage: `LAST(field)`. Return the last non-null value of a field based on natural document order. Returns NULL if no records exist, or if all records have NULL values for the field.
* `field`: mandatory. The field to return the last value for.  
  
### Example
  
```ppl
source=accounts
| stats last(firstname) by gender
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+-----------------+--------+
| last(firstname) | gender |
|-----------------+--------|
| Nanette         | F      |
| Dale            | M      |
+-----------------+--------+
```
  
### LIST  

#### Description  

Usage: `LIST(expr)`. Collects all values from the specified expression into an array. Values are converted to strings, nulls are filtered, and duplicates are preserved.
The function returns up to 100 values with no guaranteed ordering.
* `expr`: The field expression to collect values from.  
* This aggregation function doesn't support Array, Struct, Object field types.  
  
Example with string fields
  
```ppl
source=accounts
| stats list(firstname)
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----------------------------+
| list(firstname)             |
|-----------------------------|
| [Amber,Hattie,Nanette,Dale] |
+-----------------------------+
```
  
### VALUES  

#### Description  

Usage: `VALUES(expr)`. Collects all unique values from the specified expression into a sorted array. Values are converted to strings, nulls are filtered, and duplicates are removed.
The maximum number of unique values returned is controlled by the `plugins.ppl.values.max.limit` setting:
* Default value is 0, which means unlimited values are returned  
* Can be configured to any positive integer to limit the number of unique values  
* See the [PPL Settings](../admin/settings.md#plugins-ppl-values-max-limit) documentation for more details  
  
Example with string fields
  
```ppl
source=accounts
| stats values(firstname)
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----------------------------+
| values(firstname)           |
|-----------------------------|
| [Amber,Dale,Hattie,Nanette] |
+-----------------------------+
```
  