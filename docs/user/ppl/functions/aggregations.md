# Aggregation functions  

Aggregation functions perform calculations across multiple rows to return a single result value. These functions are used with the `stats`, `eventstats`, and `streamstats` commands to analyze and summarize data.

The following table shows how `NULL` and missing values are handled by aggregation functions.
  
| Function | null | Missing |
| --- | --- | --- |
| `COUNT` | Not counted | Not counted |
| `SUM` | Ignored | Ignored |
| `AVG` | Ignored | Ignored |
| `MAX` | Ignored | Ignored |
| `MIN` | Ignored | Ignored |
| `FIRST` | Ignored | Ignored |
| `LAST` | Ignored | Ignored |
| `LIST` | Ignored | Ignored |
| `VALUES` | Ignored | Ignored |
  
## Functions

The following aggregation functions are available in PPL for data analysis and summarization.  

### COUNT  

**Usage**: `COUNT(expr)`, `C(expr)`, `c(expr)`, `count(expr)`

Counts the number of `expr` values in the retrieved rows. `C()`, `c()`, and `count()` are available as abbreviations for `COUNT()`. For filtered counting, use an `eval` expression to specify the filtering condition.

**Parameters**:

- `expr` (Optional): The expression whose values are to be counted.

**Return type**: `LONG`

#### Example
  
```ppl
source=accounts
| stats count(), c(), count, c
```

  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------+-----+-------+---+
| count() | c() | count | c |
|---------+-----+-------+---|
| 4       | 4   | 4     | 4 |
+---------+-----+-------+---+
```
  
The following example counts only records that match a specific condition:

```ppl
source=accounts
| stats count(eval(age > 30)) as mature_users
```

  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------+
| mature_users |
|--------------|
| 3            |
+--------------+
```
  
### SUM  

**Usage**: `SUM(expr)`

Returns the sum of `expr` values.

**Parameters**:

- `expr` (Required): The expression whose values are to be summed.

**Return type**: Same as input type (`INTEGER`, `LONG`, `FLOAT`, or `DOUBLE`)

#### Example
  
```ppl
source=accounts
| stats sum(age) by gender
```

  
The query returns the following results:
  
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

**Usage**: `AVG(expr)`

Returns the average value of `expr`.

**Parameters**:

- `expr` (Required): The expression whose values are to be averaged.

**Return type**: `DOUBLE` for numeric inputs; same as input type for `DATE`, `TIME`, or `TIMESTAMP` inputs

#### Example
  
```ppl
source=accounts
| stats avg(age) by gender
```

  
The query returns the following results:
  
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

**Usage**: `MAX(expr)`

Returns the maximum value of `expr`. For non-numeric fields, this function returns the value that comes last in alphabetical order.

**Parameters**:

- `expr` (Required): The expression for which to find the maximum value.

**Return type**: Same as input type

#### Example
  
```ppl
source=accounts
| stats max(age)
```

  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------+
| max(age) |
|----------|
| 36       |
+----------+
```
  
The following example returns the value from the `firstname` text field that comes last in alphabetical order:

```ppl
source=accounts
| stats max(firstname)
```

  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------+
| max(firstname) |
|----------------|
| Nanette        |
+----------------+
```
  
### MIN  

**Usage**: `MIN(expr)`

Returns the minimum value of `expr`. For non-numeric fields, this function returns the value that comes first in alphabetical order.

**Parameters**:

- `expr` (Required): The expression for which to find the minimum value.

**Return type**: Same as input type

#### Example
  
```ppl
source=accounts
| stats min(age)
```

  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------+
| min(age) |
|----------|
| 28       |
+----------+
```
  
The following example returns the value from the `firstname` text field that comes first in alphabetical order:

```ppl
source=accounts
| stats min(firstname)
```

  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------+
| min(firstname) |
|----------------|
| Amber          |
+----------------+
```
  
### VAR_SAMP  

**Usage**: `VAR_SAMP(expr)`

Returns the sample variance of `expr`.

**Parameters**:

- `expr` (Required): The expression for which to calculate the sample variance.

**Return type**: `DOUBLE`

#### Example
  
```ppl
source=accounts
| stats var_samp(age)
```

  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------------+
| var_samp(age)      |
|--------------------|
| 10.916666666666666 |
+--------------------+
```
  
### VAR_POP  

**Usage**: `VAR_POP(expr)`

Returns the population variance of `expr`.

**Parameters**:

- `expr` (Required): The expression for which to calculate the population variance.

**Return type**: `DOUBLE`

#### Example
  
```ppl
source=accounts
| stats var_pop(age)
```

  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------+
| var_pop(age) |
|--------------|
| 8.1875       |
+--------------+
```
  
### STDDEV_SAMP  

**Usage**: `STDDEV_SAMP(expr)`

Returns the sample standard deviation of `expr`.

**Parameters**:

- `expr` (Required): The expression for which to calculate the sample standard deviation.

**Return type**: `DOUBLE`

#### Example
  
```ppl
source=accounts
| stats stddev_samp(age)
```

  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------+
| stddev_samp(age)  |
|-------------------|
| 3.304037933599835 |
+-------------------+
```
  
### STDDEV_POP  

**Usage**: `STDDEV_POP(expr)`

Returns the population standard deviation of `expr`.

**Parameters**:

- `expr` (Required): The expression for which to calculate the population standard deviation.

**Return type**: `DOUBLE`

#### Example
  
```ppl
source=accounts
| stats stddev_pop(age)
```

  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------------+
| stddev_pop(age)    |
|--------------------|
| 2.8613807855648994 |
+--------------------+
```
  
### DISTINCT_COUNT, DC

**Usage**: `DISTINCT_COUNT(expr)`, `DC(expr)`

Returns the approximate number of distinct values using the `HyperLogLog++` algorithm. Both functions are equivalent. For more information about algorithm accuracy and precision control, see [Controlling precision](https://docs.opensearch.org/latest/aggregations/metric/cardinality/#controlling-precision).

**Parameters**:

- `expr` (Required): The expression for which to count distinct values.

**Return type**: `LONG`

#### Example
  
```ppl
source=accounts
| stats dc(state) as distinct_states, distinct_count(state) as dc_states_alt by gender
```

  
The query returns the following results:
  
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

**Usage**: `DISTINCT_COUNT_APPROX(expr)`

Returns the approximate count of distinct values in `expr` using the `HyperLogLog++` algorithm.

**Parameters**:

- `expr` (Required): The expression for which to count approximate distinct values.

**Return type**: `LONG`

#### Example
  
```ppl
source=accounts
| stats distinct_count_approx(gender)
```

  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------------------+
| distinct_count_approx(gender) |
|-------------------------------|
| 2                             |
+-------------------------------+
```
  
### EARLIEST  

**Usage**: `EARLIEST(field [, time_field])`

Returns the earliest value of a `field` based on timestamp ordering.

**Parameters**:

- `field` (Required): The field for which to return the earliest value.
- `time_field` (Optional): The field to use for time-based ordering. Defaults to `@timestamp` if not specified.

**Return type**: Same as input field type

#### Example
  
```ppl
source=events
| stats earliest(message) by host
| sort host
```

  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+-------------------+---------+
| earliest(message) | host    |
|-------------------+---------|
| Starting up       | server1 |
| Initializing      | server2 |
+-------------------+---------+
```
  
The following example uses a custom time field instead of the default `@timestamp` field for ordering:

```ppl
source=events
| stats earliest(status, event_time) by category
| sort category
```

  
The query returns the following results:
  
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

**Usage**: `LATEST(field [, time_field])`

Returns the latest value of a `field` based on timestamp ordering.

**Parameters**:

- `field` (Required): The field for which to return the latest value.
- `time_field` (Optional): The field to use for time-based ordering. Defaults to `@timestamp` if not specified.

**Return type**: Same as input field type

#### Example
  
```ppl
source=events
| stats latest(message) by host
| sort host
```

  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+------------------+---------+
| latest(message)  | host    |
|------------------+---------|
| Shutting down    | server1 |
| Maintenance mode | server2 |
+------------------+---------+
```
  
The following example uses a custom time field instead of the default `@timestamp` field for ordering:

```ppl
source=events
| stats latest(status, event_time) by category
| sort category
```

  
The query returns the following results:
  
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

**Usage**: `TAKE(field [, size])`

Returns the original values from a field. This function does not guarantee the order of the returned values.

**Parameters**:

- `field` (Required): A text field from which to extract values.
- `size` (Optional): The number of values to return. Defaults to `10`.

**Return type**: `ARRAY`

#### Example
  
```ppl
source=accounts
| stats take(firstname)
```

  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------+
| take(firstname)             |
|-----------------------------|
| [Amber,Hattie,Nanette,Dale] |
+-----------------------------+
```
  
### PERCENTILE, PERCENTILE_APPROX

**Usage**: `PERCENTILE(expr, percent)`, `PERCENTILE_APPROX(expr, percent)`

Returns the approximate percentile value of `expr` at the specified percentage.

**Parameters**:

- `expr` (Required): The expression for which to calculate the percentile.
- `percent` (Required): A constant number between `0` and `100`.

**Return type**: Same as input type

Starting in version 3.1.0, the percentile implementation switched from `AVLTreeDigest` to `MergingDigest`. For more information, see the [corresponding issue](https://github.com/opensearch-project/OpenSearch/issues/18122).
{: .note}

#### Example
  
```ppl
source=accounts
| stats percentile(age, 90) by gender
```

  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+---------------------+--------+
| percentile(age, 90) | gender |
|---------------------+--------|
| 28                  | F      |
| 36                  | M      |
+---------------------+--------+
```
  
#### Percentile shortcut functions  

For convenience, OpenSearch PPL provides shortcut functions for common percentiles:
- `PERC<percent>(expr)` - Equivalent to `PERCENTILE(expr, <percent>)`.
- `P<percent>(expr)` - Equivalent to `PERCENTILE(expr, <percent>)`.

Both integer and decimal percentiles from `0` to `100` are supported (for example, `PERC95`, `P99.5`):
  
```ppl
source=accounts 
| stats perc99.5(age);
```

  
The query returns the following results:
  
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

  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------+
| p50(age) |
|----------|
| 33       |
+----------+
```
  
### MEDIAN  

**Usage**: `MEDIAN(expr)`

Returns the median (50th percentile) value of `expr`. This is equivalent to `PERCENTILE(expr, 50)`.

**Parameters**:

- `expr` (Required): The expression for which to calculate the median.

**Return type**: Same as input type

#### Example
  
```ppl
source=accounts
| stats median(age)
```

  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------+
| median(age) |
|-------------|
| 33          |
+-------------+
```
  
### FIRST  

**Usage**: `FIRST(field)`

Returns the first non-null value of a `field` based on natural document order. Returns `NULL` if no records exist or if all records have `NULL` values for the `field`.

**Parameters**:

- `field` (Required): The field for which to return the first value.

**Return type**: Same as input field type

#### Example
  
```ppl
source=accounts
| stats first(firstname) by gender
```

  
The query returns the following results:
  
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

**Usage**: `LAST(field)`

Returns the last non-null value of a `field` based on natural document order. Returns `NULL` if no records exist or if all records have `NULL` values for the `field`.

**Parameters**:

- `field` (Required): The field for which to return the last value.

**Return type**: Same as input field type

#### Example
  
```ppl
source=accounts
| stats last(firstname) by gender
```

  
The query returns the following results:
  
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

**Usage**: `LIST(expr)`

Collects all values from the specified expression into an array. Values are converted to strings, `NULL` values are filtered out, and duplicates are preserved. This function returns up to `100` values without a guaranteed order.

**Parameters**:

- `expr` (Required): The field expression from which to collect values.

**Return type**: `ARRAY`

This aggregation function does not support array, struct, or object field types.
{: .note}

#### Example

The following example collects all values from a string field into an array:
  
```ppl
source=accounts
| stats list(firstname)
```

  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------+
| list(firstname)             |
|-----------------------------|
| [Amber,Hattie,Nanette,Dale] |
+-----------------------------+
```
  
### VALUES  

**Usage**: `VALUES(expr)`

Collects all unique values from the specified expression into a sorted array. Values are converted to strings, `NULL` values are filtered out, and duplicates are removed.

**Parameters**:

- `expr` (Required): The expression from which to collect unique values.

**Return type**: `ARRAY`

> The `plugins.ppl.values.max.limit` setting controls the maximum number of unique values returned:
> - The default value is 0, which returns an unlimited number of values.
> - Setting this to any positive integer limits the number of unique values.
> - See the [PPL Settings](../admin/settings.md#plugins-ppl-values-max-limit) documentation for more details

#### Example

The following example collects unique values from a string field into a sorted array:
  
```ppl
source=accounts
| stats values(firstname)
```

  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------+
| values(firstname)           |
|-----------------------------|
| [Amber,Dale,Hattie,Nanette] |
+-----------------------------+
```
  