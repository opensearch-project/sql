# convert

The `convert` command uses conversion functions to transform field values into numeric values. Original field values are overwritten unless the `AS` clause is used to create new fields with the converted values.

The `convert` command has the following properties:

- All conversion functions return `null` if a value cannot be converted to a number.
- All numeric conversion functions return double-precision values to support aggregations.
- Converted values are displayed using decimal notation (for example, `1234.0` or `1234.56`).

Use the `AS` clause to preserve the original field while creating a converted field. You can apply multiple conversions within a single command (see [Example 4](#example-4-converting-multiple-fields)).

## Syntax

The `convert` command has the following syntax:

```syntax
convert [timeformat=<string>] <convert-function>(<field>) [AS <field>] [, <convert-function>(<field>) [AS <field>]]...
```

## Parameters

The `convert` command supports the following parameters.

| Parameter | Required/Optional | Description | Default |
| --- | --- | --- | --- |
| `<convert-function>` | Required | One of the conversion functions: `auto()`, `ctime()`, `dur2sec()`, `memk()`, `mktime()`, `mstime()`, `none()`, `num()`, `rmcomma()`, or `rmunit()`. | N/A |
| `<field>` | Required | Single field name to convert. | N/A |
| `AS <field>` | Optional | Creates a new field using the converted value and preserves the original field. | N/A |
| `timeformat=<string>` | Optional | A strftime format string used by `ctime()` and `mktime()`. | `%m/%d/%Y %H:%M:%S`. |

## Conversion Functions

| Function | Description |
| --- | --- |
| `auto(field)` | Automatically converts fields to numbers using intelligent conversion. Supports units, including memory unit prefixes such as `k`, `m`, or `g`, commas, and scientific notation. Returns `null` for non-convertible values. |
| `ctime(field)` | Converts a UNIX epoch timestamp to a human-readable time string. Uses the `timeformat` parameter if specified, otherwise defaults to `%m/%d/%Y %H:%M:%S`. All timestamps are interpreted in UTC timezone. |
| `dur2sec(field)` | Converts a duration string in `HH:MM:SS` format to total seconds. Hours must be less than 24. Returns `null` for invalid formats. |
| `memk(field)` | Converts values containing memory unit suffixes to kilobytes. Accepts numbers containing optional unit suffixes such as `k`, `m`, or `g` (case insensitive). If the input is a numeric string with no unit suffix, the value is assumed to be in kilobytes. Returns `null` for invalid formats. |
| `mktime(field)` | Converts a human-readable time string to a UNIX epoch timestamp. Uses the `timeformat` parameter if specified, otherwise defaults to `%m/%d/%Y %H:%M:%S`. Input strings are interpreted as UTC timezone. |
| `mstime(field)` | Converts a time string in `[MM:]SS.SSS` format to total seconds. The minutes portion is optional. Returns `null` for invalid formats. |
| `none(field)` | A no-op function that preserves the original field value. |
| `num(field)` | Extracts leading numeric portion of a string. For strings without letters, commas are interpreted as thousands separators and removed. For strings containing letters, extraction stops at the first occurrence of a letter or comma. Returns `null` for non-convertible values. |
| `rmcomma(field)` | Removes commas (thousands separators) from numeric strings and converts the result to a number. Returns `null` if the value contains letters. |
| `rmunit(field)` | Extracts the leading numeric portion of a string. Stops at the first letter or comma. Returns `null` for non-convertible values. |

## Example 1: Converting a field automatically

The following query converts the `balance` field to a number using the `auto()` function:

```ppl
source=accounts
| convert auto(balance)
| fields account_number, balance
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+----------------+---------+
| account_number | balance |
|----------------+---------|
| 1              | 39225.0 |
| 6              | 5686.0  |
| 13             | 32838.0 |
+----------------+---------+
```

## Example 2: Converting a field containing commas

The following query converts a field containing comma-separated numbers:

```ppl
source=accounts
| eval price='1,234'
| convert num(price)
| fields price
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+---------+
| price   |
|---------|
| 1234.0  |
+---------+
```

## Example 3: Converting a field containing memory units

The following query converts memory size strings to kilobytes:

```ppl
source=system_metrics
| eval memory='100m'
| convert memk(memory)
| fields memory
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+----------+
| memory   |
|----------|
| 102400.0 |
+----------+
```

## Example 4: Converting multiple fields

The following query converts multiple fields using different conversion functions:

```ppl
source=accounts
| convert auto(balance), num(age)
| fields account_number, balance, age
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+----------------+---------+------+
| account_number | balance | age  |
|----------------+---------+------|
| 1              | 39225.0 | 32.0 |
| 6              | 5686.0  | 36.0 |
| 13             | 32838.0 | 28.0 |
+----------------+---------+------+
```

## Example 5: Using an AS clause to preserve original values

The following query creates a new field that contains the converted value while preserving the original field:

```ppl
source=accounts
| convert auto(balance) AS balance_num
| fields account_number, balance, balance_num
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+----------------+---------+-------------+
| account_number | balance | balance_num |
|----------------+---------+-------------|
| 1              | 39225   | 39225.0     |
| 6              | 5686    | 5686.0      |
| 13             | 32838   | 32838.0     |
+----------------+---------+-------------+
```

## Example 6: Extracting numbers from strings containing units

The following query extracts numeric values from strings containing units:

```ppl
source=accounts
| head 1
| eval duration='2.000 sec'
| convert rmunit(duration)
| fields duration
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+----------+
| duration |
|----------|
| 2.0      |
+----------+
```

## Example 7: Using aggregation functions

The following query converts values and uses them in aggregations:

```ppl
source=accounts
| convert auto(age)
| stats sum(age) by gender
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+----------+--------+
| sum(age) | gender |
|----------+--------|
| 28.0     | F      |
| 101.0    | M      |
+----------+--------+
```

## Example 8: Using none() to preserve field values

The `none()` function returns the unchanged field value. This is useful for explicitly preserving fields in multi-field conversions:

```ppl
source=accounts
| convert auto(balance), num(age), none(account_number)
| fields account_number, balance, age
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+----------------+---------+------+
| account_number | balance | age  |
|----------------+---------+------|
| 1              | 39225.0 | 32.0 |
| 6              | 5686.0  | 36.0 |
| 13             | 32838.0 | 28.0 |
+----------------+---------+------+
```

### Using none() with an AS clause for field renaming

The `none()` function can be combined with the `AS` clause to rename a field without modifying its value:

```ppl
source=accounts
| convert none(account_number) AS account_id
| fields account_id, firstname, lastname
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+------------+-----------+----------+
| account_id | firstname | lastname |
|------------+-----------+----------|
| 1          | Amber     | Duke     |
| 6          | Hattie    | Bond     |
| 13         | Nanette   | Bates    |
+------------+-----------+----------+
```

>**Note**: The `none()` function is useful with wildcard support, allowing you to exclude specific fields from bulk conversions.

## Example 9: Converting epoch timestamp to time string with ctime()

The following query converts a UNIX epoch timestamp to a human-readable time string using the ctime() function with the default format `%m/%d/%Y %H:%M:%S`:

```ppl
source=accounts
| eval timestamp = 1066507633
| convert ctime(timestamp)
| fields timestamp
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+---------------------+
| timestamp           |
|---------------------|
| 10/18/2003 20:07:13 |
+---------------------+
```

## Example 10: Converting time string to epoch with mktime()

The following query converts a human-readable time string to a UNIX epoch timestamp using the `mktime()` function:

```ppl
source=accounts
| eval date_str = '10/18/2003 20:07:13'
| convert mktime(date_str)
| fields date_str
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+--------------+
| date_str     |
|--------------|
| 1.066507633E9|
+--------------+
```

## Example 11: Using timeformat with ctime() and mktime()

The following query uses the `timeformat` parameter to specify a custom strftime format when converting an epoch timestamp with `ctime()`:

```ppl
source=accounts
| eval timestamp = 1066507633
| convert timeformat="%Y-%m-%d %H:%M:%S" ctime(timestamp)
| fields timestamp
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+---------------------+
| timestamp           |
|---------------------|
| 2003-10-18 20:07:13 |
+---------------------+
```

The following query uses the `timeformat` parameter to parse a custom date format when converting a time string to an epoch timestamp with `mktime()`:

```ppl
source=accounts
| eval date_str = '2000-01-01 00:00:00'
| convert timeformat="%Y-%m-%d %H:%M:%S" mktime(date_str)
| fields date_str
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+------------+
| date_str   |
|------------|
| 9.466848E8 |
+------------+
```

## Example 12: Converting duration to seconds with dur2sec()

The following query converts a duration string in `HH:MM:SS` format to total seconds using the `dur2sec()` function:

```ppl
source=accounts
| eval duration = '01:23:45'
| convert dur2sec(duration)
| fields duration
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+----------+
| duration |
|----------|
| 5025.0   |
+----------+
```

## Example 13: Converting minutes and seconds with mstime()

The following query converts a time string in `[MM:]SS.SSS` format to total seconds using the `mstime()` function:

```ppl
source=accounts
| eval time_str = '03:45.5'
| convert mstime(time_str)
| fields time_str
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+----------+
| time_str |
|----------|
| 225.5    |
+----------+
```

## Limitations

The `convert` command requires `plugins.calcite.enabled` to be set to `true`.

If Apache Calcite is disabled, using any convert function results in an unsupported function error.