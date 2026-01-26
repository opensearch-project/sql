# convert

The `convert` command uses conversion functions to transform field values into numeric values. Original field values are overwritten unless the AS clause is used to create new fields with the converted values.

## Syntax

The `convert` command has the following syntax:

```syntax
convert <convert-function>(<field>) [AS <field>] [, <convert-function>(<field>) [AS <field>]]...
```

## Parameters

The `convert` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<convert-function>` | Required | One of the conversion functions: `auto()`, `num()`, `rmcomma()`, `rmunit()`, `memk()`, or `none()`. |
| `<field>` | Required | Single field name to convert, or `*` to convert all fields. |
| `AS <field>` | Optional | Create new field with converted value, preserving original field. |

## Conversion Functions

| Function | Description |
| --- | --- |
| `auto(field)` | Automatically converts fields to numbers using intelligent conversion. Handles memory sizes (k/m/g), commas, units, and scientific notation. Returns `null` for non-convertible values. |
| `num(field)` | Extracts leading numbers from strings. For strings without letters: removes commas as thousands separators. For strings with letters: extracts leading number, stops at letters or commas. Returns `null` for non-convertible values. |
| `rmcomma(field)` | Removes commas from field values and converts to a number. Returns `null` if the value contains letters. |
| `rmunit(field)` | Extracts leading numeric values from strings. Stops at the first non-numeric character (including commas). Returns `null` for non-convertible values. |
| `memk(field)` | Converts memory size strings to kilobytes. Accepts numbers with optional k/m/g suffix (case-insensitive). Default unit is kilobytes. Returns `null` for invalid formats. |
| `none(field)` | No-op function that preserves the original field value. Used for excluding specific fields from wildcard conversions. |

## Example 1: Basic auto() conversion

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

## Example 2: Convert with commas using num()

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

## Example 3: Memory size conversion with memk()

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

## Example 4: Multiple field conversions

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

## Example 5: Using AS clause to preserve original values

The following query creates a new field with the converted value while preserving the original:

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

## Example 6: Extract numbers from strings with units

The following query extracts numeric values from strings containing units:

```ppl
source=metrics
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

## Example 7: Integration with aggregation functions

The following query converts values and uses them in aggregations:

```ppl
source=accounts
| convert auto(balance)
| stats avg(balance) by gender
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+--------------+--------+
| avg(balance) | gender |
|--------------+--------|
| 25208.15     | M      |
| 27992.571... | F      |
+--------------+--------+
```

## Example 8: Using none() to preserve field values

The `none()` function acts as a pass-through, returning the field value unchanged. This is useful for explicitly preserving fields in multi-field conversions:

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

### Using none() with AS for field renaming

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
+------------+-----------+----------|
```

**Note:** The `none()` function is particularly useful when wildcard support is implemented, allowing you to exclude specific fields from bulk conversions.

## Notes

- All conversion functions return `null` for values that cannot be converted to a number
- All numeric conversion functions return double precision numbers to support aggregations
- Converted numbers display with decimal notation (e.g., `1234.0`, `1234.56`)
- Use the `AS` clause to preserve original fields while creating converted versions
- Multiple conversions can be applied in a single command

## Limitations

The `convert` command can only work with `plugins.calcite.enabled=true`.

When Calcite is disabled, attempting to use convert functions will result in an "unsupported function" error.
