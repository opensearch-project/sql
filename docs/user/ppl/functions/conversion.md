# Type conversion functions

The following type conversion functions are supported in PPL.  

## CAST

**Usage**: `cast(expr as dataType)`

Casts the expression to the specified data type and returns the converted value.

**Parameters**:

- `expr` (Required): The expression to cast to a different data type.
- `dataType` (Required): The target data type for the cast operation.

**Return type**: Specified by data type

The following table shows the conversion rules used for casting between data types:
  
| Src/Target | STRING | NUMBER | BOOLEAN | TIMESTAMP | DATE | TIME | IP |
| --- | --- | --- | --- | --- | --- | --- | --- |
| STRING |  | Note1 | Note1 | TIMESTAMP() | DATE() | TIME() | IP() |
| NUMBER | Note1 |  | v!=0 | N/A | N/A | N/A | N/A |
| BOOLEAN | Note1 | v?1:0 |  | N/A | N/A | N/A | N/A |
| TIMESTAMP | Note1 | N/A | N/A |  | DATE() | TIME() | N/A |
| DATE | Note1 | N/A | N/A | N/A |  | N/A | N/A |
| TIME | Note1 | N/A | N/A | N/A | N/A |  | N/A |
| IP | Note2 | N/A | N/A | N/A | N/A | N/A |  |
  
Note1: The conversion follows the JDK specification.
Note2: IP addresses are converted to their canonical representation. The canonical representation for IPv6 is described in [RFC 5952](https://datatracker.ietf.org/doc/html/rfc5952).

#### Example

The following example casts different data types to string:
  
```ppl
source=people
| eval `cbool` = CAST(true as string), `cint` = CAST(1 as string), `cdate` = CAST(CAST('2012-08-07' as date) as string)
| fields `cbool`, `cint`, `cdate`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------+------+------------+
| cbool | cint | cdate      |
|-------+------+------------|
| TRUE  | 1    | 2012-08-07 |
+-------+------+------------+
```
  
The following example casts values to integer type:
  
```ppl
source=people
| eval `cbool` = CAST(true as int), `cstring` = CAST('1' as int)
| fields `cbool`, `cstring`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------+---------+
| cbool | cstring |
|-------+---------|
| 1     | 1       |
+-------+---------+
```
  
The following example casts strings to date, time, and timestamp types:
  
```ppl
source=people
| eval `cdate` = CAST('2012-08-07' as date), `ctime` = CAST('01:01:01' as time), `ctimestamp` = CAST('2012-08-07 01:01:01' as timestamp)
| fields `cdate`, `ctime`, `ctimestamp`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+------------+----------+---------------------+
| cdate      | ctime    | ctimestamp          |
|------------+----------+---------------------|
| 2012-08-07 | 01:01:01 | 2012-08-07 01:01:01 |
+------------+----------+---------------------+
```
  
The following example demonstrates chaining cast functions:
  
```ppl
source=people
| eval `cbool` = CAST(CAST(true as string) as boolean)
| fields `cbool`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------+
| cbool |
|-------|
| True  |
+-------+
```
  
## Implicit type conversion

Implicit conversion is automatic casting. When a function does not have an exact match for the input types, the engine looks for another signature that can safely handle the values. It selects the option that requires the least conversion of the original types, so you can mix literals and fields without adding explicit `cast` functions.

### String to numeric type conversion

When a string is used where a numeric value is expected, the engine attempts to parse the string as a number:
- The string must represent a valid numeric value, such as `"3.14"` or `"42"`. Any other value causes the query to fail.
- If a string is used alongside numeric arguments, the engine treats it as a `DOUBLE` so that the numeric overload of the function can be applied.

#### Example

The following example demonstrates using strings in arithmetic operations:
  
```ppl
source=people
| eval divide="5"/10, multiply="5" * 10, add="5" + 10, minus="5" - 10, concat="5" + "5"
| fields divide, multiply, add, minus, concat
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------+----------+-----+-------+--------+
| divide | multiply | add | minus | concat |
|--------+----------+-----+-------+--------|
| 0.5    | 50       | 15  | -5    | 55     |
+--------+----------+-----+-------+--------+
```
  
The following example demonstrates using strings in comparison operations:
  
```ppl
source=people
| eval e="1000"==1000, en="1000"!=1000, ed="1000"==1000.0, edn="1000"!=1000.0, l="1000">999, ld="1000">999.9, i="malformed"==1000
| fields e, en, ed, edn, l, ld, i
```

The query returns the following results:


```text
fetched rows / total rows = 1/1
+------+-------+------+-------+------+------+------+
| e    | en    | ed   | edn   | l    | ld   | i    |
|------+-------+------+-------+------+------+------|
| True | False | True | False | True | True | null |
+------+-------+------+-------+------+------+------+
```
  
## TOSTRING

**Usage**: `tostring(value[, format])`

Converts the value to a string representation. If a format is provided, converts numbers to the specified format type. For Boolean values, converts to `TRUE` or `FALSE`.

**Parameters**:

- `value` (Required): The value to convert to string (any data type).
- `format` (Optional): The format type for number conversion. This parameter is only used when `value` is a number. If `value` is a Boolean, this parameter is ignored.

Format types:

- `binary`: Converts a number to a binary value.
- `hex`: Converts the number to a hexadecimal value.
- `commas`: Formats the number using commas. If the number includes a decimal, the function rounds the number to the nearest two decimal places.
- `duration`: Converts the value in seconds to the readable time format `HH:MM:SS`.
- `duration_millis`: Converts the value in milliseconds to the readable time format `HH:MM:SS`.

**Return type**: `STRING`

#### Example

The following example converts a number to its binary string representation:
  
```ppl
source=accounts
| where firstname = "Amber"
| eval balance_binary = tostring(balance, "binary")
| fields firstname, balance_binary, balance
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------+------------------+---------+
| firstname | balance_binary   | balance |
|-----------+------------------+---------|
| Amber     | 1001100100111001 | 39225   |
+-----------+------------------+---------+
```
  
The following example converts a number to its hexadecimal string representation:
  
```ppl
source=accounts
| where firstname = "Amber"
| eval balance_hex = tostring(balance, "hex")
| fields firstname, balance_hex, balance
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------+-------------+---------+
| firstname | balance_hex | balance |
|-----------+-------------+---------|
| Amber     | 9939        | 39225   |
+-----------+-------------+---------+
```
  
The following example formats numbers with comma separators:
  
```ppl
source=accounts
| where firstname = "Amber"
| eval balance_commas = tostring(balance, "commas")
| fields firstname, balance_commas, balance
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------+----------------+---------+
| firstname | balance_commas | balance |
|-----------+----------------+---------|
| Amber     | 39,225         | 39225   |
+-----------+----------------+---------+
```

### Example: Convert seconds to duration format
  
The following example converts the number of seconds to the `HH:MM:SS` format representing hours, minutes, and seconds:
  
```ppl
source=accounts
| where firstname = "Amber"
| eval duration = tostring(6500, "duration")
| fields firstname, duration
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------+----------+
| firstname | duration |
|-----------+----------|
| Amber     | 01:48:20 |
+-----------+----------+
```
  
The following example converts a Boolean value to string:
  
```ppl
source=accounts
| where firstname = "Amber"
| eval `boolean_str` = tostring(1=1)
| fields `boolean_str`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------+
| boolean_str |
|-------------|
| TRUE        |
+-------------+
```

## TONUMBER

**Usage**: `tonumber(string[, base])`

Converts the string value to a number. The optional `base` parameter specifies the base of the input string. If not provided, the function assumes base `10`.

**Parameters**:

- `string` (Required): The string representation of the number to convert.
- `base` (Optional): The base of the input string (between `2` and `36`). Defaults to `10`.

**Return type**: `NUMBER`

You can use this function with `eval` commands and as part of `eval` expressions. Base values can be between `2` and `36`.

**Value limits**:
- Base 10: Maximum is +(2-2^-52)·2^1023 and minimum is -(2-2^-52)·2^1023.
- Other bases: Maximum is 2^63-1 (or 7FFFFFFFFFFFFFFF) and minimum is -2^63 (or -7FFFFFFFFFFFFFFF).

If the `tonumber` function cannot parse a field value to a number, the function returns `NULL`. You can use this function to convert string representations of numbers in various bases to their corresponding base 10 values.

#### Example: Convert a binary string to a number

```ppl
source=people
| eval int_value = tonumber('010101',2)
| fields int_value
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+-----------+
| int_value |
|-----------|
| 21.0      |
+-----------+
```

#### Example: Convert a hexadecimal string to a number

```ppl
source=people
| eval int_value = tonumber('FA34',16)
| fields int_value
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+-----------+
| int_value |
|-----------|
| 64052.0   |
+-----------+
```

#### Example: Convert a decimal string without a decimal part to a number

```ppl
source=people
| eval int_value = tonumber('4598')
| fields int_value
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+-----------+
| int_value |
|-----------|
| 4598.0    |
+-----------+
```

#### Example: Convert a decimal string with a decimal part to a number

```ppl
source=people
| eval double_value = tonumber('4598.678')
| fields double_value
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+--------------+
| double_value |
|--------------|
| 4598.678     |
+--------------+
```
  
