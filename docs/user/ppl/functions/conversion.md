# Type Conversion Functions  

## CAST  

### Description  

Usage: `cast(expr as dateType)` cast the expr to dataType. return the value of dataType. The following conversion rules are used:
  
| Src/Target | STRING | NUMBER | BOOLEAN | TIMESTAMP | DATE | TIME | IP |
| --- | --- | --- | --- | --- | --- | --- | --- |
| STRING |  | Note1 | Note1 | TIMESTAMP() | DATE() | TIME() | IP() |
| NUMBER | Note1 |  | v!=0 | N/A | N/A | N/A | N/A |
| BOOLEAN | Note1 | v?1:0 |  | N/A | N/A | N/A | N/A |
| TIMESTAMP | Note1 | N/A | N/A |  | DATE() | TIME() | N/A |
| DATE | Note1 | N/A | N/A | N/A |  | N/A | N/A |
| TIME | Note1 | N/A | N/A | N/A | N/A |  | N/A |
| IP | Note2 | N/A | N/A | N/A | N/A | N/A |  |
  
Note1: the conversion follow the JDK specification.
Note2: IP will be converted to its canonical representation. Canonical representation
for IPv6 is described in [RFC 5952](https://datatracker.ietf.org/doc/html/rfc5952).

### Example: Cast to string
  
```ppl
source=people
| eval `cbool` = CAST(true as string), `cint` = CAST(1 as string), `cdate` = CAST(CAST('2012-08-07' as date) as string)
| fields `cbool`, `cint`, `cdate`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-------+------+------------+
| cbool | cint | cdate      |
|-------+------+------------|
| TRUE  | 1    | 2012-08-07 |
+-------+------+------------+
```
  
### Example: Cast to number
  
```ppl
source=people
| eval `cbool` = CAST(true as int), `cstring` = CAST('1' as int)
| fields `cbool`, `cstring`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-------+---------+
| cbool | cstring |
|-------+---------|
| 1     | 1       |
+-------+---------+
```
  
### Example: Cast to date
  
```ppl
source=people
| eval `cdate` = CAST('2012-08-07' as date), `ctime` = CAST('01:01:01' as time), `ctimestamp` = CAST('2012-08-07 01:01:01' as timestamp)
| fields `cdate`, `ctime`, `ctimestamp`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+------------+----------+---------------------+
| cdate      | ctime    | ctimestamp          |
|------------+----------+---------------------|
| 2012-08-07 | 01:01:01 | 2012-08-07 01:01:01 |
+------------+----------+---------------------+
```
  
### Example: Cast function can be chained
  
```ppl
source=people
| eval `cbool` = CAST(CAST(true as string) as boolean)
| fields `cbool`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-------+
| cbool |
|-------|
| True  |
+-------+
```
  
## IMPLICIT (AUTO) TYPE CONVERSION  

Implicit conversion is automatic casting. When a function does not have an exact match for the
input types, the engine looks for another signature that can safely work with the values. It picks
the option that requires the least stretching of the original types, so you can mix literals and
fields without adding `CAST` everywhere.

### String to numeric  

When a string stands in for a number we simply parse the text:
- The value must be something like `"3.14"` or `"42"`. Anything else causes the query to fail.  
- If a string appears next to numeric arguments, it is treated as a `DOUBLE` so the numeric overload of the function can run.

### Example: Use string in arithmetic operator
  
```ppl
source=people
| eval divide="5"/10, multiply="5" * 10, add="5" + 10, minus="5" - 10, concat="5" + "5"
| fields divide, multiply, add, minus, concat
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------+----------+-----+-------+--------+
| divide | multiply | add | minus | concat |
|--------+----------+-----+-------+--------|
| 0.5    | 50       | 15  | -5    | 55     |
+--------+----------+-----+-------+--------+
```
  
### Example: Use string in comparison operator
  
```ppl
source=people
| eval e="1000"==1000, en="1000"!=1000, ed="1000"==1000.0, edn="1000"!=1000.0, l="1000">999, ld="1000">999.9, i="malformed"==1000
| fields e, en, ed, edn, l, ld, i
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+------+-------+------+-------+------+------+------+
| e    | en    | ed   | edn   | l    | ld   | i    |
|------+-------+------+-------+------+------+------|
| True | False | True | False | True | True | null |
+------+-------+------+-------+------+------+------+
```
  
## TOSTRING  

### Description  

The following usage options are available, depending on the parameter types and the number of parameters.

Usage with format type: `tostring(ANY, [format])`: Converts the value in first argument to provided format type string in second argument. If second argument is not provided, then it converts to default string representation.

**Return type:** `STRING`

Usage for boolean parameter without format type `tostring(boolean)`: Converts the string to 'TRUE' or 'FALSE'.

**Return type:** `STRING`

You can use this function with the eval commands and as part of eval expressions. If first argument can be any valid type, second argument is optional and if provided, it needs to be format name to convert to where first argument contains only numbers. If first argument is boolean, then second argument is not used even if its provided.

Format types:
1. "binary" Converts a number to a binary value.  
2. "hex" Converts the number to a hexadecimal value.  
3. "commas" Formats the number with commas. If the number includes a decimal, the function rounds the number to nearest two decimal places.  
4. "duration" Converts the value in seconds to the readable time format HH:MM:SS.  
5. "duration_millis" Converts the value in milliseconds to the readable time format HH:MM:SS.  
  
The format argument is optional and is only used when the value argument is a number. The tostring function supports the following formats.

### Example: Convert number to binary string

You can use this function to convert a number to a string of its binary representation.
  
```ppl
source=accounts
| where firstname = "Amber"
| eval balance_binary = tostring(balance, "binary")
| fields firstname, balance_binary, balance
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----------+------------------+---------+
| firstname | balance_binary   | balance |
|-----------+------------------+---------|
| Amber     | 1001100100111001 | 39225   |
+-----------+------------------+---------+
```
  
### Example: Convert number to hex string

You can use this function to convert a number to a string of its hex representation.
  
```ppl
source=accounts
| where firstname = "Amber"
| eval balance_hex = tostring(balance, "hex")
| fields firstname, balance_hex, balance
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----------+-------------+---------+
| firstname | balance_hex | balance |
|-----------+-------------+---------|
| Amber     | 9939        | 39225   |
+-----------+-------------+---------+
```
  
### Example: Format number with commas

The following example formats the column totalSales to display values with commas.
  
```ppl
source=accounts
| where firstname = "Amber"
| eval balance_commas = tostring(balance, "commas")
| fields firstname, balance_commas, balance
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----------+----------------+---------+
| firstname | balance_commas | balance |
|-----------+----------------+---------|
| Amber     | 39,225         | 39225   |
+-----------+----------------+---------+
```
  
### Example: Convert seconds to duration format

The following example converts number of seconds to HH:MM:SS format representing hours, minutes and seconds.
  
```ppl
source=accounts
| where firstname = "Amber"
| eval duration = tostring(6500, "duration")
| fields firstname, duration
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----------+----------+
| firstname | duration |
|-----------+----------|
| Amber     | 01:48:20 |
+-----------+----------+
```
  
### Example: Convert boolean to string

The following example converts boolean parameter to string.
  
```ppl
source=accounts
| where firstname = "Amber"
| eval `boolean_str` = tostring(1=1)
| fields `boolean_str`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-------------+
| boolean_str |
|-------------|
| TRUE        |
+-------------+
```

## TONUMBER

### Description

Usage: `tonumber(string, [base])` converts the value in first argument.
The second argument describes the base of first argument. If second argument is not provided, then it converts to base 10 number representation.

**Return type:** `NUMBER`

You can use this function with the eval commands and as part of eval expressions. Base values can be between 2 and 36. The maximum value supported for base 10 is +(2-2^-52)·2^1023 and minimum is -(2-2^-52)·2^1023. The maximum for other supported bases is 2^63-1 (or 7FFFFFFFFFFFFFFF) and minimum is -2^63 (or -7FFFFFFFFFFFFFFF). If the tonumber function cannot parse a field value to a number, the function returns NULL. You can use this function to convert a string representation of a binary number to return the corresponding number in base 10.

### Example: Convert binary string to number

```ppl
source=people | eval int_value = tonumber('010101',2) | fields int_value | head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+-----------+
| int_value |
|-----------|
| 21.0      |
+-----------+
```

### Example: Convert hex string to number

```ppl
source=people | eval int_value = tonumber('FA34',16) | fields int_value | head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+-----------+
| int_value |
|-----------|
| 64052.0   |
+-----------+
```

### Example: Convert decimal string to number

```ppl
source=people | eval int_value = tonumber('4598') | fields int_value | head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+-----------+
| int_value |
|-----------|
| 4598.0    |
+-----------+
```

### Example: Convert decimal string with fraction to number

```ppl
source=people | eval double_value = tonumber('4598.678') | fields double_value | head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------------+
| double_value |
|--------------|
| 4598.678     |
+--------------+
```
  
