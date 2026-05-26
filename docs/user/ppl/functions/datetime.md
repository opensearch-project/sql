# Date and time functions

All PPL date and time functions use the UTC time zone. Both input and output values are interpreted as UTC. For example, an input timestamp literal such as `'2020-08-26 01:01:01'` is assumed to be in UTC, and the `now()` function also returns the current date and time in UTC.

The following date and time functions are supported in PPL.

## ADDDATE

**Usage**: `ADDDATE(date, INTERVAL expr unit)`, `ADDDATE(date, days)`

Adds the interval or number of days to the date. The first form adds an interval to the date, the second form adds the specified number of days as an integer to the date. If the first argument is `TIME`, today's date is used. If the first argument is `DATE`, the time at midnight is used.

**Parameters**:

- `date` (Required): The date, timestamp, or time value to modify.
- `INTERVAL expr unit` (Required in first form): The interval to add to the date.
- `days` (Required in second form): The number of days to add as an integer.

**Return type**: `TIMESTAMP` for the interval form, `DATE` for the integer days form when the input is `DATE`, `TIMESTAMP` when the input is `TIMESTAMP` or `TIME`.

Synonyms: [`DATE_ADD`](#date_add) (when used in interval form)

### Example
  
```ppl
source=people
| eval `'2020-08-26' + 1h` = ADDDATE(DATE('2020-08-26'), INTERVAL 1 HOUR), `'2020-08-26' + 1` = ADDDATE(DATE('2020-08-26'), 1), `ts '2020-08-26 01:01:01' + 1` = ADDDATE(TIMESTAMP('2020-08-26 01:01:01'), 1)
| fields `'2020-08-26' + 1h`, `'2020-08-26' + 1`, `ts '2020-08-26 01:01:01' + 1`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------+------------------+------------------------------+
| '2020-08-26' + 1h   | '2020-08-26' + 1 | ts '2020-08-26 01:01:01' + 1 |
|---------------------+------------------+------------------------------|
| 2020-08-26 01:00:00 | 2020-08-27       | 2020-08-27 01:01:01          |
+---------------------+------------------+------------------------------+
```
  
## ADDTIME

**Usage**: `ADDTIME(expr1, expr2)`

Adds the second expression to the first expression and returns the result. If an argument is `TIME`, today's date is used. If an argument is `DATE`, the time at midnight is used.

**Parameters**:

- `expr1` (Required): The base date, timestamp, or time value.
- `expr2` (Required): The date, timestamp, or time value to add to the first expression.

**Return type**: `TIMESTAMP` when the first argument is `DATE` or `TIMESTAMP`, `TIME` when the first argument is `TIME`.

#### Examples

The following example shows adding two DATE values:

```ppl
source=people
| eval `'2008-12-12' + 0` = ADDTIME(DATE('2008-12-12'), DATE('2008-11-15'))
| fields `'2008-12-12' + 0`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------+
| '2008-12-12' + 0    |
|---------------------|
| 2008-12-12 00:00:00 |
+---------------------+
```

The following example shows adding TIME and DATE values:
  
```ppl
source=people
| eval `'23:59:59' + 0` = ADDTIME(TIME('23:59:59'), DATE('2004-01-01'))
| fields `'23:59:59' + 0`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------+
| '23:59:59' + 0 |
|----------------|
| 23:59:59       |
+----------------+
```

The following example shows combining DATE and TIME into a timestamp:
  
```ppl
source=people
| eval `'2004-01-01' + '23:59:59'` = ADDTIME(DATE('2004-01-01'), TIME('23:59:59'))
| fields `'2004-01-01' + '23:59:59'`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------------+
| '2004-01-01' + '23:59:59' |
|---------------------------|
| 2004-01-01 23:59:59       |
+---------------------------+
```

The following example shows adding two TIME values:
  
```ppl
source=people
| eval `'10:20:30' + '00:05:42'` = ADDTIME(TIME('10:20:30'), TIME('00:05:42'))
| fields `'10:20:30' + '00:05:42'`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------------+
| '10:20:30' + '00:05:42' |
|-------------------------|
| 10:26:12                |
+-------------------------+
```

The following example shows adding two TIMESTAMP values:
  
```ppl
source=people
| eval `'2007-02-28 10:20:30' + '20:40:50'` = ADDTIME(TIMESTAMP('2007-02-28 10:20:30'), TIMESTAMP('2002-03-04 20:40:50'))
| fields `'2007-02-28 10:20:30' + '20:40:50'`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+------------------------------------+
| '2007-02-28 10:20:30' + '20:40:50' |
|------------------------------------|
| 2007-03-01 07:01:20                |
+------------------------------------+
```
  
## CONVERT_TZ

**Usage**: `CONVERT_TZ(timestamp, from_timezone, to_timezone)`

Constructs a local timestamp converted from the source time zone to the target time zone. Returns `NULL` when any of the three function arguments is invalid: the timestamp is not in the format `yyyy-MM-dd HH:mm:ss`, a time zone is not in `(+/-)HH:mm` format, dates are invalid (such as February 30th), or time zones are outside the valid range of -13:59 to +14:00.

**Parameters**:

- `timestamp` (Required): The timestamp or string to convert in `yyyy-MM-dd HH:mm:ss` format.
- `from_timezone` (Required): The source time zone in `(+/-)HH:mm` format.
- `to_timezone` (Required): The target time zone in `(+/-)HH:mm` format.

**Return type**: `TIMESTAMP`

#### Examples

```ppl
source=people
| eval `convert_tz('2008-05-15 12:00:00','+00:00','+10:00')` = convert_tz('2008-05-15 12:00:00','+00:00','+10:00')
| fields `convert_tz('2008-05-15 12:00:00','+00:00','+10:00')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------------------------------+
| convert_tz('2008-05-15 12:00:00','+00:00','+10:00') |
|-----------------------------------------------------|
| 2008-05-15 22:00:00                                 |
+-----------------------------------------------------+
```
  
The valid time zone range for `convert_tz` is (-13:59, +14:00) inclusive. Time zones outside of the range, such as +15:00 in this example, return `NULL`:
  
```ppl
source=people
| eval `convert_tz('2008-05-15 12:00:00','+00:00','+15:00')` = convert_tz('2008-05-15 12:00:00','+00:00','+15:00')
| fields `convert_tz('2008-05-15 12:00:00','+00:00','+15:00')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------------------------------+
| convert_tz('2008-05-15 12:00:00','+00:00','+15:00') |
|-----------------------------------------------------|
| null                                                |
+-----------------------------------------------------+
```
  
The following example shows conversion from a positive time zone to a negative time zone that crosses the date line:
  
```ppl
source=people
| eval `convert_tz('2008-05-15 12:00:00','+03:30','-10:00')` = convert_tz('2008-05-15 12:00:00','+03:30','-10:00')
| fields `convert_tz('2008-05-15 12:00:00','+03:30','-10:00')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------------------------------+
| convert_tz('2008-05-15 12:00:00','+03:30','-10:00') |
|-----------------------------------------------------|
| 2008-05-14 22:30:00                                 |
+-----------------------------------------------------+
```
  
Valid dates are required in `convert_tz`. For invalid dates such as April 31st (not a date in the Gregorian calendar), `NULL` is returned:
  
```ppl
source=people
| eval `convert_tz('2008-04-31 12:00:00','+03:30','-10:00')` = convert_tz('2008-04-31 12:00:00','+03:30','-10:00')
| fields `convert_tz('2008-04-31 12:00:00','+03:30','-10:00')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------------------------------+
| convert_tz('2008-04-31 12:00:00','+03:30','-10:00') |
|-----------------------------------------------------|
| null                                                |
+-----------------------------------------------------+
```
  
The following example shows that February 30th also returns `NULL`:
  
```ppl
source=people
| eval `convert_tz('2008-02-30 12:00:00','+03:30','-10:00')` = convert_tz('2008-02-30 12:00:00','+03:30','-10:00')
| fields `convert_tz('2008-02-30 12:00:00','+03:30','-10:00')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------------------------------+
| convert_tz('2008-02-30 12:00:00','+03:30','-10:00') |
|-----------------------------------------------------|
| null                                                |
+-----------------------------------------------------+
```
  
February 29th 2008 is a valid date because it is a leap year:
  
```ppl
source=people
| eval `convert_tz('2008-02-29 12:00:00','+03:30','-10:00')` = convert_tz('2008-02-29 12:00:00','+03:30','-10:00')
| fields `convert_tz('2008-02-29 12:00:00','+03:30','-10:00')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------------------------------+
| convert_tz('2008-02-29 12:00:00','+03:30','-10:00') |
|-----------------------------------------------------|
| 2008-02-28 22:30:00                                 |
+-----------------------------------------------------+
```
  
The following example shows that February 29th 2007 returns `NULL` because 2007 is not a leap year:
  
```ppl
source=people
| eval `convert_tz('2007-02-29 12:00:00','+03:30','-10:00')` = convert_tz('2007-02-29 12:00:00','+03:30','-10:00')
| fields `convert_tz('2007-02-29 12:00:00','+03:30','-10:00')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------------------------------+
| convert_tz('2007-02-29 12:00:00','+03:30','-10:00') |
|-----------------------------------------------------|
| null                                                |
+-----------------------------------------------------+
```
  
The valid time zone range for `convert_tz` is [-13:59, +14:00] inclusive. Time zones outside of the range, such as +14:01 in this example, return `NULL`:
  
```ppl
source=people
| eval `convert_tz('2008-02-01 12:00:00','+14:01','+00:00')` = convert_tz('2008-02-01 12:00:00','+14:01','+00:00')
| fields `convert_tz('2008-02-01 12:00:00','+14:01','+00:00')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------------------------------+
| convert_tz('2008-02-01 12:00:00','+14:01','+00:00') |
|-----------------------------------------------------|
| null                                                |
+-----------------------------------------------------+
```
  
The valid time zone range for `convert_tz` is (-13:59, +14:00) inclusive. Time zones within the range, such as +14:00 in this example, return a correctly converted date time object:
  
```ppl
source=people
| eval `convert_tz('2008-02-01 12:00:00','+14:00','+00:00')` = convert_tz('2008-02-01 12:00:00','+14:00','+00:00')
| fields `convert_tz('2008-02-01 12:00:00','+14:00','+00:00')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------------------------------+
| convert_tz('2008-02-01 12:00:00','+14:00','+00:00') |
|-----------------------------------------------------|
| 2008-01-31 22:00:00                                 |
+-----------------------------------------------------+
```
  
The following example shows that -14:00 (outside the valid range) returns `NULL`:
  
```ppl
source=people
| eval `convert_tz('2008-02-01 12:00:00','-14:00','+00:00')` = convert_tz('2008-02-01 12:00:00','-14:00','+00:00')
| fields `convert_tz('2008-02-01 12:00:00','-14:00','+00:00')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------------------------------+
| convert_tz('2008-02-01 12:00:00','-14:00','+00:00') |
|-----------------------------------------------------|
| null                                                |
+-----------------------------------------------------+
```
  
The valid time zone range for `convert_tz` is [-13:59, +14:00] inclusive. Time zones at the lower boundary of the range, such as -13:59, are valid and return converted results:
  
```ppl
source=people
| eval `convert_tz('2008-02-01 12:00:00','-13:59','+00:00')` = convert_tz('2008-02-01 12:00:00','-13:59','+00:00')
| fields `convert_tz('2008-02-01 12:00:00','-13:59','+00:00')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------------------------------+
| convert_tz('2008-02-01 12:00:00','-13:59','+00:00') |
|-----------------------------------------------------|
| 2008-02-02 01:59:00                                 |
+-----------------------------------------------------+
```
  
## CURDATE

**Usage**: `CURDATE()`

Returns the current date as a value in `YYYY-MM-DD` format. The function returns the current date in UTC at the time when the statement is executed.

**Parameters**: None

**Return type**: `DATE`

### Example
  
```ppl ignore
source=people
| eval `CURDATE()` = CURDATE()
| fields `CURDATE()`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+------------+
| CURDATE()  |
|------------|
| 2025-08-02 |
+------------+
```
  
## CURRENT_DATE

**Usage**: `CURRENT_DATE()`

A synonym for `CURDATE()`.

**Parameters**: None

**Return type**: `DATE`

### Example
  
```ppl ignore
source=people
| eval `CURRENT_DATE()` = CURRENT_DATE()
| fields `CURRENT_DATE()`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+------------------+
| CURRENT_DATE()   |
|------------------+
| 2025-08-02       |
+------------------+
```
  
## CURRENT_TIME

**Usage**: `CURRENT_TIME()`

A synonym for `CURTIME()`.

**Parameters**: None

**Return type**: `TIME`

### Example
  
```ppl ignore
source=people
| eval `CURRENT_TIME()` = CURRENT_TIME()
| fields `CURRENT_TIME()`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+------------------+
| CURRENT_TIME()   |
|------------------+
| 15:39:05         |
+------------------+
```
  
## CURRENT_TIMESTAMP

**Usage**: `CURRENT_TIMESTAMP()`

A synonym for `NOW()`.

**Parameters**: None

**Return type**: `TIMESTAMP`

### Example
  
```ppl ignore
source=people
| eval `CURRENT_TIMESTAMP()` = CURRENT_TIMESTAMP()
| fields `CURRENT_TIMESTAMP()`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------+
| CURRENT_TIMESTAMP()   |
|-----------------------+
| 2025-08-02 15:54:19   |
+-----------------------+
```
  
## CURTIME

**Usage**: `CURTIME()`

Returns the current time as a value in the `hh:mm:ss` format in the UTC time zone. `CURTIME()` returns the time at which the statement began to execute, as [`NOW()`](#now) does.

**Parameters**: None

**Return type**: `TIME`

#### Example

```ppl ignore
source=people
| eval `value_1` = CURTIME(), `value_2` = CURTIME()
| fields `value_1`, `value_2`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------+----------+
| value_1  | value_2  |
|----------+----------|
| 15:39:05 | 15:39:05 |
+----------+----------+
```
  
## DATE

**Usage**: `DATE(expr)`

Constructs a date type from the input string `expr`. If the argument is a date or timestamp, it extracts the date value part from the expression.

**Parameters**:
- `expr` (Required): A `STRING`, `DATE`, or `TIMESTAMP` value.

**Return type**: `DATE`

#### Examples

The following example extracts a date from a string:

```ppl
source=people
| eval `DATE('2020-08-26')` = DATE('2020-08-26')
| fields `DATE('2020-08-26')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------------+
| DATE('2020-08-26') |
|--------------------|
| 2020-08-26         |
+--------------------+
```

The following example extracts the date from a timestamp:
  
```ppl
source=people
| eval `DATE(TIMESTAMP('2020-08-26 13:49:00'))` = DATE(TIMESTAMP('2020-08-26 13:49:00'))
| fields `DATE(TIMESTAMP('2020-08-26 13:49:00'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------------------------------+
| DATE(TIMESTAMP('2020-08-26 13:49:00')) |
|----------------------------------------|
| 2020-08-26                             |
+----------------------------------------+
```

The following example extracts the date from a string containing both date and time:
  
```ppl
source=people
| eval `DATE('2020-08-26 13:49')` = DATE('2020-08-26 13:49')
| fields `DATE('2020-08-26 13:49')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------------------+
| DATE('2020-08-26 13:49') |
|--------------------------|
| 2020-08-26               |
+--------------------------+
```
  
## DATE_ADD

**Usage**: `DATE_ADD(date, INTERVAL expr unit)`

Adds the interval `expr` to `date`. If the first argument is `TIME`, today's date is used. If the first argument is `DATE`, the time at midnight is used.

**Parameters**:
- `date` (Required): A `DATE`, `TIMESTAMP`, or `TIME` value.
- `INTERVAL expr unit` (Required): An `INTERVAL` expression.

**Return type**: `TIMESTAMP`

Synonyms: [`ADDDATE`](#adddate)
Antonyms: [`DATE_SUB`](#date_sub)

#### Example
  
```ppl
source=people
| eval `'2020-08-26' + 1h` = DATE_ADD(DATE('2020-08-26'), INTERVAL 1 HOUR), `ts '2020-08-26 01:01:01' + 1d` = DATE_ADD(TIMESTAMP('2020-08-26 01:01:01'), INTERVAL 1 DAY)
| fields `'2020-08-26' + 1h`, `ts '2020-08-26 01:01:01' + 1d`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------+-------------------------------+
| '2020-08-26' + 1h   | ts '2020-08-26 01:01:01' + 1d |
|---------------------+-------------------------------|
| 2020-08-26 01:00:00 | 2020-08-27 01:01:01           |
+---------------------+-------------------------------+
```
  
## DATE_FORMAT

**Usage**: `DATE_FORMAT(date, format)`

Formats the `date` argument using the specifiers in the `format` argument. If an argument of type `TIME` is provided, the local date is used.

**Parameters**:
- `date` (Required): A `STRING`, `DATE`, `TIME`, or `TIMESTAMP` value.
- `format` (Required): A `STRING` containing format specifiers.

**Return type**: `STRING`

The following table describes the available format specifiers.

| Specifier | Description |
| --- | --- |
| `%a` | Abbreviated weekday name (Sun..Sat) |
| `%b` | Abbreviated month name (Jan..Dec) |
| `%c` | Month, numeric (0..12) |
| `%D` | Day of the month with English suffix (0th, 1st, 2nd, 3rd, ...) |
| `%d` | Day of the month, numeric (00..31) |
| `%e` | Day of the month, numeric (0..31) |
| `%f` | Microseconds (000000..999999) |
| `%H` | Hour (00..23) |
| `%h` | Hour (01..12) |
| `%I` | Hour (01..12) |
| `%i` | Minutes, numeric (00..59) |
| `%j` | Day of year (001..366) |
| `%k` | Hour (0..23) |
| `%l` | Hour (1..12) |
| `%M` | Month name (January..December) |
| `%m` | Month, numeric (00..12) |
| `%p` | AM or PM |
| `%r` | Time, 12-hour (hh:mm:ss followed by AM or PM) |
| `%S` | Seconds (00..59) |
| `%s` | Seconds (00..59) |
| `%T` | Time, 24-hour (hh:mm:ss) |
| `%U` | Week (00..53), where Sunday is the first day of the week; WEEK() mode 0 |
| `%u` | Week (00..53), where Monday is the first day of the week; WEEK() mode 1 |
| `%V` | Week (01..53), where Sunday is the first day of the week; WEEK() mode 2; used with `%X` |
| `%v` | Week (01..53), where Monday is the first day of the week; WEEK() mode 3; used with `%x` |
| `%W` | Weekday name (Sunday..Saturday) |
| `%w` | Day of the week (0=Sunday..6=Saturday) |
| `%X` | Year for the week where Sunday is the first day of the week, numeric, four digits; used with `%V` |
| `%x` | Year for the week, where Monday is the first day of the week, numeric, four digits; used with `%v` |
| `%Y` | Year, numeric, four digits |
| `%y` | Year, numeric (two digits) |
| `%%` | A literal % character |
| `x` | x, for any lowercase/uppercase alphabet except [aydmshiHIMYDSEL] |

#### Example
  
```ppl
source=people
| eval `DATE_FORMAT('1998-01-31 13:14:15.012345', '%T.%f')` = DATE_FORMAT('1998-01-31 13:14:15.012345', '%T.%f'), `DATE_FORMAT(TIMESTAMP('1998-01-31 13:14:15.012345'), '%Y-%b-%D %r')` = DATE_FORMAT(TIMESTAMP('1998-01-31 13:14:15.012345'), '%Y-%b-%D %r')
| fields `DATE_FORMAT('1998-01-31 13:14:15.012345', '%T.%f')`, `DATE_FORMAT(TIMESTAMP('1998-01-31 13:14:15.012345'), '%Y-%b-%D %r')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------------------------------------------+---------------------------------------------------------------------+
| DATE_FORMAT('1998-01-31 13:14:15.012345', '%T.%f') | DATE_FORMAT(TIMESTAMP('1998-01-31 13:14:15.012345'), '%Y-%b-%D %r') |
|----------------------------------------------------+---------------------------------------------------------------------|
| 13:14:15.012345                                    | 1998-Jan-31st 01:14:15 PM                                           |
+----------------------------------------------------+---------------------------------------------------------------------+
```
  
## DATETIME

**Usage**: `DATETIME(timestamp)` or `DATETIME(date, to_timezone)`

Converts the datetime to a new time zone.

**Parameters**:
- `timestamp` (Required): A `TIMESTAMP` or `STRING` value.
- `to_timezone` (Optional): A `STRING` time zone value.

**Return type**: `TIMESTAMP`

#### Examples

The following example converts a datetime to a different time zone:

```ppl
source=people
| eval `DATETIME('2004-02-28 23:00:00-10:00', '+10:00')` = DATETIME('2004-02-28 23:00:00-10:00', '+10:00')
| fields `DATETIME('2004-02-28 23:00:00-10:00', '+10:00')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------------------------------------+
| DATETIME('2004-02-28 23:00:00-10:00', '+10:00') |
|-------------------------------------------------|
| 2004-02-29 19:00:00                             |
+-------------------------------------------------+
```
  
The valid time zone range is (-13:59, +14:00) inclusive. The following example shows that time zones outside of this range return `NULL`:
  
```ppl
source=people
| eval  `DATETIME('2008-01-01 02:00:00', '-14:00')` = DATETIME('2008-01-01 02:00:00', '-14:00')
| fields `DATETIME('2008-01-01 02:00:00', '-14:00')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------------------------------+
| DATETIME('2008-01-01 02:00:00', '-14:00') |
|-------------------------------------------|
| null                                      |
+-------------------------------------------+
```
  
## DATE_SUB

**Usage**: `DATE_SUB(date, INTERVAL expr unit)`

Subtracts the interval `expr` from `date`. If the first argument is `TIME`, today's date is used. If the first argument is `DATE`, the time at midnight is used.

**Parameters**:
- `date` (Required): A `DATE`, `TIMESTAMP`, or `TIME` value.
- `INTERVAL expr unit` (Required): An `INTERVAL` expression.

**Return type**: `TIMESTAMP`

Synonyms: [`SUBDATE`](#subdate)
Antonyms: [`DATE_ADD`](#date_add)

#### Example
  
```ppl
source=people
| eval `'2008-01-02' - 31d` = DATE_SUB(DATE('2008-01-02'), INTERVAL 31 DAY), `ts '2020-08-26 01:01:01' + 1h` = DATE_SUB(TIMESTAMP('2020-08-26 01:01:01'), INTERVAL 1 HOUR)
| fields `'2008-01-02' - 31d`, `ts '2020-08-26 01:01:01' + 1h`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------+-------------------------------+
| '2008-01-02' - 31d  | ts '2020-08-26 01:01:01' + 1h |
|---------------------+-------------------------------|
| 2007-12-02 00:00:00 | 2020-08-26 00:01:01           |
+---------------------+-------------------------------+
```
  
## DATEDIFF

**Usage**: `DATEDIFF(date1, date2)`
Calculates the difference of the date parts of given values. If the first argument is `TIME`, today's date is used.

**Parameters**:
- `date1` (Required): A `DATE`, `TIMESTAMP`, or `TIME` value.
- `date2` (Required): A `DATE`, `TIMESTAMP`, or `TIME` value.

**Return type**: `LONG`

#### Example
  
```ppl
source=people
| eval `'2000-01-02' - '2000-01-01'` = DATEDIFF(TIMESTAMP('2000-01-02 00:00:00'), TIMESTAMP('2000-01-01 23:59:59')), `'2001-02-01' - '2004-01-01'` = DATEDIFF(DATE('2001-02-01'), TIMESTAMP('2004-01-01 00:00:00')), `today - today` = DATEDIFF(TIME('23:59:59'), TIME('00:00:00'))
| fields `'2000-01-02' - '2000-01-01'`, `'2001-02-01' - '2004-01-01'`, `today - today`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------+-----------------------------+---------------+
| '2000-01-02' - '2000-01-01' | '2001-02-01' - '2004-01-01' | today - today |
|-----------------------------+-----------------------------+---------------|
| 1                           | -1064                       | 0             |
+-----------------------------+-----------------------------+---------------+
```
  
## DAY

**Usage**: `DAY(date)`

Extracts the day of the month for `date`, in the range 1 to 31.

**Parameters**:
- `date` (Required): A `STRING`, `DATE`, or `TIMESTAMP` value.

**Return type**: `INTEGER`

Synonyms: [`DAYOFMONTH`](#dayofmonth), [`DAY_OF_MONTH`](#day_of_month)

#### Example
  
```ppl
source=people
| eval `DAY(DATE('2020-08-26'))` = DAY(DATE('2020-08-26'))
| fields `DAY(DATE('2020-08-26'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------------+
| DAY(DATE('2020-08-26')) |
|-------------------------|
| 26                      |
+-------------------------+
```
  
## DAYNAME

**Usage**: `DAYNAME(date)`

Returns the name of the weekday for `date`.

**Parameters**:
- `date` (Required): A `STRING`, `DATE`, or `TIMESTAMP` value.

**Return type**: `STRING`

#### Example
  
```ppl
source=people
| eval `DAYNAME(DATE('2020-08-26'))` = DAYNAME(DATE('2020-08-26'))
| fields `DAYNAME(DATE('2020-08-26'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------+
| DAYNAME(DATE('2020-08-26')) |
|-----------------------------|
| Wednesday                   |
+-----------------------------+
```
  
## DAYOFMONTH

**Usage**: `DAYOFMONTH(date)`

Extracts the day of the month for `date`, in the range 1 to 31.

**Parameters**:
- `date` (Required): A `STRING`, `DATE`, or `TIMESTAMP` value.

**Return type**: `INTEGER`

Synonyms: [`DAY`](#day), [`DAY_OF_MONTH`](#day_of_month)

#### Example
  
```ppl
source=people
| eval `DAYOFMONTH(DATE('2020-08-26'))` = DAYOFMONTH(DATE('2020-08-26'))
| fields `DAYOFMONTH(DATE('2020-08-26'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------------------------+
| DAYOFMONTH(DATE('2020-08-26')) |
|--------------------------------|
| 26                             |
+--------------------------------+
```
  
## DAY_OF_MONTH

**Usage**: `DAY_OF_MONTH(date)`

Extracts the day of the month for `date`, in the range 1 to 31.

**Parameters**:
- `date` (Required): A `STRING`, `DATE`, or `TIMESTAMP` value.

**Return type**: `INTEGER`

Synonyms: [`DAY`](#day), [`DAYOFMONTH`](#dayofmonth)

#### Example
  
```ppl
source=people
| eval `DAY_OF_MONTH(DATE('2020-08-26'))` = DAY_OF_MONTH(DATE('2020-08-26'))
| fields `DAY_OF_MONTH(DATE('2020-08-26'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------------------------+
| DAY_OF_MONTH(DATE('2020-08-26')) |
|----------------------------------|
| 26                               |
+----------------------------------+
```
  
## DAYOFWEEK

**Usage**: `DAYOFWEEK(date)`

Returns the weekday index for `date` (1 = Sunday, 2 = Monday, ..., 7 = Saturday).

**Parameters**:
- `date` (Required): A `STRING`, `DATE`, or `TIMESTAMP` value.

**Return type**: `INTEGER`

Synonyms: [`DAY_OF_WEEK`](#day_of_week)

#### Example
  
```ppl
source=people
| eval `DAYOFWEEK(DATE('2020-08-26'))` = DAYOFWEEK(DATE('2020-08-26'))
| fields `DAYOFWEEK(DATE('2020-08-26'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------------------+
| DAYOFWEEK(DATE('2020-08-26')) |
|-------------------------------|
| 4                             |
+-------------------------------+
```
  
## DAY_OF_WEEK

**Usage**: `DAY_OF_WEEK(date)`

Returns the weekday index for `date` (1 = Sunday, 2 = Monday, ..., 7 = Saturday).

**Parameters**:
- `date` (Required): A `STRING`, `DATE`, or `TIMESTAMP` value.

**Return type**: `INTEGER`

Synonyms: [`DAYOFWEEK`](#dayofweek)

#### Example
  
```ppl
source=people
| eval `DAY_OF_WEEK(DATE('2020-08-26'))` = DAY_OF_WEEK(DATE('2020-08-26'))
| fields `DAY_OF_WEEK(DATE('2020-08-26'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------------------+
| DAY_OF_WEEK(DATE('2020-08-26')) |
|---------------------------------|
| 4                               |
+---------------------------------+
```
  
## DAYOFYEAR

**Usage**: `DAYOFYEAR(date)`

Returns the day of the year for `date`, in the range 1 to 366.

**Parameters**:
- `date` (Required): A `STRING`, `DATE`, or `TIMESTAMP` value.

**Return type**: `INTEGER`

Synonyms: [`DAY_OF_YEAR`](#day_of_year)

#### Example
  
```ppl
source=people
| eval `DAYOFYEAR(DATE('2020-08-26'))` = DAYOFYEAR(DATE('2020-08-26'))
| fields `DAYOFYEAR(DATE('2020-08-26'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------------------+
| DAYOFYEAR(DATE('2020-08-26')) |
|-------------------------------|
| 239                           |
+-------------------------------+
```
  
## DAY_OF_YEAR

**Usage**: `DAY_OF_YEAR(date)`

Returns the day of the year for `date`, in the range 1 to 366.

**Parameters**:
- `date` (Required): A `STRING`, `DATE`, or `TIMESTAMP` value.

**Return type**: `INTEGER`

Synonyms: [`DAYOFYEAR`](#dayofyear)

#### Example
  
```ppl
source=people
| eval `DAY_OF_YEAR(DATE('2020-08-26'))` = DAY_OF_YEAR(DATE('2020-08-26'))
| fields `DAY_OF_YEAR(DATE('2020-08-26'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------------------+
| DAY_OF_YEAR(DATE('2020-08-26')) |
|---------------------------------|
| 239                             |
+---------------------------------+
```
  
## EXTRACT

**Usage**: `EXTRACT(part FROM date)`

Returns a `LONG` containing digits in order according to the given `part` argument. The specific format of the returned `LONG` is determined by the following table.

**Parameters**:
- `part` (Required): A part token (see following table).
- `date` (Required): A `STRING`, `DATE`, `TIME`, or `TIMESTAMP` value.

**Return type**: `LONG`

The format specifiers found in this table are the same as those found in the [`DATE_FORMAT`](#date_format) function. The following table describes the mapping of a `part` to a particular format.


| `part` | Format |
| --- | --- |
| `MICROSECOND` | `%f` |
| `SECOND` | `%s` |
| `MINUTE` | `%i` |
| `HOUR` | `%H` |
| `DAY` | `%d` |
| `WEEK` | `%X` |
| `MONTH` | `%m` |
| `YEAR` | `%V` |
| `SECOND_MICROSECOND` | `%s%f` |
| `MINUTE_MICROSECOND` | `%i%s%f` |
| `MINUTE_SECOND` | `%i%s` |
| `HOUR_MICROSECOND` | `%H%i%s%f` |
| `HOUR_SECOND` | `%H%i%s` |
| `HOUR_MINUTE` | `%H%i` |
| `DAY_MICROSECOND` | `%d%H%i%s%f` |
| `DAY_SECOND` | `%d%H%i%s` |
| `DAY_MINUTE` | `%d%H%i` |
| `DAY_HOUR` | `%d%H%` |
| `YEAR_MONTH` | `%V%m` |

#### Example
  
```ppl
source=people
| eval `extract(YEAR_MONTH FROM "2023-02-07 10:11:12")` = extract(YEAR_MONTH FROM "2023-02-07 10:11:12")
| fields `extract(YEAR_MONTH FROM "2023-02-07 10:11:12")`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+------------------------------------------------+
| extract(YEAR_MONTH FROM "2023-02-07 10:11:12") |
|------------------------------------------------|
| 202302                                         |
+------------------------------------------------+
```
  
## FROM_DAYS

**Usage**: `FROM_DAYS(N)`

Returns the date value given the day number `N`.

**Parameters**:
- `N` (Required): An `INTEGER` or `LONG` value.

**Return type**: `DATE`

#### Example
  
```ppl
source=people
| eval `FROM_DAYS(733687)` = FROM_DAYS(733687)
| fields `FROM_DAYS(733687)`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------+
| FROM_DAYS(733687) |
|-------------------|
| 2008-10-07        |
+-------------------+
```
  
## FROM_UNIXTIME

**Usage**: `FROM_UNIXTIME(timestamp)` or `FROM_UNIXTIME(timestamp, format)`

Returns a representation of the argument as a timestamp or character string value. Performs the reverse conversion for the [`UNIX_TIMESTAMP`](#unix_timestamp) function. If the second argument is provided, it is used to format the result in the same way as the format string used for the [`DATE_FORMAT`](#date_format) function. If the timestamp is outside the range 1970-01-01 00:00:00 - 3001-01-18 23:59:59.999999 (0 to 32536771199.999999 epoch time), the function returns `NULL`.

**Parameters**:
- `timestamp` (Required): A `DOUBLE` value representing Unix timestamp.
- `format` (Optional): A `STRING` format specifier.

**Return type**: `TIMESTAMP` (without format), `STRING` (with format)

**Examples**
  
```ppl
source=people
| eval `FROM_UNIXTIME(1220249547)` = FROM_UNIXTIME(1220249547)
| fields `FROM_UNIXTIME(1220249547)`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------------+
| FROM_UNIXTIME(1220249547) |
|---------------------------|
| 2008-09-01 06:12:27       |
+---------------------------+
```
  
```ppl
source=people
| eval `FROM_UNIXTIME(1220249547, '%T')` = FROM_UNIXTIME(1220249547, '%T')
| fields `FROM_UNIXTIME(1220249547, '%T')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------------------+
| FROM_UNIXTIME(1220249547, '%T') |
|---------------------------------|
| 06:12:27                        |
+---------------------------------+
```
  
## GET_FORMAT

**Usage**: `GET_FORMAT(type, format)`

Returns a string value containing string format specifiers based on the input arguments.

**Parameters**:
- `type` (Required): One of the following tokens: `DATE`, `TIME`, `TIMESTAMP`.
- `format` (Required): A `STRING` that must be one of: `USA`, `JIS`, `ISO`, `EUR`, `INTERNAL`.

**Return type**: `STRING`

**Examples**
  
```ppl
source=people
| eval `GET_FORMAT(DATE, 'USA')` = GET_FORMAT(DATE, 'USA')
| fields `GET_FORMAT(DATE, 'USA')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------------+
| GET_FORMAT(DATE, 'USA') |
|-------------------------|
| %m.%d.%Y                |
+-------------------------+
```
  
## HOUR

**Usage**: `HOUR(time)`

Extracts the hour value for `time`. Different from a time of day value, the time value has a large range and can be greater than 23, so the return value of `HOUR(time)` can also be greater than 23.

**Parameters**:
- `time` (Required): A `STRING`, `TIME`, or `TIMESTAMP` value.

**Return type**: `INTEGER`

Synonyms: [`HOUR_OF_DAY`](#hour_of_day)

#### Example
  
```ppl
source=people
| eval `HOUR(TIME('01:02:03'))` = HOUR(TIME('01:02:03'))
| fields `HOUR(TIME('01:02:03'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+------------------------+
| HOUR(TIME('01:02:03')) |
|------------------------|
| 1                      |
+------------------------+
```
  
## HOUR_OF_DAY

**Usage**: `HOUR_OF_DAY(time)`

Extracts the hour value for `time`. Different from a time of day value, the time value has a large range and can be greater than 23, so the return value of `HOUR_OF_DAY(time)` can also be greater than 23.

**Parameters**:
- `time` (Required): A `STRING`, `TIME`, or `TIMESTAMP` value.

**Return type**: `INTEGER`

Synonyms: [`HOUR`](#hour)

#### Example
  
```ppl
source=people
| eval `HOUR_OF_DAY(TIME('01:02:03'))` = HOUR_OF_DAY(TIME('01:02:03'))
| fields `HOUR_OF_DAY(TIME('01:02:03'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------------------+
| HOUR_OF_DAY(TIME('01:02:03')) |
|-------------------------------|
| 1                             |
+-------------------------------+
```
  
## LAST_DAY  

**Usage**: `LAST_DAY(date)`

Returns the last day of the month as a `DATE` for a valid argument.

**Parameters**:
- `date` (Required): A `DATE`, `STRING`, `TIMESTAMP`, or `TIME` value.

**Return type**: `DATE`

#### Example
  
```ppl
source=people
| eval `last_day('2023-02-06')` = last_day('2023-02-06')
| fields `last_day('2023-02-06')`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+------------------------+
| last_day('2023-02-06') |
|------------------------|
| 2023-02-28             |
+------------------------+
```
  
## LOCALTIMESTAMP

**Usage**: `LOCALTIMESTAMP()`

`LOCALTIMESTAMP()` is a synonym for [`NOW()`](#now).

**Parameters**: None

**Return type**: `TIMESTAMP`

#### Example
  
```ppl ignore
source=people
| eval `LOCALTIMESTAMP()` = LOCALTIMESTAMP()
| fields `LOCALTIMESTAMP()`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------+
| LOCALTIMESTAMP()    |
|---------------------+
| 2025-08-02 15:54:19 |
+---------------------+
```
  
## LOCALTIME

**Usage**: `LOCALTIME()`

`LOCALTIME()` is a synonym for [`NOW()`](#now).

**Parameters**: None

**Return type**: `TIMESTAMP`

#### Example
  
```ppl ignore
source=people
| eval `LOCALTIME()` = LOCALTIME()
| fields `LOCALTIME()`
```
  
The query returns the following results:
  
```text ignore
fetched rows / total rows = 1/1
+---------------------+
| LOCALTIME()         |
|---------------------+
| 2025-08-02 15:54:19 |
+---------------------+
```
  
## MAKEDATE

**Usage**: `MAKEDATE(year, dayofyear)`

Returns a date, given `year` and `day-of-year` values. `dayofyear` must be greater than 0, otherwise the result is `NULL`. The result is also `NULL` if either argument is `NULL`. Arguments are rounded to an integer.

**Parameters**:
- `year` (Required): A `DOUBLE` value for the year.
- `dayofyear` (Required): A `DOUBLE` value for the day of year.

**Return type**: `DATE`

Limitations:
- A zero `year` is interpreted as 2000
- A negative `year` is not accepted
- `day-of-year` should be greater than zero
- `day-of-year` can be greater than 365/366, and the calculation switches to the next year(s) (see example)

#### Example
  
```ppl
source=people
| eval `MAKEDATE(1945, 5.9)` = MAKEDATE(1945, 5.9), `MAKEDATE(1984, 1984)` = MAKEDATE(1984, 1984)
| fields `MAKEDATE(1945, 5.9)`, `MAKEDATE(1984, 1984)`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------+----------------------+
| MAKEDATE(1945, 5.9) | MAKEDATE(1984, 1984) |
|---------------------+----------------------|
| 1945-01-06          | 1989-06-06           |
+---------------------+----------------------+
```
  
## MAKETIME

**Usage**: `MAKETIME(hour, minute, second)`

Returns a time value calculated from the hour, minute, and second arguments. Returns `NULL` if any of its arguments are `NULL`. The second argument can have a fractional part, and the rest of the arguments are rounded to an integer.

**Parameters**:
- `hour` (Required): A `DOUBLE` value for the hour.
- `minute` (Required): A `DOUBLE` value for the minute.
- `second` (Required): A `DOUBLE` value for the second.

**Return type**: `TIME`

Limitations:
- A 24-hour clock is used, and the available time range is [00:00:00.0 - 23:59:59.(9)]
- Up to 9 digits of the second fraction part are taken (nanosecond precision)

#### Example
  
```ppl
source=people
| eval `MAKETIME(20, 30, 40)` = MAKETIME(20, 30, 40), `MAKETIME(20.2, 49.5, 42.100502)` = MAKETIME(20.2, 49.5, 42.100502)
| fields `MAKETIME(20, 30, 40)`, `MAKETIME(20.2, 49.5, 42.100502)`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------------+---------------------------------+
| MAKETIME(20, 30, 40) | MAKETIME(20.2, 49.5, 42.100502) |
|----------------------+---------------------------------|
| 20:30:40             | 20:50:42.100502                 |
+----------------------+---------------------------------+
```
  
## MICROSECOND

**Usage**: `MICROSECOND(expr)`

Returns the microseconds from the time or timestamp expression `expr` as a number in the range from 0 to 999999.

**Parameters**:
- `expr` (Required): A `STRING`, `TIME`, or `TIMESTAMP` value.

**Return type**: `INTEGER`

#### Example
  
```ppl
source=people
| eval `MICROSECOND(TIME('01:02:03.123456'))` = MICROSECOND(TIME('01:02:03.123456'))
| fields `MICROSECOND(TIME('01:02:03.123456'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------------------------------+
| MICROSECOND(TIME('01:02:03.123456')) |
|--------------------------------------|
| 123456                               |
+--------------------------------------+
```
  
## MINUTE

**Usage**: `MINUTE(time)`

Returns the minute for `time`, in the range 0 to 59.

**Parameters**:
- `time` (Required): A `STRING`, `TIME`, or `TIMESTAMP` value.

**Return type**: `INTEGER`

Synonyms: [`MINUTE_OF_HOUR`](#minute_of_hour)

#### Example
  
```ppl
source=people
| eval `MINUTE(TIME('01:02:03'))` =  MINUTE(TIME('01:02:03'))
| fields `MINUTE(TIME('01:02:03'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------------------+
| MINUTE(TIME('01:02:03')) |
|--------------------------|
| 2                        |
+--------------------------+
```
  
## MINUTE_OF_DAY

**Usage**: `MINUTE_OF_DAY(time)`

Returns the amount of minutes in the day, in the range of 0 to 1439.

**Parameters**:
- `time` (Required): A `STRING`, `TIME`, or `TIMESTAMP` value.

**Return type**: `INTEGER`

#### Example
  
```ppl
source=people
| eval `MINUTE_OF_DAY(TIME('01:02:03'))` = MINUTE_OF_DAY(TIME('01:02:03'))
| fields `MINUTE_OF_DAY(TIME('01:02:03'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------------------+
| MINUTE_OF_DAY(TIME('01:02:03')) |
|---------------------------------|
| 62                              |
+---------------------------------+
```
  
## MINUTE_OF_HOUR

**Usage**: `MINUTE_OF_HOUR(time)`

Returns the minute for `time`, in the range 0 to 59.

**Parameters**:
- `time` (Required): A `STRING`, `TIME`, or `TIMESTAMP` value.

**Return type**: `INTEGER`

Synonyms: [`MINUTE`](#minute)

#### Example
  
```ppl
source=people
| eval `MINUTE_OF_HOUR(TIME('01:02:03'))` =  MINUTE_OF_HOUR(TIME('01:02:03'))
| fields `MINUTE_OF_HOUR(TIME('01:02:03'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------------------------+
| MINUTE_OF_HOUR(TIME('01:02:03')) |
|----------------------------------|
| 2                                |
+----------------------------------+
```
  
## MONTH

**Usage**: `MONTH(date)`

Returns the month for `date`, in the range 1 to 12 for January to December.

**Parameters**:
- `date` (Required): A `STRING`, `DATE`, or `TIMESTAMP` value.

**Return type**: `INTEGER`

Synonyms: [`MONTH_OF_YEAR`](#month_of_year)

#### Example
  
```ppl
source=people
| eval `MONTH(DATE('2020-08-26'))` =  MONTH(DATE('2020-08-26'))
| fields `MONTH(DATE('2020-08-26'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------------+
| MONTH(DATE('2020-08-26')) |
|---------------------------|
| 8                         |
+---------------------------+
```
  
## MONTH_OF_YEAR

**Usage**: `MONTH_OF_YEAR(date)`

Returns the month for `date`, in the range 1 to 12 for January to December.

**Parameters**:
- `date` (Required): A `STRING`, `DATE`, or `TIMESTAMP` value.

**Return type**: `INTEGER`

Synonyms: [`MONTH`](#month)

#### Example
  
```ppl
source=people
| eval `MONTH_OF_YEAR(DATE('2020-08-26'))` =  MONTH_OF_YEAR(DATE('2020-08-26'))
| fields `MONTH_OF_YEAR(DATE('2020-08-26'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------------+
| MONTH_OF_YEAR(DATE('2020-08-26')) |
|-----------------------------------|
| 8                                 |
+-----------------------------------+
```
  
## MONTHNAME

**Usage**: `MONTHNAME(date)`

Returns the full name of the month for `date`.

**Parameters**:
- `date` (Required): A `STRING`, `DATE`, or `TIMESTAMP` value.

**Return type**: `STRING`

#### Example
  
```ppl
source=people
| eval `MONTHNAME(DATE('2020-08-26'))` = MONTHNAME(DATE('2020-08-26'))
| fields `MONTHNAME(DATE('2020-08-26'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------------------+
| MONTHNAME(DATE('2020-08-26')) |
|-------------------------------|
| August                        |
+-------------------------------+
```
  
## NOW

**Usage**: `NOW()`

Returns the current date and time as a value in 'YYYY-MM-DD hh:mm:ss' format. The value is expressed in the UTC time zone. `NOW()` returns a constant time that indicates the time at which the statement began to execute. This differs from the behavior for [`SYSDATE()`](#sysdate), which returns the exact time at which it executes.

**Parameters**: None

**Return type**: `TIMESTAMP`

#### Example
  
```ppl ignore
source=people
| eval `value_1` = NOW(), `value_2` = NOW()
| fields `value_1`, `value_2`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------+---------------------+
| value_1             | value_2             |
|---------------------+---------------------|
| 2025-08-02 15:39:05 | 2025-08-02 15:39:05 |
+---------------------+---------------------+
```
  
## PERIOD_ADD

**Usage**: `PERIOD_ADD(P, N)`

Adds `N` months to period `P` (in the format YYMM or YYYYMM). Returns a value in the format YYYYMM.

**Parameters**:
- `P` (Required): An `INTEGER` value representing a period in YYMM or YYYYMM format.
- `N` (Required): An `INTEGER` number of months to add.

**Return type**: `INTEGER`

#### Example
  
```ppl
source=people
| eval `PERIOD_ADD(200801, 2)` = PERIOD_ADD(200801, 2), `PERIOD_ADD(200801, -12)` = PERIOD_ADD(200801, -12)
| fields `PERIOD_ADD(200801, 2)`, `PERIOD_ADD(200801, -12)`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------+-------------------------+
| PERIOD_ADD(200801, 2) | PERIOD_ADD(200801, -12) |
|-----------------------+-------------------------|
| 200803                | 200701                  |
+-----------------------+-------------------------+
```
  
## PERIOD_DIFF

**Usage**: `PERIOD_DIFF(P1, P2)`

Returns the number of months between periods `P1` and `P2` given in the format YYMM or YYYYMM.

**Parameters**:
- `P1` (Required): An `INTEGER` value representing a period in YYMM or YYYYMM format.
- `P2` (Required): An `INTEGER` value representing a period in YYMM or YYYYMM format.

**Return type**: `INTEGER`

#### Example
  
```ppl
source=people
| eval `PERIOD_DIFF(200802, 200703)` = PERIOD_DIFF(200802, 200703), `PERIOD_DIFF(200802, 201003)` = PERIOD_DIFF(200802, 201003)
| fields `PERIOD_DIFF(200802, 200703)`, `PERIOD_DIFF(200802, 201003)`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------+-----------------------------+
| PERIOD_DIFF(200802, 200703) | PERIOD_DIFF(200802, 201003) |
|-----------------------------+-----------------------------|
| 11                          | -25                         |
+-----------------------------+-----------------------------+
```
  
## QUARTER

**Usage**: `QUARTER(date)`

Returns the quarter of the year for `date`, in the range 1 to 4.

**Parameters**:
- `date` (Required): A `STRING`, `DATE`, or `TIMESTAMP` value.

**Return type**: `INTEGER`

#### Example
  
```ppl
source=people
| eval `QUARTER(DATE('2020-08-26'))` = QUARTER(DATE('2020-08-26'))
| fields `QUARTER(DATE('2020-08-26'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------------------+
| QUARTER(DATE('2020-08-26')) |
|-----------------------------|
| 3                           |
+-----------------------------+
```
  
## SEC_TO_TIME

**Usage**: `SEC_TO_TIME(number)`

Returns the time in HH:mm:ss[.nnnnnn] format. Note that the function returns a time between 00:00:00 and 23:59:59. If the input value is too large (greater than 86399), the function will wrap around and begin returning outputs starting from 00:00:00. If the input value is too small (less than 0), the function will wrap around and begin returning outputs counting down from 23:59:59.

**Parameters**:
- `number` (Required): An `INTEGER`, `LONG`, `DOUBLE`, or `FLOAT` value.

**Return type**: `TIME`

#### Example
  
```ppl
source=people
| eval `SEC_TO_TIME(3601)` = SEC_TO_TIME(3601)
| eval `SEC_TO_TIME(1234.123)` = SEC_TO_TIME(1234.123)
| fields `SEC_TO_TIME(3601)`, `SEC_TO_TIME(1234.123)`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------+-----------------------+
| SEC_TO_TIME(3601) | SEC_TO_TIME(1234.123) |
|-------------------+-----------------------|
| 01:00:01          | 00:20:34.123          |
+-------------------+-----------------------+
```
  
## SECOND

**Usage**: `SECOND(time)`

Returns the second for `time`, in the range 0 to 59.

**Parameters**:
- `time` (Required): A `STRING`, `TIME`, or `TIMESTAMP` value.

**Return type**: `INTEGER`

Synonyms: [`SECOND_OF_MINUTE`](#second_of_minute)

#### Example
  
```ppl
source=people
| eval `SECOND(TIME('01:02:03'))` = SECOND(TIME('01:02:03'))
| fields `SECOND(TIME('01:02:03'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------------------+
| SECOND(TIME('01:02:03')) |
|--------------------------|
| 3                        |
+--------------------------+
```
  
## SECOND_OF_MINUTE

**Usage**: `SECOND_OF_MINUTE(time)`

Returns the second for `time`, in the range 0 to 59.

**Parameters**:
- `time` (Required): A `STRING`, `TIME`, or `TIMESTAMP` value.

**Return type**: `INTEGER`

Synonyms: [`SECOND`](#second)

#### Example
  
```ppl
source=people
| eval `SECOND_OF_MINUTE(TIME('01:02:03'))` = SECOND_OF_MINUTE(TIME('01:02:03'))
| fields `SECOND_OF_MINUTE(TIME('01:02:03'))`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+------------------------------------+
| SECOND_OF_MINUTE(TIME('01:02:03')) |
|------------------------------------|
| 3                                  |
+------------------------------------+
```
  
## STRFTIME

**Usage**: `STRFTIME(time, format)`

Takes a UNIX timestamp (in seconds) and renders it as a string using the format specified. For numeric inputs, the UNIX time must be in seconds. Values greater than 100000000000 are automatically treated as milliseconds and converted to seconds. You can use time format variables with the strftime function. This function performs the reverse operation of [`UNIX_TIMESTAMP`](#unix_timestamp) and is similar to [`FROM_UNIXTIME`](#from_unixtime) but with POSIX-style format specifiers.

**Parameters**:
- `time` (Required): An `INTEGER`, `LONG`, `DOUBLE`, or `TIMESTAMP` value.
- `format` (Required): A `STRING` format specifier.

**Return type**: `STRING`

**Notes**:
- Available only when Calcite engine is enabled
    - All timestamps are interpreted as UTC timezone  
    - Text formatting uses language-neutral Locale.ROOT (weekday and month names appear in abbreviated form)  
    - String inputs are NOT supported - use `unix_timestamp()` to convert strings first  
    - Functions that return date/time values (like `date()`, `now()`, `timestamp()`) are supported  
  
The following table describes the available specifier arguments:


| Specifier | Description |
| --- | --- |
| `%a` | Abbreviated weekday name (Mon..Sun) |
| `%A` | Weekday name (Mon..Sun) - Note: Locale.ROOT uses abbreviated form |
| `%b` | Abbreviated month name (Jan..Dec) |
| `%B` | Month name (Jan..Dec) - Note: Locale.ROOT uses abbreviated form |
| `%c` | Date and time (e.g., Mon Jul 18 09:30:00 2019) |
| `%C` | Century as 2-digit decimal number |
| `%d` | Day of the month, zero-padded (01..31) |
| `%e` | Day of the month, space-padded ( 1..31) |
| `%Ez` | Timezone offset in minutes from UTC (e.g., +0 for UTC, +330 for IST, -300 for EST) |
| `%f` | Microseconds as decimal number (000000..999999) |
| `%F` | ISO 8601 date format (`%Y-%m-%d`) |
| `%g` | ISO 8601 year without century (00..99) |
| `%G` | ISO 8601 year with century |
| `%H` | Hour (24-hour clock) (00..23) |
| `%I` | Hour (12-hour clock) (01..12) |
| `%j` | Day of year (001..366) |
| `%k` | Hour (24-hour clock), space-padded ( 0..23) |
| `%m` | Month as decimal number (01..12) |
| `%M` | Minute (00..59) |
| `%N` | Subsecond digits (default `%9N` = nanoseconds). Accepts any precision value from 1-9 (e.g., `%3N` = 3 digits, `%5N` = 5 digits, `%9N` = 9 digits). The precision directly controls the number of digits displayed |
| `%p` | AM or PM |
| `%Q` | Subsecond component (default milliseconds). Can specify precision: `%3Q` = milliseconds, `%6Q` = microseconds, `%9Q` = nanoseconds. Other precision values (e.g., `%5Q`) default to `%3Q` |
| `%s` | UNIX Epoch timestamp in seconds |
| `%S` | Second (00..59) |
| `%T` | Time in 24-hour notation (`%H:%M:%S`) |
| `%U` | Week of year starting from 0 (00..53) |
| `%V` | ISO week number (01..53) |
| `%w` | Weekday as decimal (0=Sunday..6=Saturday) |
| `%x` | Date in MM/dd/yyyy format (e.g., 07/13/2019) |
| `%X` | Time in HH:mm:ss format (e.g., 09:30:00) |
| `%y` | Year without century (00..99) |
| `%Y` | Year with century |
| `%z` | Timezone offset (+hhmm or -hhmm) |
| `%:z` | Timezone offset with colon (+hh:mm or -hh:mm) |
| `%::z` | Timezone offset with colons (+hh:mm:ss) |
| `%:::z` | Timezone offset hour only (+hh or -hh) |
| `%Z` | Timezone abbreviation (e.g., EST, PDT) |
| `%%` | Literal % character |
  

**Examples**
  
```ppl ignore
source=people | eval `strftime(1521467703, "%Y-%m-%dT%H:%M:%S")` = strftime(1521467703, "%Y-%m-%dT%H:%M:%S") | fields `strftime(1521467703, "%Y-%m-%dT%H:%M:%S")`
```
  
```text
fetched rows / total rows = 1/1
+-------------------------------------------+
| strftime(1521467703, "%Y-%m-%dT%H:%M:%S") |
|-------------------------------------------|
| 2018-03-19T13:55:03                       |
+-------------------------------------------+
```
  
```ppl ignore
source=people | eval `strftime(1521467703, "%F %T")` = strftime(1521467703, "%F %T") | fields `strftime(1521467703, "%F %T")`
```
  
```text
fetched rows / total rows = 1/1
+-------------------------------------------+
| strftime(1521467703, "%Y-%m-%dT%H:%M:%S") |
|-------------------------------------------|
| 2018-03-19T13:55:03                       |
+-------------------------------------------+
```
  
```ppl ignore
source=people | eval `strftime(1521467703, "%a %b %d, %Y")` = strftime(1521467703, "%a %b %d, %Y") | fields `strftime(1521467703, "%a %b %d, %Y")`
```
  
```text
fetched rows / total rows = 1/1
+--------------------------------------+
| strftime(1521467703, "%a %b %d, %Y") |
|--------------------------------------|
| Mon Mar 19, 2018                     |
+--------------------------------------+
```
  
```ppl ignore
source=people | eval `strftime(1521467703, "%%Y")` = strftime(1521467703, "%%Y") | fields `strftime(1521467703, "%%Y")`
```
  
```text
fetched rows / total rows = 1/1
+---------------------------+
| strftime(1521467703, "%%Y") |
|---------------------------|
| %Y                        |
+---------------------------+
```
  
```ppl ignore
source=people | eval `strftime(date('2020-09-16'), "%Y-%m-%d")` = strftime(date('2020-09-16'), "%Y-%m-%d") | fields `strftime(date('2020-09-16'), "%Y-%m-%d")`
```text
  
fetched rows / total rows = 1/1
+----------------------------------------+
| strftime(date('2020-09-16'), "%Y-%m-%d") |
|-----------------------------------------|
| 2020-09-16                             |
  
+----------------------------------------+
  
```
```ppl ignore
  
source=people | eval `strftime(timestamp('2020-09-16 14:30:00'), "%F %T")` = strftime(timestamp('2020-09-16 14:30:00'), "%F %T") | fields `strftime(timestamp('2020-09-16 14:30:00'), "%F %T")`
  
```
```text
  
fetched rows / total rows = 1/1
+--------------------------------------------------+
| strftime(timestamp('2020-09-16 14:30:00'), "%F %T") |
|---------------------------------------------------|
| 2020-09-16 14:30:00                              |
  
+--------------------------------------------------+
  
```
```ppl ignore
  
source=people | eval `strftime(now(), "%Y-%m-%d %H:%M:%S")` = strftime(now(), "%Y-%m-%d %H:%M:%S") | fields `strftime(now(), "%Y-%m-%d %H:%M:%S")`
  
```
```text
  
fetched rows / total rows = 1/1
+------------------------------------+
| strftime(now(), "%Y-%m-%d %H:%M:%S") |
|-------------------------------------|
| 2025-09-03 12:30:45                |
  
+------------------------------------+
  
```
## STR_TO_DATE

**Usage**: `STR_TO_DATE(string, format)`

Extracts a `TIMESTAMP` from the first argument string using the formats specified in the second argument string. The input argument must have enough information to be parsed as a `DATE`, `TIMESTAMP`, or `TIME`. Acceptable string format specifiers are the same as those used in the [`DATE_FORMAT`](#date_format) function. Returns `NULL` when the statement cannot be parsed due to an invalid pair of arguments, and when 0 is provided for any `DATE` field. Otherwise, returns a `TIMESTAMP` with the parsed values (as well as default values for any field that was not parsed).

**Parameters**:
- `string` (Required): A `STRING` value to parse.
- `format` (Required): A `STRING` format specifier.

**Return type**: `TIMESTAMP`

#### Example

```ppl
  
source=people
| eval `str_to_date("01,5,2013", "%d,%m,%Y")` = str_to_date("01,5,2013", "%d,%m,%Y")
| fields `str_to_date("01,5,2013", "%d,%m,%Y")`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+--------------------------------------+
| str_to_date("01,5,2013", "%d,%m,%Y") |
|--------------------------------------|
| 2013-05-01 00:00:00                  |
  
+--------------------------------------+
  
```
  
## SUBDATE

**Usage**: `SUBDATE(date, INTERVAL expr unit)` or `SUBDATE(date, days)`

Subtracts the interval `expr` from `date`, or subtracts the second argument as an integer number of days from `date`. If the first argument is `TIME`, today's date is used. If the first argument is `DATE`, the time at midnight is used.

**Parameters**:
- `date` (Required): A `DATE`, `TIMESTAMP`, or `TIME` value.
- `expr` (Required): Either an `INTERVAL` expression or a `LONG` number of days.

**Return type**: `TIMESTAMP` (with INTERVAL), `DATE` (DATE with LONG), `TIMESTAMP` (TIMESTAMP/TIME with LONG)

Synonyms: [`DATE_SUB`](#date_sub) when invoked with the INTERVAL form of the second argument
Antonyms: [`ADDDATE`](#adddate)

#### Example

```ppl
  
source=people
| eval `'2008-01-02' - 31d` = SUBDATE(DATE('2008-01-02'), INTERVAL 31 DAY), `'2020-08-26' - 1` = SUBDATE(DATE('2020-08-26'), 1), `ts '2020-08-26 01:01:01' - 1` = SUBDATE(TIMESTAMP('2020-08-26 01:01:01'), 1)
| fields `'2008-01-02' - 31d`, `'2020-08-26' - 1`, `ts '2020-08-26 01:01:01' - 1`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+---------------------+------------------+------------------------------+
| '2008-01-02' - 31d  | '2020-08-26' - 1 | ts '2020-08-26 01:01:01' - 1 |
|---------------------+------------------+------------------------------|
| 2007-12-02 00:00:00 | 2020-08-25       | 2020-08-25 01:01:01          |
  
+---------------------+------------------+------------------------------+
  
```
  
## SUBTIME

**Usage**: `SUBTIME(expr1, expr2)`

Subtracts `expr2` from `expr1` and returns the result. If an argument is `TIME`, today's date is used. If an argument is `DATE`, the time at midnight is used.

**Parameters**:
- `expr1` (Required): A `DATE`, `TIMESTAMP`, or `TIME` value.
- `expr2` (Required): A `DATE`, `TIMESTAMP`, or `TIME` value.

**Return type**: `TIMESTAMP` (DATE/TIMESTAMP with DATE/TIMESTAMP/TIME), `TIME` (TIME with DATE/TIMESTAMP/TIME)

Antonyms: [`ADDTIME`](#addtime)

#### Example

```ppl
  
source=people
| eval `'2008-12-12' - 0` = SUBTIME(DATE('2008-12-12'), DATE('2008-11-15'))
| fields `'2008-12-12' - 0`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+---------------------+
| '2008-12-12' - 0    |
|---------------------|
| 2008-12-12 00:00:00 |
  
+---------------------+
  
```
  
```ppl
  
source=people
| eval `'23:59:59' - 0` = SUBTIME(TIME('23:59:59'), DATE('2004-01-01'))
| fields `'23:59:59' - 0`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+----------------+
| '23:59:59' - 0 |
|----------------|
| 23:59:59       |
  
+----------------+
  
```
  
```ppl
  
source=people
| eval `'2004-01-01' - '23:59:59'` = SUBTIME(DATE('2004-01-01'), TIME('23:59:59'))
| fields `'2004-01-01' - '23:59:59'`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+---------------------------+
| '2004-01-01' - '23:59:59' |
|---------------------------|
| 2003-12-31 00:00:01       |
  
+---------------------------+
  
```
  
```ppl
  
source=people
| eval `'10:20:30' - '00:05:42'` = SUBTIME(TIME('10:20:30'), TIME('00:05:42'))
| fields `'10:20:30' - '00:05:42'`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+-------------------------+
| '10:20:30' - '00:05:42' |
|-------------------------|
| 10:14:48                |
  
+-------------------------+
  
```
  
```ppl
  
source=people
| eval `'2007-03-01 10:20:30' - '20:40:50'` = SUBTIME(TIMESTAMP('2007-03-01 10:20:30'), TIMESTAMP('2002-03-04 20:40:50'))
| fields `'2007-03-01 10:20:30' - '20:40:50'`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+------------------------------------+
| '2007-03-01 10:20:30' - '20:40:50' |
|------------------------------------|
| 2007-02-28 13:39:40                |
  
+------------------------------------+
  
```
  
## SYSDATE

**Usage**: `SYSDATE()` or `SYSDATE(precision)`

Returns the current date and time as a value in 'YYYY-MM-DD hh:mm:ss[.nnnnnn]'. `SYSDATE()` returns the date and time at which it executes in UTC. This differs from the behavior for [`NOW()`](#now), which returns a constant time that indicates the time at which the statement began to execute. If an argument is given, it specifies a fractional seconds precision from 0 to 6, the return value includes a fractional seconds part of that many digits.

**Parameters**:
- `precision` (Optional): An `INTEGER` value from 0 to 6 for fractional seconds precision.

**Return type**: `TIMESTAMP`

#### Example

```ppl ignore
  
source=people
| eval `value_1` = SYSDATE(), `value_2` = SYSDATE(6)
| fields `value_1`, `value_2`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+---------------------+----------------------------+
| value_1             | value_2                    |
|---------------------+----------------------------|
| 2025-08-02 15:39:05 | 2025-08-02 15:39:05.123456 |
  
+---------------------+----------------------------+
  
```
  
## TIME

**Usage**: `TIME(expr)`

Constructs a time type with the input string `expr` as a time. If the argument is of date/time/timestamp, it extracts the time value part from the expression.

**Parameters**:
- `expr` (Required): A `STRING`, `DATE`, `TIME`, or `TIMESTAMP` value.

**Return type**: `TIME`

#### Example

```ppl
  
source=people
| eval `TIME('13:49:00')` = TIME('13:49:00')
| fields `TIME('13:49:00')`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+------------------+
| TIME('13:49:00') |
|------------------|
| 13:49:00         |
  
+------------------+
  
```
  
```ppl
  
source=people
| eval `TIME('13:49')` = TIME('13:49')
| fields `TIME('13:49')`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+---------------+
| TIME('13:49') |
|---------------|
| 13:49:00      |
  
+---------------+
  
```
  
```ppl
  
source=people
| eval `TIME('2020-08-26 13:49:00')` = TIME('2020-08-26 13:49:00')
| fields `TIME('2020-08-26 13:49:00')`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+-----------------------------+
| TIME('2020-08-26 13:49:00') |
|-----------------------------|
| 13:49:00                    |
  
+-----------------------------+
  
```
  
```ppl
  
source=people
| eval `TIME('2020-08-26 13:49')` = TIME('2020-08-26 13:49')
| fields `TIME('2020-08-26 13:49')`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+--------------------------+
| TIME('2020-08-26 13:49') |
|--------------------------|
| 13:49:00                 |
  
+--------------------------+
  
```
  
## TIME_FORMAT

**Usage**: `TIME_FORMAT(time, format)`

Formats the `time` argument using the specifiers in the `format` argument. This supports a subset of the time format specifiers available for the [`DATE_FORMAT`](#date_format) function. Using date format specifiers supported by [`DATE_FORMAT`](#date_format) will return 0 or `NULL`. Acceptable format specifiers are listed in the following table. If an argument of type `DATE` is passed in, it is treated as a `TIMESTAMP` at midnight (i.e., 00:00:00).

**Parameters**:
- `time` (Required): A `STRING`, `DATE`, `TIME`, or `TIMESTAMP` value.
- `format` (Required): A `STRING` format specifier.

**Return type**: `STRING`

The following table describes the available specifier arguments:

| Specifier | Description |
| --- | --- |
| `%f` | Microseconds (000000..999999) |
| `%H` | Hour (00..23) |
| `%h` | Hour (01..12) |
| `%I` | Hour (01..12) |
| `%i` | Minutes, numeric (00..59) |
| `%p` | `AM` or `PM` |
| `%r` | Time, 12-hour (hh:mm:ss followed by `AM` or `PM`) |
| `%S` | Seconds (00..59) |
| `%s` | Seconds (00..59) |
| `%T` | Time, 24-hour (hh:mm:ss) |

#### Example

```ppl
  
source=people
| eval `TIME_FORMAT('1998-01-31 13:14:15.012345', '%f %H %h %I %i %p %r %S %s %T')` = TIME_FORMAT('1998-01-31 13:14:15.012345', '%f %H %h %I %i %p %r %S %s %T')
| fields `TIME_FORMAT('1998-01-31 13:14:15.012345', '%f %H %h %I %i %p %r %S %s %T')`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+----------------------------------------------------------------------------+
| TIME_FORMAT('1998-01-31 13:14:15.012345', '%f %H %h %I %i %p %r %S %s %T') |
|----------------------------------------------------------------------------|
| 012345 13 01 01 14 PM 01:14:15 PM 15 15 13:14:15                           |
  
+----------------------------------------------------------------------------+
  
```
  
## TIME_TO_SEC

**Usage**: `TIME_TO_SEC(time)`

Returns the `time` argument, converted to seconds.

**Parameters**:
- `time` (Required): A `STRING`, `TIME`, or `TIMESTAMP` value.

**Return type**: `LONG`

#### Example

```ppl
  
source=people
| eval `TIME_TO_SEC(TIME('22:23:00'))` = TIME_TO_SEC(TIME('22:23:00'))
| fields `TIME_TO_SEC(TIME('22:23:00'))`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+-------------------------------+
| TIME_TO_SEC(TIME('22:23:00')) |
|-------------------------------|
| 80580                         |
  
+-------------------------------+
  
```
  
## TIMEDIFF

**Usage**: `TIMEDIFF(time1, time2)`

Returns the difference between two time expressions as a time.

**Parameters**:
- `time1` (Required): A `TIME` value.
- `time2` (Required): A `TIME` value.

**Return type**: `TIME`

#### Example

```ppl
  
source=people
| eval `TIMEDIFF('23:59:59', '13:00:00')` = TIMEDIFF('23:59:59', '13:00:00')
| fields `TIMEDIFF('23:59:59', '13:00:00')`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+----------------------------------+
| TIMEDIFF('23:59:59', '13:00:00') |
|----------------------------------|
| 10:59:59                         |
  
+----------------------------------+
  
```
  
## TIMESTAMP

**Usage**: `TIMESTAMP(expr)` or `TIMESTAMP(expr1, expr2)`

Constructs a timestamp type with the input string `expr` as a timestamp. If the argument is not a string, it casts `expr` to a timestamp type with the default time zone UTC. If the argument is a time, it applies today's date before the cast. With two arguments, adds the time expression `expr2` to the date or timestamp expression `expr1` and returns the result as a timestamp value.

**Parameters**:
- `expr` (Required): A `STRING`, `DATE`, `TIME`, or `TIMESTAMP` value.
- `expr2` (Optional): A `STRING`, `DATE`, `TIME`, or `TIMESTAMP` value.

**Return type**: `TIMESTAMP`

#### Example

```ppl
  
source=people
| eval `TIMESTAMP('2020-08-26 13:49:00')` = TIMESTAMP('2020-08-26 13:49:00'), `TIMESTAMP('2020-08-26 13:49:00', TIME('12:15:42'))` = TIMESTAMP('2020-08-26 13:49:00', TIME('12:15:42'))
| fields `TIMESTAMP('2020-08-26 13:49:00')`, `TIMESTAMP('2020-08-26 13:49:00', TIME('12:15:42'))`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+----------------------------------+----------------------------------------------------+
| TIMESTAMP('2020-08-26 13:49:00') | TIMESTAMP('2020-08-26 13:49:00', TIME('12:15:42')) |
|----------------------------------+----------------------------------------------------|
| 2020-08-26 13:49:00              | 2020-08-27 02:04:42                                |
  
+----------------------------------+----------------------------------------------------+
  
```
  
## TIMESTAMPADD

**Usage**: `TIMESTAMPADD(interval, count, datetime)`

Returns a `TIMESTAMP` value based on a passed-in `DATE`/`TIME`/`TIMESTAMP`/`STRING` argument and an `INTERVAL` and `INTEGER` argument which determine the amount of time to be added. If the third argument is a `STRING`, it must be formatted as a valid `TIMESTAMP`. If only a `TIME` is provided, a `TIMESTAMP` is still returned with the `DATE` portion filled in using the current date. If the third argument is a `DATE`, it will be automatically converted to a `TIMESTAMP`.

**Parameters**:
- `interval` (Required): One of: `MICROSECOND`, `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`, `MONTH`, `QUARTER`, `YEAR`.
- `count` (Required): An `INTEGER` number of intervals to add.
- `datetime` (Required): A `DATE`, `TIME`, `TIMESTAMP`, or `STRING` value.

**Return type**: `TIMESTAMP`

**Examples**

```ppl
  
source=people
| eval `TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00')` = TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00')
| eval `TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00')` = TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00')
| fields `TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00')`, `TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00')`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+----------------------------------------------+--------------------------------------------------+
| TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00') | TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00') |
|----------------------------------------------+--------------------------------------------------|
| 2000-01-18 00:00:00                          | 1999-10-01 00:00:00                              |
  
+----------------------------------------------+--------------------------------------------------+
  
```
  
## TIMESTAMPDIFF

**Usage**: `TIMESTAMPDIFF(interval, start, end)`

Returns the difference between the start and end date/times in interval units. If a `TIME` is provided as an argument, it will be converted to a `TIMESTAMP` with the `DATE` portion filled in using the current date. Arguments will be automatically converted to a `TIME`/`TIMESTAMP` when appropriate. Any argument that is a `STRING` must be formatted as a valid `TIMESTAMP`.

**Parameters**:
- `interval` (Required): One of: `MICROSECOND`, `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`, `MONTH`, `QUARTER`, `YEAR`.
- `start` (Required): A `DATE`, `TIME`, `TIMESTAMP`, or `STRING` value.
- `end` (Required): A `DATE`, `TIME`, `TIMESTAMP`, or `STRING` value.

**Return type**: `LONG`

**Examples**

```ppl
  
source=people
| eval `TIMESTAMPDIFF(YEAR, '1997-01-01 00:00:00', '2001-03-06 00:00:00')` = TIMESTAMPDIFF(YEAR, '1997-01-01 00:00:00', '2001-03-06 00:00:00')
| eval `TIMESTAMPDIFF(SECOND, time('00:00:23'), time('00:00:00'))` = TIMESTAMPDIFF(SECOND, time('00:00:23'), time('00:00:00'))
| fields `TIMESTAMPDIFF(YEAR, '1997-01-01 00:00:00', '2001-03-06 00:00:00')`, `TIMESTAMPDIFF(SECOND, time('00:00:23'), time('00:00:00'))`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+-------------------------------------------------------------------+-----------------------------------------------------------+
| TIMESTAMPDIFF(YEAR, '1997-01-01 00:00:00', '2001-03-06 00:00:00') | TIMESTAMPDIFF(SECOND, time('00:00:23'), time('00:00:00')) |
|-------------------------------------------------------------------+-----------------------------------------------------------|
| 4                                                                 | -23                                                       |
  
+-------------------------------------------------------------------+-----------------------------------------------------------+
  
```
  
## TO_DAYS

**Usage**: `TO_DAYS(date)`

Returns the day number (the number of days since year 0) of the given date. Returns `NULL` if date is invalid.

**Parameters**:
- `date` (Required): A `STRING`, `DATE`, or `TIMESTAMP` value.

**Return type**: `LONG`

#### Example

```ppl
  
source=people
| eval `TO_DAYS(DATE('2008-10-07'))` = TO_DAYS(DATE('2008-10-07'))
| fields `TO_DAYS(DATE('2008-10-07'))`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+-----------------------------+
| TO_DAYS(DATE('2008-10-07')) |
|-----------------------------|
| 733687                      |
  
+-----------------------------+
  
```
  
## TO_SECONDS

**Usage**: `TO_SECONDS(date)`

Returns the number of seconds since the year 0 of the given value. Returns `NULL` if value is invalid. An argument of a `LONG` type can be used. It must be formatted as YMMDD, YYMMDD, YYYMMDD, or YYYYMMDD. Note that a `LONG` type argument cannot have leading 0s as it will be parsed using an octal numbering system.

**Parameters**:
- `date` (Required): A `STRING`, `LONG`, `DATE`, `TIME`, or `TIMESTAMP` value.

**Return type**: `LONG`

#### Example

```ppl
  
source=people
| eval `TO_SECONDS(DATE('2008-10-07'))` = TO_SECONDS(DATE('2008-10-07'))
| eval `TO_SECONDS(950228)` = TO_SECONDS(950228)
| fields `TO_SECONDS(DATE('2008-10-07'))`, `TO_SECONDS(950228)`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+--------------------------------+--------------------+
| TO_SECONDS(DATE('2008-10-07')) | TO_SECONDS(950228) |
|--------------------------------+--------------------|
| 63390556800                    | 62961148800        |
  
+--------------------------------+--------------------+
  
```
  
## UNIX_TIMESTAMP

**Usage**: `UNIX_TIMESTAMP()` or `UNIX_TIMESTAMP(date)`

Converts the given argument to Unix time (seconds since Epoch - the very beginning of the year 1970). If no argument is given, it returns the current Unix time. The date argument may be a `DATE`, or `TIMESTAMP` string, or a number in YYMMDD, YYMMDDhhmmss, YYYYMMDD, or YYYYMMDDhhmmss format. If the argument includes a time part, it may optionally include a fractional seconds part. If the argument is in an invalid format or outside the range 1970-01-01 00:00:00 - 3001-01-18 23:59:59.999999 (0 to 32536771199.999999 epoch time), the function returns `NULL`. You can use [`FROM_UNIXTIME`](#from_unixtime) to perform the reverse conversion.

**Parameters**:
- `date` (Optional): A `DOUBLE`, `DATE`, or `TIMESTAMP` value.

**Return type**: `DOUBLE`

#### Example

```ppl
  
source=people
| eval `UNIX_TIMESTAMP(double)` = UNIX_TIMESTAMP(20771122143845), `UNIX_TIMESTAMP(timestamp)` = UNIX_TIMESTAMP(TIMESTAMP('1996-11-15 17:05:42'))
| fields `UNIX_TIMESTAMP(double)`, `UNIX_TIMESTAMP(timestamp)`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+------------------------+---------------------------+
| UNIX_TIMESTAMP(double) | UNIX_TIMESTAMP(timestamp) |
|------------------------+---------------------------|
| 3404817525.0           | 848077542.0               |
  
+------------------------+---------------------------+
  
```
  
## UTC_DATE

**Usage**: `UTC_DATE()`

Returns the current UTC date as a value in `YYYY-MM-DD` format.

**Parameters**: None

**Return type**: `DATE`

#### Example

```ppl ignore
  
source=people
| eval `UTC_DATE()` = UTC_DATE()
| fields `UTC_DATE()`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+------------+
| UTC_DATE() |
|------------|
| 2025-10-03 |
  
+------------+
  
```
  
## UTC_TIME

**Usage**: `UTC_TIME()`

Returns the current UTC time as a value in 'hh:mm:ss'.

**Parameters**: None

**Return type**: `TIME`

#### Example

```ppl ignore
  
source=people
| eval `UTC_TIME()` = UTC_TIME()
| fields `UTC_TIME()`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+------------+
| UTC_TIME() |
|------------|
| 17:54:27   |
  
+------------+
  
```
  
## UTC_TIMESTAMP

**Usage**: `UTC_TIMESTAMP()`

Returns the current UTC timestamp as a value in 'YYYY-MM-DD hh:mm:ss'.

**Parameters**: None

**Return type**: `TIMESTAMP`

#### Example

```ppl ignore
  
source=people
| eval `UTC_TIMESTAMP()` = UTC_TIMESTAMP()
| fields `UTC_TIMESTAMP()`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+---------------------+
| UTC_TIMESTAMP()     |
|---------------------|
| 2025-10-03 17:54:28 |
  
+---------------------+
  
```
  
## WEEK

**Usage**: `WEEK(date)` or `WEEK(date, mode)`

Returns the week number for `date`. If the mode argument is omitted, the default mode 0 is used.

**Parameters**:
- `date` (Required): A `DATE`, `TIMESTAMP`, or `STRING` value.
- `mode` (Optional): An `INTEGER` mode value (0-7).

**Return type**: `INTEGER`

Synonyms: [`WEEK_OF_YEAR`](#week_of_year)

The following table describes how the `mode` parameter works.

| Mode | First day of week | Range | Week 1 is the first week ... |
| --- | --- | --- | --- |
| 0 | Sunday | 0-53 | with a Sunday in this year |
| 1 | Monday | 0-53 | with 4 or more days this year |
| 2 | Sunday | 1-53 | with a Sunday in this year |
| 3 | Monday | 1-53 | with 4 or more days this year |
| 4 | Sunday | 0-53 | with 4 or more days this year |
| 5 | Monday | 0-53 | with a Monday in this year |
| 6 | Sunday | 1-53 | with 4 or more days this year |
| 7 | Monday | 1-53 | with a Monday in this year |

#### Example

```ppl
  
source=people
| eval `WEEK(DATE('2008-02-20'))` = WEEK(DATE('2008-02-20')), `WEEK(DATE('2008-02-20'), 1)` = WEEK(DATE('2008-02-20'), 1)
| fields `WEEK(DATE('2008-02-20'))`, `WEEK(DATE('2008-02-20'), 1)`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+--------------------------+-----------------------------+
| WEEK(DATE('2008-02-20')) | WEEK(DATE('2008-02-20'), 1) |
|--------------------------+-----------------------------|
| 7                        | 8                           |
  
+--------------------------+-----------------------------+
  
```
  
## WEEKDAY

**Usage**: `WEEKDAY(date)`

Returns the weekday index for `date` (0 = Monday, 1 = Tuesday, ..., 6 = Sunday). It is similar to the [`DAYOFWEEK`](#dayofweek) function, but returns different indexes for each day.

**Parameters**:
- `date` (Required): A `STRING`, `DATE`, `TIME`, or `TIMESTAMP` value.

**Return type**: `INTEGER`

#### Example

```ppl
  
source=people
| eval `weekday(DATE('2020-08-26'))` = weekday(DATE('2020-08-26'))
| eval `weekday(DATE('2020-08-27'))` = weekday(DATE('2020-08-27'))
| fields `weekday(DATE('2020-08-26'))`, `weekday(DATE('2020-08-27'))`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+-----------------------------+-----------------------------+
| weekday(DATE('2020-08-26')) | weekday(DATE('2020-08-27')) |
|-----------------------------+-----------------------------|
| 2                           | 3                           |
  
+-----------------------------+-----------------------------+
  
```
  
## WEEK_OF_YEAR

**Usage**: `WEEK_OF_YEAR(date)` or `WEEK_OF_YEAR(date, mode)`

Returns the week number for `date`. If the mode argument is omitted, the default mode 0 is used.

**Parameters**:
- `date` (Required): A `DATE`, `TIMESTAMP`, or `STRING` value.
- `mode` (Optional): An `INTEGER` mode value (0-7).

**Return type**: `INTEGER`

Synonyms: [`WEEK`](#week)

The following table describes how the mode argument works:

| Mode | First day of week | Range | Week 1 is the first week ... |
| --- | --- | --- | --- |
| 0 | Sunday | 0-53 | with a Sunday in this year |
| 1 | Monday | 0-53 | with 4 or more days this year |
| 2 | Sunday | 1-53 | with a Sunday in this year |
| 3 | Monday | 1-53 | with 4 or more days this year |
| 4 | Sunday | 0-53 | with 4 or more days this year |
| 5 | Monday | 0-53 | with a Monday in this year |
| 6 | Sunday | 1-53 | with 4 or more days this year |
| 7 | Monday | 1-53 | with a Monday in this year |

#### Example

```ppl
  
source=people
| eval `WEEK_OF_YEAR(DATE('2008-02-20'))` = WEEK(DATE('2008-02-20')), `WEEK_OF_YEAR(DATE('2008-02-20'), 1)` = WEEK_OF_YEAR(DATE('2008-02-20'), 1)
| fields `WEEK_OF_YEAR(DATE('2008-02-20'))`, `WEEK_OF_YEAR(DATE('2008-02-20'), 1)`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+----------------------------------+-------------------------------------+
| WEEK_OF_YEAR(DATE('2008-02-20')) | WEEK_OF_YEAR(DATE('2008-02-20'), 1) |
|----------------------------------+-------------------------------------|
| 7                                | 8                                   |
  
+----------------------------------+-------------------------------------+
  
```
  
## YEAR

**Usage**: `YEAR(date)`

Returns the year for `date`, in the range 1000 to 9999, or 0 for the "zero" date.

**Parameters**:
- `date` (Required): A `STRING`, `DATE`, or `TIMESTAMP` value.

**Return type**: `INTEGER`

#### Example

```ppl
  
source=people
| eval `YEAR(DATE('2020-08-26'))` = YEAR(DATE('2020-08-26'))
| fields `YEAR(DATE('2020-08-26'))`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+--------------------------+
| YEAR(DATE('2020-08-26')) |
|--------------------------|
| 2020                     |
  
+--------------------------+
  
```
  
## YEARWEEK

**Usage**: `YEARWEEK(date)` or `YEARWEEK(date, mode)`

Returns the year and week for `date` as an integer. It accepts an optional mode argument aligned with those available for the [`WEEK`](#week) function.

**Parameters**:
- `date` (Required): A `STRING`, `DATE`, `TIME`, or `TIMESTAMP` value.
- `mode` (Optional): An `INTEGER` mode value (0-7).

**Return type**: `INTEGER`

#### Example

```ppl
  
source=people
| eval `YEARWEEK('2020-08-26')` = YEARWEEK('2020-08-26')
| eval `YEARWEEK('2019-01-05', 1)` = YEARWEEK('2019-01-05', 1)
| fields `YEARWEEK('2020-08-26')`, `YEARWEEK('2019-01-05', 1)`
  
```
  
The query returns the following results:

```text
  
fetched rows / total rows = 1/1
+------------------------+---------------------------+
| YEARWEEK('2020-08-26') | YEARWEEK('2019-01-05', 1) |
|------------------------+---------------------------|
| 202034                 | 201901                    |
  
+------------------------+---------------------------+
  
```
  