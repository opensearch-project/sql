=======================
Date and Time Functions
=======================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1

ADDDATE
-------

Description
>>>>>>>>>>>

Usage: adddate(date, INTERVAL expr unit) / adddate(date, days) adds the interval of second argument to date; adddate(date, days) adds the second argument as integer number of days to date.
If first argument is TIME, today's date is used; if first argument is DATE, time at midnight is used.

Argument type: DATE/TIMESTAMP/TIME, INTERVAL/LONG

Return type map:

(DATE/TIMESTAMP/TIME, INTERVAL) -> TIMESTAMP

(DATE, LONG) -> DATE

(TIMESTAMP/TIME, LONG) -> TIMESTAMP

Synonyms: `DATE_ADD`_ when invoked with the INTERVAL form of the second argument.

Antonyms: `SUBDATE`_

Example::

    os> source=people | eval `'2020-08-26' + 1h` = ADDDATE(DATE('2020-08-26'), INTERVAL 1 HOUR), `'2020-08-26' + 1` = ADDDATE(DATE('2020-08-26'), 1), `ts '2020-08-26 01:01:01' + 1` = ADDDATE(TIMESTAMP('2020-08-26 01:01:01'), 1) | fields `'2020-08-26' + 1h`, `'2020-08-26' + 1`, `ts '2020-08-26 01:01:01' + 1`
    fetched rows / total rows = 1/1
    +---------------------+------------------+------------------------------+
    | '2020-08-26' + 1h   | '2020-08-26' + 1 | ts '2020-08-26 01:01:01' + 1 |
    |---------------------+------------------+------------------------------|
    | 2020-08-26 01:00:00 | 2020-08-27       | 2020-08-27 01:01:01          |
    +---------------------+------------------+------------------------------+



ADDTIME
-------

Description
>>>>>>>>>>>

Usage: addtime(expr1, expr2) adds expr2 to expr1 and returns the result. If argument is TIME, today's date is used; if argument is DATE, time at midnight is used.

Argument type: DATE/TIMESTAMP/TIME, DATE/TIMESTAMP/TIME

Return type map:

(DATE/TIMESTAMP, DATE/TIMESTAMP/TIME) -> TIMESTAMP

(TIME, DATE/TIMESTAMP/TIME) -> TIME

Antonyms: `SUBTIME`_

Example::

    os> source=people | eval `'2008-12-12' + 0` = ADDTIME(DATE('2008-12-12'), DATE('2008-11-15')) | fields `'2008-12-12' + 0`
    fetched rows / total rows = 1/1
    +---------------------+
    | '2008-12-12' + 0    |
    |---------------------|
    | 2008-12-12 00:00:00 |
    +---------------------+

    os> source=people | eval `'23:59:59' + 0` = ADDTIME(TIME('23:59:59'), DATE('2004-01-01')) | fields `'23:59:59' + 0`
    fetched rows / total rows = 1/1
    +----------------+
    | '23:59:59' + 0 |
    |----------------|
    | 23:59:59       |
    +----------------+

    os> source=people | eval `'2004-01-01' + '23:59:59'` = ADDTIME(DATE('2004-01-01'), TIME('23:59:59')) | fields `'2004-01-01' + '23:59:59'`
    fetched rows / total rows = 1/1
    +---------------------------+
    | '2004-01-01' + '23:59:59' |
    |---------------------------|
    | 2004-01-01 23:59:59       |
    +---------------------------+

    os> source=people | eval `'10:20:30' + '00:05:42'` = ADDTIME(TIME('10:20:30'), TIME('00:05:42')) | fields `'10:20:30' + '00:05:42'`
    fetched rows / total rows = 1/1
    +-------------------------+
    | '10:20:30' + '00:05:42' |
    |-------------------------|
    | 10:26:12                |
    +-------------------------+

    os> source=people | eval `'2007-02-28 10:20:30' + '20:40:50'` = ADDTIME(TIMESTAMP('2007-02-28 10:20:30'), TIMESTAMP('2002-03-04 20:40:50')) | fields `'2007-02-28 10:20:30' + '20:40:50'`
    fetched rows / total rows = 1/1
    +------------------------------------+
    | '2007-02-28 10:20:30' + '20:40:50' |
    |------------------------------------|
    | 2007-03-01 07:01:20                |
    +------------------------------------+


CONVERT_TZ
----------

Description
>>>>>>>>>>>

Usage: convert_tz(timestamp, from_timezone, to_timezone) constructs a local timestamp converted from the from_timezone to the to_timezone. CONVERT_TZ returns null when any of the three function arguments are invalid, i.e. timestamp is not in the format yyyy-MM-dd HH:mm:ss or the timeszone is not in (+/-)HH:mm. It also is invalid for invalid dates, such as February 30th and invalid timezones, which are ones outside of -13:59 and +14:00.

Argument type: TIMESTAMP, STRING, STRING

Return type: TIMESTAMP

Conversion from +00:00 timezone to +10:00 timezone. Returns the timestamp argument converted from +00:00 to +10:00
Example::

    os> source=people | eval `convert_tz('2008-05-15 12:00:00','+00:00','+10:00')` = convert_tz('2008-05-15 12:00:00','+00:00','+10:00') | fields `convert_tz('2008-05-15 12:00:00','+00:00','+10:00')`
    fetched rows / total rows = 1/1
    +-----------------------------------------------------+
    | convert_tz('2008-05-15 12:00:00','+00:00','+10:00') |
    |-----------------------------------------------------|
    | 2008-05-15 22:00:00                                 |
    +-----------------------------------------------------+

The valid timezone range for convert_tz is (-13:59, +14:00) inclusive. Timezones outside of the range, such as +15:00 in this example will return null.
Example::

    os> source=people | eval `convert_tz('2008-05-15 12:00:00','+00:00','+15:00')` = convert_tz('2008-05-15 12:00:00','+00:00','+15:00')| fields `convert_tz('2008-05-15 12:00:00','+00:00','+15:00')`
    fetched rows / total rows = 1/1
    +-----------------------------------------------------+
    | convert_tz('2008-05-15 12:00:00','+00:00','+15:00') |
    |-----------------------------------------------------|
    | null                                                |
    +-----------------------------------------------------+

Conversion from a positive timezone to a negative timezone that goes over date line.
Example::

    os> source=people | eval `convert_tz('2008-05-15 12:00:00','+03:30','-10:00')` = convert_tz('2008-05-15 12:00:00','+03:30','-10:00') | fields `convert_tz('2008-05-15 12:00:00','+03:30','-10:00')`
    fetched rows / total rows = 1/1
    +-----------------------------------------------------+
    | convert_tz('2008-05-15 12:00:00','+03:30','-10:00') |
    |-----------------------------------------------------|
    | 2008-05-14 22:30:00                                 |
    +-----------------------------------------------------+

Valid dates are required in convert_tz, invalid dates such as April 31st (not a date in the Gregorian calendar) will result in null.
Example::

    os> source=people | eval `convert_tz('2008-04-31 12:00:00','+03:30','-10:00')` = convert_tz('2008-04-31 12:00:00','+03:30','-10:00') | fields `convert_tz('2008-04-31 12:00:00','+03:30','-10:00')`
    fetched rows / total rows = 1/1
    +-----------------------------------------------------+
    | convert_tz('2008-04-31 12:00:00','+03:30','-10:00') |
    |-----------------------------------------------------|
    | null                                                |
    +-----------------------------------------------------+

Valid dates are required in convert_tz, invalid dates such as February 30th (not a date in the Gregorian calendar) will result in null.
Example::

    os> source=people | eval `convert_tz('2008-02-30 12:00:00','+03:30','-10:00')` = convert_tz('2008-02-30 12:00:00','+03:30','-10:00') | fields `convert_tz('2008-02-30 12:00:00','+03:30','-10:00')`
    fetched rows / total rows = 1/1
    +-----------------------------------------------------+
    | convert_tz('2008-02-30 12:00:00','+03:30','-10:00') |
    |-----------------------------------------------------|
    | null                                                |
    +-----------------------------------------------------+

February 29th 2008 is a valid date because it is a leap year.
Example::

    os> source=people | eval `convert_tz('2008-02-29 12:00:00','+03:30','-10:00')` = convert_tz('2008-02-29 12:00:00','+03:30','-10:00') | fields `convert_tz('2008-02-29 12:00:00','+03:30','-10:00')`
    fetched rows / total rows = 1/1
    +-----------------------------------------------------+
    | convert_tz('2008-02-29 12:00:00','+03:30','-10:00') |
    |-----------------------------------------------------|
    | 2008-02-28 22:30:00                                 |
    +-----------------------------------------------------+

Valid dates are required in convert_tz, invalid dates such as February 29th 2007 (2007 is not a leap year) will result in null.
Example::

    os> source=people | eval `convert_tz('2007-02-29 12:00:00','+03:30','-10:00')` = convert_tz('2007-02-29 12:00:00','+03:30','-10:00') | fields `convert_tz('2007-02-29 12:00:00','+03:30','-10:00')`
    fetched rows / total rows = 1/1
    +-----------------------------------------------------+
    | convert_tz('2007-02-29 12:00:00','+03:30','-10:00') |
    |-----------------------------------------------------|
    | null                                                |
    +-----------------------------------------------------+

The valid timezone range for convert_tz is (-13:59, +14:00) inclusive. Timezones outside of the range, such as +14:01 in this example will return null.
Example::

    os> source=people | eval `convert_tz('2008-02-01 12:00:00','+14:01','+00:00')` = convert_tz('2008-02-01 12:00:00','+14:01','+00:00') | fields `convert_tz('2008-02-01 12:00:00','+14:01','+00:00')`
    fetched rows / total rows = 1/1
    +-----------------------------------------------------+
    | convert_tz('2008-02-01 12:00:00','+14:01','+00:00') |
    |-----------------------------------------------------|
    | null                                                |
    +-----------------------------------------------------+

The valid timezone range for convert_tz is (-13:59, +14:00) inclusive. Timezones outside of the range, such as +14:00 in this example will return a correctly converted date time object.
Example::

    os> source=people | eval `convert_tz('2008-02-01 12:00:00','+14:00','+00:00')` = convert_tz('2008-02-01 12:00:00','+14:00','+00:00') | fields `convert_tz('2008-02-01 12:00:00','+14:00','+00:00')`
    fetched rows / total rows = 1/1
    +-----------------------------------------------------+
    | convert_tz('2008-02-01 12:00:00','+14:00','+00:00') |
    |-----------------------------------------------------|
    | 2008-01-31 22:00:00                                 |
    +-----------------------------------------------------+

The valid timezone range for convert_tz is (-13:59, +14:00) inclusive. Timezones outside of the range, such as -14:00 will result in null
Example::

    os> source=people | eval `convert_tz('2008-02-01 12:00:00','-14:00','+00:00')` = convert_tz('2008-02-01 12:00:00','-14:00','+00:00') | fields `convert_tz('2008-02-01 12:00:00','-14:00','+00:00')`
    fetched rows / total rows = 1/1
    +-----------------------------------------------------+
    | convert_tz('2008-02-01 12:00:00','-14:00','+00:00') |
    |-----------------------------------------------------|
    | null                                                |
    +-----------------------------------------------------+

The valid timezone range for convert_tz is (-13:59, +14:00) inclusive. This timezone is within range so it is valid and will convert the time.
Example::

    os> source=people | eval `convert_tz('2008-02-01 12:00:00','-13:59','+00:00')` = convert_tz('2008-02-01 12:00:00','-13:59','+00:00') | fields `convert_tz('2008-02-01 12:00:00','-13:59','+00:00')`
    fetched rows / total rows = 1/1
    +-----------------------------------------------------+
    | convert_tz('2008-02-01 12:00:00','-13:59','+00:00') |
    |-----------------------------------------------------|
    | 2008-02-02 01:59:00                                 |
    +-----------------------------------------------------+


CURDATE
-------

Description
>>>>>>>>>>>

Returns the current time as a value in 'YYYY-MM-DD'.
CURDATE() returns the time at which it executes as `SYSDATE() <#sysdate>`_ does.

Return type: DATE

Specification: CURDATE() -> DATE

Example::

    > source=people | eval `CURDATE()` = CURDATE() | fields `CURDATE()`
    fetched rows / total rows = 1/1
    +------------+
    | CURDATE()  |
    |------------|
    | 2022-08-02 |
    +------------+


CURRENT_DATE
------------

Description
>>>>>>>>>>>

`CURRENT_DATE()` are synonyms for `CURDATE() <#curdate>`_.

Example::

    > source=people | eval `CURRENT_DATE()` = CURRENT_DATE() | fields `CURRENT_DATE()`
    fetched rows / total rows = 1/1
    +------------------+
    | CURRENT_DATE()   |
    |------------------+
    | 2022-08-02       |
    +------------------+


CURRENT_TIME
------------

Description
>>>>>>>>>>>

`CURRENT_TIME()` are synonyms for `CURTIME() <#curtime>`_.

Example::

    > source=people | eval `CURRENT_TIME()` = CURRENT_TIME() | fields `CURRENT_TIME()`
    fetched rows / total rows = 1/1
    +------------------+
    | CURRENT_TIME()   |
    |------------------+
    | 15:39:05         |
    +------------------+


CURRENT_TIMESTAMP
-----------------

Description
>>>>>>>>>>>

`CURRENT_TIMESTAMP()` are synonyms for `NOW() <#now>`_.

Example::

    > source=people | eval `CURRENT_TIMESTAMP()` = CURRENT_TIMESTAMP() | fields `CURRENT_TIMESTAMP()`
    fetched rows / total rows = 1/1
    +-----------------------+
    | CURRENT_TIMESTAMP()   |
    |-----------------------+
    | 2022-08-02 15:54:19   |
    +-----------------------+


CURTIME
-------

Description
>>>>>>>>>>>

Returns the current time as a value in 'hh:mm:ss'.
CURTIME() returns the time at which the statement began to execute as `NOW() <#now>`_ does.

Return type: TIME

Specification: CURTIME() -> TIME

Example::

    > source=people | eval `value_1` = CURTIME(), `value_2` = CURTIME() | fields `value_1`, `value_2`
    fetched rows / total rows = 1/1
    +----------+----------+
    | value_1  | value_2  |
    |----------+----------|
    | 15:39:05 | 15:39:05 |
    +----------+----------+


DATE
----

Description
>>>>>>>>>>>

Usage: date(expr) constructs a date type with the input string expr as a date. If the argument is of date/timestamp, it extracts the date value part from the expression.

Argument type: STRING/DATE/TIMESTAMP

Return type: DATE

Example::

    os> source=people | eval `DATE('2020-08-26')` = DATE('2020-08-26') | fields `DATE('2020-08-26')`
    fetched rows / total rows = 1/1
    +--------------------+
    | DATE('2020-08-26') |
    |--------------------|
    | 2020-08-26         |
    +--------------------+

    os> source=people | eval `DATE(TIMESTAMP('2020-08-26 13:49:00'))` = DATE(TIMESTAMP('2020-08-26 13:49:00')) | fields `DATE(TIMESTAMP('2020-08-26 13:49:00'))`
    fetched rows / total rows = 1/1
    +----------------------------------------+
    | DATE(TIMESTAMP('2020-08-26 13:49:00')) |
    |----------------------------------------|
    | 2020-08-26                             |
    +----------------------------------------+

    os> source=people | eval `DATE('2020-08-26 13:49')` = DATE('2020-08-26 13:49') | fields `DATE('2020-08-26 13:49')`
    fetched rows / total rows = 1/1
    +--------------------------+
    | DATE('2020-08-26 13:49') |
    |--------------------------|
    | 2020-08-26               |
    +--------------------------+

    os> source=people | eval `DATE('2020-08-26 13:49')` = DATE('2020-08-26 13:49') | fields `DATE('2020-08-26 13:49')`
    fetched rows / total rows = 1/1
    +--------------------------+
    | DATE('2020-08-26 13:49') |
    |--------------------------|
    | 2020-08-26               |
    +--------------------------+


DATE_ADD
--------

Description
>>>>>>>>>>>

Usage: date_add(date, INTERVAL expr unit) adds the interval expr to date. If first argument is TIME, today's date is used; if first argument is DATE, time at midnight is used.

Argument type: DATE/TIMESTAMP/TIME, INTERVAL

Return type: TIMESTAMP

Synonyms: `ADDDATE`_

Antonyms: `DATE_SUB`_

Example::

    os> source=people | eval `'2020-08-26' + 1h` = DATE_ADD(DATE('2020-08-26'), INTERVAL 1 HOUR), `ts '2020-08-26 01:01:01' + 1d` = DATE_ADD(TIMESTAMP('2020-08-26 01:01:01'), INTERVAL 1 DAY) | fields `'2020-08-26' + 1h`, `ts '2020-08-26 01:01:01' + 1d`
    fetched rows / total rows = 1/1
    +---------------------+-------------------------------+
    | '2020-08-26' + 1h   | ts '2020-08-26 01:01:01' + 1d |
    |---------------------+-------------------------------|
    | 2020-08-26 01:00:00 | 2020-08-27 01:01:01           |
    +---------------------+-------------------------------+


DATE_FORMAT
-----------

Description
>>>>>>>>>>>

Usage: date_format(date, format) formats the date argument using the specifiers in the format argument.
If an argument of type TIME is provided, the local date is used.

.. list-table:: The following table describes the available specifier arguments.
   :widths: 20 80
   :header-rows: 1

   * - Specifier
     - Description
   * - %a
     - Abbreviated weekday name (Sun..Sat)
   * - %b
     - Abbreviated month name (Jan..Dec)
   * - %c
     - Month, numeric (0..12)
   * - %D
     - Day of the month with English suffix (0th, 1st, 2nd, 3rd, ...)
   * - %d
     - Day of the month, numeric (00..31)
   * - %e
     - Day of the month, numeric (0..31)
   * - %f
     - Microseconds (000000..999999)
   * - %H
     - Hour (00..23)
   * - %h
     - Hour (01..12)
   * - %I
     - Hour (01..12)
   * - %i
     - Minutes, numeric (00..59)
   * - %j
     - Day of year (001..366)
   * - %k
     - Hour (0..23)
   * - %l
     - Hour (1..12)
   * - %M
     - Month name (January..December)
   * - %m
     - Month, numeric (00..12)
   * - %p
     - AM or PM
   * - %r
     - Time, 12-hour (hh:mm:ss followed by AM or PM)
   * - %S
     - Seconds (00..59)
   * - %s
     - Seconds (00..59)
   * - %T
     - Time, 24-hour (hh:mm:ss)
   * - %U
     - Week (00..53), where Sunday is the first day of the week; WEEK() mode 0
   * - %u
     - Week (00..53), where Monday is the first day of the week; WEEK() mode 1
   * - %V
     - Week (01..53), where Sunday is the first day of the week; WEEK() mode 2; used with %X
   * - %v
     - Week (01..53), where Monday is the first day of the week; WEEK() mode 3; used with %x
   * - %W
     - Weekday name (Sunday..Saturday)
   * - %w
     - Day of the week (0=Sunday..6=Saturday)
   * - %X
     - Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
   * - %x
     - Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
   * - %Y
     - Year, numeric, four digits
   * - %y
     - Year, numeric (two digits)
   * - %%
     - A literal % character
   * - %x
     - x, for any “x” not listed above
   * - x
     - x, for any smallcase/uppercase alphabet except [aydmshiHIMYDSEL]

Argument type: STRING/DATE/TIME/TIMESTAMP, STRING

Return type: STRING

Example::

    os> source=people | eval `DATE_FORMAT('1998-01-31 13:14:15.012345', '%T.%f')` = DATE_FORMAT('1998-01-31 13:14:15.012345', '%T.%f'), `DATE_FORMAT(TIMESTAMP('1998-01-31 13:14:15.012345'), '%Y-%b-%D %r')` = DATE_FORMAT(TIMESTAMP('1998-01-31 13:14:15.012345'), '%Y-%b-%D %r') | fields `DATE_FORMAT('1998-01-31 13:14:15.012345', '%T.%f')`, `DATE_FORMAT(TIMESTAMP('1998-01-31 13:14:15.012345'), '%Y-%b-%D %r')`
    fetched rows / total rows = 1/1
    +----------------------------------------------------+---------------------------------------------------------------------+
    | DATE_FORMAT('1998-01-31 13:14:15.012345', '%T.%f') | DATE_FORMAT(TIMESTAMP('1998-01-31 13:14:15.012345'), '%Y-%b-%D %r') |
    |----------------------------------------------------+---------------------------------------------------------------------|
    | 13:14:15.012345                                    | 1998-Jan-31st 01:14:15 PM                                           |
    +----------------------------------------------------+---------------------------------------------------------------------+


DATETIME
--------

Description
>>>>>>>>>>>

Usage: DATETIME(timestamp)/ DATETIME(date, to_timezone) Converts the datetime to a new timezone

Argument type: timestamp/STRING

Return type map:

(TIMESTAMP, STRING) -> TIMESTAMP

(TIMESTAMP) -> TIMESTAMP


Converting timestamp with timezone to the second argument timezone.
Example::

    os> source=people | eval `DATETIME('2004-02-28 23:00:00-10:00', '+10:00')` = DATETIME('2004-02-28 23:00:00-10:00', '+10:00') | fields `DATETIME('2004-02-28 23:00:00-10:00', '+10:00')`
    fetched rows / total rows = 1/1
    +-------------------------------------------------+
    | DATETIME('2004-02-28 23:00:00-10:00', '+10:00') |
    |-------------------------------------------------|
    | 2004-02-29 19:00:00                             |
    +-------------------------------------------------+


The valid timezone range for convert_tz is (-13:59, +14:00) inclusive. Timezones outside of the range will result in null.
Example::

    os> source=people | eval  `DATETIME('2008-01-01 02:00:00', '-14:00')` = DATETIME('2008-01-01 02:00:00', '-14:00') | fields `DATETIME('2008-01-01 02:00:00', '-14:00')`
    fetched rows / total rows = 1/1
    +-------------------------------------------+
    | DATETIME('2008-01-01 02:00:00', '-14:00') |
    |-------------------------------------------|
    | null                                      |
    +-------------------------------------------+


DATE_SUB
--------

Description
>>>>>>>>>>>

Usage: date_sub(date, INTERVAL expr unit) subtracts the interval expr from date. If first argument is TIME, today's date is used; if first argument is DATE, time at midnight is used.

Argument type: DATE/TIMESTAMP/TIME, INTERVAL

Return type: TIMESTAMP

Synonyms: `SUBDATE`_

Antonyms: `DATE_ADD`_

Example::

    os> source=people | eval `'2008-01-02' - 31d` = DATE_SUB(DATE('2008-01-02'), INTERVAL 31 DAY), `ts '2020-08-26 01:01:01' + 1h` = DATE_SUB(TIMESTAMP('2020-08-26 01:01:01'), INTERVAL 1 HOUR) | fields `'2008-01-02' - 31d`, `ts '2020-08-26 01:01:01' + 1h`
    fetched rows / total rows = 1/1
    +---------------------+-------------------------------+
    | '2008-01-02' - 31d  | ts '2020-08-26 01:01:01' + 1h |
    |---------------------+-------------------------------|
    | 2007-12-02 00:00:00 | 2020-08-26 00:01:01           |
    +---------------------+-------------------------------+


DATEDIFF
--------

Usage: Calculates the difference of date parts of given values. If the first argument is time, today's date is used.

Argument type: DATE/TIMESTAMP/TIME, DATE/TIMESTAMP/TIME

Return type: LONG

Example::

    os> source=people | eval `'2000-01-02' - '2000-01-01'` = DATEDIFF(TIMESTAMP('2000-01-02 00:00:00'), TIMESTAMP('2000-01-01 23:59:59')), `'2001-02-01' - '2004-01-01'` = DATEDIFF(DATE('2001-02-01'), TIMESTAMP('2004-01-01 00:00:00')), `today - today` = DATEDIFF(TIME('23:59:59'), TIME('00:00:00')) | fields `'2000-01-02' - '2000-01-01'`, `'2001-02-01' - '2004-01-01'`, `today - today`
    fetched rows / total rows = 1/1
    +-----------------------------+-----------------------------+---------------+
    | '2000-01-02' - '2000-01-01' | '2001-02-01' - '2004-01-01' | today - today |
    |-----------------------------+-----------------------------+---------------|
    | 1                           | -1064                       | 0             |
    +-----------------------------+-----------------------------+---------------+


DAY
---

Description
>>>>>>>>>>>

Usage: day(date) extracts the day of the month for date, in the range 1 to 31.

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Synonyms: `DAYOFMONTH`_, `DAY_OF_MONTH`_

Example::

    os> source=people | eval `DAY(DATE('2020-08-26'))` = DAY(DATE('2020-08-26')) | fields `DAY(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +-------------------------+
    | DAY(DATE('2020-08-26')) |
    |-------------------------|
    | 26                      |
    +-------------------------+


DAYNAME
-------

Description
>>>>>>>>>>>

Usage: dayname(date) returns the name of the weekday for date, including Monday, Tuesday, Wednesday, Thursday, Friday, Saturday and Sunday.

Argument type: STRING/DATE/TIMESTAMP

Return type: STRING

Example::

    os> source=people | eval `DAYNAME(DATE('2020-08-26'))` = DAYNAME(DATE('2020-08-26')) | fields `DAYNAME(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +-----------------------------+
    | DAYNAME(DATE('2020-08-26')) |
    |-----------------------------|
    | Wednesday                   |
    +-----------------------------+


DAYOFMONTH
----------

Description
>>>>>>>>>>>

Usage: dayofmonth(date) extracts the day of the month for date, in the range 1 to 31.

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Synonyms: `DAY`_, `DAY_OF_MONTH`_

Example::

    os> source=people | eval `DAYOFMONTH(DATE('2020-08-26'))` = DAYOFMONTH(DATE('2020-08-26')) | fields `DAYOFMONTH(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +--------------------------------+
    | DAYOFMONTH(DATE('2020-08-26')) |
    |--------------------------------|
    | 26                             |
    +--------------------------------+


DAY_OF_MONTH
------------

Description
>>>>>>>>>>>

Usage: day_of_month(date) extracts the day of the month for date, in the range 1 to 31.

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Synonyms: `DAY`_, `DAYOFMONTH`_

Example::

    os> source=people | eval `DAY_OF_MONTH(DATE('2020-08-26'))` = DAY_OF_MONTH(DATE('2020-08-26')) | fields `DAY_OF_MONTH(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +----------------------------------+
    | DAY_OF_MONTH(DATE('2020-08-26')) |
    |----------------------------------|
    | 26                               |
    +----------------------------------+


DAYOFWEEK
---------

Description
>>>>>>>>>>>

Usage: dayofweek(date) returns the weekday index for date (1 = Sunday, 2 = Monday, ..., 7 = Saturday).

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Synonyms: `DAY_OF_WEEK`_

Example::

    os> source=people | eval `DAYOFWEEK(DATE('2020-08-26'))` = DAYOFWEEK(DATE('2020-08-26')) | fields `DAYOFWEEK(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +-------------------------------+
    | DAYOFWEEK(DATE('2020-08-26')) |
    |-------------------------------|
    | 4                             |
    +-------------------------------+


DAY_OF_WEEK
-----------

Description
>>>>>>>>>>>

Usage: day_of_week(date) returns the weekday index for date (1 = Sunday, 2 = Monday, ..., 7 = Saturday).

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Synonyms: `DAYOFWEEK`_

Example::

    os> source=people | eval `DAY_OF_WEEK(DATE('2020-08-26'))` = DAY_OF_WEEK(DATE('2020-08-26')) | fields `DAY_OF_WEEK(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +---------------------------------+
    | DAY_OF_WEEK(DATE('2020-08-26')) |
    |---------------------------------|
    | 4                               |
    +---------------------------------+


DAYOFYEAR
---------

Description
>>>>>>>>>>>

Usage:  dayofyear(date) returns the day of the year for date, in the range 1 to 366.

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Synonyms: `DAY_OF_YEAR`_

Example::

    os> source=people | eval `DAYOFYEAR(DATE('2020-08-26'))` = DAYOFYEAR(DATE('2020-08-26')) | fields `DAYOFYEAR(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +-------------------------------+
    | DAYOFYEAR(DATE('2020-08-26')) |
    |-------------------------------|
    | 239                           |
    +-------------------------------+


DAY_OF_YEAR
-----------

Description
>>>>>>>>>>>

Usage:  day_of_year(date) returns the day of the year for date, in the range 1 to 366.

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Synonyms: `DAYOFYEAR`_

Example::

    os> source=people | eval `DAY_OF_YEAR(DATE('2020-08-26'))` = DAY_OF_YEAR(DATE('2020-08-26')) | fields `DAY_OF_YEAR(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +---------------------------------+
    | DAY_OF_YEAR(DATE('2020-08-26')) |
    |---------------------------------|
    | 239                             |
    +---------------------------------+


EXTRACT
-------

Description
>>>>>>>>>>>

Usage: extract(part FROM date) returns a LONG with digits in order according to the given 'part' arguments.
The specific format of the returned long is determined by the table below.

Argument type: PART, where PART is one of the following tokens in the table below.

The format specifiers found in this table are the same as those found in the `DATE_FORMAT`_ function.

.. list-table:: The following table describes the mapping of a 'part' to a particular format.
   :widths: 20 80
   :header-rows: 1

   * - Part
     - Format
   * - MICROSECOND
     - %f
   * - SECOND
     - %s
   * - MINUTE
     - %i
   * - HOUR
     - %H
   * - DAY
     - %d
   * - WEEK
     - %X
   * - MONTH
     - %m
   * - YEAR
     - %V
   * - SECOND_MICROSECOND
     - %s%f
   * - MINUTE_MICROSECOND
     - %i%s%f
   * - MINUTE_SECOND
     - %i%s
   * - HOUR_MICROSECOND
     - %H%i%s%f
   * - HOUR_SECOND
     - %H%i%s
   * - HOUR_MINUTE
     - %H%i
   * - DAY_MICROSECOND
     - %d%H%i%s%f
   * - DAY_SECOND
     - %d%H%i%s
   * - DAY_MINUTE
     - %d%H%i
   * - DAY_HOUR
     - %d%H%
   * - YEAR_MONTH
     - %V%m

Return type: LONG

Example::

    os> source=people | eval `extract(YEAR_MONTH FROM "2023-02-07 10:11:12")` = extract(YEAR_MONTH FROM "2023-02-07 10:11:12") | fields `extract(YEAR_MONTH FROM "2023-02-07 10:11:12")`
    fetched rows / total rows = 1/1
    +------------------------------------------------+
    | extract(YEAR_MONTH FROM "2023-02-07 10:11:12") |
    |------------------------------------------------|
    | 202302                                         |
    +------------------------------------------------+


FROM_DAYS
---------

Description
>>>>>>>>>>>

Usage: from_days(N) returns the date value given the day number N.

Argument type: INTEGER/LONG

Return type: DATE

Example::

    os> source=people | eval `FROM_DAYS(733687)` = FROM_DAYS(733687) | fields `FROM_DAYS(733687)`
    fetched rows / total rows = 1/1
    +-------------------+
    | FROM_DAYS(733687) |
    |-------------------|
    | 2008-10-07        |
    +-------------------+


FROM_UNIXTIME
-------------

Description
>>>>>>>>>>>

Usage: Returns a representation of the argument given as a timestamp or character string value. Perform reverse conversion for `UNIX_TIMESTAMP`_ function.
If second argument is provided, it is used to format the result in the same way as the format string used for the `DATE_FORMAT`_ function.
If timestamp is outside of range 1970-01-01 00:00:00 - 3001-01-18 23:59:59.999999 (0 to 32536771199.999999 epoch time), function returns NULL.
Argument type: DOUBLE, STRING

Return type map:

DOUBLE -> TIMESTAMP

DOUBLE, STRING -> STRING

Examples::

    os> source=people | eval `FROM_UNIXTIME(1220249547)` = FROM_UNIXTIME(1220249547) | fields `FROM_UNIXTIME(1220249547)`
    fetched rows / total rows = 1/1
    +---------------------------+
    | FROM_UNIXTIME(1220249547) |
    |---------------------------|
    | 2008-09-01 06:12:27       |
    +---------------------------+

    os> source=people | eval `FROM_UNIXTIME(1220249547, '%T')` = FROM_UNIXTIME(1220249547, '%T') | fields `FROM_UNIXTIME(1220249547, '%T')`
    fetched rows / total rows = 1/1
    +---------------------------------+
    | FROM_UNIXTIME(1220249547, '%T') |
    |---------------------------------|
    | 06:12:27                        |
    +---------------------------------+


GET_FORMAT
----------

Description
>>>>>>>>>>>

Usage: Returns a string value containing string format specifiers based on the input arguments.

Argument type: TYPE, STRING, where TYPE must be one of the following tokens: [DATE, TIME, TIMESTAMP], and
STRING must be one of the following tokens: ["USA", "JIS", "ISO", "EUR", "INTERNAL"] (" can be replaced by ').

Examples::

    os> source=people | eval `GET_FORMAT(DATE, 'USA')` = GET_FORMAT(DATE, 'USA') | fields `GET_FORMAT(DATE, 'USA')`
    fetched rows / total rows = 1/1
    +-------------------------+
    | GET_FORMAT(DATE, 'USA') |
    |-------------------------|
    | %m.%d.%Y                |
    +-------------------------+


HOUR
----

Description
>>>>>>>>>>>

Usage: hour(time) extracts the hour value for time. Different from the time of day value, the time value has a large range and can be greater than 23, so the return value of hour(time) can be also greater than 23.

Argument type: STRING/TIME/TIMESTAMP

Return type: INTEGER

Synonyms: `HOUR_OF_DAY`_

Example::

    os> source=people | eval `HOUR(TIME('01:02:03'))` = HOUR(TIME('01:02:03')) | fields `HOUR(TIME('01:02:03'))`
    fetched rows / total rows = 1/1
    +------------------------+
    | HOUR(TIME('01:02:03')) |
    |------------------------|
    | 1                      |
    +------------------------+


HOUR_OF_DAY
-----------

Description
>>>>>>>>>>>

Usage: hour_of_day(time) extracts the hour value for time. Different from the time of day value, the time value has a large range and can be greater than 23, so the return value of hour_of_day(time) can be also greater than 23.

Argument type: STRING/TIME/TIMESTAMP

Return type: INTEGER

Synonyms: `HOUR`_

Example::

    os> source=people | eval `HOUR_OF_DAY(TIME('01:02:03'))` = HOUR_OF_DAY(TIME('01:02:03')) | fields `HOUR_OF_DAY(TIME('01:02:03'))`
    fetched rows / total rows = 1/1
    +-------------------------------+
    | HOUR_OF_DAY(TIME('01:02:03')) |
    |-------------------------------|
    | 1                             |
    +-------------------------------+


LAST_DAY
--------

Usage: Returns the last day of the month as a DATE for a valid argument.

Argument type: DATE/STRING/TIMESTAMP/TIME

Return type: DATE

Example::

    os> source=people | eval `last_day('2023-02-06')` = last_day('2023-02-06') | fields `last_day('2023-02-06')`
    fetched rows / total rows = 1/1
    +------------------------+
    | last_day('2023-02-06') |
    |------------------------|
    | 2023-02-28             |
    +------------------------+


LOCALTIMESTAMP
--------------

Description
>>>>>>>>>>>

`LOCALTIMESTAMP()` are synonyms for `NOW() <#now>`_.

Example::

    > source=people | eval `LOCALTIMESTAMP()` = LOCALTIMESTAMP() | fields `LOCALTIMESTAMP()`
    fetched rows / total rows = 1/1
    +---------------------+
    | LOCALTIMESTAMP()    |
    |---------------------+
    | 2022-08-02 15:54:19 |
    +---------------------+


LOCALTIME
---------

Description
>>>>>>>>>>>

`LOCALTIME()` are synonyms for `NOW() <#now>`_.

Example::

    > source=people | eval `LOCALTIME()` = LOCALTIME() | fields `LOCALTIME()`
    fetched rows / total rows = 1/1
    +---------------------+
    | LOCALTIME()         |
    |---------------------+
    | 2022-08-02 15:54:19 |
    +---------------------+


MAKEDATE
--------

Description
>>>>>>>>>>>

Returns a date, given `year` and `day-of-year` values. `dayofyear` must be greater than 0 or the result is `NULL`. The result is also `NULL` if either argument is `NULL`.
Arguments are rounded to an integer.

Limitations:
- Zero `year` interpreted as 2000;
- Negative `year` is not accepted;
- `day-of-year` should be greater than zero;
- `day-of-year` could be greater than 365/366, calculation switches to the next year(s) (see example).

Specifications:

1. MAKEDATE(DOUBLE, DOUBLE) -> DATE

Argument type: DOUBLE

Return type: DATE

Example::

    os> source=people | eval `MAKEDATE(1945, 5.9)` = MAKEDATE(1945, 5.9), `MAKEDATE(1984, 1984)` = MAKEDATE(1984, 1984) | fields `MAKEDATE(1945, 5.9)`, `MAKEDATE(1984, 1984)`
    fetched rows / total rows = 1/1
    +---------------------+----------------------+
    | MAKEDATE(1945, 5.9) | MAKEDATE(1984, 1984) |
    |---------------------+----------------------|
    | 1945-01-06          | 1989-06-06           |
    +---------------------+----------------------+


MAKETIME
--------

Description
>>>>>>>>>>>

Returns a time value calculated from the hour, minute, and second arguments. Returns `NULL` if any of its arguments are `NULL`.
The second argument can have a fractional part, rest arguments are rounded to an integer.

Limitations:
- 24-hour clock is used, available time range is [00:00:00.0 - 23:59:59.(9)];
- Up to 9 digits of second fraction part is taken (nanosecond precision).

Specifications:

1. MAKETIME(DOUBLE, DOUBLE, DOUBLE) -> TIME

Argument type: DOUBLE

Return type: TIME

Example::

    os> source=people | eval `MAKETIME(20, 30, 40)` = MAKETIME(20, 30, 40), `MAKETIME(20.2, 49.5, 42.100502)` = MAKETIME(20.2, 49.5, 42.100502) | fields `MAKETIME(20, 30, 40)`, `MAKETIME(20.2, 49.5, 42.100502)`
    fetched rows / total rows = 1/1
    +----------------------+---------------------------------+
    | MAKETIME(20, 30, 40) | MAKETIME(20.2, 49.5, 42.100502) |
    |----------------------+---------------------------------|
    | 20:30:40             | 20:50:42.100502                 |
    +----------------------+---------------------------------+


MICROSECOND
-----------

Description
>>>>>>>>>>>

Usage: microsecond(expr) returns the microseconds from the time or timestamp expression expr as a number in the range from 0 to 999999.

Argument type: STRING/TIME/TIMESTAMP

Return type: INTEGER

Example::

    os> source=people | eval `MICROSECOND(TIME('01:02:03.123456'))` = MICROSECOND(TIME('01:02:03.123456')) | fields `MICROSECOND(TIME('01:02:03.123456'))`
    fetched rows / total rows = 1/1
    +--------------------------------------+
    | MICROSECOND(TIME('01:02:03.123456')) |
    |--------------------------------------|
    | 123456                               |
    +--------------------------------------+


MINUTE
------

Description
>>>>>>>>>>>

Usage: minute(time) returns the minute for time, in the range 0 to 59.

Argument type: STRING/TIME/TIMESTAMP

Return type: INTEGER

Synonyms: `MINUTE_OF_HOUR`_

Example::

    os> source=people | eval `MINUTE(TIME('01:02:03'))` =  MINUTE(TIME('01:02:03')) | fields `MINUTE(TIME('01:02:03'))`
    fetched rows / total rows = 1/1
    +--------------------------+
    | MINUTE(TIME('01:02:03')) |
    |--------------------------|
    | 2                        |
    +--------------------------+


MINUTE_OF_DAY
------

Description
>>>>>>>>>>>

Usage: minute(time) returns the amount of minutes in the day, in the range of 0 to 1439.

Argument type: STRING/TIME/TIMESTAMP

Return type: INTEGER

Example::

    os> source=people | eval `MINUTE_OF_DAY(TIME('01:02:03'))` = MINUTE_OF_DAY(TIME('01:02:03')) | fields `MINUTE_OF_DAY(TIME('01:02:03'))`
    fetched rows / total rows = 1/1
    +---------------------------------+
    | MINUTE_OF_DAY(TIME('01:02:03')) |
    |---------------------------------|
    | 62                              |
    +---------------------------------+


MINUTE_OF_HOUR
--------------

Description
>>>>>>>>>>>

Usage: minute(time) returns the minute for time, in the range 0 to 59.

Argument type: STRING/TIME/TIMESTAMP

Return type: INTEGER

Synonyms: `MINUTE`_

Example::

    os> source=people | eval `MINUTE_OF_HOUR(TIME('01:02:03'))` =  MINUTE_OF_HOUR(TIME('01:02:03')) | fields `MINUTE_OF_HOUR(TIME('01:02:03'))`
    fetched rows / total rows = 1/1
    +----------------------------------+
    | MINUTE_OF_HOUR(TIME('01:02:03')) |
    |----------------------------------|
    | 2                                |
    +----------------------------------+


MONTH
-----

Description
>>>>>>>>>>>

Usage: month(date) returns the month for date, in the range 1 to 12 for January to December.

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Synonyms: `MONTH_OF_YEAR`_

Example::

    os> source=people | eval `MONTH(DATE('2020-08-26'))` =  MONTH(DATE('2020-08-26')) | fields `MONTH(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +---------------------------+
    | MONTH(DATE('2020-08-26')) |
    |---------------------------|
    | 8                         |
    +---------------------------+


MONTH_OF_YEAR
-------------

Description
>>>>>>>>>>>

Usage: month_of_year(date) returns the month for date, in the range 1 to 12 for January to December.

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Synonyms: `MONTH`_

Example::

    os> source=people | eval `MONTH_OF_YEAR(DATE('2020-08-26'))` =  MONTH_OF_YEAR(DATE('2020-08-26')) | fields `MONTH_OF_YEAR(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +-----------------------------------+
    | MONTH_OF_YEAR(DATE('2020-08-26')) |
    |-----------------------------------|
    | 8                                 |
    +-----------------------------------+


MONTHNAME
---------

Description
>>>>>>>>>>>

Usage: monthname(date) returns the full name of the month for date.

Argument type: STRING/DATE/TIMESTAMP

Return type: STRING

Example::

    os> source=people | eval `MONTHNAME(DATE('2020-08-26'))` = MONTHNAME(DATE('2020-08-26')) | fields `MONTHNAME(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +-------------------------------+
    | MONTHNAME(DATE('2020-08-26')) |
    |-------------------------------|
    | August                        |
    +-------------------------------+


NOW
---

Description
>>>>>>>>>>>

Returns the current date and time as a value in 'YYYY-MM-DD hh:mm:ss' format. The value is expressed in the cluster time zone.
`NOW()` returns a constant time that indicates the time at which the statement began to execute. This differs from the behavior for `SYSDATE() <#sysdate>`_, which returns the exact time at which it executes.

Return type: TIMESTAMP

Specification: NOW() -> TIMESTAMP

Example::

    > source=people | eval `value_1` = NOW(), `value_2` = NOW() | fields `value_1`, `value_2`
    fetched rows / total rows = 1/1
    +---------------------+---------------------+
    | value_1             | value_2             |
    |---------------------+---------------------|
    | 2022-08-02 15:39:05 | 2022-08-02 15:39:05 |
    +---------------------+---------------------+


PERIOD_ADD
----------

Description
>>>>>>>>>>>

Usage: period_add(P, N) add N months to period P (in the format YYMM or YYYYMM). Returns a value in the format YYYYMM.

Argument type: INTEGER, INTEGER

Return type: INTEGER

Example::

    os> source=people | eval `PERIOD_ADD(200801, 2)` = PERIOD_ADD(200801, 2), `PERIOD_ADD(200801, -12)` = PERIOD_ADD(200801, -12) | fields `PERIOD_ADD(200801, 2)`, `PERIOD_ADD(200801, -12)`
    fetched rows / total rows = 1/1
    +-----------------------+-------------------------+
    | PERIOD_ADD(200801, 2) | PERIOD_ADD(200801, -12) |
    |-----------------------+-------------------------|
    | 200803                | 200701                  |
    +-----------------------+-------------------------+


PERIOD_DIFF
-----------

Description
>>>>>>>>>>>

Usage: period_diff(P1, P2) returns the number of months between periods P1 and P2 given in the format YYMM or YYYYMM.

Argument type: INTEGER, INTEGER

Return type: INTEGER

Example::

    os> source=people | eval `PERIOD_DIFF(200802, 200703)` = PERIOD_DIFF(200802, 200703), `PERIOD_DIFF(200802, 201003)` = PERIOD_DIFF(200802, 201003) | fields `PERIOD_DIFF(200802, 200703)`, `PERIOD_DIFF(200802, 201003)`
    fetched rows / total rows = 1/1
    +-----------------------------+-----------------------------+
    | PERIOD_DIFF(200802, 200703) | PERIOD_DIFF(200802, 201003) |
    |-----------------------------+-----------------------------|
    | 11                          | -25                         |
    +-----------------------------+-----------------------------+


QUARTER
-------

Description
>>>>>>>>>>>

Usage: quarter(date) returns the quarter of the year for date, in the range 1 to 4.

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Example::

    os> source=people | eval `QUARTER(DATE('2020-08-26'))` = QUARTER(DATE('2020-08-26')) | fields `QUARTER(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +-----------------------------+
    | QUARTER(DATE('2020-08-26')) |
    |-----------------------------|
    | 3                           |
    +-----------------------------+


SEC_TO_TIME
-----------

Description
>>>>>>>>>>>

Usage: sec_to_time(number) returns the time in HH:mm:ssss[.nnnnnn] format.
Note that the function returns a time between 00:00:00 and 23:59:59.
If an input value is too large (greater than 86399), the function will wrap around and begin returning outputs starting from 00:00:00.
If an input value is too small (less than 0), the function will wrap around and begin returning outputs counting down from 23:59:59.

Argument type: INTEGER, LONG, DOUBLE, FLOAT

Return type: TIME

Example::

    os> source=people | eval `SEC_TO_TIME(3601)` = SEC_TO_TIME(3601) | eval `SEC_TO_TIME(1234.123)` = SEC_TO_TIME(1234.123) | fields `SEC_TO_TIME(3601)`, `SEC_TO_TIME(1234.123)`
    fetched rows / total rows = 1/1
    +-------------------+-----------------------+
    | SEC_TO_TIME(3601) | SEC_TO_TIME(1234.123) |
    |-------------------+-----------------------|
    | 01:00:01          | 00:20:34.123          |
    +-------------------+-----------------------+


SECOND
------

Description
>>>>>>>>>>>

Usage: second(time) returns the second for time, in the range 0 to 59.

Argument type: STRING/TIME/TIMESTAMP

Return type: INTEGER

Synonyms: `SECOND_OF_MINUTE`_

Example::

    os> source=people | eval `SECOND(TIME('01:02:03'))` = SECOND(TIME('01:02:03')) | fields `SECOND(TIME('01:02:03'))`
    fetched rows / total rows = 1/1
    +--------------------------+
    | SECOND(TIME('01:02:03')) |
    |--------------------------|
    | 3                        |
    +--------------------------+


SECOND_OF_MINUTE
----------------

Description
>>>>>>>>>>>

Usage: second_of_minute(time) returns the second for time, in the range 0 to 59.

Argument type: STRING/TIME/TIMESTAMP

Return type: INTEGER

Synonyms: `SECOND`_

Example::

    os> source=people | eval `SECOND_OF_MINUTE(TIME('01:02:03'))` = SECOND_OF_MINUTE(TIME('01:02:03')) | fields `SECOND_OF_MINUTE(TIME('01:02:03'))`
    fetched rows / total rows = 1/1
    +------------------------------------+
    | SECOND_OF_MINUTE(TIME('01:02:03')) |
    |------------------------------------|
    | 3                                  |
    +------------------------------------+


STR_TO_DATE
-----------

Description
>>>>>>>>>>>

Usage: str_to_date(string, string) is used to extract a TIMESTAMP from the first argument string using the formats specified in the second argument string.
The input argument must have enough information to be parsed as a DATE, TIMESTAMP, or TIME.
Acceptable string format specifiers are the same as those used in the `DATE_FORMAT`_ function.
It returns NULL when a statement cannot be parsed due to an invalid pair of arguments, and when 0 is provided for any DATE field. Otherwise, it will return a TIMESTAMP with the parsed values (as well as default values for any field that was not parsed).

Argument type: STRING, STRING

Return type: TIMESTAMP

Example::

    OS> source=people | eval `str_to_date("01,5,2013", "%d,%m,%Y")` = str_to_date("01,5,2013", "%d,%m,%Y") | fields = `str_to_date("01,5,2013", "%d,%m,%Y")`
    fetched rows / total rows = 1/1
    +--------------------------------------+
    | str_to_date("01,5,2013", "%d,%m,%Y") |
    |--------------------------------------|
    | 2013-05-01 00:00:00                  |
    +--------------------------------------+


SUBDATE
-------

Description
>>>>>>>>>>>

Usage: subdate(date, INTERVAL expr unit) / subdate(date, days) subtracts the interval expr from date; subdate(date, days) subtracts the second argument as integer number of days from date.
If first argument is TIME, today's date is used; if first argument is DATE, time at midnight is used.

Argument type: DATE/TIMESTAMP/TIME, INTERVAL/LONG

Return type map:

(DATE/TIMESTAMP/TIME, INTERVAL) -> TIMESTAMP

(DATE, LONG) -> DATE

(TIMESTAMP/TIME, LONG) -> TIMESTAMP

Synonyms: `DATE_SUB`_ when invoked with the INTERVAL form of the second argument.

Antonyms: `ADDDATE`_

Example::

    os> source=people | eval `'2008-01-02' - 31d` = SUBDATE(DATE('2008-01-02'), INTERVAL 31 DAY), `'2020-08-26' - 1` = SUBDATE(DATE('2020-08-26'), 1), `ts '2020-08-26 01:01:01' - 1` = SUBDATE(TIMESTAMP('2020-08-26 01:01:01'), 1) | fields `'2008-01-02' - 31d`, `'2020-08-26' - 1`, `ts '2020-08-26 01:01:01' - 1`
    fetched rows / total rows = 1/1
    +---------------------+------------------+------------------------------+
    | '2008-01-02' - 31d  | '2020-08-26' - 1 | ts '2020-08-26 01:01:01' - 1 |
    |---------------------+------------------+------------------------------|
    | 2007-12-02 00:00:00 | 2020-08-25       | 2020-08-25 01:01:01          |
    +---------------------+------------------+------------------------------+


SUBTIME
-------

Description
>>>>>>>>>>>

Usage: subtime(expr1, expr2) subtracts expr2 from expr1 and returns the result. If argument is TIME, today's date is used; if argument is DATE, time at midnight is used.

Argument type: DATE/TIMESTAMP/TIME, DATE/TIMESTAMP/TIME

Return type map:

(DATE/TIMESTAMP, DATE/TIMESTAMP/TIME) -> TIMESTAMP

(TIME, DATE/TIMESTAMP/TIME) -> TIME

Antonyms: `ADDTIME`_

Example::

    os> source=people | eval `'2008-12-12' - 0` = SUBTIME(DATE('2008-12-12'), DATE('2008-11-15')) | fields `'2008-12-12' - 0`
    fetched rows / total rows = 1/1
    +---------------------+
    | '2008-12-12' - 0    |
    |---------------------|
    | 2008-12-12 00:00:00 |
    +---------------------+

    os> source=people | eval `'23:59:59' - 0` = SUBTIME(TIME('23:59:59'), DATE('2004-01-01')) | fields `'23:59:59' - 0`
    fetched rows / total rows = 1/1
    +----------------+
    | '23:59:59' - 0 |
    |----------------|
    | 23:59:59       |
    +----------------+

    os> source=people | eval `'2004-01-01' - '23:59:59'` = SUBTIME(DATE('2004-01-01'), TIME('23:59:59')) | fields `'2004-01-01' - '23:59:59'`
    fetched rows / total rows = 1/1
    +---------------------------+
    | '2004-01-01' - '23:59:59' |
    |---------------------------|
    | 2003-12-31 00:00:01       |
    +---------------------------+

    os> source=people | eval `'10:20:30' - '00:05:42'` = SUBTIME(TIME('10:20:30'), TIME('00:05:42')) | fields `'10:20:30' - '00:05:42'`
    fetched rows / total rows = 1/1
    +-------------------------+
    | '10:20:30' - '00:05:42' |
    |-------------------------|
    | 10:14:48                |
    +-------------------------+

    os> source=people | eval `'2007-03-01 10:20:30' - '20:40:50'` = SUBTIME(TIMESTAMP('2007-03-01 10:20:30'), TIMESTAMP('2002-03-04 20:40:50')) | fields `'2007-03-01 10:20:30' - '20:40:50'`
    fetched rows / total rows = 1/1
    +------------------------------------+
    | '2007-03-01 10:20:30' - '20:40:50' |
    |------------------------------------|
    | 2007-02-28 13:39:40                |
    +------------------------------------+


SYSDATE
-------

Description
>>>>>>>>>>>

Returns the current date and time as a value in 'YYYY-MM-DD hh:mm:ss[.nnnnnn]'.
SYSDATE() returns the time at which it executes. This differs from the behavior for `NOW() <#now>`_, which returns a constant time that indicates the time at which the statement began to execute.
If the argument is given, it specifies a fractional seconds precision from 0 to 6, the return value includes a fractional seconds part of that many digits.

Optional argument type: INTEGER

Return type: TIMESTAMP

Specification: SYSDATE([INTEGER]) -> TIMESTAMP

Example::

    > source=people | eval `value_1` = SYSDATE(), `value_2` = SYSDATE(6) | fields `value_1`, `value_2`
    fetched rows / total rows = 1/1
    +---------------------+----------------------------+
    | value_1             | value_2                    |
    |---------------------+----------------------------|
    | 2022-08-02 15:39:05 | 2022-08-02 15:39:05.123456 |
    +---------------------+----------------------------+


TIME
----

Description
>>>>>>>>>>>

Usage: time(expr) constructs a time type with the input string expr as a time. If the argument is of date/time/timestamp, it extracts the time value part from the expression.

Argument type: STRING/DATE/TIME/TIMESTAMP

Return type: TIME

Example::

    os> source=people | eval `TIME('13:49:00')` = TIME('13:49:00') | fields `TIME('13:49:00')`
    fetched rows / total rows = 1/1
    +------------------+
    | TIME('13:49:00') |
    |------------------|
    | 13:49:00         |
    +------------------+

    os> source=people | eval `TIME('13:49')` = TIME('13:49') | fields `TIME('13:49')`
    fetched rows / total rows = 1/1
    +---------------+
    | TIME('13:49') |
    |---------------|
    | 13:49:00      |
    +---------------+

    os> source=people | eval `TIME('2020-08-26 13:49:00')` = TIME('2020-08-26 13:49:00') | fields `TIME('2020-08-26 13:49:00')`
    fetched rows / total rows = 1/1
    +-----------------------------+
    | TIME('2020-08-26 13:49:00') |
    |-----------------------------|
    | 13:49:00                    |
    +-----------------------------+

    os> source=people | eval `TIME('2020-08-26 13:49')` = TIME('2020-08-26 13:49') | fields `TIME('2020-08-26 13:49')`
    fetched rows / total rows = 1/1
    +--------------------------+
    | TIME('2020-08-26 13:49') |
    |--------------------------|
    | 13:49:00                 |
    +--------------------------+


TIME_FORMAT
-----------

Description
>>>>>>>>>>>

Usage: time_format(time, format) formats the time argument using the specifiers in the format argument.
This supports a subset of the time format specifiers available for the `date_format`_ function.
Using date format specifiers supported by `date_format`_ will return 0 or null.
Acceptable format specifiers are listed in the table below.
If an argument of type DATE is passed in, it is treated as a TIMESTAMP at midnight (i.e., 00:00:00).

.. list-table:: The following table describes the available specifier arguments.
   :widths: 20 80
   :header-rows: 1

   * - Specifier
     - Description
   * - %f
     - Microseconds (000000..999999)
   * - %H
     - Hour (00..23)
   * - %h
     - Hour (01..12)
   * - %I
     - Hour (01..12)
   * - %i
     - Minutes, numeric (00..59)
   * - %p
     - AM or PM
   * - %r
     - Time, 12-hour (hh:mm:ss followed by AM or PM)
   * - %S
     - Seconds (00..59)
   * - %s
     - Seconds (00..59)
   * - %T
     - Time, 24-hour (hh:mm:ss)


Argument type: STRING/DATE/TIME/TIMESTAMP, STRING

Return type: STRING

Example::

    os> source=people | eval `TIME_FORMAT('1998-01-31 13:14:15.012345', '%f %H %h %I %i %p %r %S %s %T')` = TIME_FORMAT('1998-01-31 13:14:15.012345', '%f %H %h %I %i %p %r %S %s %T') | fields `TIME_FORMAT('1998-01-31 13:14:15.012345', '%f %H %h %I %i %p %r %S %s %T')`
    fetched rows / total rows = 1/1
    +----------------------------------------------------------------------------+
    | TIME_FORMAT('1998-01-31 13:14:15.012345', '%f %H %h %I %i %p %r %S %s %T') |
    |----------------------------------------------------------------------------|
    | 012345 13 01 01 14 PM 01:14:15 PM 15 15 13:14:15                           |
    +----------------------------------------------------------------------------+


TIME_TO_SEC
-----------

Description
>>>>>>>>>>>

Usage: time_to_sec(time) returns the time argument, converted to seconds.

Argument type: STRING/TIME/TIMESTAMP

Return type: LONG

Example::

    os> source=people | eval `TIME_TO_SEC(TIME('22:23:00'))` = TIME_TO_SEC(TIME('22:23:00')) | fields `TIME_TO_SEC(TIME('22:23:00'))`
    fetched rows / total rows = 1/1
    +-------------------------------+
    | TIME_TO_SEC(TIME('22:23:00')) |
    |-------------------------------|
    | 80580                         |
    +-------------------------------+


TIMEDIFF
--------

Description
>>>>>>>>>>>

Usage: returns the difference between two time expressions as a time.

Argument type: TIME, TIME

Return type: TIME

Example::

    os> source=people | eval `TIMEDIFF('23:59:59', '13:00:00')` = TIMEDIFF('23:59:59', '13:00:00') | fields `TIMEDIFF('23:59:59', '13:00:00')`
    fetched rows / total rows = 1/1
    +----------------------------------+
    | TIMEDIFF('23:59:59', '13:00:00') |
    |----------------------------------|
    | 10:59:59                         |
    +----------------------------------+


TIMESTAMP
---------

Description
>>>>>>>>>>>

Usage: timestamp(expr) constructs a timestamp type with the input string `expr` as an timestamp. If the argument is not a string, it casts `expr` to timestamp type with default timezone UTC. If argument is a time, it applies today's date before cast.
With two arguments `timestamp(expr1, expr2)` adds the time expression `expr2` to the date or timestamp expression `expr1` and returns the result as a timestamp value.

Argument type: STRING/DATE/TIME/TIMESTAMP

Return type map:

(STRING/DATE/TIME/TIMESTAMP) -> TIMESTAMP

(STRING/DATE/TIME/TIMESTAMP, STRING/DATE/TIME/TIMESTAMP) -> TIMESTAMP

Example::

    os> source=people | eval `TIMESTAMP('2020-08-26 13:49:00')` = TIMESTAMP('2020-08-26 13:49:00'), `TIMESTAMP('2020-08-26 13:49:00', TIME('12:15:42'))` = TIMESTAMP('2020-08-26 13:49:00', TIME('12:15:42')) | fields `TIMESTAMP('2020-08-26 13:49:00')`, `TIMESTAMP('2020-08-26 13:49:00', TIME('12:15:42'))`
    fetched rows / total rows = 1/1
    +----------------------------------+----------------------------------------------------+
    | TIMESTAMP('2020-08-26 13:49:00') | TIMESTAMP('2020-08-26 13:49:00', TIME('12:15:42')) |
    |----------------------------------+----------------------------------------------------|
    | 2020-08-26 13:49:00              | 2020-08-27 02:04:42                                |
    +----------------------------------+----------------------------------------------------+


TIMESTAMPADD
------------

Description
>>>>>>>>>>>

Usage: Returns a TIMESTAMP value based on a passed in DATE/TIME/TIMESTAMP/STRING argument and an INTERVAL and INTEGER argument which determine the amount of time to be added.
If the third argument is a STRING, it must be formatted as a valid TIMESTAMP. If only a TIME is provided, a TIMESTAMP is still returned with the DATE portion filled in using the current date.
If the third argument is a DATE, it will be automatically converted to a TIMESTAMP.

Argument type: INTERVAL, INTEGER, DATE/TIME/TIMESTAMP/STRING

INTERVAL must be one of the following tokens: [MICROSECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR]

Examples::

    os> source=people | eval `TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00')` = TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00') | eval `TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00')` = TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00') | fields `TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00')`, `TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00')`
    fetched rows / total rows = 1/1
    +----------------------------------------------+--------------------------------------------------+
    | TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00') | TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00') |
    |----------------------------------------------+--------------------------------------------------|
    | 2000-01-18 00:00:00                          | 1999-10-01 00:00:00                              |
    +----------------------------------------------+--------------------------------------------------+


TIMESTAMPDIFF
-------------

Description
>>>>>>>>>>>

Usage: TIMESTAMPDIFF(interval, start, end) returns the difference between the start and end date/times in interval units.
If a TIME is provided as an argument, it will be converted to a TIMESTAMP with the DATE portion filled in using the current date.
Arguments will be automatically converted to a TIME/TIMESTAMP when appropriate.
Any argument that is a STRING must be formatted as a valid TIMESTAMP.

Argument type: INTERVAL, DATE/TIME/TIMESTAMP/STRING, DATE/TIME/TIMESTAMP/STRING

INTERVAL must be one of the following tokens: [MICROSECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR]

Examples::

    os> source=people | eval `TIMESTAMPDIFF(YEAR, '1997-01-01 00:00:00', '2001-03-06 00:00:00')` = TIMESTAMPDIFF(YEAR, '1997-01-01 00:00:00', '2001-03-06 00:00:00') | eval `TIMESTAMPDIFF(SECOND, time('00:00:23'), time('00:00:00'))` = TIMESTAMPDIFF(SECOND, time('00:00:23'), time('00:00:00')) | fields `TIMESTAMPDIFF(YEAR, '1997-01-01 00:00:00', '2001-03-06 00:00:00')`, `TIMESTAMPDIFF(SECOND, time('00:00:23'), time('00:00:00'))`
    fetched rows / total rows = 1/1
    +-------------------------------------------------------------------+-----------------------------------------------------------+
    | TIMESTAMPDIFF(YEAR, '1997-01-01 00:00:00', '2001-03-06 00:00:00') | TIMESTAMPDIFF(SECOND, time('00:00:23'), time('00:00:00')) |
    |-------------------------------------------------------------------+-----------------------------------------------------------|
    | 4                                                                 | -23                                                       |
    +-------------------------------------------------------------------+-----------------------------------------------------------+


TO_DAYS
-------

Description
>>>>>>>>>>>

Usage: to_days(date) returns the day number (the number of days since year 0) of the given date. Returns NULL if date is invalid.

Argument type: STRING/DATE/TIMESTAMP

Return type: LONG

Example::

    os> source=people | eval `TO_DAYS(DATE('2008-10-07'))` = TO_DAYS(DATE('2008-10-07')) | fields `TO_DAYS(DATE('2008-10-07'))`
    fetched rows / total rows = 1/1
    +-----------------------------+
    | TO_DAYS(DATE('2008-10-07')) |
    |-----------------------------|
    | 733687                      |
    +-----------------------------+


TO_SECONDS
----------

Description
>>>>>>>>>>>

Usage: to_seconds(date) returns the number of seconds since the year 0 of the given value. Returns NULL if value is invalid.
An argument of a LONG type can be used. It must be formatted as YMMDD, YYMMDD, YYYMMDD or YYYYMMDD. Note that a LONG type argument cannot have leading 0s as it will be parsed using an octal numbering system.

Argument type: STRING/LONG/DATE/TIME/TIMESTAMP

Return type: LONG

Example::

    os> source=people | eval `TO_SECONDS(DATE('2008-10-07'))` = TO_SECONDS(DATE('2008-10-07')) | eval `TO_SECONDS(950228)` = TO_SECONDS(950228) | fields `TO_SECONDS(DATE('2008-10-07'))`, `TO_SECONDS(950228)`
    fetched rows / total rows = 1/1
    +--------------------------------+--------------------+
    | TO_SECONDS(DATE('2008-10-07')) | TO_SECONDS(950228) |
    |--------------------------------+--------------------|
    | 63390556800                    | 62961148800        |
    +--------------------------------+--------------------+


UNIX_TIMESTAMP
--------------

Description
>>>>>>>>>>>

Usage: Converts given argument to Unix time (seconds since Epoch - very beginning of year 1970). If no argument given, it returns the current Unix time.
The date argument may be a DATE, or TIMESTAMP string, or a number in YYMMDD, YYMMDDhhmmss, YYYYMMDD, or YYYYMMDDhhmmss format. If the argument includes a time part, it may optionally include a fractional seconds part.
If argument is in invalid format or outside of range 1970-01-01 00:00:00 - 3001-01-18 23:59:59.999999 (0 to 32536771199.999999 epoch time), function returns NULL.
You can use `FROM_UNIXTIME`_ to do reverse conversion.

Argument type: <NONE>/DOUBLE/DATE/TIMESTAMP

Return type: DOUBLE

Example::

    os> source=people | eval `UNIX_TIMESTAMP(double)` = UNIX_TIMESTAMP(20771122143845), `UNIX_TIMESTAMP(timestamp)` = UNIX_TIMESTAMP(TIMESTAMP('1996-11-15 17:05:42')) | fields `UNIX_TIMESTAMP(double)`, `UNIX_TIMESTAMP(timestamp)`
    fetched rows / total rows = 1/1
    +------------------------+---------------------------+
    | UNIX_TIMESTAMP(double) | UNIX_TIMESTAMP(timestamp) |
    |------------------------+---------------------------|
    | 3404817525.0           | 848077542.0               |
    +------------------------+---------------------------+


UTC_DATE
--------

Description
>>>>>>>>>>>

Returns the current UTC date as a value in 'YYYY-MM-DD'.

Return type: DATE

Specification: UTC_DATE() -> DATE

Example::

    > source=people | eval `UTC_DATE()` = UTC_DATE() | fields `UTC_DATE()`
    fetched rows / total rows = 1/1
    +------------+
    | UTC_DATE() |
    |------------|
    | 2022-10-03 |
    +------------+


UTC_TIME
--------

Description
>>>>>>>>>>>

Returns the current UTC time as a value in 'hh:mm:ss'.

Return type: TIME

Specification: UTC_TIME() -> TIME

Example::

    > source=people | eval `UTC_TIME()` = UTC_TIME() | fields `UTC_TIME()`
    fetched rows / total rows = 1/1
    +------------+
    | UTC_TIME() |
    |------------|
    | 17:54:27   |
    +------------+


UTC_TIMESTAMP
-------------

Description
>>>>>>>>>>>

Returns the current UTC timestamp as a value in 'YYYY-MM-DD hh:mm:ss'.

Return type: TIMESTAMP

Specification: UTC_TIMESTAMP() -> TIMESTAMP

Example::

    > source=people | eval `UTC_TIMESTAMP()` = UTC_TIMESTAMP() | fields `UTC_TIMESTAMP()`
    fetched rows / total rows = 1/1
    +---------------------+
    | UTC_TIMESTAMP()     |
    |---------------------|
    | 2022-10-03 17:54:28 |
    +---------------------+


WEEK
----

Description
>>>>>>>>>>>

Usage: week(date[, mode]) returns the week number for date. If the mode argument is omitted, the default mode 0 is used.

.. list-table:: The following table describes how the mode argument works.
   :widths: 25 50 25 75
   :header-rows: 1

   * - Mode
     - First day of week
     - Range
     - Week 1 is the first week ...
   * - 0
     - Sunday
     - 0-53
     - with a Sunday in this year
   * - 1
     - Monday
     - 0-53
     - with 4 or more days this year
   * - 2
     - Sunday
     - 1-53
     - with a Sunday in this year
   * - 3
     - Monday
     - 1-53
     - with 4 or more days this year
   * - 4
     - Sunday
     - 0-53
     - with 4 or more days this year
   * - 5
     - Monday
     - 0-53
     - with a Monday in this year
   * - 6
     - Sunday
     - 1-53
     - with 4 or more days this year
   * - 7
     - Monday
     - 1-53
     - with a Monday in this year

Argument type: DATE/TIMESTAMP/STRING

Return type: INTEGER

Synonyms: `WEEK_OF_YEAR`_

Example::

    os> source=people | eval `WEEK(DATE('2008-02-20'))` = WEEK(DATE('2008-02-20')), `WEEK(DATE('2008-02-20'), 1)` = WEEK(DATE('2008-02-20'), 1) | fields `WEEK(DATE('2008-02-20'))`, `WEEK(DATE('2008-02-20'), 1)`
    fetched rows / total rows = 1/1
    +--------------------------+-----------------------------+
    | WEEK(DATE('2008-02-20')) | WEEK(DATE('2008-02-20'), 1) |
    |--------------------------+-----------------------------|
    | 7                        | 8                           |
    +--------------------------+-----------------------------+


WEEKDAY
-------

Description
>>>>>>>>>>>

Usage: weekday(date) returns the weekday index for date (0 = Monday, 1 = Tuesday, ..., 6 = Sunday).

It is similar to the `dayofweek`_ function, but returns different indexes for each day.

Argument type: STRING/DATE/TIME/TIMESTAMP

Return type: INTEGER

Example::

    os> source=people | eval `weekday(DATE('2020-08-26'))` = weekday(DATE('2020-08-26')) | eval `weekday(DATE('2020-08-27'))` = weekday(DATE('2020-08-27')) | fields `weekday(DATE('2020-08-26'))`, `weekday(DATE('2020-08-27'))`
    fetched rows / total rows = 1/1
    +-----------------------------+-----------------------------+
    | weekday(DATE('2020-08-26')) | weekday(DATE('2020-08-27')) |
    |-----------------------------+-----------------------------|
    | 2                           | 3                           |
    +-----------------------------+-----------------------------+


WEEK_OF_YEAR
------------

Description
>>>>>>>>>>>

Usage: week_of_year(date[, mode]) returns the week number for date. If the mode argument is omitted, the default mode 0 is used.

.. list-table:: The following table describes how the mode argument works.
   :widths: 25 50 25 75
   :header-rows: 1

   * - Mode
     - First day of week
     - Range
     - Week 1 is the first week ...
   * - 0
     - Sunday
     - 0-53
     - with a Sunday in this year
   * - 1
     - Monday
     - 0-53
     - with 4 or more days this year
   * - 2
     - Sunday
     - 1-53
     - with a Sunday in this year
   * - 3
     - Monday
     - 1-53
     - with 4 or more days this year
   * - 4
     - Sunday
     - 0-53
     - with 4 or more days this year
   * - 5
     - Monday
     - 0-53
     - with a Monday in this year
   * - 6
     - Sunday
     - 1-53
     - with 4 or more days this year
   * - 7
     - Monday
     - 1-53
     - with a Monday in this year

Argument type: DATE/TIMESTAMP/STRING

Return type: INTEGER

Synonyms: `WEEK`_

Example::

    os> source=people | eval `WEEK_OF_YEAR(DATE('2008-02-20'))` = WEEK(DATE('2008-02-20')), `WEEK_OF_YEAR(DATE('2008-02-20'), 1)` = WEEK_OF_YEAR(DATE('2008-02-20'), 1) | fields `WEEK_OF_YEAR(DATE('2008-02-20'))`, `WEEK_OF_YEAR(DATE('2008-02-20'), 1)`
    fetched rows / total rows = 1/1
    +----------------------------------+-------------------------------------+
    | WEEK_OF_YEAR(DATE('2008-02-20')) | WEEK_OF_YEAR(DATE('2008-02-20'), 1) |
    |----------------------------------+-------------------------------------|
    | 7                                | 8                                   |
    +----------------------------------+-------------------------------------+


YEAR
----

Description
>>>>>>>>>>>

Usage: year(date) returns the year for date, in the range 1000 to 9999, or 0 for the “zero” date.

Argument type: STRING/DATE/TIMESTAMP

Return type: INTEGER

Example::

    os> source=people | eval `YEAR(DATE('2020-08-26'))` = YEAR(DATE('2020-08-26')) | fields `YEAR(DATE('2020-08-26'))`
    fetched rows / total rows = 1/1
    +--------------------------+
    | YEAR(DATE('2020-08-26')) |
    |--------------------------|
    | 2020                     |
    +--------------------------+


YEARWEEK
--------

Description
>>>>>>>>>>>

Usage: yearweek(date[, mode]) returns the year and week for date as an integer. It accepts and optional mode arguments aligned with those available for the `WEEK`_ function.

Argument type: STRING/DATE/TIME/TIMESTAMP

Return type: INTEGER

Example::

    os> source=people | eval `YEARWEEK('2020-08-26')` = YEARWEEK('2020-08-26') | eval `YEARWEEK('2019-01-05', 1)` = YEARWEEK('2019-01-05', 1) | fields `YEARWEEK('2020-08-26')`, `YEARWEEK('2019-01-05', 1)`
    fetched rows / total rows = 1/1
    +------------------------+---------------------------+
    | YEARWEEK('2020-08-26') | YEARWEEK('2019-01-05', 1) |
    |------------------------+---------------------------|
    | 202034                 | 201901                    |
    +------------------------+---------------------------+

    os> source=people | eval `YEARWEEK('2025-01-04')` = YEARWEEK('2025-01-04') | fields `YEARWEEK('2025-01-04', 1)`
    fetched rows / total rows = 1/1
    +---------------------------+
    | YEARWEEK('2025-01-04', 1) |
    |---------------------------|
    | 202452                    |
    +---------------------------+

