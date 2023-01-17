=========
Functions
=========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Introduction
============

There is support for a wide variety of functions shared by SQL/PPL. We are intend to generate this part of documentation automatically from our type system. However, the type system is missing descriptive information for now. So only formal specifications of all functions supported are listed at the moment. More details will be added in future.

Most of the specifications can be self explained just as a regular function with data type as argument. The only notation that needs elaboration is generic type ``T`` which binds to an actual type and can be used as return type. For example, ``ABS(NUMBER T) -> T`` means function ``ABS`` accepts an numerical argument of type ``T`` which could be any sub-type of ``NUMBER`` type and returns the actual type of ``T`` as return type. The actual type binds to generic type at runtime dynamically.


Type Conversion
===============

CAST
----

Description
>>>>>>>>>>>

Usage: cast(expr as dateType) cast the expr to dataType. return the value of dataType. The following conversion rules are used:

+------------+--------+--------+---------+-------------+--------+--------+
| Src/Target | STRING | NUMBER | BOOLEAN | TIMESTAMP   | DATE   | TIME   |
+------------+--------+--------+---------+-------------+--------+--------+
| STRING     |        | Note1  | Note1   | TIMESTAMP() | DATE() | TIME() |
+------------+--------+--------+---------+-------------+--------+--------+
| NUMBER     | Note1  |        | v!=0    | N/A         | N/A    | N/A    |
+------------+--------+--------+---------+-------------+--------+--------+
| BOOLEAN    | Note1  | v?1:0  |         | N/A         | N/A    | N/A    |
+------------+--------+--------+---------+-------------+--------+--------+
| TIMESTAMP  | Note1  | N/A    | N/A     |             | DATE() | TIME() |
+------------+--------+--------+---------+-------------+--------+--------+
| DATE       | Note1  | N/A    | N/A     | N/A         |        | N/A    |
+------------+--------+--------+---------+-------------+--------+--------+
| TIME       | Note1  | N/A    | N/A     | N/A         | N/A    |        |
+------------+--------+--------+---------+-------------+--------+--------+

Note1: the conversion follow the JDK specification.

Cast to string example::

    os> SELECT cast(true as string) as cbool, cast(1 as string) as cint, cast(DATE '2012-08-07' as string) as cdate
    fetched rows / total rows = 1/1
    +---------+--------+------------+
    | cbool   | cint   | cdate      |
    |---------+--------+------------|
    | true    | 1      | 2012-08-07 |
    +---------+--------+------------+

Cast to number example::

    os> SELECT cast(true as int) as cbool, cast('1' as integer) as cstring
    fetched rows / total rows = 1/1
    +---------+-----------+
    | cbool   | cstring   |
    |---------+-----------|
    | 1       | 1         |
    +---------+-----------+

Cast to date example::

    os> SELECT cast('2012-08-07' as date) as cdate, cast('01:01:01' as time) as ctime, cast('2012-08-07 01:01:01' as timestamp) as ctimestamp
    fetched rows / total rows = 1/1
    +------------+----------+---------------------+
    | cdate      | ctime    | ctimestamp          |
    |------------+----------+---------------------|
    | 2012-08-07 | 01:01:01 | 2012-08-07 01:01:01 |
    +------------+----------+---------------------+

Cast function can be chained::

    os> SELECT cast(cast(true as string) as boolean) as cbool
    fetched rows / total rows = 1/1
    +---------+
    | cbool   |
    |---------|
    | True    |
    +---------+


Mathematical Functions
======================

ABS
---

Description
>>>>>>>>>>>

Specifications:

1. ABS(NUMBER T) -> T


ACOS
----

Description
>>>>>>>>>>>

Usage: acos(x) calculate the arc cosine of x. Returns NULL if x is not in the range -1 to 1.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> SELECT ACOS(0)
    fetched rows / total rows = 1/1
    +--------------------+
    | ACOS(0)            |
    |--------------------|
    | 1.5707963267948966 |
    +--------------------+


ADD
---

Description
>>>>>>>>>>>

Specifications:

1. ADD(NUMBER T, NUMBER) -> T


ASIN
----

Description
>>>>>>>>>>>

Usage: asin(x) calculate the arc sine of x. Returns NULL if x is not in the range -1 to 1.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> SELECT ASIN(0)
    fetched rows / total rows = 1/1
    +-----------+
    | ASIN(0)   |
    |-----------|
    | 0.0       |
    +-----------+


ATAN
----

Description
>>>>>>>>>>>

Usage: atan(x) calculates the arc tangent of x. atan(y, x) calculates the arc tangent of y / x, except that the signs of both arguments are used to determine the quadrant of the result.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> SELECT ATAN(2), ATAN(2, 3)
    fetched rows / total rows = 1/1
    +--------------------+--------------------+
    | ATAN(2)            | ATAN(2, 3)         |
    |--------------------+--------------------|
    | 1.1071487177940904 | 0.5880026035475675 |
    +--------------------+--------------------+


ATAN2
-----

Description
>>>>>>>>>>>

Usage: atan2(y, x) calculates the arc tangent of y / x, except that the signs of both arguments are used to determine the quadrant of the result.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> SELECT ATAN2(2, 3)
    fetched rows / total rows = 1/1
    +--------------------+
    | ATAN2(2, 3)        |
    |--------------------|
    | 0.5880026035475675 |
    +--------------------+


CBRT
----

Description
>>>>>>>>>>>

Usage: CBRT(number) calculates the cube root of a number

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE

Example::

    opensearchsql> SELECT CBRT(8), CBRT(9.261), CBRT(-27);
    fetched rows / total rows = 1/1
    +-----------+---------------+-------------+
    | CBRT(8)   | CBRT(9.261)   | CBRT(-27)   |
    |-----------+---------------+-------------|
    | 2.0       | 2.1           | -3.0        |
    +-----------+---------------+-------------+


CEIL
----

An alias for `CEILING`_ function.


CEILING
-------

Description
>>>>>>>>>>>

Usage: CEILING(T) takes the ceiling of value T.

Note: `CEIL`_ and CEILING functions have the same implementation & functionality

Limitation: CEILING only works as expected when IEEE 754 double type displays decimal when stored.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: LONG

Example::

   os> SELECT CEILING(0), CEILING(50.00005), CEILING(-50.00005);
    fetched rows / total rows = 1/1
    +--------------+---------------------+----------------------+
    | CEILING(0)   | CEILING(50.00005)   | CEILING(-50.00005)   |
    |--------------+---------------------+----------------------|
    | 0            | 51                  | -50                  |
    +--------------+---------------------+----------------------+

   os> SELECT CEILING(3147483647.12345), CEILING(113147483647.12345), CEILING(3147483647.00001);
    fetched rows / total rows = 1/1
    +-----------------------------+-------------------------------+-----------------------------+
    | CEILING(3147483647.12345)   | CEILING(113147483647.12345)   | CEILING(3147483647.00001)   |
    |-----------------------------+-------------------------------+-----------------------------|
    | 3147483648                  | 113147483648                  | 3147483648                  |
    +-----------------------------+-------------------------------+-----------------------------+


CONV
----

Description
>>>>>>>>>>>

Usage: CONV(x, a, b) converts the number x from a base to b base.

Argument type: x: STRING, a: INTEGER, b: INTEGER

Return type: STRING

Example::

    os> SELECT CONV('12', 10, 16), CONV('2C', 16, 10), CONV(12, 10, 2), CONV(1111, 2, 10)
    fetched rows / total rows = 1/1
    +----------------------+----------------------+-------------------+---------------------+
    | CONV('12', 10, 16)   | CONV('2C', 16, 10)   | CONV(12, 10, 2)   | CONV(1111, 2, 10)   |
    |----------------------+----------------------+-------------------+---------------------|
    | c                    | 44                   | 1100              | 15                  |
    +----------------------+----------------------+-------------------+---------------------+

COS
---

Description
>>>>>>>>>>>

Usage: cos(x) calculate the cosine of x, where x is given in radians.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> SELECT COS(0)
    fetched rows / total rows = 1/1
    +----------+
    | COS(0)   |
    |----------|
    | 1.0      |
    +----------+


COSH
----

Description
>>>>>>>>>>>

Specifications:

1. COSH(NUMBER T) -> DOUBLE


COT
---

Description
>>>>>>>>>>>

Usage: cot(x) calculate the cotangent of x. Returns out-of-range error if x equals to 0.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> SELECT COT(1)
    fetched rows / total rows = 1/1
    +--------------------+
    | COT(1)             |
    |--------------------|
    | 0.6420926159343306 |
    +--------------------+


CRC32
-----

Description
>>>>>>>>>>>

Usage: Calculates a cyclic redundancy check value and returns a 32-bit unsigned value.

Argument type: STRING

Return type: LONG

Example::

    os> SELECT CRC32('MySQL')
    fetched rows / total rows = 1/1
    +------------------+
    | CRC32('MySQL')   |
    |------------------|
    | 3259397556       |
    +------------------+


DEGREES
-------

Description
>>>>>>>>>>>

Usage: degrees(x) converts x from radians to degrees.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> SELECT DEGREES(1.57)
    fetched rows / total rows  = 1/1
    +-------------------+
    | DEGREES(1.57)     |
    |-------------------|
    | 89.95437383553924 |
    +-------------------+


DIVIDE
------

Description
>>>>>>>>>>>

Specifications:

1. DIVIDE(NUMBER T, NUMBER) -> T


E
-

Description
>>>>>>>>>>>

Usage: E() returns the Euler's number

Return type: DOUBLE

Example::

    os> SELECT E()
    fetched rows / total rows = 1/1
    +-------------------+
    | E()               |
    |-------------------|
    | 2.718281828459045 |
    +-------------------+


EXP
---

Description
>>>>>>>>>>>

Specifications:

1. EXP(NUMBER T) -> T


EXPM1
-----

Description
>>>>>>>>>>>

Specifications:

1. EXPM1(NUMBER T) -> T


FLOOR
-----

Description
>>>>>>>>>>>

Usage: FLOOR(T) takes the floor of value T.

Limitation: FLOOR only works as expected when IEEE 754 double type displays decimal when stored.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: LONG

Example::

   os> SELECT FLOOR(0), FLOOR(50.00005), FLOOR(-50.00005);
    fetched rows / total rows = 1/1
    +------------+-------------------+--------------------+
    | FLOOR(0)   | FLOOR(50.00005)   | FLOOR(-50.00005)   |
    |------------+-------------------+--------------------|
    | 0          | 50                | -51                |
    +------------+-------------------+--------------------+

   os> SELECT FLOOR(3147483647.12345), FLOOR(113147483647.12345), FLOOR(3147483647.00001);
    fetched rows / total rows = 1/1
    +---------------------------+-----------------------------+---------------------------+
    | FLOOR(3147483647.12345)   | FLOOR(113147483647.12345)   | FLOOR(3147483647.00001)   |
    |---------------------------+-----------------------------+---------------------------|
    | 3147483647                | 113147483647                | 3147483647                |
    +---------------------------+-----------------------------+---------------------------+

    os> SELECT FLOOR(282474973688888.022), FLOOR(9223372036854775807.022), FLOOR(9223372036854775807.0000001);
    fetched rows / total rows = 1/1
    +------------------------------+----------------------------------+--------------------------------------+
    | FLOOR(282474973688888.022)   | FLOOR(9223372036854775807.022)   | FLOOR(9223372036854775807.0000001)   |
    |------------------------------+----------------------------------+--------------------------------------|
    | 282474973688888              | 9223372036854775807              | 9223372036854775807                  |
    +------------------------------+----------------------------------+--------------------------------------+

LN
--

Description
>>>>>>>>>>>

Specifications:

1. LN(NUMBER T) -> DOUBLE


LOG
---

Description
>>>>>>>>>>>

Specifications:

1. LOG(NUMBER T) -> DOUBLE
2. LOG(NUMBER T, NUMBER) -> DOUBLE


LOG2
----

Description
>>>>>>>>>>>

Specifications:

1. LOG2(NUMBER T) -> DOUBLE


LOG10
-----

Description
>>>>>>>>>>>

Specifications:

1. LOG10(NUMBER T) -> DOUBLE


MOD
---

Description
>>>>>>>>>>>

Usage: MOD(n, m) calculates the remainder of the number n divided by m.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: Wider type between types of n and m if m is nonzero value. If m equals to 0, then returns NULL.

Example::

    os> SELECT MOD(3, 2), MOD(3.1, 2)
    fetched rows / total rows = 1/1
    +-------------+---------------+
    | MOD(3, 2)   | MOD(3.1, 2)   |
    |-------------+---------------|
    | 1           | 1.1           |
    +-------------+---------------+


MULTIPLY
--------

Description
>>>>>>>>>>>

Specifications:

1. MULTIPLY(NUMBER T, NUMBER) -> NUMBER

PI
--

Description
>>>>>>>>>>>

Usage: PI() returns the constant pi

Return type: DOUBLE

Example::

    os> SELECT PI()
    fetched rows / total rows = 1/1
    +-------------------+
    | PI()              |
    |-------------------|
    | 3.141592653589793 |
    +-------------------+


POW
---

Description
>>>>>>>>>>>

Usage: POW(x, y) calculates the value of x raised to the power of y. Bad inputs return NULL result.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Synonyms: `POWER`_

Example::

    os> SELECT POW(3, 2), POW(-3, 2), POW(3, -2)
    fetched rows / total rows = 1/1
    +-------------+--------------+--------------------+
    | POW(3, 2)   | POW(-3, 2)   | POW(3, -2)         |
    |-------------+--------------+--------------------|
    | 9.0         | 9.0          | 0.1111111111111111 |
    +-------------+--------------+--------------------+


POWER
-----

Description
>>>>>>>>>>>

Usage: POWER(x, y) calculates the value of x raised to the power of y. Bad inputs return NULL result.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Synonyms: `POW`_

Example::

    os> SELECT POWER(3, 2), POWER(-3, 2), POWER(3, -2)
    fetched rows / total rows = 1/1
    +---------------+----------------+--------------------+
    | POWER(3, 2)   | POWER(-3, 2)   | POWER(3, -2)       |
    |---------------+----------------+--------------------|
    | 9.0           | 9.0            | 0.1111111111111111 |
    +---------------+----------------+--------------------+


RADIANS
-------

Description
>>>>>>>>>>>

Usage: radians(x) converts x from degrees to radians.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> SELECT RADIANS(90)
    fetched rows / total rows  = 1/1
    +--------------------+
    | RADIANS(90)        |
    |--------------------|
    | 1.5707963267948966 |
    +--------------------+


RAND
----

Description
>>>>>>>>>>>

Usage: RAND()/RAND(N) returns a random floating-point value in the range 0 <= value < 1.0. If integer N is specified, the seed is initialized prior to execution. One implication of this behavior is with identical argument N, rand(N) returns the same value each time, and thus produces a repeatable sequence of column values.

Argument type: INTEGER

Return type: FLOAT

Example::

    os> SELECT RAND(3)
    fetched rows / total rows = 1/1
    +------------+
    | RAND(3)    |
    |------------|
    | 0.73105735 |
    +------------+


RINT
----

Description
>>>>>>>>>>>

Specifications:

1. RINT(NUMBER T) -> T


ROUND
-----

Description
>>>>>>>>>>>

Usage: ROUND(x, d) rounds the argument x to d decimal places, d defaults to 0 if not specified

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type map:

(INTEGER/LONG [,INTEGER]) -> LONG
(FLOAT/DOUBLE [,INTEGER]) -> LONG

Example::

    os> SELECT ROUND(12.34), ROUND(12.34, 1), ROUND(12.34, -1), ROUND(12, 1)
    fetched rows / total rows = 1/1
    +----------------+-------------------+--------------------+----------------+
    | ROUND(12.34)   | ROUND(12.34, 1)   | ROUND(12.34, -1)   | ROUND(12, 1)   |
    |----------------+-------------------+--------------------+----------------|
    | 12.0           | 12.3              | 10.0               | 12             |
    +----------------+-------------------+--------------------+----------------+


SIGN
----

Description
>>>>>>>>>>>

Usage: Returns the sign of the argument as -1, 0, or 1, depending on whether the number is negative, zero, or positive

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: INTEGER

Example::

    os> SELECT SIGN(1), SIGN(0), SIGN(-1.1)
    fetched rows / total rows = 1/1
    +-----------+-----------+--------------+
    | SIGN(1)   | SIGN(0)   | SIGN(-1.1)   |
    |-----------+-----------+--------------|
    | 1         | 0         | -1           |
    +-----------+-----------+--------------+


SIGNUM
------

Description
>>>>>>>>>>>

Specifications:

1. SIGNUM(NUMBER T) -> T


SIN
---

Description
>>>>>>>>>>>

Usage: sin(x) calculate the sine of x, where x is given in radians.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> SELECT SIN(0)
    fetched rows / total rows = 1/1
    +----------+
    | SIN(0)   |
    |----------|
    | 0.0      |
    +----------+


SINH
----

Description
>>>>>>>>>>>

Specifications:

1. SINH(NUMBER T) -> DOUBLE


SQRT
----

Description
>>>>>>>>>>>

Usage: Calculates the square root of a non-negative number

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type map:

(Non-negative) INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE
(Negative) INTEGER/LONG/FLOAT/DOUBLE -> NULL

Example::

    os> SELECT SQRT(4), SQRT(4.41)
    fetched rows / total rows = 1/1
    +-----------+--------------+
    | SQRT(4)   | SQRT(4.41)   |
    |-----------+--------------|
    | 2.0       | 2.1          |
    +-----------+--------------+


STRCMP
------

Description
>>>>>>>>>>>

Usage: strcmp(str1, str2) returns 0 if strings are same, -1 if first arg < second arg according to current sort order, and 1 otherwise.

Argument type: STRING, STRING

Return type: INTEGER

Example::

    os> SELECT STRCMP('hello', 'world'), STRCMP('hello', 'hello')
    fetched rows / total rows = 1/1
    +----------------------------+----------------------------+
    | STRCMP('hello', 'world')   | STRCMP('hello', 'hello')   |
    |----------------------------+----------------------------|
    | -1                         | 0                          |
    +----------------------------+----------------------------+



SUBTRACT
--------

Description
>>>>>>>>>>>

Specifications:

1. SUBTRACT(NUMBER T, NUMBER) -> T


TAN
---

Description
>>>>>>>>>>>

Usage: tan(x) calculate the tangent of x, where x is given in radians.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> SELECT TAN(0)
    fetched rows / total rows = 1/1
    +----------+
    | TAN(0)   |
    |----------|
    | 0.0      |
    +----------+


TRUNCATE
--------

Description
>>>>>>>>>>>

Usage: TRUNCATE(x, d) returns the number x, truncated to d decimal place

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type map:

INTEGER/LONG -> LONG
FLOAT/DOUBLE -> DOUBLE

Example::

    fetched rows / total rows = 1/1
    +----------------------+-----------------------+-------------------+
    | TRUNCATE(56.78, 1)   | TRUNCATE(56.78, -1)   | TRUNCATE(56, 1)   |
    |----------------------+-----------------------+-------------------|
    | 56.7                 | 50                    | 56                |
    +----------------------+-----------------------+-------------------+



Date and Time Functions
=======================

ADDDATE
-------

Description
>>>>>>>>>>>

Usage: adddate(date, INTERVAL expr unit)/ adddate(date, expr) adds the time interval of second argument to date; adddate(date, days) adds the second argument as integer number of days to date.

Argument type: DATE/DATETIME/TIMESTAMP/STRING, INTERVAL/LONG

Return type map:

(DATE/DATETIME/TIMESTAMP/STRING, INTERVAL) -> DATETIME

(DATE, LONG) -> DATE

(DATETIME/TIMESTAMP/STRING, LONG) -> DATETIME

Synonyms: `DATE_ADD`_

Example::

    os> SELECT ADDDATE(DATE('2020-08-26'), INTERVAL 1 HOUR), ADDDATE(DATE('2020-08-26'), 1), ADDDATE(TIMESTAMP('2020-08-26 01:01:01'), 1)
    fetched rows / total rows = 1/1
    +------------------------------------------------+----------------------------------+------------------------------------------------+
    | ADDDATE(DATE('2020-08-26'), INTERVAL 1 HOUR)   | ADDDATE(DATE('2020-08-26'), 1)   | ADDDATE(TIMESTAMP('2020-08-26 01:01:01'), 1)   |
    |------------------------------------------------+----------------------------------+------------------------------------------------|
    | 2020-08-26 01:00:00                            | 2020-08-27                       | 2020-08-27 01:01:01                            |
    +------------------------------------------------+----------------------------------+------------------------------------------------+


ADDTIME
-------

Description
>>>>>>>>>>>

Usage: addtime(expr1, expr2) adds expr2 to expr1 and returns the result. If argument is TIME, today's date is used; if argument is DATE, time at midnight is used.

Argument type: DATE/DATETIME/TIMESTAMP/TIME, DATE/DATETIME/TIMESTAMP/TIME

Return type map:

(DATE/DATETIME/TIMESTAMP, DATE/DATETIME/TIMESTAMP/TIME) -> DATETIME

(TIME, DATE/DATETIME/TIMESTAMP/TIME) -> TIME

Antonyms: `SUBTIME`_

Example::

    os> SELECT ADDTIME(DATE('2008-12-12'), DATE('2008-11-15')) AS `'2008-12-12' + 0 `
    fetched rows / total rows = 1/1
    +---------------------+
    | '2008-12-12' + 0    |
    |---------------------|
    | 2008-12-12 00:00:00 |
    +---------------------+

    os> SELECT ADDTIME(TIME('23:59:59'), DATE('2004-01-01')) AS `'23:59:59' + 0`
    fetched rows / total rows = 1/1
    +------------------+
    | '23:59:59' + 0   |
    |------------------|
    | 23:59:59         |
    +------------------+

    os> SELECT ADDTIME(DATE('2004-01-01'), TIME('23:59:59')) AS `'2004-01-01' + '23:59:59'`
    fetched rows / total rows = 1/1
    +-----------------------------+
    | '2004-01-01' + '23:59:59'   |
    |-----------------------------|
    | 2004-01-01 23:59:59         |
    +-----------------------------+

    os> SELECT ADDTIME(TIME('10:20:30'), TIME('00:05:42')) AS `'10:20:30' + '00:05:42'`
    fetched rows / total rows = 1/1
    +---------------------------+
    | '10:20:30' + '00:05:42'   |
    |---------------------------|
    | 10:26:12                  |
    +---------------------------+

    os> SELECT ADDTIME(TIMESTAMP('2007-02-28 10:20:30'), DATETIME('2002-03-04 20:40:50')) AS `'2007-02-28 10:20:30' + '20:40:50'`
    fetched rows / total rows = 1/1
    +--------------------------------------+
    | '2007-02-28 10:20:30' + '20:40:50'   |
    |--------------------------------------|
    | 2007-03-01 07:01:20                  |
    +--------------------------------------+


CONVERT_TZ
----------

Description
>>>>>>>>>>>

Usage: convert_tz(datetime, from_timezone, to_timezone) constructs a datetime object converted from the from_timezone to the to_timezone.

Argument type: DATETIME, STRING, STRING

Return type: DATETIME

Example::

  os> SELECT CONVERT_TZ('2008-12-25 05:30:00', '+00:00', 'America/Los_Angeles')
    fetched rows / total rows = 1/1
    +----------------------------------------------------------------------+
    | CONVERT_TZ('2008-12-25 05:30:00', '+00:00', 'America/Los_Angeles')   |
    |----------------------------------------------------------------------|
    | 2008-12-24 21:30:00                                                  |
    +----------------------------------------------------------------------+


  os> SELECT CONVERT_TZ("2010-10-10 10:10:10", "+01:00", "-10:00")
    fetched rows / total rows = 1/1
    +---------------------------------------------------------+
    | CONVERT_TZ("2010-10-10 10:10:10", "+01:00", "-10:00")   |
    |---------------------------------------------------------|
    | 2010-10-09 23:10:10                                     |
    +---------------------------------------------------------+

When the datedate, or either of the two time zone fields are invalid format, then the result is null. In this example any datetime that is not <yyyy-MM-dd HH:mm:ss> will result in null.
Example::

  os> SELECT CONVERT_TZ("test", "+01:00", "-10:00")
      fetched rows / total rows = 1/1
      +------------------------------------------+
      | CONVERT_TZ("test", "+01:00", "-10:00")   |
      |------------------------------------------|
      | null                                     |
      +------------------------------------------+

When the datetime, or either of the two time zone fields are invalid format, then the result is null. In this example any timezone that is not <+HH:mm> or <-HH:mm> will result in null.
Example::

  os> SELECT CONVERT_TZ("2010-10-10 10:10:10", "test", "-10:00")
      fetched rows / total rows = 1/1
      +-------------------------------------------------------+
      | CONVERT_TZ("2010-10-10 10:10:10", "test", "-10:00")   |
      |-------------------------------------------------------|
      | null                                                  |
      +-------------------------------------------------------+

The valid timezone range for convert_tz is (-13:59, +14:00) inclusive. Timezones outside of the range will result in null.
Example::

  os> SELECT CONVERT_TZ("2010-10-10 10:10:10", "+01:00", "+14:00")
    fetched rows / total rows = 1/1
    +---------------------------------------------------------+
    | CONVERT_TZ("2010-10-10 10:10:10", "+01:00", "+14:00")   |
    |---------------------------------------------------------|
    | 2010-10-10 23:10:10                                     |
    +---------------------------------------------------------+

The valid timezone range for convert_tz is (-13:59, +14:00) inclusive. Timezones outside of the range will result in null.
Example::

  os> SELECT CONVERT_TZ("2010-10-10 10:10:10", "+01:00", "+14:01")
    fetched rows / total rows = 1/1
    +---------------------------------------------------------+
    | CONVERT_TZ("2010-10-10 10:10:10", "+01:00", "+14:01")   |
    |---------------------------------------------------------|
    | null                                                    | 
    +---------------------------------------------------------+

The valid timezone range for convert_tz is (-13:59, +14:00) inclusive. Timezones outside of the range will result in null.
Example::

  os> SELECT CONVERT_TZ("2010-10-10 10:10:10", "+01:00", "-13:59")
    fetched rows / total rows = 1/1
    +---------------------------------------------------------+
    | CONVERT_TZ("2010-10-10 10:10:10", "+01:00", "-13:59")   |
    |---------------------------------------------------------|
    | 2010-10-09 19:11:10                                     |
    +---------------------------------------------------------+

The valid timezone range for convert_tz is (-13:59, +14:00) inclusive. Timezones outside of the range will result in null.
Example::

  os> SELECT CONVERT_TZ("2010-10-10 10:10:10", "+01:00", "-14:00")
    fetched rows / total rows = 1/1
    +---------------------------------------------------------+
    | CONVERT_TZ("2010-10-10 10:10:10", "+01:00", "-14:00")   |
    |---------------------------------------------------------|
    | null                                                    | 
    +---------------------------------------------------------+


CURDATE
-------

Description
>>>>>>>>>>>

Returns the current time as a value in 'YYYY-MM-DD'.
CURDATE() returns the time at which it executes as `SYSDATE() <#sysdate>`_ does.

Return type: DATE

Specification: CURDATE() -> DATE

Example::

    > SELECT CURDATE();
    fetched rows / total rows = 1/1
    +-------------+
    | CURDATE()   |
    |-------------|
    | 2022-08-02  |
    +-------------+


CURRENT_DATE
------------

Description
>>>>>>>>>>>

`CURRENT_DATE()` are synonyms for `CURDATE() <#curdate>`_.

Example::

    > SELECT CURRENT_DATE();
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

    > SELECT CURRENT_TIME();
    fetched rows / total rows = 1/1
    +-----------------+
    | CURRENT_TIME()  |
    |-----------------+
    | 15:39:05        |
    +-----------------+


CURRENT_TIMESTAMP
-----------------

Description
>>>>>>>>>>>

`CURRENT_TIMESTAMP()` are synonyms for `NOW() <#now>`_.

Example::

    > SELECT CURRENT_TIMESTAMP();
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

    > SELECT CURTIME() as value_1, CURTIME()  as value_2;
    fetched rows / total rows = 1/1
    +-----------+-----------+
    | value_1   | value_2   |
    |-----------+-----------|
    | 15:39:05  | 15:39:05  |
    +-----------+-----------+


DATE
----

Description
>>>>>>>>>>>

Usage: date(expr) constructs a date type with the input string expr as a date. If the argument is of date/datetime/timestamp, it extracts the date value part from the expression.

Argument type: STRING/DATE/DATETIME/TIMESTAMP

Return type: DATE

Example::

    os> SELECT DATE('2020-08-26'), DATE(TIMESTAMP('2020-08-26 13:49:00')), DATE('2020-08-26 13:49:00'), DATE('2020-08-26 13:49')
    fetched rows / total rows = 1/1
    +----------------------+------------------------------------------+-------------------------------+----------------------------+
    | DATE('2020-08-26')   | DATE(TIMESTAMP('2020-08-26 13:49:00'))   | DATE('2020-08-26 13:49:00')   | DATE('2020-08-26 13:49')   |
    |----------------------+------------------------------------------+-------------------------------+----------------------------|
    | 2020-08-26           | 2020-08-26                               | 2020-08-26                    | 2020-08-26                 |
    +----------------------+------------------------------------------+-------------------------------+----------------------------+


DATETIME
--------

Description
>>>>>>>>>>>

Usage: datetime(datetime)/ datetime(date, to_timezone) Converts the datetime to a new timezone

Argument type: DATETIME/STRING

Return type map:

DATETIME, STRING -> DATETIME

DATETIME -> DATETIME

Example::

    os> SELECT DATETIME('2008-12-25 05:30:00+00:00', 'America/Los_Angeles')
    fetched rows / total rows = 1/1
    +----------------------------------------------------------------+
    | DATETIME('2008-12-25 05:30:00+00:00', 'America/Los_Angeles')   |
    |----------------------------------------------------------------|
    | 2008-12-24 21:30:00                                            |
    +----------------------------------------------------------------+

This example converts from -10:00 timezone to +10:00 timezone.
Example::

    os> SELECT DATETIME('2004-02-28 23:00:00-10:00', '+10:00')
    fetched rows / total rows = 1/1
    +---------------------------------------------------+
    | DATETIME('2004-02-28 23:00:00-10:00', '+10:00')   |
    |---------------------------------------------------|
    | 2004-02-29 19:00:00                               |
    +---------------------------------------------------+

This example uses the timezone -14:00, which is outside of the range -13:59 and +14:00. This results in a null value.
Example::

    os> SELECT DATETIME('2008-01-01 02:00:00', '-14:00')
    fetched rows / total rows = 1/1
    +---------------------------------------------+
    | DATETIME('2008-01-01 02:00:00', '-14:00')   |
    |---------------------------------------------|
    | null                                        |
    +---------------------------------------------+

February 30th is not a day, so it returns null.
Example::

    os> SELECT DATETIME('2008-02-30 02:00:00', '-00:00')
    fetched rows / total rows = 1/1
    +---------------------------------------------+
    | DATETIME('2008-02-30 02:00:00', '-00:00')   |
    |---------------------------------------------|
    | null                                        |
    +---------------------------------------------+

DATETIME(datetime) examples

DATETIME with no timezone specified does no conversion.
Example::

    os> SELECT DATETIME('2008-02-10 02:00:00')
    fetched rows / total rows = 1/1
    +-----------------------------------+
    | DATETIME('2008-02-10 02:00:00')   |
    |-----------------------------------|
    | 2008-02-10 02:00:00               |
    +-----------------------------------+

February 30th is not a day, so it returns null.
Example::

    os> SELECT DATETIME('2008-02-30 02:00:00')
    fetched rows / total rows = 1/1
    +-----------------------------------+
    | DATETIME('2008-02-30 02:00:00')   |
    |-----------------------------------|
    | null                              |
    +-----------------------------------+

DATETIME with a datetime and no seperate timezone to convert to returns the datetime object without a timezone.
Example::

    os> SELECT DATETIME('2008-02-10 02:00:00+04:00')
    fetched rows / total rows = 1/1
    +-----------------------------------------+
    | DATETIME('2008-02-10 02:00:00+04:00')   |
    |-----------------------------------------|
    | 2008-02-10 02:00:00                     |
    +-----------------------------------------+

DATE_ADD
--------

Description
>>>>>>>>>>>

Usage: date_add(date, INTERVAL expr unit)/ date_add(date, expr) adds the time interval expr to date

Argument type: DATE/DATETIME/TIMESTAMP/STRING, INTERVAL/LONG

Return type map:

DATE/DATETIME/TIMESTAMP/STRING, INTERVAL -> DATETIME

DATE, LONG -> DATE

DATETIME/TIMESTAMP/STRING, LONG -> DATETIME

Synonyms: `ADDDATE`_

Example::

    os> SELECT DATE_ADD(DATE('2020-08-26'), INTERVAL 1 HOUR), DATE_ADD(DATE('2020-08-26'), 1), DATE_ADD(TIMESTAMP('2020-08-26 01:01:01'), 1)
    fetched rows / total rows = 1/1
    +-------------------------------------------------+-----------------------------------+-------------------------------------------------+
    | DATE_ADD(DATE('2020-08-26'), INTERVAL 1 HOUR)   | DATE_ADD(DATE('2020-08-26'), 1)   | DATE_ADD(TIMESTAMP('2020-08-26 01:01:01'), 1)   |
    |-------------------------------------------------+-----------------------------------+-------------------------------------------------|
    | 2020-08-26 01:00:00                             | 2020-08-27                        | 2020-08-27 01:01:01                             |
    +-------------------------------------------------+-----------------------------------+-------------------------------------------------+


DATE_FORMAT
-----------

Description
>>>>>>>>>>>

Usage: date_format(date, format) formats the date argument using the specifiers in the format argument.

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
     - Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
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

Argument type: STRING/DATE/DATETIME/TIMESTAMP, STRING

Return type: STRING

Example::

    >od SELECT DATE_FORMAT('1998-01-31 13:14:15.012345', '%T.%f'), DATE_FORMAT(TIMESTAMP('1998-01-31 13:14:15.012345'), '%Y-%b-%D %r')
    fetched rows / total rows = 1/1
    +-----------------------------------------------+----------------------------------------------------------------+
    | DATE('1998-01-31 13:14:15.012345', '%T.%f')   | DATE(TIMESTAMP('1998-01-31 13:14:15.012345'), '%Y-%b-%D %r')   |
    |-----------------------------------------------+----------------------------------------------------------------|
    | '13:14:15.012345'                             | '1998-Jan-31st 01:14:15 PM'                                    |
    +-----------------------------------------------+----------------------------------------------------------------+


DATE_SUB
--------

Description
>>>>>>>>>>>

Usage: date_sub(date, INTERVAL expr unit)/ date_sub(date, expr) subtracts the time interval expr from date

Argument type: DATE/DATETIME/TIMESTAMP/STRING, INTERVAL/LONG

Return type map:

DATE/DATETIME/TIMESTAMP/STRING, INTERVAL -> DATETIME

DATE, LONG -> DATE

DATETIME/TIMESTAMP/STRING, LONG -> DATETIME

Synonyms: `SUBDATE`_

Example::

    os> SELECT DATE_SUB(DATE('2008-01-02'), INTERVAL 31 DAY), DATE_SUB(DATE('2020-08-26'), 1), DATE_SUB(TIMESTAMP('2020-08-26 01:01:01'), 1)
    fetched rows / total rows = 1/1
    +-------------------------------------------------+-----------------------------------+-------------------------------------------------+
    | DATE_SUB(DATE('2008-01-02'), INTERVAL 31 DAY)   | DATE_SUB(DATE('2020-08-26'), 1)   | DATE_SUB(TIMESTAMP('2020-08-26 01:01:01'), 1)   |
    |-------------------------------------------------+-----------------------------------+-------------------------------------------------|
    | 2007-12-02                                      | 2020-08-25                        | 2020-08-25 01:01:01                             |
    +-------------------------------------------------+-----------------------------------+-------------------------------------------------+


DATEDIFF
--------

Usage: Calculates the difference of date parts of the given values. If the first argument is time, today's date is used.

Argument type: DATE/DATETIME/TIMESTAMP/TIME, DATE/DATETIME/TIMESTAMP/TIME

Return type: LONG

Example::

    os> SELECT DATEDIFF(TIMESTAMP('2000-01-02 00:00:00'), TIMESTAMP('2000-01-01 23:59:59')) AS `'2000-01-02' - '2000-01-01'`, DATEDIFF(DATE('2001-02-01'), TIMESTAMP('2004-01-01 00:00:00')) AS `'2001-02-01' - '2004-01-01'`, DATEDIFF(TIME('23:59:59'), TIME('00:00:00')) AS `today - today`
    fetched rows / total rows = 1/1
    +-------------------------------+-------------------------------+-----------------+
    | '2000-01-02' - '2000-01-01'   | '2001-02-01' - '2004-01-01'   | today - today   |
    |-------------------------------+-------------------------------+-----------------|
    | 1                             | -1064                         | 0               |
    +-------------------------------+-------------------------------+-----------------+


DAY
---

Description
>>>>>>>>>>>

Usage: day(date) extracts the day of the month for date, in the range 1 to 31.

Argument type: STRING/DATE/DATETIME/TIME/TIMESTAMP

Return type: INTEGER

Synonyms: `DAYOFMONTH`_, `DAY_OF_MONTH`_

Example::

    os> SELECT DAY(DATE('2020-08-26'))
    fetched rows / total rows = 1/1
    +---------------------------+
    | DAY(DATE('2020-08-26'))   |
    |---------------------------|
    | 26                        |
    +---------------------------+


DAYNAME
-------

Description
>>>>>>>>>>>

Usage: dayname(date) returns the name of the weekday for date, including Monday, Tuesday, Wednesday, Thursday, Friday, Saturday and Sunday.

Argument type: STRING/DATE/DATETIME/TIMESTAMP

Return type: STRING

Example::

    os> SELECT DAYNAME(DATE('2020-08-26'))
    fetched rows / total rows = 1/1
    +-------------------------------+
    | DAYNAME(DATE('2020-08-26'))   |
    |-------------------------------|
    | Wednesday                     |
    +-------------------------------+


DAYOFMONTH
----------

Description
>>>>>>>>>>>

Usage: dayofmonth(date) extracts the day of the month for date, in the range 1 to 31.

Argument type: STRING/DATE/DATETIME/TIME/TIMESTAMP

Return type: INTEGER

Synonyms: `DAY`_, `DAY_OF_MONTH`_

Example::

    os> SELECT DAYOFMONTH(DATE('2020-08-26'))
    fetched rows / total rows = 1/1
    +----------------------------------+
    | DAYOFMONTH(DATE('2020-08-26'))   |
    |----------------------------------|
    | 26                               |
    +----------------------------------+

DAY_OF_MONTH
------------

Description
>>>>>>>>>>>

Usage: day_of_month(date) extracts the day of the month for date, in the range 1 to 31.

Argument type: STRING/DATE/TIME/DATETIME/TIMESTAMP

Return type: INTEGER

Synonyms: `DAY`_, `DAYOFMONTH`_

Example::

    os> SELECT DAY_OF_MONTH('2020-08-26')
    fetched rows / total rows = 1/1
    +------------------------------+
    | DAY_OF_MONTH('2020-08-26')   |
    |------------------------------|
    | 26                           |
    +------------------------------+

DAYOFWEEK
---------

Description
>>>>>>>>>>>

Usage: dayofweek(date) returns the weekday index for date (1 = Sunday, 2 = Monday, …, 7 = Saturday).

The `day_of_week` function is also provided as an alias.

Argument type: STRING/DATE/DATETIME/TIMESTAMP

Return type: INTEGER

Example::

    os> SELECT DAYOFWEEK('2020-08-26'), DAY_OF_WEEK('2020-08-26')
    fetched rows / total rows = 1/1
    +---------------------------+-----------------------------+
    | DAYOFWEEK('2020-08-26')   | DAY_OF_WEEK('2020-08-26')   |
    |---------------------------+-----------------------------|
    | 4                         | 4                           |
    +---------------------------+-----------------------------+


DAYOFYEAR
---------

Description
>>>>>>>>>>>

Usage:  dayofyear(date) returns the day of the year for date, in the range 1 to 366.
If an argument of type `TIME` is given, the function will use the current date.
The function `day_of_year`_ is also provided as an alias.

Argument type: STRING/DATE/DATETIME/TIME/TIMESTAMP

Return type: INTEGER

Example::

    os> SELECT DAYOFYEAR(DATE('2020-08-26'))
    fetched rows / total rows = 1/1
    +---------------------------------+
    | DAYOFYEAR(DATE('2020-08-26'))   |
    |---------------------------------|
    | 239                             |
    +---------------------------------+

    os> SELECT DAYOFYEAR(DATETIME('2020-08-26 00:00:00'))
    fetched rows / total rows = 1/1
    +----------------------------------------------+
    | DAYOFYEAR(DATETIME('2020-08-26 00:00:00'))   |
    |----------------------------------------------|
    | 239                                          |
    +----------------------------------------------+

    os> SELECT DAYOFYEAR(TIMESTAMP('2020-08-26 00:00:00'))
    fetched rows / total rows = 1/1
    +-----------------------------------------------+
    | DAYOFYEAR(TIMESTAMP('2020-08-26 00:00:00'))   |
    |-----------------------------------------------|
    | 239                                           |
    +-----------------------------------------------+


DAY_OF_YEAR
---------

Description
>>>>>>>>>>>

If an argument of type `TIME` is given, the function will use the current date.
This function is an alias to the `dayofyear`_ function

Argument type: STRING/DATE/DATETIME/TIME/TIMESTAMP

Return type: INTEGER

Example::

    os> SELECT DAY_OF_YEAR(DATE('2020-08-26'))
    fetched rows / total rows = 1/1
    +-----------------------------------+
    | DAY_OF_YEAR(DATE('2020-08-26'))   |
    |-----------------------------------|
    | 239                               |
    +-----------------------------------+

    os> SELECT DAY_OF_YEAR(DATETIME('2020-08-26 00:00:00'))
    fetched rows / total rows = 1/1
    +------------------------------------------------+
    | DAY_OF_YEAR(DATETIME('2020-08-26 00:00:00'))   |
    |------------------------------------------------|
    | 239                                            |
    +------------------------------------------------+

    os> SELECT DAY_OF_YEAR(TIMESTAMP('2020-08-26 00:00:00'))
    fetched rows / total rows = 1/1
    +-------------------------------------------------+
    | DAY_OF_YEAR(TIMESTAMP('2020-08-26 00:00:00'))   |
    |-------------------------------------------------|
    | 239                                             |
    +-------------------------------------------------+



FROM_DAYS
---------

Description
>>>>>>>>>>>

Usage: from_days(N) returns the date value given the day number N.

Argument type: INTEGER/LONG

Return type: DATE

Example::

    os> SELECT FROM_DAYS(733687)
    fetched rows / total rows = 1/1
    +---------------------+
    | FROM_DAYS(733687)   |
    |---------------------|
    | 2008-10-07          |
    +---------------------+


FROM_UNIXTIME
-------------

Description
>>>>>>>>>>>

Usage: Returns a representation of the argument given as a datetime or character string value. Perform reverse conversion for `UNIX_TIMESTAMP`_ function.
If second argument is provided, it is used to format the result in the same way as the format string used for the `DATE_FORMAT`_ function.
If timestamp is outside of range 1970-01-01 00:00:00 - 3001-01-18 23:59:59.999999 (0 to 32536771199.999999 epoch time), function returns NULL.

Argument type: DOUBLE, STRING

Return type map:

DOUBLE -> DATETIME

DOUBLE, STRING -> STRING

Examples::

    os> select FROM_UNIXTIME(1220249547)
    fetched rows / total rows = 1/1
    +-----------------------------+
    | FROM_UNIXTIME(1220249547)   |
    |-----------------------------|
    | 2008-09-01 06:12:27         |
    +-----------------------------+

    os> select FROM_UNIXTIME(1220249547, '%T')
    fetched rows / total rows = 1/1
    +-----------------------------------+
    | FROM_UNIXTIME(1220249547, '%T')   |
    |-----------------------------------|
    | 06:12:27                          |
    +-----------------------------------+


HOUR
----

Description
>>>>>>>>>>>

Usage: hour(time) extracts the hour value for time. Different from the time of day value, the time value has a large range and can be greater than 23, so the return value of hour(time) can be also greater than 23.
The function `hour_of_day` is also provided as an alias.

Argument type: STRING/TIME/DATETIME/TIMESTAMP

Return type: INTEGER

Example::

    os> SELECT HOUR('01:02:03'), HOUR_OF_DAY('01:02:03')
    fetched rows / total rows = 1/1
    +--------------------+---------------------------+
    | HOUR('01:02:03')   | HOUR_OF_DAY('01:02:03')   |
    |--------------------+---------------------------|
    | 1                  | 1                         |
    +--------------------+---------------------------+

LOCALTIMESTAMP
--------------

Description
>>>>>>>>>>>

`LOCALTIMESTAMP()` are synonyms for `NOW() <#now>`_.

Example::

    > SELECT LOCALTIMESTAMP();
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

    > SELECT LOCALTIME(), LOCALTIME;
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

    os> select MAKEDATE(1945, 5.9), MAKEDATE(1984, 1984)
    fetched rows / total rows = 1/1
    +-----------------------+------------------------+
    | MAKEDATE(1945, 5.9)   | MAKEDATE(1984, 1984)   |
    |-----------------------+------------------------|
    | 1945-01-06            | 1989-06-06             |
    +-----------------------+------------------------+


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

    os> select MAKETIME(20, 30, 40), MAKETIME(20.2, 49.5, 42.100502)
    fetched rows / total rows = 1/1
    +------------------------+-----------------------------------+
    | MAKETIME(20, 30, 40)   | MAKETIME(20.2, 49.5, 42.100502)   |
    |------------------------+-----------------------------------|
    | 20:30:40               | 20:50:42.100502                   |
    +------------------------+-----------------------------------+


MICROSECOND
-----------

Description
>>>>>>>>>>>

Usage: microsecond(expr) returns the microseconds from the time or datetime expression expr as a number in the range from 0 to 999999.

Argument type: STRING/TIME/DATETIME/TIMESTAMP

Return type: INTEGER

Example::

    os> SELECT MICROSECOND((TIME '01:02:03.123456'))
    fetched rows / total rows = 1/1
    +-----------------------------------------+
    | MICROSECOND((TIME '01:02:03.123456'))   |
    |-----------------------------------------|
    | 123456                                  |
    +-----------------------------------------+


MINUTE
------

Description
>>>>>>>>>>>

Usage: minute(time) returns the minute for time, in the range 0 to 59.
The `minute_of_hour` function is provided as an alias.

Argument type: STRING/TIME/DATETIME/TIMESTAMP

Return type: INTEGER

Example::

    os> SELECT MINUTE(time('01:02:03')), MINUTE_OF_HOUR(time('01:02:03'))
    fetched rows / total rows = 1/1
    +----------------------------+------------------------------------+
    | MINUTE(time('01:02:03'))   | MINUTE_OF_HOUR(time('01:02:03'))   |
    |----------------------------+------------------------------------|
    | 2                          | 2                                  |
    +----------------------------+------------------------------------+

MINUTE_OF_DAY
------

Description
>>>>>>>>>>>

Usage: minute_of_day(time) returns the minute value for time within a 24 hour day, in the range 0 to 1439.

Argument type: STRING/TIME/DATETIME/TIMESTAMP

Return type: INTEGER

Example::

    os> SELECT MINUTE_OF_DAY((TIME '01:02:03'))
    fetched rows / total rows = 1/1
    +------------------------------------+
    | MINUTE_OF_DAY((TIME '01:02:03'))   |
    |------------------------------------|
    | 62                                 |
    +------------------------------------+


MONTH
-----

Description
>>>>>>>>>>>

Usage: month(date) returns the month for date, in the range 1 to 12 for January to December. The dates with value 0 such as '0000-00-00' or '2008-00-00' are invalid.
If an argument of type `TIME` is given, the function will use the current date.
The function `month_of_year`_ is also provided as an alias.

Argument type: STRING/DATE/DATETIME/TIME/TIMESTAMP

Return type: INTEGER

Example::

    os> SELECT MONTH(DATE('2020-08-26'))
    fetched rows / total rows = 1/1
    +-----------------------------+
    | MONTH(DATE('2020-08-26'))   |
    |-----------------------------|
    | 8                           |
    +-----------------------------+


    os> SELECT MONTH_OF_YEAR(DATE('2020-08-26'))
    fetched rows / total rows = 1/1
    +-------------------------------------+
    | MONTH_OF_YEAR(DATE('2020-08-26'))   |
    |-------------------------------------|
    | 8                                   |
    +-------------------------------------+


MONTHNAME
---------

Description
>>>>>>>>>>>

Usage: monthname(date) returns the full name of the month for date.

Argument type: STRING/DATE/DATETIME/TIMESTAMP

Return type: STRING

Example::

    os> SELECT MONTHNAME(DATE('2020-08-26'))
    fetched rows / total rows = 1/1
    +---------------------------------+
    | MONTHNAME(DATE('2020-08-26'))   |
    |---------------------------------|
    | August                          |
    +---------------------------------+


NOW
---

Description
>>>>>>>>>>>

Returns the current date and time as a value in 'YYYY-MM-DD hh:mm:ss' format. The value is expressed in the cluster time zone.
`NOW()` returns a constant time that indicates the time at which the statement began to execute. This differs from the behavior for `SYSDATE() <#sysdate>`_, which returns the exact time at which it executes.

Return type: DATETIME

Specification: NOW() -> DATETIME

Example::

    > SELECT NOW() as value_1, NOW() as value_2;
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

    os> SELECT PERIOD_ADD(200801, 2), PERIOD_ADD(200801, -12)
    fetched rows / total rows = 1/1
    +-------------------------+---------------------------+
    | PERIOD_ADD(200801, 2)   | PERIOD_ADD(200801, -12)   |
    |-------------------------+---------------------------|
    | 200803                  | 200701                    |
    +-------------------------+---------------------------+


PERIOD_DIFF
-----------

Description
>>>>>>>>>>>

Usage: period_diff(P1, P2) returns the number of months between periods P1 and P2 given in the format YYMM or YYYYMM.

Argument type: INTEGER, INTEGER

Return type: INTEGER

Example::

    os> SELECT PERIOD_DIFF(200802, 200703), PERIOD_DIFF(200802, 201003)
    fetched rows / total rows = 1/1
    +-------------------------------+-------------------------------+
    | PERIOD_DIFF(200802, 200703)   | PERIOD_DIFF(200802, 201003)   |
    |-------------------------------+-------------------------------|
    | 11                            | -25                           |
    +-------------------------------+-------------------------------+


QUARTER
-------

Description
>>>>>>>>>>>

Usage: quarter(date) returns the quarter of the year for date, in the range 1 to 4.

Argument type: STRING/DATE/DATETIME/TIMESTAMP

Return type: INTEGER

Example::

    os> SELECT QUARTER(DATE('2020-08-26'))
    fetched rows / total rows = 1/1
    +-------------------------------+
    | QUARTER(DATE('2020-08-26'))   |
    |-------------------------------|
    | 3                             |
    +-------------------------------+


SECOND
------

Description
>>>>>>>>>>>

Usage: second(time) returns the second for time, in the range 0 to 59.
The function `second_of_minute`_ is provided as an alias

Argument type: STRING/TIME/DATETIME/TIMESTAMP

Return type: INTEGER

Example::

    os> SELECT SECOND((TIME '01:02:03'))
    fetched rows / total rows = 1/1
    +-----------------------------+
    | SECOND((TIME '01:02:03'))   |
    |-----------------------------|
    | 3                           |
    +-----------------------------+

    os> SELECT SECOND_OF_MINUTE(time('01:02:03'))
    fetched rows / total rows = 1/1
    +--------------------------------------+
    | SECOND_OF_MINUTE(time('01:02:03'))   |
    |--------------------------------------|
    | 3                                    |
    +--------------------------------------+


SUBDATE
-------

Description
>>>>>>>>>>>

Usage: subdate(date, INTERVAL expr unit)/ subdate(date, expr) subtracts the time interval expr from date

Argument type: DATE/DATETIME/TIMESTAMP/STRING, INTERVAL/LONG

Return type map:

DATE/DATETIME/TIMESTAMP/STRING, INTERVAL -> DATETIME

DATE, LONG -> DATE

DATETIME/TIMESTAMP/STRING, LONG -> DATETIME

Synonyms: `DATE_SUB`_

Example::

    os> SELECT SUBDATE(DATE('2008-01-02'), INTERVAL 31 DAY), SUBDATE(DATE('2020-08-26'), 1), SUBDATE(TIMESTAMP('2020-08-26 01:01:01'), 1)
    fetched rows / total rows = 1/1
    +------------------------------------------------+----------------------------------+------------------------------------------------+
    | SUBDATE(DATE('2008-01-02'), INTERVAL 31 DAY)   | SUBDATE(DATE('2020-08-26'), 1)   | SUBDATE(TIMESTAMP('2020-08-26 01:01:01'), 1)   |
    |------------------------------------------------+----------------------------------+------------------------------------------------|
    | 2007-12-02                                     | 2020-08-25                       | 2020-08-25 01:01:01                            |
    +------------------------------------------------+----------------------------------+------------------------------------------------+


SUBTIME
-------

Description
>>>>>>>>>>>

Usage: subtime(expr1, expr2) subtracts expr2 from expr1 and returns the result. If argument is TIME, today's date is used; if argument is DATE, time at midnight is used.

Argument type: DATE/DATETIME/TIMESTAMP/TIME, DATE/DATETIME/TIMESTAMP/TIME

Return type map:

(DATE/DATETIME/TIMESTAMP, DATE/DATETIME/TIMESTAMP/TIME) -> DATETIME

(TIME, DATE/DATETIME/TIMESTAMP/TIME) -> TIME

Antonyms: `ADDTIME`_

Example::

    os> SELECT SUBTIME(DATE('2008-12-12'), DATE('2008-11-15')) AS `'2008-12-12' - 0`
    fetched rows / total rows = 1/1
    +---------------------+
    | '2008-12-12' - 0    |
    |---------------------|
    | 2008-12-12 00:00:00 |
    +---------------------+

    os> SELECT SUBTIME(TIME('23:59:59'), DATE('2004-01-01')) AS `'23:59:59' - 0`
    fetched rows / total rows = 1/1
    +------------------+
    | '23:59:59' - 0   |
    |------------------|
    | 23:59:59         |
    +------------------+

    os> SELECT SUBTIME(DATE('2004-01-01'), TIME('23:59:59')) AS `'2004-01-01' - '23:59:59'`
    fetched rows / total rows = 1/1
    +-----------------------------+
    | '2004-01-01' - '23:59:59'   |
    |-----------------------------|
    | 2003-12-31 00:00:01         |
    +-----------------------------+

    os> SELECT SUBTIME(TIME('10:20:30'), TIME('00:05:42')) AS `'10:20:30' - '00:05:42'`
    fetched rows / total rows = 1/1
    +---------------------------+
    | '10:20:30' - '00:05:42'   |
    |---------------------------|
    | 10:14:48                  |
    +---------------------------+

    os> SELECT SUBTIME(TIMESTAMP('2007-03-01 10:20:30'), DATETIME('2002-03-04 20:40:50')) AS `'2007-03-01 10:20:30' - '20:40:50'`
    fetched rows / total rows = 1/1
    +--------------------------------------+
    | '2007-03-01 10:20:30' - '20:40:50'   |
    |--------------------------------------|
    | 2007-02-28 13:39:40                  |
    +--------------------------------------+


SYSDATE
-------

Description
>>>>>>>>>>>

Returns the current date and time as a value in 'YYYY-MM-DD hh:mm:ss[.nnnnnn]'.
SYSDATE() returns the time at which it executes. This differs from the behavior for `NOW() <#now>`_, which returns a constant time that indicates the time at which the statement began to execute.
If the argument is given, it specifies a fractional seconds precision from 0 to 6, the return value includes a fractional seconds part of that many digits.

Optional argument type: INTEGER

Return type: DATETIME

Specification: SYSDATE([INTEGER]) -> DATETIME

Example::

    > SELECT SYSDATE() as value_1, SYSDATE(6) as value_2;
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

Usage: time(expr) constructs a time type with the input string expr as a time. If the argument is of date/datetime/time/timestamp, it extracts the time value part from the expression.

Argument type: STRING/DATE/DATETIME/TIME/TIMESTAMP

Return type: TIME

Example::

    os> SELECT TIME('13:49:00'), TIME('13:49'), TIME(TIMESTAMP('2020-08-26 13:49:00')), TIME('2020-08-26 13:49:00')
    fetched rows / total rows = 1/1
    +--------------------+-----------------+------------------------------------------+-------------------------------+
    | TIME('13:49:00')   | TIME('13:49')   | TIME(TIMESTAMP('2020-08-26 13:49:00'))   | TIME('2020-08-26 13:49:00')   |
    |--------------------+-----------------+------------------------------------------+-------------------------------|
    | 13:49:00           | 13:49:00        | 13:49:00                                 | 13:49:00                      |
    +--------------------+-----------------+------------------------------------------+-------------------------------+


TIME_TO_SEC
-----------

Description
>>>>>>>>>>>

Usage: time_to_sec(time) returns the time argument, converted to seconds.

Argument type: STRING/TIME/DATETIME/TIMESTAMP

Return type: LONG

Example::

    os> SELECT TIME_TO_SEC(TIME '22:23:00')
    fetched rows / total rows = 1/1
    +--------------------------------+
    | TIME_TO_SEC(TIME '22:23:00')   |
    |--------------------------------|
    | 80580                          |
    +--------------------------------+


TIMEDIFF
--------

Description
>>>>>>>>>>>

Usage: returns the difference between two time expressions as a time.

Argument type: TIME, TIME

Return type: TIME

Example::

    os> SELECT TIMEDIFF('23:59:59', '13:00:00')
    fetched rows / total rows = 1/1
    +------------------------------------+
    | TIMEDIFF('23:59:59', '13:00:00')   |
    |------------------------------------|
    | 10:59:59                           |
    +------------------------------------+


TIMESTAMP
---------

Description
>>>>>>>>>>>

Usage: timestamp(expr) construct a timestamp type with the input string expr as an timestamp. If the argument is of date/datetime/timestamp type, cast expr to timestamp type with default timezone UTC.

Argument type: STRING/DATE/DATETIME/TIMESTAMP

Return type: TIMESTAMP

Example::

    >od SELECT TIMESTAMP('2020-08-26 13:49:00')
    fetched rows / total rows = 1/1
    +------------------------------------+
    | TIMESTAMP('2020-08-26 13:49:00')   |
    |------------------------------------|
    | TIMESTAMP '2020-08-26 13:49:00     |
    +------------------------------------+


TO_DAYS
-------

Description
>>>>>>>>>>>

Usage: to_days(date) returns the day number (the number of days since year 0) of the given date. Returns NULL if date is invalid.

Argument type: STRING/DATE/DATETIME/TIMESTAMP

Return type: LONG

Example::

    os> SELECT TO_DAYS(DATE '2008-10-07')
    fetched rows / total rows = 1/1
    +------------------------------+
    | TO_DAYS(DATE '2008-10-07')   |
    |------------------------------|
    | 733687                       |
    +------------------------------+


UNIX_TIMESTAMP
--------------

Description
>>>>>>>>>>>

Usage: Converts given argument to Unix time (seconds since January 1st, 1970 at 00:00:00 UTC). If no argument given, it returns current Unix time.
The date argument may be a DATE, DATETIME, or TIMESTAMP string, or a number in YYMMDD, YYMMDDhhmmss, YYYYMMDD, or YYYYMMDDhhmmss format. If the argument includes a time part, it may optionally include a fractional seconds part.
If argument is in invalid format or outside of range 1970-01-01 00:00:00 - 3001-01-18 23:59:59.999999 (0 to 32536771199.999999 epoch time), function returns NULL.
You can use `FROM_UNIXTIME`_ to do reverse conversion.

Argument type: <NONE>/DOUBLE/DATE/DATETIME/TIMESTAMP

Return type: DOUBLE

Examples::

    os> select UNIX_TIMESTAMP(20771122143845)
    fetched rows / total rows = 1/1
    +----------------------------------+
    | UNIX_TIMESTAMP(20771122143845)   |
    |----------------------------------|
    | 3404817525.0                     |
    +----------------------------------+

    os> select UNIX_TIMESTAMP(TIMESTAMP('1996-11-15 17:05:42'))
    fetched rows / total rows = 1/1
    +----------------------------------------------------+
    | UNIX_TIMESTAMP(TIMESTAMP('1996-11-15 17:05:42'))   |
    |----------------------------------------------------|
    | 848077542.0                                        |
    +----------------------------------------------------+


UTC_DATE
--------

Description
>>>>>>>>>>>

Returns the current UTC date as a value in 'YYYY-MM-DD'.

Return type: DATE

Specification: UTC_DATE() -> DATE

Example::

    > SELECT UTC_DATE();
    fetched rows / total rows = 1/1
    +--------------+
    | utc_date()   |
    |--------------|
    | 2022-10-03   |
    +--------------+


UTC_TIME
--------

Description
>>>>>>>>>>>

Returns the current UTC time as a value in 'hh:mm:ss'.

Return type: TIME

Specification: UTC_TIME() -> TIME

Example::

    > SELECT UTC_TIME();
    fetched rows / total rows = 1/1
    +--------------+
    | utc_time()   |
    |--------------|
    | 17:54:27     |
    +--------------+


UTC_TIMESTAMP
-------------

Description
>>>>>>>>>>>

Returns the current UTC timestamp as a value in 'YYYY-MM-DD hh:mm:ss'.

Return type: DATETIME

Specification: UTC_TIMESTAMP() -> DATETIME

Example::

    > SELECT UTC_TIMESTAMP();
    fetched rows / total rows = 1/1
    +---------------------+
    | utc_timestamp()     |
    |---------------------|
    | 2022-10-03 17:54:28 |
    +---------------------+

WEEK
----

Description
>>>>>>>>>>>

Usage: week(date[, mode]) returns the week number for date. If the mode argument is omitted, the default mode 0 is used.
If an argument of type `TIME` is given, the function will use the current date.
The function `week_of_year` is also provided as an alias.

.. list-table:: The following table describes how the mode argument works.
   :widths: 25 50 25 75
   :header-rows: 1

   * - Mode
     - First day of week
     - Range
     - Week 1 is the first week …
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

Argument type: DATE/DATETIME/TIME/TIMESTAMP/STRING

Return type: INTEGER

Example::

    os> SELECT WEEK(DATE('2008-02-20')), WEEK(DATE('2008-02-20'), 1)
    fetched rows / total rows = 1/1
    +----------------------------+-------------------------------+
    | WEEK(DATE('2008-02-20'))   | WEEK(DATE('2008-02-20'), 1)   |
    |----------------------------+-------------------------------|
    | 7                          | 8                             |
    +----------------------------+-------------------------------+

WEEK_OF_YEAR
----

Description
>>>>>>>>>>>

The week_of_year function is a synonym for the `week`_ function.
If an argument of type `TIME` is given, the function will use the current date.

Argument type: DATE/DATETIME/TIME/TIMESTAMP/STRING

Return type: INTEGER

Example::

    os> SELECT WEEK_OF_YEAR(DATE('2008-02-20')), WEEK_OF_YEAR(DATE('2008-02-20'), 1)
    fetched rows / total rows = 1/1
    +------------------------------------+---------------------------------------+
    | WEEK_OF_YEAR(DATE('2008-02-20'))   | WEEK_OF_YEAR(DATE('2008-02-20'), 1)   |
    |------------------------------------+---------------------------------------|
    | 7                                  | 8                                     |
    +------------------------------------+---------------------------------------+


YEAR
----

Description
>>>>>>>>>>>

Usage: year(date) returns the year for date, in the range 1000 to 9999, or 0 for the “zero” date.

Argument type: STRING/DATE/DATETIME/TIMESTAMP

Return type: INTEGER

Example::

    os> SELECT YEAR(DATE('2020-08-26'))
    fetched rows / total rows = 1/1
    +----------------------------+
    | YEAR(DATE('2020-08-26'))   |
    |----------------------------|
    | 2020                       |
    +----------------------------+


String Functions
================

ASCII
-----

Description
>>>>>>>>>>>

Usage: ASCII(expr) returns the numeric value of the leftmost character of the string str. Returns 0 if str is the empty string. Returns NULL if str is NULL. ASCII() works for 8-bit characters.

Argument type: STRING

Return type: INTEGER

Example::

    os> SELECT ASCII('hello')
    fetched rows / total rows = 1/1
    +------------------+
    | ASCII('hello')   |
    |------------------|
    | 104              |
    +------------------+


CONCAT
------

Description
>>>>>>>>>>>

Usage: CONCAT(str1, str2) returns str1 and str strings concatenated together.

Argument type: STRING, STRING

Return type: STRING

Example::

    os> SELECT CONCAT('hello', 'world')
    fetched rows / total rows = 1/1
    +----------------------------+
    | CONCAT('hello', 'world')   |
    |----------------------------|
    | helloworld                 |
    +----------------------------+


CONCAT_WS
---------

Description
>>>>>>>>>>>

Usage: CONCAT_WS(sep, str1, str2) returns str1 concatenated with str2 using sep as a separator between them.

Argument type: STRING, STRING, STRING

Return type: STRING

Example::

    os> SELECT CONCAT_WS(',', 'hello', 'world')
    fetched rows / total rows = 1/1
    +------------------------------------+
    | CONCAT_WS(',', 'hello', 'world')   |
    |------------------------------------|
    | hello,world                        |
    +------------------------------------+


LEFT
----

Usage: left(str, len) returns the leftmost len characters from the string str, or NULL if any argument is NULL.

Argument type: STRING, INTEGER

Return type: STRING

Example::

    os> SELECT LEFT('helloworld', 5), LEFT('HELLOWORLD', 0)
    fetched rows / total rows = 1/1
    +-------------------------+-------------------------+
    | LEFT('helloworld', 5)   | LEFT('HELLOWORLD', 0)   |
    |-------------------------+-------------------------|
    | hello                   |                         |
    +-------------------------+-------------------------+


LENGTH
------

Description
>>>>>>>>>>>

Specifications:

1. LENGTH(STRING) -> INTEGER

Usage: length(str) returns length of string measured in bytes.

Argument type: STRING

Return type: INTEGER

Example::

    os> SELECT LENGTH('helloworld')
    fetched rows / total rows = 1/1
    +------------------------+
    | LENGTH('helloworld')   |
    |------------------------|
    | 10                     |
    +------------------------+


LOCATE
------

Description
>>>>>>>>>>>

Usage: The first syntax LOCATE(substr, str) returns the position of the first occurrence of substring substr in string str. The second syntax LOCATE(substr, str, pos) returns the position of the first occurrence of substring substr in string str, starting at position pos. Returns 0 if substr is not in str. Returns NULL if any argument is NULL.

Argument type: STRING, STRING, INTEGER

Return type map:

(STRING, STRING) -> INTEGER
(STRING, STRING, INTEGER) -> INTEGER

Example::

    os> SELECT LOCATE('world', 'helloworld'), LOCATE('world', 'helloworldworld', 7)
    fetched rows / total rows = 1/1
    +---------------------------------+-----------------------------------------+
    | LOCATE('world', 'helloworld')   | LOCATE('world', 'helloworldworld', 7)   |
    |---------------------------------+-----------------------------------------|
    | 6                               | 11                                      |
    +---------------------------------+-----------------------------------------+


LOWER
-----

Description
>>>>>>>>>>>

Usage: lower(string) converts the string to lowercase.

Argument type: STRING

Return type: STRING

Example::

    os> SELECT LOWER('helloworld'), LOWER('HELLOWORLD')
    fetched rows / total rows = 1/1
    +-----------------------+-----------------------+
    | LOWER('helloworld')   | LOWER('HELLOWORLD')   |
    |-----------------------+-----------------------|
    | helloworld            | helloworld            |
    +-----------------------+-----------------------+


LTRIM
-----

Description
>>>>>>>>>>>

Usage: ltrim(str) trims leading space characters from the string.

Argument type: STRING

Return type: STRING

Example::

    os> SELECT LTRIM('   hello'), LTRIM('hello   ')
    fetched rows / total rows = 1/1
    +---------------------+---------------------+
    | LTRIM('   hello')   | LTRIM('hello   ')   |
    |---------------------+---------------------|
    | hello               | hello               |
    +---------------------+---------------------+


POSITION
------

Description
>>>>>>>>>>>

Usage: The syntax POSITION(substr IN str) returns the position of the first occurrence of substring substr in string str. Returns 0 if substr is not in str. Returns NULL if any argument is NULL.

Argument type: STRING, STRING

Return type integer:

(STRING IN STRING) -> INTEGER

Example::

    os> SELECT POSITION('world' IN 'helloworld'), POSITION('invalid' IN 'helloworld');
    fetched rows / total rows = 1/1
    +-------------------------------------+---------------------------------------+
    | POSITION('world' IN 'helloworld')   | POSITION('invalid' IN 'helloworld')   |
    |-------------------------------------+---------------------------------------|
    | 6                                   | 0                                     |
    +-------------------------------------+---------------------------------------+


REPLACE
-------

Description
>>>>>>>>>>>

Usage: REPLACE(str, from_str, to_str) returns the string str with all occurrences of the string from_str replaced by the string to_str. REPLACE() performs a case-sensitive match when searching for from_str.

Argument type: STRING, STRING, STRING

Return type: STRING

Example::

    os> SELECT REPLACE('Hello World!', 'World', 'OpenSearch')
    fetched rows / total rows = 1/1
    +--------------------------------------------------+
    | REPLACE('Hello World!', 'World', 'OpenSearch')   |
    |--------------------------------------------------|
    | Hello OpenSearch!                                |
    +--------------------------------------------------+


REVERSE
-------

Description
>>>>>>>>>>>

Usage: REVERSE(str) returns reversed string of the string supplied as an argument. Returns NULL if the argument is NULL.

Argument type: STRING

Return type: STRING

Example::

    os> SELECT REVERSE('abcde'), REVERSE(null)
    fetched rows / total rows = 1/1
    +--------------------+-----------------+
    | REVERSE('abcde')   | REVERSE(null)   |
    |--------------------+-----------------|
    | edcba              | null            |
    +--------------------+-----------------+


RIGHT
-----

Description
>>>>>>>>>>>

Usage: right(str, len) returns the rightmost len characters from the string str, or NULL if any argument is NULL.

Argument type: STRING, INTEGER

Return type: STRING

Example::

    os> SELECT RIGHT('helloworld', 5), RIGHT('HELLOWORLD', 0)
    fetched rows / total rows = 1/1
    +--------------------------+--------------------------+
    | RIGHT('helloworld', 5)   | RIGHT('HELLOWORLD', 0)   |
    |--------------------------+--------------------------|
    | world                    |                          |
    +--------------------------+--------------------------+


RTRIM
-----

Description
>>>>>>>>>>>

Usage: rtrim(str) trims trailing space characters from the string.

Argument type: STRING

Return type: STRING

Example::

    os> SELECT RTRIM('   hello'), RTRIM('hello   ')
    fetched rows / total rows = 1/1
    +---------------------+---------------------+
    | RTRIM('   hello')   | RTRIM('hello   ')   |
    |---------------------+---------------------|
    |    hello            | hello               |
    +---------------------+---------------------+


SUBSTRING
---------

Description
>>>>>>>>>>>

Usage: substring(str, start) or substring(str, start, length) returns substring using start and length. With no length, entire string from start is returned.

Argument type: STRING, INTEGER, INTEGER

Return type: STRING

Synonyms: SUBSTR

Example::

    os> SELECT SUBSTRING('helloworld', 5), SUBSTRING('helloworld', 5, 3)
    fetched rows / total rows = 1/1
    +------------------------------+---------------------------------+
    | SUBSTRING('helloworld', 5)   | SUBSTRING('helloworld', 5, 3)   |
    |------------------------------+---------------------------------|
    | oworld                       | owo                             |
    +------------------------------+---------------------------------+


TRIM
----

Description
>>>>>>>>>>>

Argument Type: STRING

Return type: STRING

Example::

    os> SELECT TRIM('   hello'), TRIM('hello   ')
    fetched rows / total rows = 1/1
    +--------------------+--------------------+
    | TRIM('   hello')   | TRIM('hello   ')   |
    |--------------------+--------------------|
    | hello              | hello              |
    +--------------------+--------------------+


UPPER
-----

Description
>>>>>>>>>>>

Usage: upper(string) converts the string to uppercase.

Argument type: STRING

Return type: STRING

Example::

    os> SELECT UPPER('helloworld'), UPPER('HELLOWORLD')
    fetched rows / total rows = 1/1
    +-----------------------+-----------------------+
    | UPPER('helloworld')   | UPPER('HELLOWORLD')   |
    |-----------------------+-----------------------|
    | HELLOWORLD            | HELLOWORLD            |
    +-----------------------+-----------------------+

Conditional Functions
=====================

IF
--

Description
>>>>>>>>>>>

Specifications:

1. IF(BOOLEAN, ES_TYPE, ES_TYPE) -> ES_TYPE


IFNULL
------

Description
>>>>>>>>>>>

Specifications:

1. IFNULL(ES_TYPE, ES_TYPE) -> ES_TYPE

Usage: return parameter2 if parameter1 is null, otherwise return parameter1

Argument type: Any

Return type: Any (NOTE : if two parameters has different type, you will fail semantic check"

Example One::

    os> SELECT IFNULL(123, 321), IFNULL(321, 123)
    fetched rows / total rows = 1/1
    +--------------------+--------------------+
    | IFNULL(123, 321)   | IFNULL(321, 123)   |
    |--------------------+--------------------|
    | 123                | 321                |
    +--------------------+--------------------+

Example Two::

    os> SELECT IFNULL(321, 1/0), IFNULL(1/0, 123)
    fetched rows / total rows = 1/1
    +--------------------+--------------------+
    | IFNULL(321, 1/0)   | IFNULL(1/0, 123)   |
    |--------------------+--------------------|
    | 321                | 123                |
    +--------------------+--------------------+

Example Three::

    os> SELECT IFNULL(1/0, 1/0)
    fetched rows / total rows = 1/1
    +--------------------+
    | IFNULL(1/0, 1/0)   |
    |--------------------|
    | null               |
    +--------------------+


NULLIF
------

Description
>>>>>>>>>>>

Specifications:

1. NULLIF(ES_TYPE, ES_TYPE) -> ES_TYPE

Usage: return null if two parameters are same, otherwise return parameer1

Argument type: Any

Return type: Any (NOTE : if two parametershas different type, you will fail semantic check")

Example::

    os> SELECT NULLIF(123, 123), NULLIF(321, 123), NULLIF(1/0, 321), NULLIF(321, 1/0), NULLIF(1/0, 1/0)
    fetched rows / total rows = 1/1
    +--------------------+--------------------+--------------------+--------------------+--------------------+
    | NULLIF(123, 123)   | NULLIF(321, 123)   | NULLIF(1/0, 321)   | NULLIF(321, 1/0)   | NULLIF(1/0, 1/0)   |
    |--------------------+--------------------+--------------------+--------------------+--------------------|
    | null               | 321                | null               | 321                | null               |
    +--------------------+--------------------+--------------------+--------------------+--------------------+


ISNULL
------

Description
>>>>>>>>>>>

Specifications:

1. ISNULL(ES_TYPE) -> INTEGER

Usage: return true if parameter is null, otherwise return false

Argument type: Any

Return type: boolean

Example::

    os> SELECT ISNULL(1/0), ISNULL(123)
    fetched rows / total rows = 1/1
    +---------------+---------------+
    | ISNULL(1/0)   | ISNULL(123)   |
    |---------------+---------------|
    | True          | False         |
    +---------------+---------------+

IF
------

Description
>>>>>>>>>>>

Specifications:

1. IF(condition, ES_TYPE1, ES_TYPE2) -> ES_TYPE1 or ES_TYPE2

Usage: if first parameter is true, return second parameter, otherwise return third one.

Argument type: condition as BOOLEAN, second and third can by any type

Return type: Any (NOTE : if parameters #2 and #3 has different type, you will fail semantic check"

Example::

    os> SELECT IF(100 > 200, '100', '200')
    fetched rows / total rows = 1/1
    +-------------------------------+
    | IF(100 > 200, '100', '200')   |
    |-------------------------------|
    | 200                           |
    +-------------------------------+

    os> SELECT IF(200 > 100, '100', '200')
    fetched rows / total rows = 1/1
    +-------------------------------+
    | IF(200 > 100, '100', '200')   |
    |-------------------------------|
    | 100                           |
    +-------------------------------+

CASE
----

Description
>>>>>>>>>>>

``CASE`` statement has two forms with slightly different syntax: Simple Case and Searched Case.

Simple case syntax compares a case value expression with each compare expression in ``WHEN`` clause and return its result if matched. Otherwise, result expression's value in ``ELSE`` clause is returned (or ``NULL`` if absent)::

   CASE case_value_expression
     WHEN compare_expression THEN result_expression
     [WHEN compare_expression THEN result_expression] ...
     [ELSE result_expression]
   END

Similarly, searched case syntax evaluates each search condition and return result if true. A search condition must be a predicate that returns a bool when evaluated::

   CASE
     WHEN search_condition THEN result_expression
     [WHEN search_condition THEN result_expression] ...
     [ELSE result_expression]
   END

Type Check
>>>>>>>>>>

All result types in ``WHEN`` and ``ELSE`` clause are required to be exactly the same. Otherwise, take the following query for example, you'll see an semantic analysis exception thrown::

   CASE age
     WHEN 30 THEN 'Thirty'
     WHEN 50 THEN true
   END

Examples
>>>>>>>>

Here are examples for simple case syntax::

    os> SELECT
    ...   CASE 1
    ...     WHEN 1 THEN 'One'
    ...   END AS simple_case,
    ...   CASE ABS(-2)
    ...     WHEN 1 THEN 'One'
    ...     WHEN 2 THEN 'Absolute two'
    ...   END AS func_case_value,
    ...   CASE ABS(-3)
    ...     WHEN 1 THEN 'One'
    ...     ELSE TRIM(' Absolute three ')
    ...   END AS func_result;
    fetched rows / total rows = 1/1
    +---------------+-------------------+----------------+
    | simple_case   | func_case_value   | func_result    |
    |---------------+-------------------+----------------|
    | One           | Absolute two      | Absolute three |
    +---------------+-------------------+----------------+

Here are examples for searched case syntax::

    os> SELECT
    ...   CASE
    ...     WHEN 1 = 1 THEN 'One'
    ...   END AS single_search,
    ...   CASE
    ...     WHEN 2 = 1 THEN 'One'
    ...     WHEN 'hello' = 'hello' THEN 'Hello' END AS multi_searches,
    ...   CASE
    ...     WHEN 2 = 1 THEN 'One'
    ...     WHEN 'hello' = 'world' THEN 'Hello'
    ...   END AS no_else;
    fetched rows / total rows = 1/1
    +-----------------+------------------+-----------+
    | single_search   | multi_searches   | no_else   |
    |-----------------+------------------+-----------|
    | One             | Hello            | null      |
    +-----------------+------------------+-----------+


RELEVANCE
=========

The relevance based functions enable users to search the index for documents by the relevance of the input query. The functions are built on the top of the search queries of the OpenSearch engine, but in memory execution within the plugin is not supported. These functions are able to perform the global filter of a query, for example the condition expression in a ``WHERE`` clause or in a ``HAVING`` clause. For more details of the relevance based search, check out the design here: `Relevance Based Search With SQL/PPL Query Engine <https://github.com/opensearch-project/sql/issues/182>`_

MATCH
-----

Description
>>>>>>>>>>>

``match(field_expression, query_expression[, option=<option_value>]*)``

The match function maps to the match query used in search engine, to return the documents that match a provided text, number, date or boolean value with a given field. Available parameters include:

- analyzer
- auto_generate_synonyms_phrase
- fuzziness
- max_expansions
- prefix_length
- fuzzy_transpositions
- fuzzy_rewrite
- lenient
- operator
- minimum_should_match
- zero_terms_query
- boost

Example with only ``field`` and ``query`` expressions, and all other parameters are set default values::

    os> SELECT lastname, address FROM accounts WHERE match(address, 'Street');
    fetched rows / total rows = 2/2
    +------------+--------------------+
    | lastname   | address            |
    |------------+--------------------|
    | Bond       | 671 Bristol Street |
    | Bates      | 789 Madison Street |
    +------------+--------------------+

Another example to show how to set custom values for the optional parameters::

    os> SELECT lastname FROM accounts WHERE match(firstname, 'Hattie', operator='AND', boost=2.0);
    fetched rows / total rows = 1/1
    +------------+
    | lastname   |
    |------------|
    | Bond       |
    +------------+


MATCHQUERY
-----

Description
>>>>>>>>>>>

The matchquery function is a synonym for the `match`_ function.

Example with only ``field`` and ``query`` expressions, and all other parameters are set default values::

    os> SELECT lastname, address FROM accounts WHERE matchquery(address, 'Street');
    fetched rows / total rows = 2/2
    +------------+--------------------+
    | lastname   | address            |
    |------------+--------------------|
    | Bond       | 671 Bristol Street |
    | Bates      | 789 Madison Street |
    +------------+--------------------+

Another example to show how to set custom values for the optional parameters::

    os> SELECT lastname FROM accounts WHERE matchquery(firstname, 'Hattie', operator='AND', boost=2.0);
    fetched rows / total rows = 1/1
    +------------+
    | lastname   |
    |------------|
    | Bond       |
    +------------+

    The matchquery function also supports an alternative syntax::

    os> SELECT firstname FROM accounts WHERE firstname = matchquery('Hattie');
    fetched rows / total rows = 1/1
    +-------------+
    | firstname   |
    |-------------|
    | Hattie      |
    +-------------+

MATCH_QUERY
-----

Description
>>>>>>>>>>>

The match_query function is a synonym for the `match`_ function.

Example with only ``field`` and ``query`` expressions, and all other parameters are set default values::

    os> SELECT lastname, address FROM accounts WHERE match_query(address, 'Street');
    fetched rows / total rows = 2/2
    +------------+--------------------+
    | lastname   | address            |
    |------------+--------------------|
    | Bond       | 671 Bristol Street |
    | Bates      | 789 Madison Street |
    +------------+--------------------+

Another example to show how to set custom values for the optional parameters::

    os> SELECT lastname FROM accounts WHERE match_query(firstname, 'Hattie', operator='AND', boost=2.0);
    fetched rows / total rows = 1/1
    +------------+
    | lastname   |
    |------------|
    | Bond       |
    +------------+

The match_query function also supports an alternative syntax::

    os> SELECT firstname FROM accounts WHERE firstname = match_query('Hattie');
    fetched rows / total rows = 1/1
    +-------------+
    | firstname   |
    |-------------|
    | Hattie      |
    +-------------+


MATCH_PHRASE
------------

Description
>>>>>>>>>>>

``match_phrase(field_expression, query_expression[, option=<option_value>]*)``

The match_phrase function maps to the match_phrase query used in search engine, to return the documents that match a provided text with a given field. Available parameters include:

- analyzer
- slop
- zero_terms_query

`matchphrase` and `matchphrasequery` are synonyms for `match_phrase`_

Example with only ``field`` and ``query`` expressions, and all other parameters are set default values::

    os> SELECT author, title FROM books WHERE match_phrase(author, 'Alexander Milne');
    fetched rows / total rows = 2/2
    +----------------------+--------------------------+
    | author               | title                    |
    |----------------------+--------------------------|
    | Alan Alexander Milne | The House at Pooh Corner |
    | Alan Alexander Milne | Winnie-the-Pooh          |
    +----------------------+--------------------------+

Another example to show how to set custom values for the optional parameters::

    os> SELECT author, title FROM books WHERE match_phrase(author, 'Alan Milne', slop = 2);
    fetched rows / total rows = 2/2
    +----------------------+--------------------------+
    | author               | title                    |
    |----------------------+--------------------------|
    | Alan Alexander Milne | The House at Pooh Corner |
    | Alan Alexander Milne | Winnie-the-Pooh          |
    +----------------------+--------------------------+

The match_phrase function also supports an alternative syntax::

    os> SELECT firstname FROM accounts WHERE firstname = match_phrase('Hattie');
    fetched rows / total rows = 1/1
    +-------------+
    | firstname   |
    |-------------|
    | Hattie      |
    +-------------+

    os> SELECT firstname FROM accounts WHERE firstname = matchphrase('Hattie');
    fetched rows / total rows = 1/1
    +-------------+
    | firstname   |
    |-------------|
    | Hattie      |
    +-------------+

MATCH_BOOL_PREFIX
-----

Description
>>>>>>>>>>>

``match_bool_prefix(field_expression, query_expression)``

The match_bool_prefix function maps to the match_bool_prefix query in the search engine. match_bool_prefix creates a match query from all but the last term in the query string. The last term is used to create a prefix query.

- fuzziness
- max_expansions
- prefix_length
- fuzzy_transpositions
- fuzzy_rewrite
- minimum_should_match
- boost
- operator
- analyzer

Example with only ``field`` and ``query`` expressions, and all other parameters are set default values::

    os> SELECT firstname, address FROM accounts WHERE match_bool_prefix(address, 'Bristol Stre');
    fetched rows / total rows = 2/2
    +-------------+--------------------+
    | firstname   | address            |
    |-------------+--------------------|
    | Hattie      | 671 Bristol Street |
    | Nanette     | 789 Madison Street |
    +-------------+--------------------+

Another example to show how to set custom values for the optional parameters::

    os> SELECT firstname, address FROM accounts WHERE match_bool_prefix(address, 'Bristol Street', minimum_should_match=2);
    fetched rows / total rows = 1/1
    +-------------+--------------------+
    | firstname   | address            |
    |-------------+--------------------|
    | Hattie      | 671 Bristol Street |
    +-------------+--------------------+

MATCH_PHRASE_PREFIX
------------

Description
>>>>>>>>>>>

``match_phrase_prefix(field_expression, query_expression[, option=<option_value>]*)``

The match_phrase_prefix function maps to the match_phrase_prefix query used in search engine,
to return the documents that match a provided text with a given field. Available parameters include:

- analyzer
- slop
- zero_terms_query
- max_expansions
- boost


Example with only ``field`` and ``query`` expressions, and all other parameters are set default values::

    os> SELECT author, title FROM books WHERE match_phrase_prefix(author, 'Alexander Mil');
    fetched rows / total rows = 2/2
    +----------------------+--------------------------+
    | author               | title                    |
    |----------------------+--------------------------|
    | Alan Alexander Milne | The House at Pooh Corner |
    | Alan Alexander Milne | Winnie-the-Pooh          |
    +----------------------+--------------------------+

Another example to show how to set custom values for the optional parameters::

    os> SELECT author, title FROM books WHERE match_phrase_prefix(author, 'Alan Mil', slop = 2);
    fetched rows / total rows = 2/2
    +----------------------+--------------------------+
    | author               | title                    |
    |----------------------+--------------------------|
    | Alan Alexander Milne | The House at Pooh Corner |
    | Alan Alexander Milne | Winnie-the-Pooh          |
    +----------------------+--------------------------+


MULTI_MATCH
-----------

Description
>>>>>>>>>>>

``multi_match([field_expression+], query_expression[, option=<option_value>]*)``
``multi_match(query=query_expression+, fields=[field_expression+][, option=<option_value>]*)``

The multi_match function maps to the multi_match query used in search engine, to return the documents that match a provided text, number, date or boolean value with a given field or fields.
The **^** lets you *boost* certain fields. Boosts are multipliers that weigh matches in one field more heavily than matches in other fields. The syntax allows to specify the fields in double quotes, single quotes, in backtick or even without any wrap. All fields search using star ``"*"`` is also available (star symbol should be wrapped). The weight is optional and should be specified using after the field name, it could be delimeted by the `caret` character or by whitespace. Please, refer to examples below:


- ``MULTI_MATCH(...)``
- ``MULTIMATCH(...)``
- ``MULTIMATCHQUERY(...)``

| ``multi_match(["Tags" ^ 2, 'Title' 3.4, `Body`, Comments ^ 0.3], ...)``
| ``multi_match(["*"], ...)``
| ``multimatch(query='query value', fields=["Tags^2,Title^3.4,Body"], ...)``
| ``multimatchquery('query'='query value', 'fields'='Title', ...)``

Available parameters include:

- analyzer
- auto_generate_synonyms_phrase
- cutoff_frequency
- fuzziness
- fuzzy_transpositions
- lenient
- max_expansions
- minimum_should_match
- operator
- prefix_length
- tie_breaker
- type
- slop
- boost

Example with only ``fields`` and ``query`` expressions, and all other parameters are set default values::

    os> select * from books where multi_match(['title'], 'Pooh House');
    fetched rows / total rows = 2/2
    +------+--------------------------+----------------------+
    | id   | title                    | author               |
    |------+--------------------------+----------------------|
    | 1    | The House at Pooh Corner | Alan Alexander Milne |
    | 2    | Winnie-the-Pooh          | Alan Alexander Milne |
    +------+--------------------------+----------------------+

Another example to show how to set custom values for the optional parameters::

    os> select * from books where multi_match(['title'], 'Pooh House', operator='AND', analyzer=default);
    fetched rows / total rows = 1/1
    +------+--------------------------+----------------------+
    | id   | title                    | author               |
    |------+--------------------------+----------------------|
    | 1    | The House at Pooh Corner | Alan Alexander Milne |
    +------+--------------------------+----------------------+

The multi_match function also supports an alternative syntax::

    os> SELECT firstname FROM accounts WHERE firstname = multi_match('Hattie');
    fetched rows / total rows = 1/1
    +-------------+
    | firstname   |
    |-------------|
    | Hattie      |
    +-------------+

    os> SELECT firstname FROM accounts WHERE firstname = multimatch('Hattie');
    fetched rows / total rows = 1/1
    +-------------+
    | firstname   |
    |-------------|
    | Hattie      |
    +-------------+

SIMPLE_QUERY_STRING
-------------------

Description
>>>>>>>>>>>

``simple_query_string([field_expression+], query_expression[, option=<option_value>]*)``

The simple_query_string function maps to the simple_query_string query used in search engine, to return the documents that match a provided text, number, date or boolean value with a given field or fields.
The **^** lets you *boost* certain fields. Boosts are multipliers that weigh matches in one field more heavily than matches in other fields. The syntax allows to specify the fields in double quotes, single quotes, in backtick or even without any wrap. All fields search using star ``"*"`` is also available (star symbol should be wrapped). The weight is optional and should be specified using after the field name, it could be delimeted by the `caret` character or by whitespace. Please, refer to examples below:

| ``simple_query_string(["Tags" ^ 2, 'Title' 3.4, `Body`, Comments ^ 0.3], ...)``
| ``simple_query_string(["*"], ...)``

Available parameters include:

- analyze_wildcard
- analyzer
- auto_generate_synonyms_phrase
- flags
- fuzziness
- fuzzy_max_expansions
- fuzzy_prefix_length
- fuzzy_transpositions
- lenient
- default_operator
- minimum_should_match
- quote_field_suffix
- boost

Example with only ``fields`` and ``query`` expressions, and all other parameters are set default values::

    os> select * from books where simple_query_string(['title'], 'Pooh House');
    fetched rows / total rows = 2/2
    +------+--------------------------+----------------------+
    | id   | title                    | author               |
    |------+--------------------------+----------------------|
    | 1    | The House at Pooh Corner | Alan Alexander Milne |
    | 2    | Winnie-the-Pooh          | Alan Alexander Milne |
    +------+--------------------------+----------------------+

Another example to show how to set custom values for the optional parameters::

    os> select * from books where simple_query_string(['title'], 'Pooh House', flags='ALL', default_operator='AND');
    fetched rows / total rows = 1/1
    +------+--------------------------+----------------------+
    | id   | title                    | author               |
    |------+--------------------------+----------------------|
    | 1    | The House at Pooh Corner | Alan Alexander Milne |
    +------+--------------------------+----------------------+


QUERY_STRING
------------

Description
>>>>>>>>>>>

``query_string([field_expression+], query_expression[, option=<option_value>]*)``

The query_string function maps to the query_string query used in search engine, to return the documents that match a provided text, number, date or boolean value with a given field or fields.
The **^** lets you *boost* certain fields. Boosts are multipliers that weigh matches in one field more heavily than matches in other fields. The syntax allows to specify the fields in double quotes, single quotes, backticks or without any wrap. All fields search using star ``"*"`` is also available (star symbol should be wrapped). The weight is optional and should be specified after the field name, it could be delimeted by the `caret` character or by whitespace. Please refer to examples below:

| ``query_string(["Tags" ^ 2, 'Title' 3.4, `Body`, Comments ^ 0.3], ...)``
| ``query_string(["*"], ...)``

Available parameters include:

- analyzer
- escape
- allow_leading_wildcard
- analyze_wildcard
- auto_generate_synonyms_phrase_query
- boost
- default_operator
- enable_position_increments
- fuzziness
- fuzzy_max_expansions
- fuzzy_prefix_length
- fuzzy_transpositions
- fuzzy_rewrite
- tie_breaker
- lenient
- type
- max_determinized_states
- minimum_should_match
- quote_analyzer
- phrase_slop
- quote_field_suffix
- rewrite
- time_zone

Example with only ``fields`` and ``query`` expressions, and all other parameters are set default values::

    os> select * from books where query_string(['title'], 'Pooh House');
    fetched rows / total rows = 2/2
    +------+--------------------------+----------------------+
    | id   | title                    | author               |
    |------+--------------------------+----------------------|
    | 1    | The House at Pooh Corner | Alan Alexander Milne |
    | 2    | Winnie-the-Pooh          | Alan Alexander Milne |
    +------+--------------------------+----------------------+

Another example to show how to set custom values for the optional parameters::

    os> select * from books where query_string(['title'], 'Pooh House', default_operator='AND');
    fetched rows / total rows = 1/1
    +------+--------------------------+----------------------+
    | id   | title                    | author               |
    |------+--------------------------+----------------------|
    | 1    | The House at Pooh Corner | Alan Alexander Milne |
    +------+--------------------------+----------------------+


QUERY
-----

Description
>>>>>>>>>>>

``query("query_expression" [, option=<option_value>]*)``

The `query` function is an alternative syntax to the `query_string`_ function. It maps to the query_string query used in search engine, to return the documents that match a provided text, number, date or boolean value with a given query expression.
``query_expression`` must be a string provided in Lucene query string syntax. Please refer to examples below:

| ``query('Tags:taste OR Body:taste', ...)``
| ``query("Tags:taste AND Body:taste", ...)``

Available parameters include:

- analyzer
- escape
- allow_leading_wildcard
- analyze_wildcard
- auto_generate_synonyms_phrase_query
- boost
- default_operator
- enable_position_increments
- fuzziness
- fuzzy_max_expansions
- fuzzy_prefix_length
- fuzzy_transpositions
- fuzzy_rewrite
- tie_breaker
- lenient
- type
- max_determinized_states
- minimum_should_match
- quote_analyzer
- phrase_slop
- quote_field_suffix
- rewrite
- time_zone

Example with only ``query_expressions``, and all other parameters are set default values::

    os> select * from books where query('title:Pooh House');
    fetched rows / total rows = 2/2
    +------+--------------------------+----------------------+
    | id   | title                    | author               |
    |------+--------------------------+----------------------|
    | 1    | The House at Pooh Corner | Alan Alexander Milne |
    | 2    | Winnie-the-Pooh          | Alan Alexander Milne |
    +------+--------------------------+----------------------+

Another example to show how to set custom values for the optional parameters::

    os> select * from books where query('title:Pooh House', default_operator='AND');
    fetched rows / total rows = 1/1
    +------+--------------------------+----------------------+
    | id   | title                    | author               |
    |------+--------------------------+----------------------|
    | 1    | The House at Pooh Corner | Alan Alexander Milne |
    +------+--------------------------+----------------------+

HIGHLIGHT
------------

Description
>>>>>>>>>>>

``highlight(field_expression)``

The highlight function maps to the highlight function used in search engine to return highlight fields for the given search.
The syntax allows to specify the field in double quotes or single quotes or without any wrap.
Please refer to examples below:

| ``highlight(title)``

Example searching for field Tags::

    os> select highlight(title) from books where query_string(['title'], 'Pooh House');
    fetched rows / total rows = 2/2
    +----------------------------------------------+
    | highlight(title)                             |
    |----------------------------------------------|
    | [The <em>House</em> at <em>Pooh</em> Corner] |
    | [Winnie-the-<em>Pooh</em>]                   |
    +----------------------------------------------+

WILDCARD_QUERY
------------

Description
>>>>>>>>>>>

``wildcard_query(field_expression, query_expression[, option=<option_value>]*)``

The ``wildcard_query`` function maps to the ``wildcard_query`` query used in search engine. It returns documents that match provided text in the specified field.
OpenSearch supports wildcard characters ``*`` and ``?``.  See the full description here: https://opensearch.org/docs/latest/opensearch/query-dsl/term/#wildcards.
You may include a backslash ``\`` to escape SQL wildcard characters ``\%`` and ``\_``.

Available parameters include:

- boost
- case_insensitive
- rewrite

For backward compatibility, ``wildcardquery`` is also supported and mapped to ``wildcard_query`` query as well.

Example with only ``field`` and ``query`` expressions, and all other parameters are set default values::

    os> select Body from wildcard where wildcard_query(Body, 'test wildcard*');
    fetched rows / total rows = 7/7
    +-------------------------------------------+
    | Body                                      |
    |-------------------------------------------|
    | test wildcard                             |
    | test wildcard in the end of the text%     |
    | test wildcard in % the middle of the text |
    | test wildcard %% beside each other        |
    | test wildcard in the end of the text_     |
    | test wildcard in _ the middle of the text |
    | test wildcard __ beside each other        |
    +-------------------------------------------+

Another example to show how to set custom values for the optional parameters::

    os> select Body from wildcard where wildcard_query(Body, 'test wildcard*', boost=0.7, case_insensitive=true, rewrite='constant_score');
    fetched rows / total rows = 8/8
    +-------------------------------------------+
    | Body                                      |
    |-------------------------------------------|
    | test wildcard                             |
    | test wildcard in the end of the text%     |
    | test wildcard in % the middle of the text |
    | test wildcard %% beside each other        |
    | test wildcard in the end of the text_     |
    | test wildcard in _ the middle of the text |
    | test wildcard __ beside each other        |
    | tEsT wIlDcArD sensitive cases             |
    +-------------------------------------------+

System Functions
================

TYPEOF
------

Description
>>>>>>>>>>>

Usage: typeof(expr) function returns name of the data type of the value that is passed to it. This can be helpful for troubleshooting or dynamically constructing SQL queries.

Argument type: ANY

Return type: STRING

Example::

    os> select typeof(DATE('2008-04-14')) as `typeof(date)`, typeof(1) as `typeof(int)`, typeof(now()) as `typeof(now())`, typeof(accounts) as `typeof(column)` from people
    fetched rows / total rows = 1/1
    +----------------+---------------+-----------------+------------------+
    | typeof(date)   | typeof(int)   | typeof(now())   | typeof(column)   |
    |----------------+---------------+-----------------+------------------|
    | DATE           | INTEGER       | DATETIME        | STRUCT           |
    +----------------+---------------+-----------------+------------------+


