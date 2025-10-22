======================
Mathematical Functions
======================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1


ABS
---

Description
>>>>>>>>>>>

Usage: abs(x) calculates the abs x.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: INTEGER/LONG/FLOAT/DOUBLE

Example::

    os> source=people | eval `ABS(-1)` = ABS(-1) | fields `ABS(-1)`
    fetched rows / total rows = 1/1
    +---------+
    | ABS(-1) |
    |---------|
    | 1       |
    +---------+


ADD
---

Description
>>>>>>>>>>>

Usage: add(x, y) calculates x plus y.

Argument type: INTEGER/LONG/FLOAT/DOUBLE, INTEGER/LONG/FLOAT/DOUBLE

Return type: Wider number between x and y

Synonyms: Addition Symbol (+)

Example::

    os> source=people | eval `ADD(2, 1)` = ADD(2, 1) | fields `ADD(2, 1)`
    fetched rows / total rows = 1/1
    +-----------+
    | ADD(2, 1) |
    |-----------|
    | 3         |
    +-----------+


SUBTRACT
--------

Description
>>>>>>>>>>>

Usage: subtract(x, y) calculates x minus y.

Argument type: INTEGER/LONG/FLOAT/DOUBLE, INTEGER/LONG/FLOAT/DOUBLE

Return type: Wider number between x and y

Synonyms: Subtraction Symbol (-)

Example::

    os> source=people | eval `SUBTRACT(2, 1)` = SUBTRACT(2, 1) | fields `SUBTRACT(2, 1)`
    fetched rows / total rows = 1/1
    +----------------+
    | SUBTRACT(2, 1) |
    |----------------|
    | 1              |
    +----------------+


MULTIPLY
--------

Description
>>>>>>>>>>>

Usage: multiply(x, y) calculates the multiplication of x and y.

Argument type: INTEGER/LONG/FLOAT/DOUBLE, INTEGER/LONG/FLOAT/DOUBLE

Return type: Wider number between x and y. If y equals to 0, then returns NULL.

Synonyms: Multiplication Symbol (\*)

Example::

    os> source=people | eval `MULTIPLY(2, 1)` = MULTIPLY(2, 1) | fields `MULTIPLY(2, 1)`
    fetched rows / total rows = 1/1
    +----------------+
    | MULTIPLY(2, 1) |
    |----------------|
    | 2              |
    +----------------+


DIVIDE
------

Description
>>>>>>>>>>>

Usage: divide(x, y) calculates x divided by y.

Argument type: INTEGER/LONG/FLOAT/DOUBLE, INTEGER/LONG/FLOAT/DOUBLE

Return type: Wider number between x and y

Synonyms: Division Symbol (/)

Example::

    os> source=people | eval `DIVIDE(2, 1)` = DIVIDE(2, 1) | fields `DIVIDE(2, 1)`
    fetched rows / total rows = 1/1
    +--------------+
    | DIVIDE(2, 1) |
    |--------------|
    | 2            |
    +--------------+


SUM
---

Description
>>>>>>>>>>>

Usage: sum(x, y, ...) calculates the sum of all provided arguments. This function accepts a variable number of arguments.

Note: This function is only available in the eval command context and is rewritten to arithmetic addition while query parsing.

Argument type: Variable number of INTEGER/LONG/FLOAT/DOUBLE arguments

Return type: Wider number type among all arguments

Example::

    os> source=accounts | eval `SUM(1, 2, 3)` = SUM(1, 2, 3) | fields `SUM(1, 2, 3)`
    fetched rows / total rows = 4/4
    +--------------+
    | SUM(1, 2, 3) |
    |--------------|
    | 6            |
    | 6            |
    | 6            |
    | 6            |
    +--------------+

    os> source=accounts | eval total = SUM(age, 10, 5) | fields age, total
    fetched rows / total rows = 4/4
    +-----+-------+
    | age | total |
    |-----+-------|
    | 32  | 47    |
    | 36  | 51    |
    | 28  | 43    |
    | 33  | 48    |
    +-----+-------+


AVG
---

Description
>>>>>>>>>>>

Usage: avg(x, y, ...) calculates the average (arithmetic mean) of all provided arguments. This function accepts a variable number of arguments.

Note: This function is only available in the eval command context and is rewritten to arithmetic expression (sum / count) at query parsing time.

Argument type: Variable number of INTEGER/LONG/FLOAT/DOUBLE arguments

Return type: DOUBLE

Example::

    os> source=accounts | eval `AVG(1, 2, 3)` = AVG(1, 2, 3) | fields `AVG(1, 2, 3)`
    fetched rows / total rows = 4/4
    +--------------+
    | AVG(1, 2, 3) |
    |--------------|
    | 2.0          |
    | 2.0          |
    | 2.0          |
    | 2.0          |
    +--------------+

    os> source=accounts | eval average = AVG(age, 30) | fields age, average
    fetched rows / total rows = 4/4
    +-----+---------+
    | age | average |
    |-----+---------|
    | 32  | 31.0    |
    | 36  | 33.0    |
    | 28  | 29.0    |
    | 33  | 31.5    |
    +-----+---------+


ACOS
----

Description
>>>>>>>>>>>

Usage: acos(x) calculates the arc cosine of x. Returns NULL if x is not in the range -1 to 1.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> source=people | eval `ACOS(0)` = ACOS(0) | fields `ACOS(0)`
    fetched rows / total rows = 1/1
    +--------------------+
    | ACOS(0)            |
    |--------------------|
    | 1.5707963267948966 |
    +--------------------+


ASIN
----

Description
>>>>>>>>>>>

Usage: asin(x) calculate the arc sine of x. Returns NULL if x is not in the range -1 to 1.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> source=people | eval `ASIN(0)` = ASIN(0) | fields `ASIN(0)`
    fetched rows / total rows = 1/1
    +---------+
    | ASIN(0) |
    |---------|
    | 0.0     |
    +---------+


ATAN
----

Description
>>>>>>>>>>>

Usage: atan(x) calculates the arc tangent of x. atan(y, x) calculates the arc tangent of y / x, except that the signs of both arguments are used to determine the quadrant of the result.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> source=people | eval `ATAN(2)` = ATAN(2), `ATAN(2, 3)` = ATAN(2, 3) | fields `ATAN(2)`, `ATAN(2, 3)`
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

Argument type: INTEGER/LONG/FLOAT/DOUBLE, INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> source=people | eval `ATAN2(2, 3)` = ATAN2(2, 3) | fields `ATAN2(2, 3)`
    fetched rows / total rows = 1/1
    +--------------------+
    | ATAN2(2, 3)        |
    |--------------------|
    | 0.5880026035475675 |
    +--------------------+


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

Return type: same type with input

Example::

    os> source=people | eval `CEILING(0)` = CEILING(0), `CEILING(50.00005)` = CEILING(50.00005), `CEILING(-50.00005)` = CEILING(-50.00005) | fields `CEILING(0)`, `CEILING(50.00005)`, `CEILING(-50.00005)`
    fetched rows / total rows = 1/1
    +------------+-------------------+--------------------+
    | CEILING(0) | CEILING(50.00005) | CEILING(-50.00005) |
    |------------+-------------------+--------------------|
    | 0          | 51.0              | -50.0              |
    +------------+-------------------+--------------------+

    os> source=people | eval `CEILING(3147483647.12345)` = CEILING(3147483647.12345), `CEILING(113147483647.12345)` = CEILING(113147483647.12345), `CEILING(3147483647.00001)` = CEILING(3147483647.00001) | fields `CEILING(3147483647.12345)`, `CEILING(113147483647.12345)`, `CEILING(3147483647.00001)`
    fetched rows / total rows = 1/1
    +---------------------------+-----------------------------+---------------------------+
    | CEILING(3147483647.12345) | CEILING(113147483647.12345) | CEILING(3147483647.00001) |
    |---------------------------+-----------------------------+---------------------------|
    | 3147483648.0              | 113147483648.0              | 3147483648.0              |
    +---------------------------+-----------------------------+---------------------------+


CONV
----

Description
>>>>>>>>>>>

Usage: CONV(x, a, b) converts the number x from a base to b base.

Argument type: x: STRING, a: INTEGER, b: INTEGER

Return type: STRING

Example::

    os> source=people | eval `CONV('12', 10, 16)` = CONV('12', 10, 16), `CONV('2C', 16, 10)` = CONV('2C', 16, 10), `CONV(12, 10, 2)` = CONV(12, 10, 2), `CONV(1111, 2, 10)` = CONV(1111, 2, 10) | fields `CONV('12', 10, 16)`, `CONV('2C', 16, 10)`, `CONV(12, 10, 2)`, `CONV(1111, 2, 10)`
    fetched rows / total rows = 1/1
    +--------------------+--------------------+-----------------+-------------------+
    | CONV('12', 10, 16) | CONV('2C', 16, 10) | CONV(12, 10, 2) | CONV(1111, 2, 10) |
    |--------------------+--------------------+-----------------+-------------------|
    | c                  | 44                 | 1100            | 15                |
    +--------------------+--------------------+-----------------+-------------------+


COS
---

Description
>>>>>>>>>>>

Usage: cos(x) calculates the cosine of x, where x is given in radians.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> source=people | eval `COS(0)` = COS(0) | fields `COS(0)`
    fetched rows / total rows = 1/1
    +--------+
    | COS(0) |
    |--------|
    | 1.0    |
    +--------+


COSH
----

Description
>>>>>>>>>>>

Usage: cosh(x) calculates the hyperbolic cosine of x, defined as (((e^x) + (e^(-x))) / 2).

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> source=people | eval `COSH(2)` = COSH(2) | fields `COSH(2)`
    fetched rows / total rows = 1/1
    +--------------------+
    | COSH(2)            |
    |--------------------|
    | 3.7621956910836314 |
    +--------------------+


COT
---

Description
>>>>>>>>>>>

Usage: cot(x) calculates the cotangent of x. Returns out-of-range error if x equals to 0.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> source=people | eval `COT(1)` = COT(1) | fields `COT(1)`
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

    os> source=people | eval `CRC32('MySQL')` = CRC32('MySQL') | fields `CRC32('MySQL')`
    fetched rows / total rows = 1/1
    +----------------+
    | CRC32('MySQL') |
    |----------------|
    | 3259397556     |
    +----------------+


DEGREES
-------

Description
>>>>>>>>>>>

Usage: degrees(x) converts x from radians to degrees.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> source=people | eval `DEGREES(1.57)` = DEGREES(1.57) | fields `DEGREES(1.57)`
    fetched rows / total rows  = 1/1
    +-------------------+
    | DEGREES(1.57)     |
    |-------------------|
    | 89.95437383553924 |
    +-------------------+


E
-

Description
>>>>>>>>>>>

Usage: E() returns the Euler's number

Return type: DOUBLE

Example::

    os> source=people | eval `E()` = E() | fields `E()`
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

Usage: exp(x) return e raised to the power of x.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> source=people | eval `EXP(2)` = EXP(2) | fields `EXP(2)`
    fetched rows / total rows = 1/1
    +------------------+
    | EXP(2)           |
    |------------------|
    | 7.38905609893065 |
    +------------------+


EXPM1
-----

Description
>>>>>>>>>>>

Usage: expm1(NUMBER T) returns the exponential of T, minus 1.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> source=people | eval `EXPM1(1)` = EXPM1(1) | fields `EXPM1(1)`
    fetched rows / total rows = 1/1
    +-------------------+
    | EXPM1(1)          |
    |-------------------|
    | 1.718281828459045 |
    +-------------------+


FLOOR
-----

Description
>>>>>>>>>>>

Usage: FLOOR(T) takes the floor of value T.

Limitation: FLOOR only works as expected when IEEE 754 double type displays decimal when stored.

Argument type: a: INTEGER/LONG/FLOAT/DOUBLE

Return type: same type with input

Example::

    os> source=people | eval `FLOOR(0)` = FLOOR(0), `FLOOR(50.00005)` = FLOOR(50.00005), `FLOOR(-50.00005)` = FLOOR(-50.00005) | fields `FLOOR(0)`, `FLOOR(50.00005)`, `FLOOR(-50.00005)`
    fetched rows / total rows = 1/1
    +----------+-----------------+------------------+
    | FLOOR(0) | FLOOR(50.00005) | FLOOR(-50.00005) |
    |----------+-----------------+------------------|
    | 0        | 50.0            | -51.0            |
    +----------+-----------------+------------------+

    os> source=people | eval `FLOOR(3147483647.12345)` = FLOOR(3147483647.12345), `FLOOR(113147483647.12345)` = FLOOR(113147483647.12345), `FLOOR(3147483647.00001)` = FLOOR(3147483647.00001) | fields `FLOOR(3147483647.12345)`, `FLOOR(113147483647.12345)`, `FLOOR(3147483647.00001)`
    fetched rows / total rows = 1/1
    +-------------------------+---------------------------+-------------------------+
    | FLOOR(3147483647.12345) | FLOOR(113147483647.12345) | FLOOR(3147483647.00001) |
    |-------------------------+---------------------------+-------------------------|
    | 3147483647.0            | 113147483647.0            | 3147483647.0            |
    +-------------------------+---------------------------+-------------------------+

    os> source=people | eval `FLOOR(282474973688888.022)` = FLOOR(282474973688888.022), `FLOOR(9223372036854775807.022)` = FLOOR(9223372036854775807.022), `FLOOR(9223372036854775807.0000001)` = FLOOR(9223372036854775807.0000001) | fields `FLOOR(282474973688888.022)`, `FLOOR(9223372036854775807.022)`, `FLOOR(9223372036854775807.0000001)`
    fetched rows / total rows = 1/1
    +----------------------------+--------------------------------+------------------------------------+
    | FLOOR(282474973688888.022) | FLOOR(9223372036854775807.022) | FLOOR(9223372036854775807.0000001) |
    |----------------------------+--------------------------------+------------------------------------|
    | 282474973688888.0          | 9.223372036854776e+18          | 9.223372036854776e+18              |
    +----------------------------+--------------------------------+------------------------------------+


LN
--

Description
>>>>>>>>>>>

Usage: ln(x) return the the natural logarithm of x.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> source=people | eval `LN(2)` = LN(2) | fields `LN(2)`
    fetched rows / total rows = 1/1
    +--------------------+
    | LN(2)              |
    |--------------------|
    | 0.6931471805599453 |
    +--------------------+


LOG
---

Description
>>>>>>>>>>>

Specifications:

Usage: log(x) returns the natural logarithm of x that is the base e logarithm of the x. log(B, x) is equivalent to log(x)/log(B).

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> source=people | eval `LOG(2)` = LOG(2), `LOG(2, 8)` = LOG(2, 8) | fields `LOG(2)`, `LOG(2, 8)`
    fetched rows / total rows = 1/1
    +--------------------+-----------+
    | LOG(2)             | LOG(2, 8) |
    |--------------------+-----------|
    | 0.6931471805599453 | 3.0       |
    +--------------------+-----------+


LOG2
----

Description
>>>>>>>>>>>

Specifications:

Usage: log2(x) is equivalent to log(x)/log(2).

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> source=people | eval `LOG2(8)` = LOG2(8) | fields `LOG2(8)`
    fetched rows / total rows = 1/1
    +---------+
    | LOG2(8) |
    |---------|
    | 3.0     |
    +---------+


LOG10
-----

Description
>>>>>>>>>>>

Specifications:

Usage: log10(x) is equivalent to log(x)/log(10).

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> source=people | eval `LOG10(100)` = LOG10(100) | fields `LOG10(100)`
    fetched rows / total rows = 1/1
    +------------+
    | LOG10(100) |
    |------------|
    | 2.0        |
    +------------+


MOD
---

Description
>>>>>>>>>>>

Usage: MOD(n, m) calculates the remainder of the number n divided by m.

Argument type: INTEGER/LONG/FLOAT/DOUBLE, INTEGER/LONG/FLOAT/DOUBLE

Return type: Wider type between types of n and m if m is nonzero value. If m equals to 0, then returns NULL.

Example::

    os> source=people | eval `MOD(3, 2)` = MOD(3, 2), `MOD(3.1, 2)` = MOD(3.1, 2) | fields `MOD(3, 2)`, `MOD(3.1, 2)`
    fetched rows / total rows = 1/1
    +-----------+-------------+
    | MOD(3, 2) | MOD(3.1, 2) |
    |-----------+-------------|
    | 1         | 1.1         |
    +-----------+-------------+


MODULUS
-------

Description
>>>>>>>>>>>

Usage: MODULUS(n, m) calculates the remainder of the number n divided by m.

Argument type: INTEGER/LONG/FLOAT/DOUBLE, INTEGER/LONG/FLOAT/DOUBLE

Return type: Wider type between types of n and m if m is nonzero value. If m equals to 0, then returns NULL.

Example::

    os> source=people | eval `MODULUS(3, 2)` = MODULUS(3, 2), `MODULUS(3.1, 2)` = MODULUS(3.1, 2) | fields `MODULUS(3, 2)`, `MODULUS(3.1, 2)`
    fetched rows / total rows = 1/1
    +---------------+-----------------+
    | MODULUS(3, 2) | MODULUS(3.1, 2) |
    |---------------+-----------------|
    | 1             | 1.1             |
    +---------------+-----------------+


PI
--

Description
>>>>>>>>>>>

Usage: PI() returns the constant pi

Return type: DOUBLE

Example::

    os> source=people | eval `PI()` = PI() | fields `PI()`
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

Argument type: INTEGER/LONG/FLOAT/DOUBLE, INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Synonyms: `POWER`_

Example::

    os> source=people | eval `POW(3, 2)` = POW(3, 2), `POW(-3, 2)` = POW(-3, 2), `POW(3, -2)` = POW(3, -2) | fields `POW(3, 2)`, `POW(-3, 2)`, `POW(3, -2)`
    fetched rows / total rows = 1/1
    +-----------+------------+--------------------+
    | POW(3, 2) | POW(-3, 2) | POW(3, -2)         |
    |-----------+------------+--------------------|
    | 9.0       | 9.0        | 0.1111111111111111 |
    +-----------+------------+--------------------+


POWER
-----

Description
>>>>>>>>>>>

Usage: POWER(x, y) calculates the value of x raised to the power of y. Bad inputs return NULL result.

Argument type: INTEGER/LONG/FLOAT/DOUBLE, INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Synonyms: `POW`_

Example::

    os> source=people | eval `POWER(3, 2)` = POWER(3, 2), `POWER(-3, 2)` = POWER(-3, 2), `POWER(3, -2)` = POWER(3, -2) | fields `POWER(3, 2)`, `POWER(-3, 2)`, `POWER(3, -2)`
    fetched rows / total rows = 1/1
    +-------------+--------------+--------------------+
    | POWER(3, 2) | POWER(-3, 2) | POWER(3, -2)       |
    |-------------+--------------+--------------------|
    | 9.0         | 9.0          | 0.1111111111111111 |
    +-------------+--------------+--------------------+


RADIANS
-------

Description
>>>>>>>>>>>

Usage: radians(x) converts x from degrees to radians.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> source=people | eval `RADIANS(90)` = RADIANS(90) | fields `RADIANS(90)`
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

    os> source=people | eval `RAND(3)` = RAND(3) | fields `RAND(3)`
    fetched rows / total rows = 1/1
    +---------------------+
    | RAND(3)             |
    |---------------------|
    | 0.34346429521113886 |
    +---------------------+


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

    os> source=people | eval `ROUND(12.34)` = ROUND(12.34), `ROUND(12.34, 1)` = ROUND(12.34, 1), `ROUND(12.34, -1)` = ROUND(12.34, -1), `ROUND(12, 1)` = ROUND(12, 1) | fields `ROUND(12.34)`, `ROUND(12.34, 1)`, `ROUND(12.34, -1)`, `ROUND(12, 1)`
    fetched rows / total rows = 1/1
    +--------------+-----------------+------------------+--------------+
    | ROUND(12.34) | ROUND(12.34, 1) | ROUND(12.34, -1) | ROUND(12, 1) |
    |--------------+-----------------+------------------+--------------|
    | 12.0         | 12.3            | 10.0             | 12           |
    +--------------+-----------------+------------------+--------------+


SIGN
----

Description
>>>>>>>>>>>

Usage: Returns the sign of the argument as -1, 0, or 1, depending on whether the number is negative, zero, or positive

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: same type with input

Example::

    os> source=people | eval `SIGN(1)` = SIGN(1), `SIGN(0)` = SIGN(0), `SIGN(-1.1)` = SIGN(-1.1) | fields `SIGN(1)`, `SIGN(0)`, `SIGN(-1.1)`
    fetched rows / total rows = 1/1
    +---------+---------+------------+
    | SIGN(1) | SIGN(0) | SIGN(-1.1) |
    |---------+---------+------------|
    | 1       | 0       | -1.0       |
    +---------+---------+------------+


SIGNUM
------

Description
>>>>>>>>>>>

Usage: Returns the sign of the argument as -1, 0, or 1, depending on whether the number is negative, zero, or positive

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: INTEGER

Synonyms: `SIGN`

Example::

    os> source=people | eval `SIGNUM(1)` = SIGNUM(1), `SIGNUM(0)` = SIGNUM(0), `SIGNUM(-1.1)` = SIGNUM(-1.1) | fields `SIGNUM(1)`, `SIGNUM(0)`, `SIGNUM(-1.1)`
    fetched rows / total rows = 1/1
    +-----------+-----------+--------------+
    | SIGNUM(1) | SIGNUM(0) | SIGNUM(-1.1) |
    |-----------+-----------+--------------|
    | 1         | 0         | -1.0         |
    +-----------+-----------+--------------+


SIN
---

Description
>>>>>>>>>>>

Usage: sin(x) calculates the sine of x, where x is given in radians.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> source=people | eval `SIN(0)` = SIN(0) | fields `SIN(0)`
    fetched rows / total rows = 1/1
    +--------+
    | SIN(0) |
    |--------|
    | 0.0    |
    +--------+


SINH
----

Description
>>>>>>>>>>>

Usage: sinh(x) calculates the hyperbolic sine of x, defined as (((e^x) - (e^(-x))) / 2).

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> source=people | eval `SINH(2)` = SINH(2) | fields `SINH(2)`
    fetched rows / total rows = 1/1
    +-------------------+
    | SINH(2)           |
    |-------------------|
    | 3.626860407847019 |
    +-------------------+


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

    os> source=people | eval `SQRT(4)` = SQRT(4), `SQRT(4.41)` = SQRT(4.41) | fields `SQRT(4)`, `SQRT(4.41)`
    fetched rows / total rows = 1/1
    +---------+------------+
    | SQRT(4) | SQRT(4.41) |
    |---------+------------|
    | 2.0     | 2.1        |
    +---------+------------+


CBRT
----

Description
>>>>>>>>>>>

Usage: Calculates the cube root of a number

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type DOUBLE:

INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE

Example::

    opensearchsql> source=location | eval `CBRT(8)` = CBRT(8), `CBRT(9.261)` = CBRT(9.261), `CBRT(-27)` = CBRT(-27) | fields `CBRT(8)`, `CBRT(9.261)`, `CBRT(-27)`;
    fetched rows / total rows = 2/2
    +---------+-------------+-----------+
    | CBRT(8) | CBRT(9.261) | CBRT(-27) |
    |---------+-------------+-----------|
    | 2.0     | 2.1         | -3.0      |
    | 2.0     | 2.1         | -3.0      |
    +---------+-------------+-----------+


RINT
----

Description
>>>>>>>>>>>

Usage: rint(NUMBER T) returns T rounded to the closest whole integer number.

Argument type: INTEGER/LONG/FLOAT/DOUBLE

Return type: DOUBLE

Example::

    os> source=people | eval `RINT(1.7)` = RINT(1.7) | fields `RINT(1.7)`
    fetched rows / total rows = 1/1
    +-----------+
    | RINT(1.7) |
    |-----------|
    | 2.0       |
    +-----------+