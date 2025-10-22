=========================
Type Conversion Functions
=========================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1

CAST
----

Description
>>>>>>>>>>>

Usage: cast(expr as dateType) cast the expr to dataType. return the value of dataType. The following conversion rules are used:

+------------+--------+--------+---------+-------------+--------+--------+--------+
| Src/Target | STRING | NUMBER | BOOLEAN | TIMESTAMP   | DATE   | TIME   | IP     |
+------------+--------+--------+---------+-------------+--------+--------+--------+
| STRING     |        | Note1  | Note1   | TIMESTAMP() | DATE() | TIME() | IP()   |
+------------+--------+--------+---------+-------------+--------+--------+--------+
| NUMBER     | Note1  |        | v!=0    | N/A         | N/A    | N/A    | N/A    |
+------------+--------+--------+---------+-------------+--------+--------+--------+
| BOOLEAN    | Note1  | v?1:0  |         | N/A         | N/A    | N/A    | N/A    |
+------------+--------+--------+---------+-------------+--------+--------+--------+
| TIMESTAMP  | Note1  | N/A    | N/A     |             | DATE() | TIME() | N/A    |
+------------+--------+--------+---------+-------------+--------+--------+--------+
| DATE       | Note1  | N/A    | N/A     | N/A         |        | N/A    | N/A    |
+------------+--------+--------+---------+-------------+--------+--------+--------+
| TIME       | Note1  | N/A    | N/A     | N/A         | N/A    |        | N/A    |
+------------+--------+--------+---------+-------------+--------+--------+--------+
| IP         | Note2  | N/A    | N/A     | N/A         | N/A    | N/A    |        |
+------------+--------+--------+---------+-------------+--------+--------+--------+

Note1: the conversion follow the JDK specification.

Note2: IP will be converted to its canonical representation. Canonical representation
for IPv6 is described in `RFC 5952 <https://datatracker.ietf.org/doc/html/rfc5952>`_.

Cast to string example::

    os> source=people | eval `cbool` = CAST(true as string), `cint` = CAST(1 as string), `cdate` = CAST(CAST('2012-08-07' as date) as string) | fields `cbool`, `cint`, `cdate`
    fetched rows / total rows = 1/1
    +-------+------+------------+
    | cbool | cint | cdate      |
    |-------+------+------------|
    | TRUE  | 1    | 2012-08-07 |
    +-------+------+------------+

Cast to number example::

    os> source=people | eval `cbool` = CAST(true as int), `cstring` = CAST('1' as int) | fields `cbool`, `cstring`
    fetched rows / total rows = 1/1
    +-------+---------+
    | cbool | cstring |
    |-------+---------|
    | 1     | 1       |
    +-------+---------+

Cast to date example::

    os> source=people | eval `cdate` = CAST('2012-08-07' as date), `ctime` = CAST('01:01:01' as time), `ctimestamp` = CAST('2012-08-07 01:01:01' as timestamp) | fields `cdate`, `ctime`, `ctimestamp`
    fetched rows / total rows = 1/1
    +------------+----------+---------------------+
    | cdate      | ctime    | ctimestamp          |
    |------------+----------+---------------------|
    | 2012-08-07 | 01:01:01 | 2012-08-07 01:01:01 |
    +------------+----------+---------------------+

Cast function can be chained::

    os> source=people | eval `cbool` = CAST(CAST(true as string) as boolean) | fields `cbool`
    fetched rows / total rows = 1/1
    +-------+
    | cbool |
    |-------|
    | True  |
    +-------+


IMPLICIT (AUTO) TYPE CONVERSION
-------------------------------

Implicit conversion is automatic casting. When a function does not have an exact match for the
input types, the engine looks for another signature that can safely work with the values. It picks
the option that requires the least stretching of the original types, so you can mix literals and
fields without adding ``CAST`` everywhere.

String to numeric
>>>>>>>>>>>>>>>>>

When a string stands in for a number we simply parse the text:

- The value must be something like ``"3.14"`` or ``"42"``. Anything else causes the query to fail.
- If a string appears next to numeric arguments, it is treated as a ``DOUBLE`` so the numeric
  overload of the function can run.

Use string in arithmetic operator example ::

    os> source=people | eval divide="5"/10, multiply="5" * 10, add="5" + 10, minus="5" - 10, concat="5" + "5" | fields divide, multiply, add, minus, concat
    fetched rows / total rows = 1/1
    +--------+----------+------+-------+--------+
    | divide | multiply | add  | minus | concat |
    |--------+----------+------+-------+--------|
    | 0.5    | 50.0     | 15.0 | -5.0  | 55     |
    +--------+----------+------+-------+--------+

Use string in comparison operator example ::

    os> source=people | eval e="1000"==1000, en="1000"!=1000, ed="1000"==1000.0, edn="1000"!=1000.0, l="1000">999, ld="1000">999.9, i="malformed"==1000 | fields e, en, ed, edn, l, ld, i
    fetched rows / total rows = 1/1
    +------+-------+------+-------+------+------+------+
    | e    | en    | ed   | edn   | l    | ld   | i    |
    |------+-------+------+-------+------+------+------|
    | True | False | True | False | True | True | null |
    +------+-------+------+-------+------+------+------+

