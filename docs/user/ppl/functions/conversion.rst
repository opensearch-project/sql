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

    os> source=people | eval `cbool` = CAST(true as string), `cint` = CAST(1 as string), `cdate` = CAST(CAST('2012-08-07' as date) as string) | fields `cbool`, `cint`, `cdate`
    fetched rows / total rows = 1/1
    +-------+------+------------+
    | cbool | cint | cdate      |
    |-------+------+------------|
    | true  | 1    | 2012-08-07 |
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
