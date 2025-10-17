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

TOSTRING
-----------

Description
>>>>>>>>>>>
The following usage options are available, depending on the parameter types and the number of parameters.

Usage with format type: tostring(ANY, [format]): Converts the value in first argument  to provided format type string in second argument. If second argument is not provided, then it converts to default string representation.
Return type: string

Usage for boolean parameter without format type tostring(boolean): Converts the string to 'TRUE' or 'FALSE'.
Return type: string

You can use this function with the eval commands and as part of eval expressions. If first argument can be any valid type , second argument is optional and if provided , it needs to be format name to convert to where first argument contains only numbers. If first argument is boolean, then second argument is not used even if its provided.

Format types:

a) "binary" Converts a number to a binary value.
b) "hex" Converts the number to a hexadecimal value.
c) "commas" Formats the number with commas. If the number includes a decimal, the function rounds the number to nearest two decimal places.
d) "duration" Converts the value in seconds to the readable time format HH:MM:SS.
5) "duration_millis" Converts the value in milliseconds to the readable time format HH:MM:SS.

The format argument is optional and is only used when the value argument is a number. The tostring function supports the following formats.

Basic examples:

The following example returns "TRUE 0xF 12,345.68".

    ... | eval n=tostring(1==1) + " " + tostring(15, "hex") + " " + tostring(12345.6789, "commas")

The following example returns foo=615 and foo2=00:10:15. The 615 seconds is converted into minutes and seconds.

    ... | eval foo=615 | eval foo2 = tostring(foo, "duration")



You can use this function to convert a number to a string of its binary representation.
Example::

    os> source=EMP |  eval salary_binary = tostring(SAL, "binary") | fields ENAME, salary_binary, SAL"
    fetched rows / total rows = 1/1
    +---------------+------------------+------------+
    | ENAME         |   salary_binary  |  SAL       |
    |---------------+------------------+------------+
    | SMITH         | 1001110001000000 | 80000.00   |
    +---------------+------------------+------------+


You can use this function to convert a number to a string of its hex representation.
Example::

    os> source=EMP |  eval salary_hex = tostring(SAL, "hex") | fields ENAME, salary_hex, SAL"
    fetched rows / total rows = 1/1
    +---------------+------------------+------------+
    | ENAME         |   salary_hex  |  SAL          |
    |---------------+------------------+------------+
    | SMITH         |   13880       | 80000.00      |
    +---------------+---------------+---------------+

The following example formats the column totalSales to display values  with commas.
Example::

     os> source=EMP |  eval salary_commas = tostring(SAL, "commas") | fields ENAME, salary_commas, SAL"
     fetched rows / total rows = 1/1
    +---------------+------------------+------------+
    | ENAME         |   salary_commas  |  SAL       |
    |---------------+------------------+------------+
    | SMITH         |   80,000         | 80000.00   |
    +---------------+------------------+------------+

The following example converts number of seconds to HH:MM:SS format representing hours, minutes and seconds.
Example::

      os> source=EMP |  eval duration = tostring(6500, "duration") | fields ENAME, duration"
          fetched rows / total rows = 1/1
    +---------------+-------------+
    | ENAME         |   duration  |
    |---------------+-------------+
    | SMITH         |   01:48:20  |
    +---------------+-------------+

The following example for converts boolean parameter to string.
Example::

    os> source=people | eval `boolean_str` = tostring(1=1)| fields `boolean_str`
    fetched rows / total rows = 1/1

    +---------------------+
    | boolean_str         |
    |---------------------+
    | TRUE                |
    +---------------------+