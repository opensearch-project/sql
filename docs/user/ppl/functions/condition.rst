===================
Condition Functions
===================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1

ISNULL
------

Description
>>>>>>>>>>>

Usage: isnull(field) return true if field is null.

Argument type: all the supported data type.

Return type: BOOLEAN

Example::

    os> source=accounts | eval result = isnull(employer) | fields result, employer, firstname
    fetched rows / total rows = 4/4
    +--------+----------+-----------+
    | result | employer | firstname |
    |--------+----------+-----------|
    | False  | Pyrami   | Amber     |
    | False  | Netagy   | Hattie    |
    | False  | Quility  | Nanette   |
    | True   | null     | Dale      |
    +--------+----------+-----------+

ISNOTNULL
---------

Description
>>>>>>>>>>>

Usage: isnotnull(field) return true if field is not null.

Argument type: all the supported data type.

Return type: BOOLEAN

Synonyms: `ISPRESENT`_

Example::

    os> source=accounts | where not isnotnull(employer) | fields account_number, employer
    fetched rows / total rows = 1/1
    +----------------+----------+
    | account_number | employer |
    |----------------+----------|
    | 18             | null     |
    +----------------+----------+

EXISTS
------

`Because OpenSearch doesn't differentiate null and missing <https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-exists-query.html>`_. so we can't provide function like ismissing/isnotmissing to test field exist or not. But you can still use isnull/isnotnull for such purpose.

Example, the account 13 doesn't have email field::

    os> source=accounts | where isnull(email) | fields account_number, email
    fetched rows / total rows = 1/1
    +----------------+-------+
    | account_number | email |
    |----------------+-------|
    | 13             | null  |
    +----------------+-------+

IFNULL
------

Description
>>>>>>>>>>>

Usage: ifnull(field1, field2) return field2 if field1 is null.

Argument type: all the supported data type, (NOTE : if two parameters has different type, you will fail semantic check.)

Return type: any

Example::

    os> source=accounts | eval result = ifnull(employer, 'default') | fields result, employer, firstname
    fetched rows / total rows = 4/4
    +---------+----------+-----------+
    | result  | employer | firstname |
    |---------+----------+-----------|
    | Pyrami  | Pyrami   | Amber     |
    | Netagy  | Netagy   | Hattie    |
    | Quility | Quility  | Nanette   |
    | default | null     | Dale      |
    +---------+----------+-----------+

NULLIF
------

Description
>>>>>>>>>>>

Usage: nullif(field1, field2) return null if two parameters are same, otherwise return field1.

Argument type: all the supported data type, (NOTE : if two parameters has different type, if two parameters has different type, you will fail semantic check)

Return type: any

Example::

    os> source=accounts | eval result = nullif(employer, 'Pyrami') | fields result, employer, firstname
    fetched rows / total rows = 4/4
    +---------+----------+-----------+
    | result  | employer | firstname |
    |---------+----------+-----------|
    | null    | Pyrami   | Amber     |
    | Netagy  | Netagy   | Hattie    |
    | Quility | Quility  | Nanette   |
    | null    | null     | Dale      |
    +---------+----------+-----------+


ISNULL
------

Description
>>>>>>>>>>>

Usage: isnull(field1, field2) return null if two parameters are same, otherwise return field1.

Argument type: all the supported data type

Return type: any

Example::

    os> source=accounts | eval result = isnull(employer) | fields result, employer, firstname
    fetched rows / total rows = 4/4
    +--------+----------+-----------+
    | result | employer | firstname |
    |--------+----------+-----------|
    | False  | Pyrami   | Amber     |
    | False  | Netagy   | Hattie    |
    | False  | Quility  | Nanette   |
    | True   | null     | Dale      |
    +--------+----------+-----------+

IF
------

Description
>>>>>>>>>>>

Usage: if(condition, expr1, expr2) return expr1 if condition is true, otherwise return expr2.

Argument type: all the supported data type, (NOTE : if expr1 and expr2 are different type,  you will fail semantic check

Return type: any

Example::

    os> source=accounts | eval result = if(true, firstname, lastname) | fields result, firstname, lastname
    fetched rows / total rows = 4/4
    +---------+-----------+----------+
    | result  | firstname | lastname |
    |---------+-----------+----------|
    | Amber   | Amber     | Duke     |
    | Hattie  | Hattie    | Bond     |
    | Nanette | Nanette   | Bates    |
    | Dale    | Dale      | Adams    |
    +---------+-----------+----------+

    os> source=accounts | eval result = if(false, firstname, lastname) | fields result, firstname, lastname
    fetched rows / total rows = 4/4
    +--------+-----------+----------+
    | result | firstname | lastname |
    |--------+-----------+----------|
    | Duke   | Amber     | Duke     |
    | Bond   | Hattie    | Bond     |
    | Bates  | Nanette   | Bates    |
    | Adams  | Dale      | Adams    |
    +--------+-----------+----------+

    os> source=accounts | eval is_vip = if(age > 30 AND isnotnull(employer), true, false) | fields is_vip, firstname, lastname
    fetched rows / total rows = 4/4
    +--------+-----------+----------+
    | is_vip | firstname | lastname |
    |--------+-----------+----------|
    | True   | Amber     | Duke     |
    | True   | Hattie    | Bond     |
    | False  | Nanette   | Bates    |
    | False  | Dale      | Adams    |
    +--------+-----------+----------+

CASE
------

Description
>>>>>>>>>>>

Usage: case(condition1, expr1, condition2, expr2, ... conditionN, exprN else default) return expr1 if condition1 is true, or return expr2 if condition2 is true, ... if no condition is true, then return the value of ELSE clause. If the ELSE clause is not defined, it returns NULL.

Argument type: all the supported data type, (NOTE : there is no comma before "else")

Return type: any

Example::

    os> source=accounts | eval result = case(age > 35, firstname, age < 30, lastname else employer) | fields result, firstname, lastname, age, employer
    fetched rows / total rows = 4/4
    +--------+-----------+----------+-----+----------+
    | result | firstname | lastname | age | employer |
    |--------+-----------+----------+-----+----------|
    | Pyrami | Amber     | Duke     | 32  | Pyrami   |
    | Hattie | Hattie    | Bond     | 36  | Netagy   |
    | Bates  | Nanette   | Bates    | 28  | Quility  |
    | null   | Dale      | Adams    | 33  | null     |
    +--------+-----------+----------+-----+----------+

    os> source=accounts | eval result = case(age > 35, firstname, age < 30, lastname) | fields result, firstname, lastname, age
    fetched rows / total rows = 4/4
    +--------+-----------+----------+-----+
    | result | firstname | lastname | age |
    |--------+-----------+----------+-----|
    | null   | Amber     | Duke     | 32  |
    | Hattie | Hattie    | Bond     | 36  |
    | Bates  | Nanette   | Bates    | 28  |
    | null   | Dale      | Adams    | 33  |
    +--------+-----------+----------+-----+

    os> source=accounts | where true = case(age > 35, false, age < 30, false else true) | fields firstname, lastname, age
    fetched rows / total rows = 2/2
    +-----------+----------+-----+
    | firstname | lastname | age |
    |-----------+----------+-----|
    | Amber     | Duke     | 32  |
    | Dale      | Adams    | 33  |
    +-----------+----------+-----+

COALESCE
--------

Description
>>>>>>>>>>>

Version: 3.1.0

Usage: coalesce(field1, field2, ...) return the first non-null value in the argument list.

Argument type: all the supported data type. The data types of all fields must be same.

Return type: any

Example::

    PPL source=accounts | eval result = coalesce(employer, firstname, lastname) | fields result, firstname, lastname, employer
    fetched rows / total rows = 4/4
    +---------+-----------+----------+----------+
    | result  | firstname | lastname | employer |
    |---------+-----------+----------+----------|
    | Pyrami  | Amber     | Duke     | Pyrami   |
    | Netagy  | Hattie    | Bond     | Netagy   |
    | Quility | Nanette   | Bates    | Quility  |
    | Dale    | Dale      | Adams    | null     |
    +---------+-----------+----------+----------+

ISPRESENT
---------

Description
>>>>>>>>>>>

Version: 3.1.0

Usage: ispresent(field) return true if the field exists.

Argument type: all the supported data type.

Return type: BOOLEAN

Synonyms: `ISNOTNULL`_

Example::

    PPL> source=accounts | where ispresent(employer) | fields employer, firstname
    fetched rows / total rows = 3/3
    +----------+-----------+
    | employer | firstname |
    |----------+-----------|
    | Pyrami   | Amber     |
    | Netagy   | Hattie    |
    | Quility  | Nanette   |
    +----------+-----------+

ISBLANK
-------

Description
>>>>>>>>>>>

Version: 3.1.0

Usage: isblank(field) returns true if the field is null, an empty string, or contains only white space.

Argument type: all the supported data type.

Return type: BOOLEAN

Example::

    PPL> source=accounts | eval temp = ifnull(employer, '   ') | eval `isblank(employer)` = isblank(employer), `isblank(temp)` = isblank(temp) | fields `isblank(temp)`, temp, `isblank(employer)`, employer
    fetched rows / total rows = 4/4
    +---------------+---------+-------------------+----------+
    | isblank(temp) | temp    | isblank(employer) | employer |
    |---------------+---------+-------------------+----------|
    | False         | Pyrami  | False             | Pyrami   |
    | False         | Netagy  | False             | Netagy   |
    | False         | Quility | False             | Quility  |
    | True          |         | True              | null     |
    +---------------+---------+-------------------+----------+


ISEMPTY
-------

Description
>>>>>>>>>>>

Version: 3.1.0

Usage: isempty(field) returns true if the field is null or is an empty string.

Argument type: all the supported data type.

Return type: BOOLEAN

Example::

    PPL> source=accounts | eval temp = ifnull(employer, '   ') | eval `isempty(employer)` = isempty(employer), `isempty(temp)` = isempty(temp) | fields `isempty(temp)`, temp, `isempty(employer)`, employer
    fetched rows / total rows = 4/4
    +---------------+---------+-------------------+----------+
    | isempty(temp) | temp    | isempty(employer) | employer |
    |---------------+---------+-------------------+----------|
    | False         | Pyrami  | False             | Pyrami   |
    | False         | Netagy  | False             | Netagy   |
    | False         | Quility | False             | Quility  |
    | False         |         | True              | null     |
    +---------------+---------+-------------------+----------+

EARLIEST
-------

Description
>>>>>>>>>>>

Version: 3.1.0

Usage: earliest(relative_string, field) returns true if the field is after transferring the relative_string.

relative_string: The relative string can be one of the following format: 1. "now" or "now()", will use the current time. 2. With format MM/dd/yyyy:HH:mm:ss, will convert it to timestamp and compare.
 3. Relative format: [+|-]<time_integer><time_unit>@<time_unit> The steps to specify a relative time are: a. Indicate the time offset from the current time. b. Define the time amount, which is a number and a unit. c.Specify a "snap to" time unit. The time unit indicates the nearest or latest time to which your time amount rounds down.
 For example, current time is 2025-05-28 14:28:34, -3d+2y = 2027-05-25 14:28:34, +1d@m = 2025-05-29 14:28:00, -3M+1y@M = 2026-02-01 00:00:00

Argument type: relative_string:STRING, field: TIMESTAMP

Return type: BOOLEAN

Example::

    PPL> source=accounts | eval now=now() | eval a = earliest("now", now), b = earliest("-2d@d", now) | fields a,b | head 1
    fetched rows / total rows = 1/1
    +-------+-------+
    | a     | b     |
    |-------+-------|
    | False | True  |
    +-------+-------+

LATEST
-------

Description
>>>>>>>>>>>

Version: 3.1.0

Usage: latest(relative_string, field) returns true if the field is before transferring the relative_string.

Argument type: relative_string:STRING, field: TIMESTAMP

Return type: BOOLEAN

Example::

    PPL> source=accounts | eval now=now() | eval a = latest("now", now), b = latest("+2d@d", now) | fields a,b | head 1
    fetched rows / total rows = 1/1
    +-------+-------+
    | a     | b     |
    |-------+-------|
    | False | True  |
    +-------+-------+