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

ISPRESENT
------

Description
>>>>>>>>>>>

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
------

Description
>>>>>>>>>>>

Usage: isblank(field) returns true if the field is missing, an empty string, or contains only white space.

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
------

Description
>>>>>>>>>>>

Usage: isempty(field) returns true if the field is missing or is an empty string.

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
