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

Nested IFNULL Pattern
>>>>>>>>>>>>>>>>>>>>>

For OpenSearch versions prior to 3.1, COALESCE-like functionality can be achieved using nested IFNULL statements. This pattern is particularly useful in observability use cases where field names may vary across different data sources.

Usage: ifnull(field1, ifnull(field2, ifnull(field3, default_value)))

Example::

    os> source=accounts | eval result = ifnull(employer, ifnull(firstname, ifnull(lastname, "unknown"))) | fields result, employer, firstname, lastname
    fetched rows / total rows = 4/4
    +---------+----------+-----------+----------+
    | result  | employer | firstname | lastname |
    |---------+----------+-----------+----------|
    | Pyrami  | Pyrami   | Amber     | Duke     |
    | Netagy  | Netagy   | Hattie    | Bond     |
    | Quility | Quility  | Nanette   | Bates    |
    | Dale    | null     | Dale      | Adams    |
    +---------+----------+-----------+----------+

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

Usage: coalesce(field1, field2, ...) return the first non-null, non-missing value in the argument list.

Argument type: all the supported data type. Supports mixed data types with automatic type coercion.

Return type: determined by the least restrictive common type among all arguments, with fallback to string if no common type can be determined

Behavior:

- Returns the first value that is not null and not missing (missing includes non-existent fields)
- Empty strings ("") and whitespace strings (" ") are considered valid values
- If all arguments are null or missing, returns null
- Automatic type coercion is applied to match the determined return type
- If type conversion fails, the value is converted to string representation
- For best results, use arguments of the same data type to avoid unexpected type conversions

Performance Considerations:

- Optimized for multiple field evaluation, more efficient than nested IFNULL patterns
- Evaluates arguments sequentially, stopping at the first non-null value
- Consider field order based on likelihood of containing values to minimize evaluation overhead

Limitations:

- Type coercion may result in unexpected string conversions for incompatible types
- Performance may degrade with very large numbers of arguments

Example::

    os> source=accounts | eval result = coalesce(employer, firstname, lastname) | fields result, firstname, lastname, employer
    fetched rows / total rows = 4/4
    +---------+-----------+----------+----------+
    | result  | firstname | lastname | employer |
    |---------+-----------+----------+----------|
    | Pyrami  | Amber     | Duke     | Pyrami   |
    | Netagy  | Hattie    | Bond     | Netagy   |
    | Quility | Nanette   | Bates    | Quility  |
    | Dale    | Dale      | Adams    | null     |
    +---------+-----------+----------+----------+

Empty String Handling Examples::

    os> source=accounts | eval empty_field = "" | eval result = coalesce(empty_field, firstname) | fields result, empty_field, firstname
    fetched rows / total rows = 4/4
    +--------+-------------+-----------+
    | result | empty_field | firstname |
    |--------+-------------+-----------|
    |        |             | Amber     |
    |        |             | Hattie    |
    |        |             | Nanette   |
    |        |             | Dale      |
    +--------+-------------+-----------+

    os> source=accounts | eval result = coalesce(" ", firstname) | fields result, firstname
    fetched rows / total rows = 4/4
    +--------+-----------+
    | result | firstname |
    |--------+-----------|
    |        | Amber     |
    |        | Hattie    |
    |        | Nanette   |
    |        | Dale      |
    +--------+-----------+

Mixed Data Types with Auto Coercion::

    os> source=accounts | eval result = coalesce(employer, balance, "fallback") | fields result, employer, balance
    fetched rows / total rows = 4/4
    +---------+----------+---------+
    | result  | employer | balance |
    |---------+----------+---------|
    | Pyrami  | Pyrami   | 39225   |
    | Netagy  | Netagy   | 5686    |
    | Quility | Quility  | 32838   |
    | 4180    | null     | 4180    |
    +---------+----------+---------+

Non-existent Field Handling::

    os> source=accounts | eval result = coalesce(nonexistent_field, firstname, "unknown") | fields result, firstname
    fetched rows / total rows = 4/4
    +---------+-----------+
    | result  | firstname |
    |---------+-----------|
    | Amber   | Amber     |
    | Hattie  | Hattie    |
    | Nanette | Nanette   |
    | Dale    | Dale      |
    +---------+-----------+


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

    os> source=accounts | where ispresent(employer) | fields employer, firstname
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

    os> source=accounts | eval temp = ifnull(employer, '   ') | eval `isblank(employer)` = isblank(employer), `isblank(temp)` = isblank(temp) | fields `isblank(temp)`, temp, `isblank(employer)`, employer
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

    os> source=accounts | eval temp = ifnull(employer, '   ') | eval `isempty(employer)` = isempty(employer), `isempty(temp)` = isempty(temp) | fields `isempty(temp)`, temp, `isempty(employer)`, employer
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
--------

Description
>>>>>>>>>>>

Version: 3.1.0

Usage: earliest(relative_string, field) returns true if the value of field is after the timestamp derived from relative_string relative to the current time. Otherwise, return false.

relative_string: 
The relative string can be one of the following formats:

1. `"now"` or `"now()"`:  
   Uses the current system time.

2. Absolute format (`MM/dd/yyyy:HH:mm:ss` or `yyyy-MM-dd HH:mm:ss`):
   Converts the string to a timestamp and compares it with the data.

3. Relative format: `(+|-)<time_integer><time_unit>[+<...>]@<snap_unit>`  
   Steps to specify a relative time:

   - **a. Time offset:** Indicate the offset from the current time using `+` or `-`.  
   - **b. Time amount:** Provide a numeric value followed by a time unit (`s`, `m`, `h`, `d`, `w`, `M`, `y`).  
   - **c. Snap to unit:** Optionally specify a snap unit with `@<unit>` to round the result down to the nearest unit (e.g., hour, day, month).

   **Examples** (assuming current time is `2025-05-28 14:28:34`):

   - `-3d+2y` → `2027-05-25 14:28:34`  
   - `+1d@m` → `2025-05-29 14:28:00`  
   - `-3M+1y@M` → `2026-02-01 00:00:00`

Read more details `here <https://github.com/opensearch-project/opensearch-spark/blob/main/docs/ppl-lang/functions/ppl-datetime.md#relative_timestamp>`_

Argument type: relative_string:STRING, field: TIMESTAMP

Return type: BOOLEAN

Example::

    os> source=accounts | eval now = utc_timestamp() | eval a = earliest("now", now), b = earliest("-2d@d", now) | fields a, b | head 1
    fetched rows / total rows = 1/1
    +-------+------+
    | a     | b    |
    |-------+------|
    | False | True |
    +-------+------+

    os> source=nyc_taxi | where earliest('07/01/2014:00:30:00', timestamp) | stats COUNT() as cnt
    fetched rows / total rows = 1/1
    +-----+
    | cnt |
    |-----|
    | 972 |
    +-----+

LATEST
------

Description
>>>>>>>>>>>

Version: 3.1.0

Usage: latest(relative_string, field) returns true if the value of field is before the timestamp derived from relative_string relative to the current time. Otherwise, return false.

Argument type: relative_string:STRING, field: TIMESTAMP

Return type: BOOLEAN

Example::

    os> source=accounts | eval now = utc_timestamp() | eval a = latest("now", now), b = latest("+2d@d", now) | fields a, b | head 1
    fetched rows / total rows = 1/1
    +------+------+
    | a    | b    |
    |------+------|
    | True | True |
    +------+------+

    os> source=nyc_taxi | where latest('07/21/2014:04:00:00', timestamp) | stats COUNT() as cnt
    fetched rows / total rows = 1/1
    +-----+
    | cnt |
    |-----|
    | 969 |
    +-----+

REGEX_MATCH
-----------

Description
>>>>>>>>>>>

Version: 3.3.0

Usage: regex_match(string, pattern) returns true if the regular expression pattern finds a match against any substring of the string value, otherwise returns false.

The function uses Java regular expression syntax for the pattern.

Argument type: STRING, STRING

Return type: BOOLEAN

Example::

    #os> source=logs | where regex_match(message, 'ERROR|WARN|FATAL') | fields timestamp, message
    fetched rows / total rows = 3/100
    +---------------------+------------------------------------------+
    | timestamp           | message                                  |
    |---------------------+------------------------------------------|
    | 2024-01-15 10:23:45 | ERROR: Connection timeout to database   |
    | 2024-01-15 10:24:12 | WARN: High memory usage detected        |
    | 2024-01-15 10:25:33 | FATAL: System crashed unexpectedly      |
    +---------------------+------------------------------------------+

    #os> source=users | where regex_match(email, '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}') | fields name, email
    fetched rows / total rows = 2/3
    +-------+----------------------+
    | name  | email                |
    |-------+----------------------|
    | John  | john@example.com     |
    | Alice | alice@company.org    |
    +-------+----------------------+

    #os> source=network | where regex_match(ip_address, '^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$') AND NOT regex_match(ip_address, '^(10\.|172\.(1[6-9]|2[0-9]|3[01])\.|192\.168\.)') | fields ip_address, status
    fetched rows / total rows = 2/10
    +---------------+--------+
    | ip_address    | status |
    |---------------+--------|
    | 8.8.8.8       | active |
    | 1.1.1.1       | active |
    +---------------+--------+

    #os> source=products | eval category = if(regex_match(name, '(?i)(laptop|computer|desktop)'), 'Computing', if(regex_match(name, '(?i)(phone|tablet|mobile)'), 'Mobile', 'Other')) | fields name, category
    fetched rows / total rows = 4/4
    +------------------------+----------+
    | name                   | category |
    |------------------------+----------|
    | Dell Laptop XPS        | Computing|
    | iPhone 15 Pro          | Mobile   |
    | Wireless Mouse         | Other    |
    | Desktop Computer Tower | Computing|
    +------------------------+----------+
