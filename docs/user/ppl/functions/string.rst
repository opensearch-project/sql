================
String Functions
================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1

CONCAT
------

Description
>>>>>>>>>>>

Usage: CONCAT(str1, str2, ...., str_9) adds up to 9 strings together.

Argument type: STRING, STRING, ...., STRING

Return type: STRING

Example::

    os> source=people | eval `CONCAT('hello', 'world')` = CONCAT('hello', 'world'), `CONCAT('hello ', 'whole ', 'world', '!')` = CONCAT('hello ', 'whole ', 'world', '!') | fields `CONCAT('hello', 'world')`, `CONCAT('hello ', 'whole ', 'world', '!')`
    fetched rows / total rows = 1/1
    +--------------------------+------------------------------------------+
    | CONCAT('hello', 'world') | CONCAT('hello ', 'whole ', 'world', '!') |
    |--------------------------+------------------------------------------|
    | helloworld               | hello whole world!                       |
    +--------------------------+------------------------------------------+


CONCAT_WS
---------

Description
>>>>>>>>>>>

Usage: CONCAT_WS(sep, str1, str2) returns str1 concatenated with str2 using sep as a separator between them.

Argument type: STRING, STRING, STRING

Return type: STRING

Example::

    os> source=people | eval `CONCAT_WS(',', 'hello', 'world')` = CONCAT_WS(',', 'hello', 'world') | fields `CONCAT_WS(',', 'hello', 'world')`
    fetched rows / total rows = 1/1
    +----------------------------------+
    | CONCAT_WS(',', 'hello', 'world') |
    |----------------------------------|
    | hello,world                      |
    +----------------------------------+


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

    os> source=people | eval `LENGTH('helloworld')` = LENGTH('helloworld') | fields `LENGTH('helloworld')`
    fetched rows / total rows = 1/1
    +----------------------+
    | LENGTH('helloworld') |
    |----------------------|
    | 10                   |
    +----------------------+


LIKE
----

Description
>>>>>>>>>>>

Usage: like(string, PATTERN) return true if the string match the PATTERN, PATTERN is case insensitive.

There are two wildcards often used in conjunction with the LIKE operator:

* ``%`` - The percent sign represents zero, one, or multiple characters
* ``_`` - The underscore represents a single character

Example::

    os> source=people | eval `LIKE('hello world', '_ello%')` = LIKE('hello world', '_ELLO%') | fields `LIKE('hello world', '_ello%')`
    fetched rows / total rows = 1/1
    +-------------------------------+
    | LIKE('hello world', '_ello%') |
    |-------------------------------|
    | True                          |
    +-------------------------------+


LOCATE
-------

Description
>>>>>>>>>>>

Usage: locate(substr, str[, start]) returns the position of the first occurrence of substring substr in string str, starting from position start. If start is not specified, it defaults to 1 (the beginning of the string). Returns 0 if substr is not found. If any argument is NULL, the function returns NULL.

Argument type: STRING, STRING[, INTEGER]

Return type: INTEGER

Example::

    os> source=people | eval `LOCATE('world', 'helloworld')` = LOCATE('world', 'helloworld'), `LOCATE('invalid', 'helloworld')` = LOCATE('invalid', 'helloworld'), `LOCATE('world', 'helloworld', 6)` = LOCATE('world', 'helloworld', 6) | fields `LOCATE('world', 'helloworld')`, `LOCATE('invalid', 'helloworld')`, `LOCATE('world', 'helloworld', 6)`
    fetched rows / total rows = 1/1
    +-----------------------------------+-------------------------------------+---------------------------------------+
    | LOCATE('world', 'helloworld')     | LOCATE('invalid', 'helloworld')     | LOCATE('world', 'helloworld', 6)      |
    |-----------------------------------+-------------------------------------+---------------------------------------|
    | 6                                 | 0                                   | 0                                     |
    +-----------------------------------+-------------------------------------+---------------------------------------+


LOWER
-----

Description
>>>>>>>>>>>

Usage: lower(string) converts the string to lowercase.

Argument type: STRING

Return type: STRING

Example::

    os> source=people | eval `LOWER('helloworld')` = LOWER('helloworld'), `LOWER('HELLOWORLD')` = LOWER('HELLOWORLD') | fields `LOWER('helloworld')`, `LOWER('HELLOWORLD')`
    fetched rows / total rows = 1/1
    +---------------------+---------------------+
    | LOWER('helloworld') | LOWER('HELLOWORLD') |
    |---------------------+---------------------|
    | helloworld          | helloworld          |
    +---------------------+---------------------+


LTRIM
-----

Description
>>>>>>>>>>>

Usage: ltrim(str) trims leading space characters from the string.

Argument type: STRING

Return type: STRING

Example::

    os> source=people | eval `LTRIM('   hello')` = LTRIM('   hello'), `LTRIM('hello   ')` = LTRIM('hello   ') | fields `LTRIM('   hello')`, `LTRIM('hello   ')`
    fetched rows / total rows = 1/1
    +-------------------+-------------------+
    | LTRIM('   hello') | LTRIM('hello   ') |
    |-------------------+-------------------|
    | hello             | hello             |
    +-------------------+-------------------+


POSITION
--------

Description
>>>>>>>>>>>

Usage: The syntax POSITION(substr IN str) returns the position of the first occurrence of substring substr in string str. Returns 0 if substr is not in str. Returns NULL if any argument is NULL.

Argument type: STRING, STRING

Return type INTEGER

(STRING IN STRING) -> INTEGER

Example::

    os> source=people | eval `POSITION('world' IN 'helloworld')` = POSITION('world' IN 'helloworld'), `POSITION('invalid' IN 'helloworld')`= POSITION('invalid' IN 'helloworld')  | fields `POSITION('world' IN 'helloworld')`, `POSITION('invalid' IN 'helloworld')`
    fetched rows / total rows = 1/1
    +-----------------------------------+-------------------------------------+
    | POSITION('world' IN 'helloworld') | POSITION('invalid' IN 'helloworld') |
    |-----------------------------------+-------------------------------------|
    | 6                                 | 0                                   |
    +-----------------------------------+-------------------------------------+


REPLACE
--------

Description
>>>>>>>>>>>

Usage: replace(str, substr, newstr) returns a string with all occurrences of substr replaced by newstr in str. If any argument is NULL, the function returns NULL.

Example::

    os> source=people | eval `REPLACE('helloworld', 'world', 'universe')` = REPLACE('helloworld', 'world', 'universe'), `REPLACE('helloworld', 'invalid', 'universe')` = REPLACE('helloworld', 'invalid', 'universe') | fields `REPLACE('helloworld', 'world', 'universe')`, `REPLACE('helloworld', 'invalid', 'universe')`
    fetched rows / total rows = 1/1
    +-------------------------------------+---------------------------------------+
    | REPLACE('helloworld', 'world', 'universe') | REPLACE('helloworld', 'invalid', 'universe') |
    |-------------------------------------+---------------------------------------|
    | hellouniverse                       | helloworld                             |
    +-------------------------------------+---------------------------------------+


REVERSE
-------

Description
>>>>>>>>>>>

Usage: REVERSE(str) returns reversed string of the string supplied as an argument.

Argument type: STRING

Return type: STRING

Example::

    os> source=people | eval `REVERSE('abcde')` = REVERSE('abcde') | fields `REVERSE('abcde')`
    fetched rows / total rows = 1/1
    +------------------+
    | REVERSE('abcde') |
    |------------------|
    | edcba            |
    +------------------+


RIGHT
-----

Description
>>>>>>>>>>>

Usage: right(str, len) returns the rightmost len characters from the string str, or NULL if any argument is NULL.

Argument type: STRING, INTEGER

Return type: STRING

Example::

    os> source=people | eval `RIGHT('helloworld', 5)` = RIGHT('helloworld', 5), `RIGHT('HELLOWORLD', 0)` = RIGHT('HELLOWORLD', 0) | fields `RIGHT('helloworld', 5)`, `RIGHT('HELLOWORLD', 0)`
    fetched rows / total rows = 1/1
    +------------------------+------------------------+
    | RIGHT('helloworld', 5) | RIGHT('HELLOWORLD', 0) |
    |------------------------+------------------------|
    | world                  |                        |
    +------------------------+------------------------+


RTRIM
-----

Description
>>>>>>>>>>>

Usage: rtrim(str) trims trailing space characters from the string.

Argument type: STRING

Return type: STRING

Example::

    os> source=people | eval `RTRIM('   hello')` = RTRIM('   hello'), `RTRIM('hello   ')` = RTRIM('hello   ') | fields `RTRIM('   hello')`, `RTRIM('hello   ')`
    fetched rows / total rows = 1/1
    +-------------------+-------------------+
    | RTRIM('   hello') | RTRIM('hello   ') |
    |-------------------+-------------------|
    | hello             | hello             |
    +-------------------+-------------------+


SUBSTRING
---------

Description
>>>>>>>>>>>

Usage: substring(str, start) or substring(str, start, length) returns substring using start and length. With no length, entire string from start is returned.

Argument type: STRING, INTEGER, INTEGER

Return type: STRING

Synonyms: SUBSTR

Example::

    os> source=people | eval `SUBSTRING('helloworld', 5)` = SUBSTRING('helloworld', 5), `SUBSTRING('helloworld', 5, 3)` = SUBSTRING('helloworld', 5, 3) | fields `SUBSTRING('helloworld', 5)`, `SUBSTRING('helloworld', 5, 3)`
    fetched rows / total rows = 1/1
    +----------------------------+-------------------------------+
    | SUBSTRING('helloworld', 5) | SUBSTRING('helloworld', 5, 3) |
    |----------------------------+-------------------------------|
    | oworld                     | owo                           |
    +----------------------------+-------------------------------+


TRIM
----

Description
>>>>>>>>>>>

Argument Type: STRING

Return type: STRING

Example::

    os> source=people | eval `TRIM('   hello')` = TRIM('   hello'), `TRIM('hello   ')` = TRIM('hello   ') | fields `TRIM('   hello')`, `TRIM('hello   ')`
    fetched rows / total rows = 1/1
    +------------------+------------------+
    | TRIM('   hello') | TRIM('hello   ') |
    |------------------+------------------|
    | hello            | hello            |
    +------------------+------------------+


UPPER
-----

Description
>>>>>>>>>>>

Usage: upper(string) converts the string to uppercase.

Argument type: STRING

Return type: STRING

Example::

    os> source=people | eval `UPPER('helloworld')` = UPPER('helloworld'), `UPPER('HELLOWORLD')` = UPPER('HELLOWORLD') | fields `UPPER('helloworld')`, `UPPER('HELLOWORLD')`
    fetched rows / total rows = 1/1
    +---------------------+---------------------+
    | UPPER('helloworld') | UPPER('HELLOWORLD') |
    |---------------------+---------------------|
    | HELLOWORLD          | HELLOWORLD          |
    +---------------------+---------------------+
