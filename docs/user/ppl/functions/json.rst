====================
IP Address Functions
====================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1

JSON_VALID
----------

Description
>>>>>>>>>>>

Usage: `json_valid(json_string)` checks if `json_string` is a valid STRING string.

Argument type: STRING

Return type: BOOLEAN

Example::

    > source=json_test | where json_valid(json_string) | fields test_name, json_string
    fetched rows / total rows = 4/4
    +--------------------+--------------------+
    | test_name          | json_string        |
    |--------------------|--------------------|
    | json object        | {"a":"1","b":"2"}  |
    | json array         | [1, 2, 3, 4]       |
    | json scalar string | [1, 2, 3, 4]       |
    | json empty string  | [1, 2, 3, 4]       |
    +--------------------+--------------------+
