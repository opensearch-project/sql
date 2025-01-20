====================
JSON Functions
====================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1

JSON_VALID
----------

Description
>>>>>>>>>>>

Usage: `json_valid(json_string)` checks if `json_string` is a valid JSON-encoded string.

Argument type: STRING

Return type: BOOLEAN

Example::

    > source=json_test | eval is_valid = json_valid(json_string) | fields test_name, json_string, is_valid
    fetched rows / total rows = 6/6
    +---------------------+---------------------------------+----------+
    | test_name           | json_string                     | is_valid |
    |---------------------|---------------------------------|----------|
    | json nested object  | {"a":"1","b":{"c":"2","d":"3"}} | True     |
    | json object         | {"a":"1","b":"2"}               | True     |
    | json array          | [1, 2, 3, 4]                    | True     |
    | json scalar string  | "abc"                           | True     |
    | json empty string   |                                 | True     |
    | json invalid object | {"invalid":"json", "string"}    | False    |
    +---------------------+---------------------------------+----------+

JSON
----------

Description
>>>>>>>>>>>

Usage: `json(value)` Evaluates whether a string can be parsed as a json-encoded string and casted as an expression. Returns the JSON value if valid, null otherwise.

Argument type: STRING

Return type: BOOLEAN/DOUBLE/INTEGER/NULL/STRUCT/ARRAY

Example::

    > source=json_test | where json_valid(json_string) | eval json=json(json_string) | fields test_name, json_string, json
    fetched rows / total rows = 4/4
    +---------------------+------------------------------+---------------+
    | test_name           | json_string                  | json          |
    |---------------------|------------------------------|---------------|
    | json object         | {"a":"1","b":"2"}            | {a:"1",b:"2"} |
    | json array          | [1, 2, 3, 4]                 | [1,2,3,4]     |
    | json scalar string  | "abc"                        | "abc"         |
    | json empty string   |                              | null          |
    +---------------------+------------------------------+---------------+
