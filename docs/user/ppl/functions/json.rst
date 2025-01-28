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
    fetched rows / total rows = 7/7
    +---------------------+-------------------------------------+----------+
    | test_name           | json_string                         | is_valid |
    |---------------------|-------------------------------------|----------|
    | json nested object  | {"a":"1","b":{"c":"2","d":"3"}}     | True     |
    | json nested list    | {"a":"1","b":[{"c":"2"},{"c":"3"}]} | True     |
    | json object         | {"a":"1","b":"2"}                   | True     |
    | json array          | [1, 2, 3, 4]                        | True     |
    | json scalar string  | "abc"                               | True     |
    | json empty string   |                                     | True     |
    | json invalid object | {"invalid":"json", "string"}        | False    |
    +---------------------+-------------------------------------+----------+

JSON
----------

Description
>>>>>>>>>>>

Usage: `json(value)` Evaluates whether a string can be parsed as a json-encoded string and casted as an expression. Returns the JSON value if valid, null otherwise.

Argument type: STRING

Return type: BOOLEAN/DOUBLE/INTEGER/NULL/STRUCT/ARRAY

Example::

    > source=json_test | where json_valid(json_string) | eval json=json(json_string) | fields test_name, json_string, json
    fetched rows / total rows = 6/6
    +---------------------+-------------------------------------+-----------------------------+
    | test_name           | json_string                         | json                        |
    |---------------------|-------------------------------------|-----------------------------|
    | json nested object  | {"a":"1","b":{"c":"2","d":"3"}}     | {a:"1",b:{c:"2",d:"3"}      |
    | json nested list    | {"a":"1","b":[{"c":"2"},{"c":"3"}]} | {a:"1",b:[{c:"2"},{c:"3"}]} |
    | json object         | {"a":"1","b":"2"}                   | {a:"1",b:"2"}               |
    | json array          | [1, 2, 3, 4]                        | [1,2,3,4]                   |
    | json scalar string  | "abc"                               | "abc"                       |
    | json empty string   |                                     | null                        |
    +---------------------+-------------------------------------+-----------------------------+

JSON_EXTRACT
____________

Description
>>>>>>>>>>>

Usage: `json_extract(doc, path)` Extracts a json value or scalar from a json document based on the path specified.

Argument type: STRING, STRING

Return type: BOOLEAN/DOUBLE/INTEGER/NULL/STRUCT/ARRAY

- Returns a JSON array for multiple paths or if the path leads to an array.
- Return null if path is not valid.
- Throws error if `doc` or `path` is malformed.
- Throws error if `doc` or `path` is MISSING or NULL.

Example::

    > source=json_test | where json_valid(json_string) | eval json_extract=json_extract(json_string, '$.b') | fields test_name, json_string, json_extract
    fetched rows / total rows = 6/6
    +---------------------+-------------------------------------+-------------------+
    | test_name           | json_string                         | json_extract      |
    |---------------------|-------------------------------------|-------------------|
    | json nested object  | {"a":"1","b":{"c":"2","d":"3"}}     | {c:"2",d:"3"}     |
    | json nested list    | {"a":"1","b":[{"c":"2"},{"c":"3"}]} | [{c:"2"},{c:"3"}] |
    | json object         | {"a":"1","b":"2"}                   | 2                 |
    | json array          | [1, 2, 3, 4]                        | null              |
    | json scalar string  | "abc"                               | null              |
    | json empty string   |                                     | null              |
    +---------------------+-------------------------------------+-------------------+

    > source=json_test | where test_name="json nested list" | eval json_extract=json_extract('{"a":[{"b":1},{"b":2}]}', '$.b[1].c')
    fetched rows / total rows = 1/1
    +---------------------+-------------------------------------+--------------+
    | test_name           | json_string                         | json_extract |
    |---------------------|-------------------------------------|--------------|
    | json nested list    | {"a":"1","b":[{"c":"2"},{"c":"3"}]} | 3            |
    +---------------------+-------------------------------------+--------------+

    > source=json_test | where test_name="json nested list" | eval json_extract=json_extract('{"a":[{"b":1},{"b":2}]}', '$.b[*].c')
    fetched rows / total rows = 1/1
    +---------------------+-------------------------------------+--------------+
    | test_name           | json_string                         | json_extract |
    |---------------------|-------------------------------------|--------------|
    | json nested list    | {"a":"1","b":[{"c":"2"},{"c":"3"}]} | [2,3]        |
    +---------------------+-------------------------------------+--------------+
