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

Usage: `json(value)` Evaluates whether a string can be parsed as a json-encoded string. Returns the value if valid, null otherwise.

Argument type: STRING

Return type: STRING

Example::

    > source=json_test | where json_valid(json_string) | eval json=json(json_string) | fields test_name, json_string, json
    fetched rows / total rows = 5/5
    +---------------------+---------------------------------+-------------------------+
    | test_name           | json_string                     | json                    |
    |---------------------|---------------------------------|-------------------------|
    | json nested object  | {"a":"1","b":{"c":"2","d":"3"}} | {a:"1",b:{c:"2",d:"3"}} |
    | json object         | {"a":"1","b":"2"}               | {a:"1",b:"2"}           |
    | json array          | [1, 2, 3, 4]                    | [1,2,3,4]               |
    | json scalar string  | "abc"                           | "abc"                   |
    | json empty string   |                                 | null                    |
    +---------------------+---------------------------------+-------------------------+

JSON_OBJECT
----------

Description
>>>>>>>>>>>

Usage: `json_object(key1, value1, key2, value2...)` create a json object string with key value pairs. The key must be string.

Argument type: key1: STRING, value1: ANY, key2: STRING, value2: ANY ...

Return type: STRING

Example::

    > source=json_test | eval test_json = json_object('key', 123.45) | head 1 | fields test_json
    fetched rows / total rows = 1/1
    +-------------------------+
    | test_json               |
    |-------------------------|
    | {"key":123.45}          |
    +-------------------------+

JSON_ARRAY
----------

Description
>>>>>>>>>>>

Usage: `json_array(element1, element2, ...)` create a json array string with elements.

Argument type: element1: ANY, element2: ANY ...

Return type: STRING

Example::

    > source=json_test | eval test_json_array = json_array('key', 123.45) | head 1 | fields test_json_array
    fetched rows / total rows = 1/1
    +-------------------------+
    | test_json_array         |
    |-------------------------|
    | ["key",123.45]          |
    +-------------------------+

JSON_ARRAY_LENGTH
----------

Description
>>>>>>>>>>>

Usage: `json_array_length(value)` parse the string to json array and return size, if can't be parsed, return null

Argument type: value: STRING

Return type: INTEGER

Example::

    > source=json_test | eval array_length = json_array_length("[1,2,3]") | head 1 | fields array_length
    fetched rows / total rows = 1/1
    +-------------------------+
    | array_length            |
    |-------------------------|
    | 3                       |
    +-------------------------+

    > source=json_test | eval array_length = json_array_length("{\"1\": 2}") | head 1 | fields array_length
    fetched rows / total rows = 1/1
    +-------------------------+
    | array_length            |
    |-------------------------|
    | null                    |
    +-------------------------+

JSON_EXTRACT
----------

Description
>>>>>>>>>>>

Usage: `json_extract(json_string, path1, path2, ...)` it first transfer json_string to json, then extract value using paths. If only one path, return the value, otherwise, return the list of values. If one path cannot find value, return null as the result for this path. The path use "{<index>}" to represent index for array, "{}" means "{*}".

Argument type: json_string: STRING, path1: STRING, path2: STRING ...

Return type: STRING

Example::

    > source=json_test | eval extract = json_extract('{\"a\": [{\"b\": 1}, {\"b\": 2}]}', 'a{}.b') | head 1 | fields extract
    fetched rows / total rows = 1/1
    +-------------------------+
    | test_json_array         |
    |-------------------------|
    | [1,2]                   |
    +-------------------------+

     > source=json_test | eval extract = json_extract('{\"a\": [{\"b\": 1}, {\"b\": 2}]}', 'a{}.b', 'a{}'') | head 1 | fields extract
    fetched rows / total rows = 1/1
    +---------------------------------+
    | test_json_array                 |
    |---------------------------------|
    | [[1,2],[{\"b\": 1}, {\"b\": 2}]]|
    +---------------------------------+