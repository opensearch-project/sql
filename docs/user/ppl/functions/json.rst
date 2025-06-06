====================
JSON Functions
====================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1




JSON Path
---------

Description
>>>>>>>>>>>

All JSON paths used in JSON functions follow the format ``<key1>{<index1>}.<key2>{<index2>}...``.

Each ``<key>`` represents a field name. The ``{<index>}`` part is optional and is only applicable when the corresponding key refers to an array.

For example::

    a{2}.b{0}

This refers to the element at index 0 of the ``b`` array, which is nested inside the element at index 2 of the ``a`` array.

Notes:

1. The ``{<index>}`` notation applies **only when** the associated key points to an array.

2. ``{}`` (without a specific index) is interpreted as a **wildcard**, equivalent to ``{*}``, meaning "all elements" in the array at that level.

JSON
----------

Description
>>>>>>>>>>>

Version: 3.1.0

Limitation: Only works when plugins.calcite.enabled=true

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

Version: 3.1.0

Limitation: Only works when plugins.calcite.enabled=true

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

Version: 3.1.0

Limitation: Only works when plugins.calcite.enabled=true

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

Version: 3.1.0

Limitation: Only works when plugins.calcite.enabled=true

Usage: `json_array_length(value)` parse the string to json array and return size,, null is returned in case of any other valid JSON string, null or an invalid JSON.

Argument type: value: A JSON STRING

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

Version: 3.1.0

Limitation: Only works when plugins.calcite.enabled=true

Usage: `json_extract(json_string, path1, path2, ...)` Extracts values using the specified JSON paths. If only one path is provided, it returns a single value. If multiple paths are provided, it returns a JSON Array in the order of the paths. If one path cannot find value, return null as the result for this path. The path use "{<index>}" to represent index for array, "{}" means "{*}".

Argument type: json_string: STRING, path1: STRING, path2: STRING ...

Return type: STRING

Example::

    > source=json_test | eval extract = json_extract('{"a": [{"b": 1}, {"b": 2}]}', 'a{}.b') | head 1 | fields extract
    fetched rows / total rows = 1/1
    +-------------------------+
    | test_json_array         |
    |-------------------------|
    | [1,2]                   |
    +-------------------------+

     > source=json_test | eval extract = json_extract('{"a": [{"b": 1}, {"b": 2}]}', 'a{}.b', 'a{}') | head 1 | fields extract
    fetched rows / total rows = 1/1
    +---------------------------------+
    | test_json_array                 |
    |---------------------------------|
    | [[1,2],[{"b": 1}, {"b": 2}]]    |
    +---------------------------------+

JSON_DELETE
----------

Description
>>>>>>>>>>>

Version: 3.1.0

Limitation: Only works when plugins.calcite.enabled=true

Usage: `json_delete(json_string, path1, path2, ...)` Delete values using the specified JSON paths. Return the json string after deleting. If one path cannot find value, do nothing.

Argument type: json_string: STRING, path1: STRING, path2: STRING ...

Return type: STRING

Example::

    > source=json_test | eval delete = json_delete('{"a": [{"b": 1}, {"b": 2}]}', 'a{0}.b') | head 1 | fields delete
    fetched rows / total rows = 1/1
    +-------------------------+
    | delete                  |
    |-------------------------|
    | {"a": [{},{"b": 1}]}    |
    +-------------------------+

    > source=json_test | eval delete = json_delete('{"a": [{"b": 1}, {"b": 2}]}', 'a{0}.b', 'a{1}.b') | head 1 | fields delete
    fetched rows / total rows = 1/1
    +-------------------------+
    | delete                  |
    |-------------------------|
    | {"a": []}               |
    +-------------------------+

    > source=json_test | eval delete = json_delete('{"a": [{"b": 1}, {"b": 2}]}', 'a{2}.b') | head 1 | fields delete
    fetched rows / total rows = 1/1
    +------------------------------+
    | delete                       |
    |------------------------------|
    | {"a": [{"b": 1}, {"b": 2}]}  |
    +------------------------------+

JSON_SET
----------

Description
>>>>>>>>>>>

Version: 3.1.0

Limitation: Only works when plugins.calcite.enabled=true

Usage: `json_set(json_string, path1, value1,  path2, value2...)` Set values to corresponding paths using the specified JSON paths. If one path's parent node is not a json object, skip the path. Return the json string after setting.

Argument type: json_string: STRING, path1: STRING, value1: ANY, path2: STRING, value2: ANY ...

Return type: STRING

Example::

    > source=json_test | eval jsonSet = json_set('{"a": [{"b": 1}]}', 'a{0}.b', 3) | head 1 | fields jsonSet
    fetched rows / total rows = 1/1
    +-------------------------+
    | jsonSet                 |
    |-------------------------|
    | {"a": [{"b": 3}]}       |
    +-------------------------+

    > source=json_test | eval jsonSet = json_set('{"a": [{"b": 1}, {"b": 2}]}', 'a{0}.b', 3, 'a{1}.b', 4) | head 1 | fields jsonSet
    fetched rows / total rows = 1/1
    +-----------------------------+
    | jsonSet                     |
    |-----------------------------|
    | {"a": [{"b": 3},{"b": 4}]}  |
    +-----------------------------+

JSON_APPEND
----------

Description
>>>>>>>>>>>

Version: 3.1.0

Limitation: Only works when plugins.calcite.enabled=true

Usage: `json_append(json_string, path1, value1,  path2, value2...)` Append values to corresponding paths using the specified JSON paths. If one path's target node is not an array, skip the path. Return the json string after setting.

Argument type: json_string: STRING, path1: STRING, value1: ANY, path2: STRING, value2: ANY ...

Return type: STRING

Example::

    > source=json_test | eval jsonAppend = json_set('{"a": [{"b": 1}]}', 'a', 3) | head 1 | fields jsonAppend
    fetched rows / total rows = 1/1
    +-------------------------+
    | jsonAppend              |
    |-------------------------|
    | {"a": [{"b": 1}, 3]}    |
    +-------------------------+

    > source=json_test | eval jsonAppend = json_append('{"a": [{"b": 1}, {"b": 2}]}', 'a{0}.b', 3, 'a{1}.b', 4) | head 1 | fields jsonAppend
    fetched rows / total rows = 1/1
    +-------------------------+
    | jsonAppend              |
    |-------------------------|
    | {"a": [{"b": 1}, 3]}    |
    +-------------------------+

     > source=json_test | eval jsonAppend = json_append('{"a": [{"b": 1}]}', 'a', '[1,2]', 'a{1}.b', 4) | head 1 | fields jsonAppend
    fetched rows / total rows = 1/1
    +----------------------------+
    | jsonAppend                 |
    |----------------------------|
    | {"a": [{"b": 1}, "[1,2]"]} |
    +----------------------------+

JSON_EXTEND
----------

Description
>>>>>>>>>>>

Version: 3.1.0

Limitation: Only works when plugins.calcite.enabled=true

Usage: `json_extend(json_string, path1, value1,  path2, value2...)` Extend values to corresponding paths using the specified JSON paths. If one path's target node is not an array, skip the path. The function will try to parse the value as an array. If it can be parsed, extend it to the target array. Otherwise, regard the value a single one. Return the json string after setting.

Argument type: json_string: STRING, path1: STRING, value1: ANY, path2: STRING, value2: ANY ...

Return type: STRING

Example::

    > source=json_test | eval jsonExtend = json_extend('{"a": [{"b": 1}]}', 'a', 3) | head 1 | fields jsonExtend
    fetched rows / total rows = 1/1
    +-------------------------+
    | jsonExtend              |
    |-------------------------|
    | {"a": [{"b": 1}, 3]}    |
    +-------------------------+

    > source=json_test | eval jsonExtend = json_extend('{"a": [{"b": 1}, {"b": 2}]}', 'a{0}.b', 3, 'a{1}.b', 4) | head 1 | fields jsonExtend
    fetched rows / total rows = 1/1
    +-------------------------+
    | jsonExtend              |
    |-------------------------|
    | {"a": [{"b": 1}, 3]}    |
    +-------------------------+

     > source=json_test | eval jsonExtend = json_extend('{"a": [{"b": 1}]}', 'a', '[1,2]') | head 1 | fields jsonExtend
    fetched rows / total rows = 1/1
    +----------------------------+
    | jsonExtend                 |
    |----------------------------|
    | {"a": [{"b": 1},1,2]}      |
    +----------------------------+

JSON_KEYS
----------

Description
>>>>>>>>>>>

Version: 3.1.0

Limitation: Only works when plugins.calcite.enabled=true

Usage: `json_keys(json_string)` Return the key list of the Json object as a Json array. Otherwise, return null.

Argument type: json_string: A JSON STRING

Return type: STRING

Example::

    > source=json_test | eval jsonKeys = json_keys('{"a": 1, "b": 2}') | head 1 | fields jsonKeys
    fetched rows / total rows = 1/1
    +-------------------------+
    | jsonKeys                |
    |-------------------------|
    | ["a","b"]               |
    +-------------------------+

    > source=json_test | eval jsonKeys = json_keys('{"a": {"c": 1}, "b": 2}') | head 1 | fields jsonKeys
    fetched rows / total rows = 1/1
    +-------------------------+
    | jsonKeys                |
    |-------------------------|
    | ["a","b"]               |
    +-------------------------+
