# JSON functions

PPL supports the following JSON functions for creating, parsing, and manipulating JSON data.

## JSON path 

All JSON paths used in JSON functions follow the format `<key1>{<index1>}.<key2>{<index2>}...`.
Each `<key>` represents a field name. The `{<index>}` part is optional and is used only when the corresponding key refers to an array.
For example:
  
```bash
a{2}.b{0}
```
  
This path accesses the element at index `0` in the `b` array, which is located within the element at index `2` of the `a` array.

**Notes**:
1. The `{<index>}` notation applies only when the associated key points to an array.
2. `{}` (without a specific index) is interpreted as a wildcard, equivalent to `{*}`, meaning `all elements` in the array at that level.
  
## JSON  

**Usage**: `JSON(value)`

Validates and parses a JSON string. Returns the parsed JSON value if the string is valid JSON, or `NULL` if invalid.

**Parameters**:

- `value` (Required): The string to validate and parse as JSON.

**Return type**: `STRING`

#### Example
  
```ppl
source=json_test
| where json_valid(json_string)
| eval json=json(json_string)
| fields test_name, json_string, json
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+--------------------+---------------------------------+---------------------------------+
| test_name          | json_string                     | json                            |
|--------------------+---------------------------------+---------------------------------|
| json nested object | {"a":"1","b":{"c":"2","d":"3"}} | {"a":"1","b":{"c":"2","d":"3"}} |
| json object        | {"a":"1","b":"2"}               | {"a":"1","b":"2"}               |
| json array         | [1, 2, 3, 4]                    | [1, 2, 3, 4]                    |
| json scalar string | "abc"                           | "abc"                           |
+--------------------+---------------------------------+---------------------------------+
```
  
## JSON_VALID

**Usage**: `JSON_VALID(value)`

Evaluates whether a string uses valid JSON syntax. Returns `TRUE` if valid, `FALSE` if invalid. `NULL` input returns `NULL`.

**Version**: 3.1.0
**Limitation**: Only works when `plugins.calcite.enabled=true`

**Parameters**:

- `value` (Required): The string to validate as JSON.

**Return type**: `BOOLEAN`

#### Example

```ppl
source=people
| eval is_valid_json = json_valid('[1,2,3,4]'), is_invalid_json = json_valid('{invalid}')
| fields is_valid_json, is_invalid_json
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------+-----------------+
| is_valid_json | is_invalid_json |
|---------------+-----------------|
| True          | False           |
+---------------+-----------------+
```
  
## JSON_OBJECT

**Usage**: `JSON_OBJECT(key1, value1, key2, value2, ...)`

Creates a JSON object string from the specified key-value pairs. All keys must be strings.

**Parameters**:

- `key1`, `value1` (Required): The first key-value pair. The key must be a string.
- `key2`, `value2`, `...` (Optional): Additional key-value pairs.

**Return type**: `STRING`

#### Example

```ppl
source=json_test
| eval test_json = json_object('key', 123.45)
| head 1
| fields test_json
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------+
| test_json      |
|----------------|
| {"key":123.45} |
+----------------+
```
  
## JSON_ARRAY

**Usage**: `JSON_ARRAY(element1, element2, ...)`

Creates a JSON array string from the specified elements.

**Parameters**:

- `element1`, `element2`, `...` (Optional): The elements to include in the array. Can be any data type.

**Return type**: `STRING`

#### Example

```ppl
source=json_test
| eval test_json_array = json_array('key', 123.45)
| head 1
| fields test_json_array
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------+
| test_json_array |
|-----------------|
| ["key",123.45]  |
+-----------------+
```
  
## JSON_ARRAY_LENGTH

**Usage**: `JSON_ARRAY_LENGTH(value)`

Returns the number of elements in a JSON array. Returns `NULL` if the input is not a valid JSON array, is `NULL`, or contains invalid JSON.

**Parameters**:

- `value` (Required): A string containing a JSON array.

**Return type**: `INTEGER`

#### Examples

The following example returns the length of a valid JSON array:

```ppl
source=json_test
| eval array_length = json_array_length("[1,2,3]")
| head 1
| fields array_length
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------+
| array_length |
|--------------|
| 3            |
+--------------+
```

The following example returns `NULL` for non-array JSON values:
  
```ppl
source=json_test
| eval array_length = json_array_length("{\"1\": 2}")
| head 1
| fields array_length
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------+
| array_length |
|--------------|
| null         |
+--------------+
```
  
## JSON_EXTRACT

**Usage**: `JSON_EXTRACT(json_string, path1, path2, ...)`

Extracts values from a JSON string using the specified JSON paths.

**Behavior**:
- **Single path**: Returns the extracted value directly.
- **Multiple paths**: Returns a JSON array containing the extracted values in path order.
- **Invalid path**: Returns `NULL` for that path in the result.

For path syntax details, see the [JSON path](#json-path) section.

**Parameters**:

- `json_string` (Required): The JSON string to extract values from.
- `path1`, `path2`, `...` (Required): One or more JSON paths specifying which values to extract.

**Return type**: `STRING`

#### Examples

The following example extracts values using a single JSON path:

```ppl
source=json_test
| eval extract = json_extract('{"a": [{"b": 1}, {"b": 2}]}', 'a{}.b')
| head 1
| fields extract
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------+
| extract |
|---------|
| [1,2]   |
+---------+
```

The following example extracts values using multiple JSON paths:
  
```ppl
source=json_test
| eval extract = json_extract('{"a": [{"b": 1}, {"b": 2}]}', 'a{}.b', 'a{}')
| head 1
| fields extract
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------------------+
| extract                   |
|---------------------------|
| [[1,2],[{"b":1},{"b":2}]] |
+---------------------------+
```
  
## JSON_DELETE

**Usage**: `JSON_DELETE(json_string, path1, path2, ...)`

Deletes values from a JSON string at the specified JSON paths. Returns the modified JSON string. If a path cannot find a value, no changes are made for that path.

**Parameters**:

- `json_string` (Required): The JSON string to delete values from.
- `path1`, `path2`, `...` (Required): One or more JSON paths specifying which values to delete.

**Return type**: `STRING`

#### Examples

The following example deletes a value using a single JSON path:

```ppl
source=json_test
| eval delete = json_delete('{"a": [{"b": 1}, {"b": 2}]}', 'a{0}.b')
| head 1
| fields delete
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------------+
| delete             |
|--------------------|
| {"a":[{},{"b":2}]} |
+--------------------+
```

The following example deletes values using multiple JSON paths:
  
```ppl
source=json_test
| eval delete = json_delete('{"a": [{"b": 1}, {"b": 2}]}', 'a{0}.b', 'a{1}.b')
| head 1
| fields delete
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------------+
| delete        |
|---------------|
| {"a":[{},{}]} |
+---------------+
```

The following example shows no changes occur when trying to delete a non-existent path:
  
```ppl
source=json_test
| eval delete = json_delete('{"a": [{"b": 1}, {"b": 2}]}', 'a{2}.b')
| head 1
| fields delete
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------------+
| delete                  |
|-------------------------|
| {"a":[{"b":1},{"b":2}]} |
+-------------------------+
```
  
## JSON_SET

**Usage**: `JSON_SET(json_string, path1, value1, path2, value2, ...)`

Sets values in a JSON string at the specified JSON paths. Returns the modified JSON string. If a path's parent node is not a JSON object, that path is skipped.

**Parameters**:

- `json_string` (Required): The JSON string to modify.
- `path1`, `value1` (Required): The first path-value pair to set.
- `path2`, `value2`, `...` (Optional): Additional path-value pairs.

**Return type**: `STRING`

#### Examples

The following example sets a single value at a JSON path:

```ppl
source=json_test
| eval jsonSet = json_set('{"a": [{"b": 1}]}', 'a{0}.b', 3)
| head 1
| fields jsonSet
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------------+
| jsonSet         |
|-----------------|
| {"a":[{"b":3}]} |
+-----------------+
```


  
```ppl
source=json_test
| eval jsonSet = json_set('{"a": [{"b": 1}, {"b": 2}]}', 'a{0}.b', 3, 'a{1}.b', 4)
| head 1
| fields jsonSet
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------------+
| jsonSet                 |
|-------------------------|
| {"a":[{"b":3},{"b":4}]} |
+-------------------------+
```
  
## JSON_APPEND

**Usage**: `JSON_APPEND(json_string, path1, value1, path2, value2, ...)`

Appends values to arrays in a JSON string at the specified JSON paths. Returns the modified JSON string. If a path's target node is not an array, that path is skipped.

**Parameters**:

- `json_string` (Required): The JSON string to modify.
- `path1`, `value1` (Required): The first path-value pair to append.
- `path2`, `value2`, `...` (Optional): Additional path-value pairs.

**Return type**: `STRING`

#### Examples

The following example appends a value to an array:

```ppl
source=json_test
| eval jsonAppend = json_append('{"a": [{"b": 1}]}', 'a', 3)
| head 1
| fields jsonAppend
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------------+
| jsonAppend           |
|----------------------|
| {"a":[{"b":1},3]}    |
+----------------------+
```

The following example shows paths to non-array targets are skipped:
  
```ppl
source=json_test
| eval jsonAppend = json_append('{"a": [{"b": 1}, {"b": 2}]}', 'a{0}.b', 3, 'a{1}.b', 4)
| head 1
| fields jsonAppend
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------------+
| jsonAppend              |
|-------------------------|
| {"a":[{"b":1},{"b":2}]} |
+-------------------------+
```

The following example appends values using mixed path types:
  
```ppl
source=json_test
| eval jsonAppend = json_append('{"a": [{"b": 1}]}', 'a', '[1,2]', 'a{1}.b', 4)
| head 1
| fields jsonAppend
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------------+
| jsonAppend              |
|-------------------------|
| {"a":[{"b":1},"[1,2]"]} |
+-------------------------+
```
  
## JSON_EXTEND

**Usage**: `JSON_EXTEND(json_string, path1, value1, path2, value2, ...)`

Extends arrays in a JSON string at the specified JSON paths with new values. Returns the modified JSON string. If a path's target node is not an array, that path is skipped.

The function attempts to parse each value as an array:
- If parsing succeeds: The parsed array elements are added to the target array.
- If parsing fails: The value is treated as a single element and added to the target array.

**Parameters**:

- `json_string` (Required): The JSON string to modify.
- `path1`, `value1` (Required): The first path-value pair to extend.
- `path2`, `value2`, `...` (Optional): Additional path-value pairs.

**Return type**: `STRING`

#### Examples

The following example extends an array with a single value:

```ppl
source=json_test
| eval jsonExtend = json_extend('{"a": [{"b": 1}]}', 'a', 3)
| head 1
| fields jsonExtend
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------+
| jsonExtend        |
|-------------------|
| {"a":[{"b":1},3]} |
+-------------------+
```

The following example shows paths to non-array targets are skipped:
  
```ppl
source=json_test
| eval jsonExtend = json_extend('{"a": [{"b": 1}, {"b": 2}]}', 'a{0}.b', 3, 'a{1}.b', 4)
| head 1
| fields jsonExtend
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------------+
| jsonExtend              |
|-------------------------|
| {"a":[{"b":1},{"b":2}]} |
+-------------------------+
```

The following example extends an array by parsing the value as an array:
  
```ppl
source=json_test
| eval jsonExtend = json_extend('{"a": [{"b": 1}]}', 'a', '[1,2]')
| head 1
| fields jsonExtend
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------------------+
| jsonExtend              |
|-------------------------|
| {"a":[{"b":1},1.0,2.0]} |
+-------------------------+
```
  
## JSON_KEYS

**Usage**: `JSON_KEYS(json_string)`

Returns the keys of a JSON object as a JSON array. Returns `NULL` if the input is not a valid JSON object.

**Parameters**:

- `json_string` (Required): A string containing a JSON object.

**Return type**: `STRING`

#### Examples

The following example gets keys from a simple JSON object:

```ppl
source=json_test
| eval jsonKeys = json_keys('{"a": 1, "b": 2}')
| head 1
| fields jsonKeys
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------+
| jsonKeys  |
|-----------|
| ["a","b"] |
+-----------+
```

The following example gets keys from a nested JSON object:
  
```ppl
source=json_test
| eval jsonKeys = json_keys('{"a": {"c": 1}, "b": 2}')
| head 1
| fields jsonKeys
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------+
| jsonKeys  |
|-----------|
| ["a","b"] |
+-----------+
```
  