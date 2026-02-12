
# spath

The `spath` command extracts fields from structured JSON data. It operates in two modes:

- **Path-based mode**: When `path` is specified, extracts a single value at the given JSON path.
- **Auto-extract mode**: When `path` is omitted, extracts all fields from the JSON into a map.

> **Note**: The `spath` command is not executed on OpenSearch data nodes. It extracts fields from data after it has been returned to the coordinator node, which is slow on large datasets. We recommend indexing fields needed for filtering directly instead of using `spath` to filter nested fields.

## Syntax

The `spath` command has the following syntax:

```syntax
spath input=<field> [output=<field>] [path=]<path>
```

## Parameters

The `spath` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `input` | Required | The field containing JSON data to parse. |
| `output` | Optional | The destination field in which the extracted data is stored. Default is the value of `path` in path-based mode, or the value of `input` in auto-extract mode. |
| `path` | Optional | The JSON path that identifies the data to extract. When omitted, all fields are extracted into a map (auto-extract mode). |  

For more information about path syntax, see [json_extract](../functions/json.md#json_extract).

## Auto-extract mode

When `path` is omitted, the `spath` command runs in auto-extract mode. Instead of extracting a single value, it flattens the entire JSON into a `map<string, string>` column using the following rules:

- Nested objects use dotted keys: `user.name`, `user.age`
- Arrays use `{}` suffix: `tags{}`, `users{}.name`
- Duplicate logical keys merge into arrays: `c{}.b = [2, 3]`
- Null values are preserved: a JSON `null` becomes the string `"null"` in the map
- All values are stringified: numbers and booleans are converted to their string representation (for example, `30` becomes `"30"`, `true` becomes `"true"`, and arrays become `"[a, b, c]"`)

> **Note**: Auto-extract mode processes the entire input field with no character limit. For large JSON payloads, consider using path-based extraction to target specific fields.

### Corner cases

- Invalid or malformed JSON returns partial results containing any fields successfully parsed before the error.
- Empty JSON object (`{}`) returns an empty map.

## Example 1: Basic field extraction

The basic use of `spath` extracts a single field from JSON data. The following query extracts the `n` field from JSON objects in the `doc_n` field:
  
```ppl
source=structured
| spath input=doc_n n
| fields doc_n n
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+----------+---+
| doc_n    | n |
|----------+---|
| {"n": 1} | 1 |
| {"n": 2} | 2 |
| {"n": 3} | 3 |
+----------+---+
```
  

## Example 2: Lists and nesting  

The following query shows how to traverse nested fields and extract list elements:
  
```ppl
source=structured
| spath input=doc_list output=first_element list{0}
| spath input=doc_list output=all_elements list{}
| spath input=doc_list output=nested nest_out.nest_in
| fields doc_list first_element all_elements nested
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+------------------------------------------------------+---------------+--------------+--------+
| doc_list                                             | first_element | all_elements | nested |
|------------------------------------------------------+---------------+--------------+--------|
| {"list": [1, 2, 3, 4], "nest_out": {"nest_in": "a"}} | 1             | [1,2,3,4]    | a      |
| {"list": [], "nest_out": {"nest_in": "a"}}           | null          | []           | a      |
| {"list": [5, 6], "nest_out": {"nest_in": "a"}}       | 5             | [5,6]        | a      |
+------------------------------------------------------+---------------+--------------+--------+
```
  

## Example 3: Sum of inner elements  

The following query shows how to use `spath` to extract the `n` field from JSON data and calculate the sum of all extracted values: 
  
```ppl
source=structured
| spath input=doc_n n
| eval n=cast(n as int)
| stats sum(n)
| fields `sum(n)`
```
  
The query returns the following results. The `spath` command always returns inner values as strings:
  
```text
fetched rows / total rows = 1/1
+--------+
| sum(n) |
|--------|
| 6      |
+--------+
```
  

## Example 4: Escaped paths  

Use quoted string syntax to access JSON field names that contain spaces, dots, or other special characters:
  
```ppl
source=structured
| spath output=a input=doc_escape "['a fancy field name']"
| spath output=b input=doc_escape "['a.b.c']"
| fields a b
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-------+---+
| a     | b |
|-------+---|
| true  | 0 |
| true  | 1 |
| false | 2 |
+-------+---+
```
  

## Example 5: Auto-extract mode  

When `path` is omitted, `spath` extracts all fields from the JSON into a map. All values are stringified, and null values are preserved:
  
```ppl
source=structured
| spath input=doc_auto output=result
| fields doc_auto result
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+---------------------------------------------------------------------------------+------------------------------------------------------------------------------------+
| doc_auto                                                                        | result                                                                             |
|---------------------------------------------------------------------------------+------------------------------------------------------------------------------------|
| {"user":{"name":"John","age":30},"tags":["java","sql"],"active":true}           | {'user.age': '30', 'tags{}': '[java, sql]', 'user.name': 'John', 'active': 'true'} |
| {"user":{"name":"Jane","age":25},"tags":["python"],"active":null}               | {'user.age': '25', 'tags{}': 'python', 'user.name': 'Jane', 'active': 'null'}      |
| {"user":{"name":"Bob","age":35},"tags":["go","rust","sql"],"user.name":"Bobby"} | {'user.age': '35', 'tags{}': '[go, rust, sql]', 'user.name': '[Bob, Bobby]'}       |
+---------------------------------------------------------------------------------+------------------------------------------------------------------------------------+
```
  
The flattening rules demonstrated in this example:

- Nested objects use dotted keys: `user.name` and `user.age` are extracted from `{"user": {"name": "John", "age": 30}}`
- Arrays use `{}` suffix: `tags{}` is extracted from `{"tags": ["java", "sql"]}`
- Duplicate logical keys merge into arrays: in the third row, both `"user": {"name": "Bob"}` (nested) and `"user.name": "Bobby"` (direct dotted key) resolve to the same key `user.name`, so their values merge into `'[Bob, Bobby]'`
- All values are strings: numeric `30` becomes `'30'`, boolean `true` becomes `'true'`, and arrays become strings like `'[java, sql]'`
- Null values are preserved: in the second row, `"active": null` is kept as `'active': 'null'` in the map
  
