
# spath

The `spath` command extracts fields from structured text data by allowing you to select JSON values using JSON paths.

The `spath` command is not executed on OpenSearch data nodes. It extracts fields from data after it has been returned to the coordinator node, which is slow on large datasets. We recommend indexing fields needed for filtering directly instead of using `spath` to filter nested fields.
{: .note}

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
| `output` | Optional | The destination field in which the extracted data is stored. Default is the value of `<path>`. |
| `<path>` | Required | The JSON path that identifies the data to extract. |  

For more information about path syntax, see [json_extract](../functions/json.md#json_extract).

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
  
