# spath

The `spath` command extracts fields from structured JSON data. It supports two modes:

1. **Path-based extraction**: Extract specific fields using JSON paths
2. **Field resolution-based extraction**: Extract multiple fields automatically based on downstream field requirements

> **Note**: The `spath` command is not executed on OpenSearch data nodes. It extracts fields from data after it has been returned to the coordinator node, which is slow on large datasets. We recommend indexing fields needed for filtering directly instead of using `spath` to filter nested fields.

## Syntax

### Path-based Extraction

```syntax
spath input=<field> [output=<field>] [path=]<path>
```

### Auto Extraction (Experimental)

```syntax
spath input=<field>
```

## Parameters

The `spath` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `input` | Required | The field containing JSON data to parse. |
| `output` | Optional | The destination field in which the extracted data is stored. Default is the value of `<path>`. Only used in path-based extraction. |
| `<path>` | Required for path-based extraction | The JSON path that identifies the data to extract. |  

For more information about path syntax, see [json_extract](../functions/json.md#json_extract).

### Auto Extraction Notes

* Automatically extracts all the fields from input field. If a field with the same name already exists, extracted values will be appended to existing field value.
* **Limitation**: When `fields` command used together with `spath` command: partial wildcard (e.g. `prefix*`, `*suffix`) is not supported; and full wildcard (`*`) needs to be placed at the end of the fields list.
* **Limitation**: All extracted fields are returned as STRING type.
* **Limitation**: Field order in the result could be inconsistent with query without `spath` command, and the behavior might change in the future version.
* **Limitation**: Filter with subquery (`where <field> in/exists [...]`) is not supported with `spath` command.
* **Limitation**: `fillnull` command requires to specify fields when used with `spath` command.
* **Limitation**: Following commands cannot be used together with `spath` command: `lookup`.
* **Performance**: Filter records before `spath` command for best performance (see Example 8)

* **Internal Implementation**: The auto extraction feature uses an internal `_MAP` system column to store dynamic fields during query processing. This column is automatically expanded into individual columns in the final results and users don't need to reference it directly. For more information, see [System Columns](../general/identifiers.md#system-columns).

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

## Example 5: Full Extraction

Extract multiple fields automatically. In case field already exists, extracted values are appended to the existing value.

```ppl
source=structured
| eval c = 1
| spath input=doc_multi
| fields doc_multi, a, b, c
```

Expected output:

```text
fetched rows / total rows = 3/3
+--------------------------------------+----+----+---------+
| doc_multi                            | a  | b  | c       |
|--------------------------------------+----+----+---------|
| {"a": 10, "b": 20, "c": 30, "d": 40} | 10 | 20 | [1, 30] |
| {"a": 15, "b": 25, "c": 35, "d": 45} | 15 | 25 | [1, 35] |
| {"a": 11, "b": 21, "c": 31, "d": 41} | 11 | 21 | [1, 31] |
+--------------------------------------+----+----+---------+
```

All extracted fields are returned as STRING type. As shown with field `c` in the example, the extracted value is appended to organize an array if an extracted field already exists.

## Example 6: Field Merge with Dotted Names

When a JSON document contains both a direct field with a dotted name and a nested object path that resolves to the same field name, `spath` merges both values into an array.

```ppl
source=structured
| spath input=doc_dotted
| fields doc_dotted, a.b
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------------------------+--------+
| doc_dotted                | a.b    |
|---------------------------+--------|
| {"a.b": 1, "a": {"b": 2}} | [1, 2] |
+---------------------------+--------+
```

In this example, the JSON contains both `"a.b": 1` (direct field with dot) and `"a": {"b": 2}` (nested path). The `spath` command extracts both values and merges them into the array `[1, 2]`.

## Example 7: Auto Extraction Limitations

**Important**: It raises error if partial wildcard is used.

```ppl
source=structured
| spath input=doc_multi
| fields a, prefix*, b   # ERROR
```

**Important**: It raises error if wildcard is used in the middle of field list.

```ppl
source=structured
| fields doc_multi
| head 1
| spath input=doc_multi
| fields doc_multi, *, b  # ERROR
```

It works when wildcard is placed at the end of field list.

```ppl
source=structured
| fields doc_multi
| head 1
| spath input=doc_multi
| fields doc_multi, b, *
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------------------------------------+----+----+----+----+
| doc_multi                            | b  | a  | c  | d  |
|--------------------------------------+----+----+----+----|
| {"a": 10, "b": 20, "c": 30, "d": 40} | 20 | 10 | 30 | 40 |
+--------------------------------------+----+----+----+----+
```

## Example 8: Performance Considerations

**Important**: The `spath` command processes data on the coordinator node after retrieval from data nodes. Commands placed after `spath` cannot utilize OpenSearch index capabilities, which significantly impacts performance on large datasets.

### Best Practice: Filter Before spath

Always place filter conditions (`where` clauses) **before** the `spath` command to leverage index pushdown:

```ppl
# Slow - filters after spath
source=structured
| spath input=doc_multi
| where id=1
| fields id, a

# Fast - filters before spath
source=structured
| where id=1
| spath input=doc_multi
| fields id, a
```

### Performance Impact

- **After spath**: All data is retrieved from OpenSearch, then filtered in memory on the coordinator
- **Before spath**: Only matching documents are retrieved from OpenSearch utilizing index

### Recommendations

1. **Index nested fields directly** when possible instead of using `spath` for filtering
2. **Place all filters before `spath`** to maximize index utilization
3. **Limit result sets** with filters before applying `spath` on large datasets
