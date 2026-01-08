# spath  

## Description  

The `spath` command extracts fields from structured JSON data. It supports two modes:

1. **Path-based extraction**: Extract specific fields using JSON paths
2. **Field resolution-based extraction**: Extract multiple fields automatically based on downstream field requirements

## Syntax  

### Path-based Extraction
spath input=\<field\> [output=\<field\>] [path=]\<path\>
* input: mandatory. The field to scan for JSON data.  
* output: optional. The destination field that the data will be loaded to. **Default:** value of `path`.  
* path: mandatory. The path of the data to load for the object. For more information on path syntax, see [json_extract](../functions/json.md#json_extract).

### Field Resolution-based Extraction (Experimental)
spath input=\<field\>
* input: mandatory. The field containing JSON data to extract from.
* Extracts fields based on downstream requirements
* **Limitation**: It raises error if extracted fields cannot be identified by following commands (i.e. `fields`, or `stats` command is needed)
* **Limitation**: Cannot use wildcards (`*`) in field selection - only explicit field names are supported
* **Limitation**: All extracted fields are returned as STRING type
* **Limitation**: Filter with query (`where <field> in/exists [...]` ) is not supported after `spath` command

## Note  

The `spath` command currently does not support pushdown behavior for extraction. It will be slow on large datasets. It's generally better to index fields needed for filtering directly instead of using `spath` to filter nested fields.
## Example 1: Simple Field Extraction  

The simplest spath is to extract a single field. This example extracts `n` from the `doc` field of type `text`.
  
```ppl
source=structured
| spath input=doc_n n
| fields doc_n n
```
  
Expected output:
  
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
  
## Example 2: Lists & Nesting  

This example demonstrates more JSON path uses, like traversing nested fields and extracting list elements.
  
```ppl
source=structured
| spath input=doc_list output=first_element list{0}
| spath input=doc_list output=all_elements list{}
| spath input=doc_list output=nested nest_out.nest_in
| fields doc_list first_element all_elements nested
```
  
Expected output:
  
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

This example shows extracting an inner field and doing statistics on it, using the docs from example 1. It also demonstrates that `spath` always returns strings for inner types.
  
```ppl
source=structured
| spath input=doc_n n
| eval n=cast(n as int)
| stats sum(n)
| fields `sum(n)`
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------+
| sum(n) |
|--------|
| 6      |
+--------+
```
  
## Example 4: Escaped paths  

`spath` can escape paths with strings to accept any path that `json_extract` does. This includes escaping complex field names as array components.
  
```ppl
source=structured
| spath output=a input=doc_escape "['a fancy field name']"
| spath output=b input=doc_escape "['a.b.c']"
| fields a b
```
  
Expected output:
  
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

## Example 5: Field Resolution-based Extraction

Extract multiple fields automatically based on downstream requirements. The `spath` command analyzes which fields are needed and extracts only those fields.

```ppl
source=structured
| spath input=doc_multi
| fields doc_multi, a, b
```

Expected output:

```text
fetched rows / total rows = 3/3
+--------------------------------------+----+----+
| doc_multi                            | a  | b  |
|--------------------------------------+----+----|
| {"a": 10, "b": 20, "c": 30, "d": 40} | 10 | 20 |
| {"a": 15, "b": 25, "c": 35, "d": 45} | 15 | 25 |
| {"a": 11, "b": 21, "c": 31, "d": 41} | 11 | 21 |
+--------------------------------------+----+----+
```

This extracts only fields `a` and `b` from the JSON in `doc_multi` field, even though the JSON contains fields `c` and `d` as well. All extracted fields are returned as STRING type.

## Example 6: Field Resolution with Eval

This example shows field resolution with computed fields. The `spath` command extracts only the fields needed by downstream commands.

```ppl
source=structured
| spath input=doc_multi
| eval sum_ab = cast(a as int) + cast(b as int)
| fields doc_multi, a, b, sum_ab
```

Expected output:

```text
fetched rows / total rows = 3/3
+--------------------------------------+----+----+--------+
| doc_multi                            | a  | b  | sum_ab |
|--------------------------------------+----+----+--------|
| {"a": 10, "b": 20, "c": 30, "d": 40} | 10 | 20 | 30     |
| {"a": 15, "b": 25, "c": 35, "d": 45} | 15 | 25 | 40     |
| {"a": 11, "b": 21, "c": 31, "d": 41} | 11 | 21 | 32     |
+--------------------------------------+----+----+--------+
```

The `spath` command extracts only fields `a` and `b` (needed by the `eval` command), which are then cast to integers and summed. Fields `c` and `d` are not extracted since they're not needed.

## Example 7: Field Resolution with Stats

This example demonstrates field resolution with aggregation. The `spath` command extracts only the fields needed for grouping and aggregation.

```ppl
source=structured
| spath input=doc_multi
| stats avg(cast(a as int)) as avg_a, sum(cast(b as int)) as sum_b by c
```

Expected output:

```text
fetched rows / total rows = 3/3
+-------+-------+----+
| avg_a | sum_b | c  |
|-------+-------+----|
| 10.0  | 20    | 30 |
| 11.0  | 21    | 31 |
| 15.0  | 25    | 35 |
+-------+-------+----+
```

The `spath` command extracts fields `a`, `b`, and `c` (needed by the `stats` command for aggregation and grouping). Field `d` is not extracted since it's not used.

## Example 8: Field Resolution Limitations

**Important**: It raises error if extracted fields cannot be identified by following commands

```ppl
source=structured
| spath input=doc_multi
| eval x = a * b  # ERROR: Requires field selection (fields or stats command)
```

**Important**: Wildcards are not supported in field resolution mode:

```ppl
source=structured
| spath input=doc_multi
| fields a, b*  # ERROR: Spath command cannot extract arbitrary fields
```
