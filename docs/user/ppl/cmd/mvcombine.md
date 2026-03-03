# mvcombine

## Description

The `mvcombine` command groups rows that are identical across all fields except a specified target field, and combines the values of that target field into a multivalue (array) field. All other fields in the input rows are preserved as group keys in the output.

`mvcombine` is a transforming command: it consumes a set of input results and produces a new result set with reduced cardinality.

### Key behaviors

- Rows are grouped by **all fields currently in the pipeline except the target field**.
- One output row is produced per group.
- The target field is **replaced** with a multivalue (array) field that contains all non-null values of the target field from the grouped rows.
- Rows where the target field is missing or null do **not** contribute a value to the combined multivalue output.
- The default output is a multivalue representation (array).

---

## Syntax

mvcombine <field>

### Arguments

- **field** (required)  
  The name of the field whose values are combined into a multivalue field.

---

## Example 1: Basic mvcombine

Given the following input rows:

```text
{"ip":"10.0.0.1","bytes":100,"tags":"t1","packets_str":"10"}
{"ip":"10.0.0.1","bytes":100,"tags":"t1","packets_str":"20"}
{"ip":"10.0.0.1","bytes":100,"tags":"t1","packets_str":"30"}
```
The following query collapses the three rows into a single row, and combines packets_str into a multivalue field:

```ppl
source=mvcombine_data
| where ip='10.0.0.1' and bytes=100 and tags='t1'
| fields ip, bytes, tags, packets_str
| mvcombine packets_str
```

Expected output:
```text
fetched rows / total rows = 1/1
+----------+-------+------+-------------+
| ip       | bytes | tags | packets_str |
|----------+-------+------+-------------|
| 10.0.0.1 | 100   | t1   | [10,20,30]  |
+----------+-------+------+-------------+
```

## Example 2: Multiple groups

Given a dataset mvcombine with the following data:
```text
{"ip":"10.0.0.7","bytes":700,"tags":"t7","packets_str":"1"}
{"ip":"10.0.0.7","bytes":700,"tags":"t7","packets_str":"2"}
{"ip":"10.0.0.8","bytes":700,"tags":"t7","packets_str":"9"}
```

The following query produces one output row per group key:
```ppl
source=mvcombine_data
| where bytes=700 and tags='t7'
| fields ip, bytes, tags, packets_str
| sort ip, packets_str
| mvcombine packets_str
| sort ip
```

Expected output:
```text
fetched rows / total rows = 2/2
+----------+-------+------+-------------+
| ip       | bytes | tags | packets_str |
|----------+-------+------+-------------|
| 10.0.0.7 | 700   | t7   | [1,2]       |
| 10.0.0.8 | 700   | t7   | [9]         |
+----------+-------+------+-------------+
```

## Example 3: Missing target field in some rows

Rows missing the target field do not contribute a value to the combined output.

Given a dataset mvcombine with the following data:
```text
{"ip":"10.0.0.3","bytes":300,"tags":"t3","packets_str":"5"}
{"ip":"10.0.0.3","bytes":300,"tags":"t3"}
{"ip":"10.0.0.3","bytes":300,"tags":"t3","letters":"a"}
```

The following query collapses the group and preserves the non-missing value:
```ppl
source=mvcombine_data
| where ip='10.0.0.3' and bytes=300 and tags='t3'
| fields ip, bytes, tags, packets_str
| mvcombine packets_str
```

Expected output:
```text
fetched rows / total rows = 1/1
+----------+-------+------+-------------+
| ip       | bytes | tags | packets_str |
|----------+-------+------+-------------|
| 10.0.0.3 | 300   | t3   | [5]       |
+----------+-------+------+-------------+
```

## Example 4: Error when field does not exist

If the specified field does not exist in the current schema, mvcombine returns an error.
```ppl
source=mvcombine_data
| mvcombine does_not_exist
```

Expected output:
```text
{'reason': 'Invalid Query', 'details': 'Field [does_not_exist] not found.', 'type': 'IllegalArgumentException'}
Error: Query returned no data
```