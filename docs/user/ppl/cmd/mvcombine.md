
# mvcombine

The `mvcombine` command groups rows that are identical across all fields except a specified target field, and combines the values of that target field into a multivalue (array) field.

> **Note**: Rows are grouped by all fields currently in the pipeline except the target field. Rows in which the target field is missing or `null` are excluded from the combined multivalue output.

## Syntax

The `mvcombine` command has the following syntax:

```syntax
mvcombine <field>
```

## Parameters

The `mvcombine` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field>` | Required | The name of the field whose values are combined into a multivalue field. |

## Example 1: Using basic mvcombine

The following query collapses rows into a single row and combines `packets_str` into a multivalue field:

```ppl
source=mvcombine_data
| where ip='10.0.0.1' and bytes=100 and tags='t1'
| fields ip, bytes, tags, packets_str
| mvcombine packets_str
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+----------+-------+------+-------------+
| ip       | bytes | tags | packets_str |
|----------+-------+------+-------------|
| 10.0.0.1 | 100   | t1   | [10,20,30]  |
+----------+-------+------+-------------+
```

## Example 2: Combining multiple groups

The following query produces one output row per group key:

```ppl
source=mvcombine_data
| where bytes=700 and tags='t7'
| fields ip, bytes, tags, packets_str
| sort ip, packets_str
| mvcombine packets_str
| sort ip
```

The query returns the following results:

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

Rows missing the target field do not contribute a value to the combined output:

```ppl
source=mvcombine_data
| where ip='10.0.0.3' and bytes=300 and tags='t3'
| fields ip, bytes, tags, packets_str
| mvcombine packets_str
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+----------+-------+------+-------------+
| ip       | bytes | tags | packets_str |
|----------+-------+------+-------------|
| 10.0.0.3 | 300   | t3   | [5]       |
+----------+-------+------+-------------+
```

## Example 4: Missing fields

The following query attempts to combine values for a field that does not exist in the current schema:

```ppl ignore
source=mvcombine_data
| mvcombine does_not_exist
```

The query returns the following error:

```text
  {'context': {'stage_description': 'Parsing and validating the query', 'stage': 'analyzing', 'requested_field': 'does_not_exist', 'available_fields': ['packets_str', 'bytes', 'case', 'letters', 'ip', 'tags', '_id', '_index', '_score', '_maxscore', '_sort', '_routing'], 'query_pos': {'column': 34, 'line': 1}}, 'reason': 'Field [does_not_exist] not found.', 'details': 'Field [does_not_exist] not found.', 'location': ['while preparing and validating the query plan'], 'code': 'FIELD_NOT_FOUND', 'type': 'IllegalArgumentException'}
Error: Query returned no data
```

=======

## Related commands

- [`nomv`](nomv.md) -- Converts a multivalue field into a single-value string
- [`mvexpand`](mvexpand.md) -- Expands multivalue fields into separate rows
