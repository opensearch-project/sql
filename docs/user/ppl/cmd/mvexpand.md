
# mvexpand

The `mvexpand` command expands each value in a multivalue (array) field into a separate row. For each document, every element in the specified array field is returned as its own row.

## Syntax

The `mvexpand` command has the following syntax:

```syntax
mvexpand <field> [limit=<int>]
```

## Parameters

The `mvexpand` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field>` | Required | The multivalue (array) field to expand. |
| `limit` | Optional | The maximum number of values per document to expand. If not specified, all array elements are expanded. |

## Example 1: Using basic expansion

The following query creates an array and expands it into separate rows:

```ppl
source=people
| eval tags = array('error', 'warning', 'info')
| fields tags
| head 1
| mvexpand tags
| fields tags
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+---------+
| tags    |
|---------|
| error   |
| warning |
| info    |
+---------+
```

## Example 2: Limiting the number of expanded rows

The following query expands an array while limiting the number of expanded rows:

```ppl
source=people
| eval ids = array(1, 2, 3, 4, 5)
| fields ids
| head 1
| mvexpand ids limit=3
| fields ids
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+-----+
| ids |
|-----|
| 1   |
| 2   |
| 3   |
+-----+
```

## Example 3: Expanding nested fields

The following query expands a multivalue `projects` field into one row per project:

```ppl
source=people
| head 1
| fields projects
| mvexpand projects
| fields projects.name
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+--------------------------------+
| projects.name                  |
|--------------------------------|
| AWS Redshift Spectrum querying |
| AWS Redshift security          |
| AWS Aurora security            |
+--------------------------------+
```

## Example 4: Single-value array

A single-element array expands to one row:

```ppl
source=people
| eval tags = array('error')
| fields tags
| head 1
| mvexpand tags
| fields tags
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+-------+
| tags  |
|-------|
| error |
+-------+
```

## Example 5: Missing fields

The following query attempts to expand a field that does not exist in the input schema:

```ppl ignore
source=people
| eval some_field = 'x'
| fields some_field
| head 1
| mvexpand tags
| fields tags
```

The query throws the following semantic check exception:

```text
{'context': {'stage': 'analyzing', 'stage_description': 'Parsing and validating the query', 'command': 'mvexpand'}, 'reason': "Field 'tags' not found in the schema", 'details': "Field 'tags' not found in the schema", 'location': ['while preparing and validating the query plan', 'while evaluating the input field for mvexpand'], 'code': 'FIELD_NOT_FOUND', 'type': 'SemanticCheckException'}
Error: Query returned no data
```

## Related commands

- [`nomv`](nomv.md) -- Converts a multivalue field into a single-value string
- [`mvcombine`](mvcombine.md) -- Combines multiple rows into a single row with multivalue fields
