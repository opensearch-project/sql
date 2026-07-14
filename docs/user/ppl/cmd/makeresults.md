
# makeresults

The `makeresults` command generates in-memory rows. With no arguments it produces a single row containing only the `@timestamp` field, set to the query time. It is commonly used as a seed for `eval` and to generate test data. The time column is named `@timestamp` (OpenSearch's implicit time field) so it is recognized by the time-aware commands such as `timechart`, `reverse`, and `span`.

> **Note**: The `makeresults` command is a leading command (it opens a query) and is executed only on the coordinating node. It has no backing index. It requires the Calcite engine (`plugins.calcite.enabled=true`).

## Syntax

The `makeresults` command has the following syntax:

```syntax
makeresults [count=<int>] [format=csv|json data=<string>]
```

## Parameters

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `count` | Optional | The number of rows to generate. Must be a non-negative integer up to 5000. A negative value produces zero rows. Each row has a single `@timestamp` (timestamp) column. Default is `1`. |
| `format` + `data` | Optional | Generate rows from an inline `csv` or `json` literal instead (up to 5000 rows). When provided, `count` is ignored. |

### Inline data typing

Column types for `data=` follow OpenSearch dynamic-mapping semantics:

- JSON: an integer becomes `long`, a decimal becomes `float`, `true`/`false` becomes `boolean`, and a string becomes `string`.
- CSV: a header token of the form `name:type` declares the column type using the same vocabulary as `cast` (for example `age:int`); a bare header token defaults to `string`.

Inline object/array values and the `date`, `time`, `timestamp`, `ip`, and `json` inline types are not yet supported on this path; use `string` and `cast`.

## Example 1: Generate rows for testing

The following query generates five rows:

```ppl
makeresults count=5
```

## Example 2: Seed a row for eval

```ppl
makeresults
| eval message="hello"
```

## Example 3: Generate typed rows from JSON

```ppl
makeresults format=json data='[{"name":"John","age":35},{"name":"Sarah","age":39}]'
```

The query returns two rows with a `name` (string) column and an `age` (bigint) column. A JSON integer is typed as a long value; because makeresults rows have no index mapping, the column reports its Calcite type name `bigint` in the response schema.

## Example 4: Generate typed rows from CSV

```ppl
makeresults format=csv data='name:string,age:int
John,35
Sarah,39'
```

The query returns two rows with a `name` (string) column and an `age` (int) column.

## Limitations

A global aggregate that references no input column, applied directly to `makeresults`, is not
currently supported and raises an error:

```ppl
makeresults count=5 | stats count() as c
```

This is due to an upstream Apache Calcite field-trimming defect on zero-column relations, not a
`makeresults`-specific issue. Use any of the following equivalent forms instead:

```ppl
makeresults count=5 | stats count(1) as c
makeresults count=5 | stats count() as c by @timestamp
makeresults count=5 | eval g=1 | stats count() as c by g
```
