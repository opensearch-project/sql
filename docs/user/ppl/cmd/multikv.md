
# multikv

The `multikv` command extracts field values from an input field and emits one row per source row. The input field can be table-formatted text (for example the aligned output of `ps`, `top`, `netstat`, or `df`), where a header row names the columns and each following line becomes its own output row. The input field can also be an object or an array of objects: a single object yields one row, an array yields one row per element, and each declared column is read from the object. Either way it is a one-to-many (row-multiplying) command.

> **Note**: `multikv` is a streaming (mid-pipeline) command that runs on the coordinating node. It requires the Calcite engine (`plugins.calcite.enabled=true`). It reads from the input field named by `field=<name>`, defaulting to `_raw`. When that field is text it is split into columns; when it is an array of objects each declared column is read from the element (`field=<name> fields <col>...`). OpenSearch has no implicit `_raw` field, so for the text form either pass `field=<name>` or place the text in `_raw` first with `eval _raw=<field>`. Version 1 is fixed-schema: the output column names must be determinable at plan time via the `fields` clause.

## Syntax

The `multikv` command has the following syntax:

```syntax
multikv [field=<name>] [fields <col>...] [forceheader=<int>] [noheader=<bool>]
```

## Parameters

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `field=<name>` | Optional | The input field that holds the table text. Defaults to `_raw`. Use this to read the text directly from a field such as `message` without a preceding `eval _raw=<field>`. |
| `fields <col>...` | Optional | Declares the output columns to extract by name. This is the only form that yields named columns. Each extracted column is typed as `string`. |
| `forceheader` | Optional | The 1-based line number to use as the header, which skips banner lines above it. Must be a positive integer. Used together with `fields`. |
| `noheader` | Optional | When `true`, there is no header row; the command performs row explosion only and does not produce named columns (for example to count data lines). Default is `false`. |

### Column typing

Every extracted column is typed as `string`, because the values come from splitting text and there is no index mapping to infer a type from. The typed engine coerces these string columns per operation, so numeric use works without an explicit cast (for example `stats avg(pctIdle)` computes numerically, and `where pctIdle > 100` compares numerically). To pin a hard type, cast downstream, for example `eval n = cast(pctIdle as double)`.

## Example 1: Extract a single column

The following query reads the table text from the `raw` field with `field=` and extracts the `pctIdle` column:

```ppl
source=metrics
| multikv field=raw fields pctIdle
| fields pctIdle
```

The query returns one row per table data row, with a single `pctIdle` (string) column.

## Example 2: Extract multiple columns

```ppl
source=metrics
| eval _raw = raw
| multikv fields CPU pctIdle
```

The query returns one row per table data row, with `CPU` (string) and `pctIdle` (string) columns.

## Example 3: Skip a banner line with forceheader

When the first line is a banner and the real header is on line 2:

```ppl
source=report
| eval _raw = raw
| multikv fields endpoint forceheader=2
```

The query uses line 2 as the header and returns the `endpoint` (string) column, one row per data line.

## Example 4: Row explosion with noheader

When there is no header row and only the number of data lines matters:

```ppl
source=lines
| eval _raw = raw
| multikv noheader=true
| stats count
```

The query explodes the table text into one row per data line and counts them.

## Example 5: Structured input (array of objects)

When `field=` points at an array of objects, `multikv` emits one row per element and reads each declared column from the element, preserving the element value:

```ppl
source=hosts
| multikv field=procs fields pid cpu
```

For a document with `procs = [{"pid":1,"cpu":0.5},{"pid":42,"cpu":9.1}]`, the query returns two rows: `(1, 0.5)` and `(42, 9.1)`. When `field=` points at a single object rather than an array, one row is returned. Nested container values are returned as-is; extract deeper fields downstream with `spath` or another `multikv field=<subfield>`.

## Limitations

Version 1 is fixed-schema, so a bare `multikv` with no `fields` clause and no `noheader=true` cannot resolve its output column names at plan time and is rejected with guidance:

```ppl
source=metrics | eval _raw = raw | multikv | fields pctIdle
```

Add an explicit `fields` clause to fix it, for example `multikv fields pctIdle`. Runtime header auto-detection is planned for a later version. The `filter` and `rmorig` options are not yet supported.
