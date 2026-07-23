# format

The `format` command collapses tabular input into one row containing a `search` string. Within each input row, it renders non-null fields as `field="value"` expressions joined by a column separator. It then joins the rendered rows with a row separator.

`format` produces the expression as data. It does not parse or execute the generated `search` string.

## Syntax

```syntax
format [mvsep="<separator>"] [maxresults=<integer>] ["<row-prefix>" "<column-prefix>" "<column-separator>" "<column-end>" "<row-separator>" "<row-end>"] [emptystr="<string>"]
```

If you specify positional delimiters, you must provide all six.

## Parameters

| Parameter | Default | Description |
| --- | --- | --- |
| `mvsep` | `OR` | Separator between values from a multivalue field. |
| `maxresults` | `0` | Maximum input rows to include. `0` means unlimited. |
| `row-prefix` | `(` | Prefix for the complete expression. |
| `column-prefix` | `(` | Prefix for each formatted input row. |
| `column-separator` | `AND` | Separator between fields in one row. |
| `column-end` | `)` | Suffix for each formatted input row. |
| `row-separator` | `OR` | Separator between formatted rows. |
| `row-end` | `)` | Suffix for the complete expression. |
| `emptystr` | `NOT( )` | Result when the input has no formattable fields or values. |

## Behavior

- Output contains one string field named `search`.
- Fields are rendered in lexicographic field-name order.
- Null and missing values are omitted. Empty strings are retained.
- Fields whose names begin with `_` are treated as internal fields and omitted.
- The field names `search` and `query` are omitted from their rendered predicates; their values are inserted directly into the expression.
- Field names containing special characters are double quoted.
- Scalar values are converted to strings. Double quotes and backslashes are escaped.
- Multivalue fields produce a parenthesized expression containing one predicate per non-null element.

## Examples

### Default formatting

The following input:

```text
status  method
200     GET
500     POST
```

with this command:

```ppl
source=logs
| fields status, method
| format
```

produces:

```text
( ( method="GET" AND status="200" ) OR ( method="POST" AND status="500" ) )
```

### Custom delimiters and row limit

```ppl
source=logs
| fields status, method
| format maxresults=2 "[" "[" "&&" "]" "||" "]"
```

### Multivalue fields

```ppl
source=alerts
| fields tags
| format mvsep="OR" "{" "[" "AND" "]" "AND" "}"
```

For a `tags` value of `[critical, network]`, the formatted value is:

```text
{ [ ( tags="critical" OR tags="network" ) ] }
```

### Empty input

```ppl
source=logs
| where status=999
| fields status
| format emptystr="no matching data"
```

## Limitations

- PPL currently supports `format` only as an explicit pipeline command. It is not implicitly appended to subsearches.
- The generated `search` value is not automatically injected into or executed by an outer query.
- Multivalue element escaping is limited by the backend array-expression support. Avoid quote and backslash characters in multivalue elements when the output will be parsed as a search expression.
- Row collection lowers to a global `ARRAY_AGG` without an aggregate order key. Distributed execution can change row order even when the input contains an upstream `sort`, so stable Splunk row-order parity is not guaranteed.

## Related commands

- [fields](fields.md) --- select the fields included in the formatted expression
- [eval](eval.md) --- build values before formatting
- [subquery](subquery.md) --- use the result of one query inside another query
