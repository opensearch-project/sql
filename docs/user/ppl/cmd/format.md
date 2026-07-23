# format

The `format` command collapses tabular input into one row containing a `search` string. Within each input row, it renders non-null fields as `field="value"` expressions joined by a column separator. It then joins the rendered rows with a row separator.

As an explicit pipeline command, `format` produces the expression as data and does not execute the
generated `search` string. A subsearch used directly in a parent `search` expression has an implicit
final `format`; the generated string is parsed as a predicate and applied to the parent search.

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
| `emptystr` | `NOT ()` | Result when the input has no formattable fields or values. |

## Behavior

- Output contains one string field named `search`.
- Fields are rendered in lexicographic field-name order.
- Null and missing values are omitted. Empty strings are retained.
- Fields whose names begin with `_` are treated as internal fields and omitted.
- The field names `search` and `query` are omitted from their rendered predicates. Their scalar
  and multivalue elements are still quoted and escaped like other values.
- Field names containing special characters are double quoted.
- Scalar values are converted to strings. Double quotes and backslashes are escaped.
- Multivalue fields produce a parenthesized expression containing one predicate per non-null element.
- At an implicit subsearch boundary, a scalar `search` field has special behavior: only the first
  result row participates, and a non-null `search` value is inserted as predicate text instead of
  formatting that row's other fields. A null `search` value falls back to formatting the first row's
  other fields. The `query` field and multivalue `search` fields retain normal `format` behavior.

## Implicit format in a search subquery

When a bracketed subsearch appears in the parent `search` expression, PPL executes the subsearch,
implicitly formats its result, parses the resulting string as search predicate syntax, and then
executes the parent search. For example:

```ppl
search source=logs [ search source=rules | where enabled=true | fields host, status ]
```

The subsearch is subject to `plugins.ppl.subsearch.maxout`. An explicit `format` command remains a
regular pipeline command and returns its generated string without executing it.

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

- Implicit format requires the Calcite query engine.
- Runtime binding is supported for bracketed subsearches in a parent `search` expression. Dynamic
  subsearch interpolation in `eval` or other command positions is not supported.
- An implicit result is parsed only as a search predicate. Pipeline command syntax in a generated
  `search` or `query` value is rejected.
- When upstream ordering metadata is available, row collection carries it into the aggregate order
  key. Without an explicit upstream `sort`, distributed execution does not guarantee row order.

## Related commands

- [fields](fields.md) --- select the fields included in the formatted expression
- [eval](eval.md) --- build values before formatting
- [subquery](subquery.md) --- use the result of one query inside another query
