# format

The `format` command converts tabular input into one row containing a string field named `search`.
Within each input row, it renders non-null fields as `field="value"` expressions joined by a column
separator. It then joins the formatted rows with a row separator.

As a pipeline command, `format` returns the generated expression as data; it does not execute the
expression. When a bracketed subsearch is used directly in a parent [`search`](search.md)
expression, PPL implicitly formats and executes the subsearch result as a parent search predicate.

## Syntax

```syntax
format [mvsep="<separator>"] [maxresults=<integer>] ["<row-prefix>" "<column-prefix>" "<column-separator>" "<column-end>" "<row-separator>" "<row-end>"] [emptystr="<string>"]
```

If you specify positional delimiters, you must provide all six.

## Parameters

| Parameter | Default | Description |
| --- | --- | --- |
| `mvsep` | `OR` | Separator between values from a multivalue field. |
| `maxresults` | `0` | Maximum number of input rows to format. `0` adds no row limit. If a parent search executes the result, OpenSearch may still reject it when it exceeds `indices.query.bool.max_clause_count`. |
| `row-prefix` | `(` | Prefix for the complete expression. |
| `column-prefix` | `(` | Prefix for each formatted input row. |
| `column-separator` | `AND` | Separator between fields in one row. |
| `column-end` | `)` | Suffix for each formatted input row. |
| `row-separator` | `OR` | Separator between formatted rows. |
| `row-end` | `)` | Suffix for the complete expression. |
| `emptystr` | `NOT ()` | Result when the input has no formattable fields or values. |

## Behavior

`format` builds the output in three steps:

1. It converts each non-null field into `field="value"`. For example, `status=500` becomes
   `status="500"`.
2. It joins the fields from the same row with the column separator, which is `AND` by default. For
   example, a row containing `host=web-1` and `status=500` becomes
   `( host="web-1" AND status="500" )`.
3. It joins different rows with the row separator, which is `OR` by default, and returns the
   complete string in one field named `search`.

A multivalue field produces one comparison for each non-null value. For example,
`host=["web-1","web-2"]` becomes `( host="web-1" OR host="web-2" )`.

The command also follows these rules:

- Fields are ordered by field name so that the output is deterministic.
- Null and missing fields are skipped, but empty strings are included.
- OpenSearch metadata fields, such as `_id`, are skipped. A user-created field beginning with `_`
  is included.
- Field names that require PPL identifier quoting are enclosed in backticks.
- Double quotes and backslashes in values are escaped.
- When `format` is written explicitly in a pipeline, fields named `search` and `query` are treated
  like any other fields. The generated string is returned as data and is not executed.

## Implicit format in a search expression

When a bracketed subsearch appears directly inside a parent `search` expression, PPL uses the
subsearch result as part of the parent search:

1. The subsearch runs.
2. Its result is formatted into one search string.
3. The string is parsed as a search predicate.
4. The predicate is combined with the parent search expression.

How the subsearch result is formatted depends on its fields:

- **No scalar `search` field:** All subsearch result rows are formatted normally. For example, rows
  containing `host=web-1` and `host=web-2` produce
  `( ( host="web-1" ) OR ( host="web-2" ) )`.
- **A non-null scalar `search` field:** Only the first result row is used. The `search` value is
  already predicate text, so it is used directly and the other fields are ignored. For example,
  `search="status>=500"` adds
  `status>=500` to the parent search.
- **A null scalar `search` field:** The `search` field is skipped and the other fields from the first
  row are formatted normally.
- **An empty scalar `search` field:** The subsearch adds no condition to the parent search.
- **A multivalue `search` field or a field named `query`:** It is formatted like an ordinary field;
  it is not treated as predicate text.

The following query selects the account numbers returned by the subsearch:

```ppl
search source=accounts [ search source=accounts account_number=6 | fields account_number ]
| fields account_number, firstname
```

The query returns the following result:

```text
fetched rows / total rows = 1/1
+----------------+-----------+
| account_number | firstname |
|----------------+-----------|
| 6              | Hattie    |
+----------------+-----------+
```

The implicit formatter consumes at most the number of result rows configured by
`plugins.ppl.subsearch.maxout`. If the subsearch ends with an explicit `format`, that command first
reduces its input to one result row, and its `maxresults` option controls how many input rows it
formats. An explicit `format` remains a regular pipeline command and does not execute its generated
string. Neither row limit guarantees that a parent search can execute the generated expression;
OpenSearch may reject it when it exceeds `indices.query.bool.max_clause_count`.

## Examples

### Example 1: Default formatting

The following query formats one account:

```ppl
source=accounts
| where account_number=1
| fields firstname, lastname, account_number
| format
```

The query returns the following result:

```text
fetched rows / total rows = 1/1
+----------------------------------------------------------------------+
| search                                                               |
|----------------------------------------------------------------------|
| ( ( account_number="1" AND firstname="Amber" AND lastname="Duke" ) ) |
+----------------------------------------------------------------------+
```

### Example 2: Custom delimiters and row limit

The following query formats at most two rows and replaces all six positional delimiters:

```ppl
source=accounts
| sort account_number
| fields account_number
| format maxresults=2 "[" "[" "&&" "]" "||" "]"
```

The query returns the following result:

```text
fetched rows / total rows = 1/1
+------------------------------------------------------+
| search                                               |
|------------------------------------------------------|
| [ [ account_number="1" ] || [ account_number="6" ] ] |
+------------------------------------------------------+
```

### Example 3: Multivalue fields

The following query uses `OR` between the values of a multivalue field:

```ppl
source=accounts
| where account_number=1
| eval names=array(firstname, lastname)
| fields names
| format mvsep="OR" "{" "[" "AND" "]" "AND" "}"
```

The query returns the following result:

```text
fetched rows / total rows = 1/1
+-------------------------------------------+
| search                                    |
|-------------------------------------------|
| { [ ( names="Amber" OR names="Duke" ) ] } |
+-------------------------------------------+
```

### Example 4: Empty input

The following query supplies a custom result for empty input:

```ppl
source=accounts
| where account_number < 0
| fields account_number
| format emptystr="no matching data"
```

The query returns the following result:

```text
fetched rows / total rows = 1/1
+------------------+
| search           |
|------------------|
| no matching data |
+------------------+
```

## Limitations

- Implicit formatting is supported only for a bracketed subsearch used directly in a `search`
  expression. A subsearch cannot be inserted dynamically into `eval` or another command argument.
- When the first result row of an implicit subsearch contains a non-null scalar `search` field, the
  parent search uses that value as predicate text. Pipeline commands can run normally inside the
  subsearch, so `| eval search="status=500" | head 10` is valid. However, the `search` value itself
  cannot contain a pipeline, so `| eval search="status=500 | head 10"` is rejected.

## Related commands

- [fields](fields.md) --- select the fields included in the formatted expression
- [eval](eval.md) --- build values before formatting
- [search](search.md) --- filter parent results with an implicit format subsearch
- [subquery](subquery.md) --- use relational subqueries such as `IN`
