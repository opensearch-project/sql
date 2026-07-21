# xyseries

## Description

The `xyseries` command converts row-oriented grouped results into a wide table format suitable for chart visualizations. One field serves as the X axis (row key), one field provides pivot values for generating output column names, and one or more data fields fill the pivoted cells. Only rows matching the explicitly provided pivot values in the `in` clause are included.

## Syntax

```syntax
xyseries [sep=<string>] [format=<string>] <x-field> <y-name-field> in (<value1>, <value2>, ...) <y-data-field>[, <y-data-field2>, ...]
```

## Parameters

| Parameter | Required/Optional | Description | Default |
| --- | --- | --- | --- |
| `<x-field>` | Required | The field used as the row key in the output. Results are grouped and sorted by this field. | N/A |
| `<y-name-field>` | Required | The field whose values are used to generate output column names. Only the values listed in the `in` clause are pivoted into columns. | N/A |
| `in (<value1>, <value2>, ...)` | Required | Explicit list of pivot values to select from `<y-name-field>`. Each value generates one output column per `<y-data-field>`. Values must be quoted strings. | N/A |
| `<y-data-field>` | Required (at least one) | One or more fields containing the data to pivot. If multiple fields are specified, separate them with commas. | N/A |
| `sep` | Optional | Separator between the `<y-data-field>` name and the pivot value in output column names. Ignored if `format` is specified. | `": "` |
| `format` | Optional | Naming template for output column names. Use `$AGG$` as a placeholder for the `<y-data-field>` name and `$VAL$` as a placeholder for the pivot value. When specified, overrides `sep`. | N/A |

## Notes

The following considerations apply when using the `xyseries` command:

* The `xyseries` command is typically used after a `stats` command that groups results by both the `<x-field>` and `<y-name-field>`.
* Output column names follow the pattern `<y-data-field><sep><pivot-value>` by default (for example, `host_cnt: 200`). Use the `format` option to customize this pattern.
* When a pivot value has no matching data for a given `<x-field>` row, the output cell is `null`.
* The `<y-name-field>` values are compared as strings. Non-string fields are cast to string automatically.
* Results are sorted by `<x-field>` in ascending order.
* This command requires the Calcite engine to be enabled (`plugins.calcite.enabled: true`).

## Example 1: Basic xyseries with a single data field

This example pivots HTTP response codes into columns for a count of hosts per URL:

```ppl
source=weblogs
| stats count(host) as host_cnt by url, response
| xyseries url response in ("200", "404", "500") host_cnt
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+--------+---------------+---------------+---------------+
| url    | host_cnt: 200 | host_cnt: 404 | host_cnt: 500 |
|--------+---------------+---------------+---------------|
| /page1 | 3             | 1             | null          |
| /page2 | 5             | null          | 2             |
+--------+---------------+---------------+---------------+
```

## Example 2: Multiple data fields

This example pivots multiple aggregated fields at once:

```ppl
source=weblogs
| stats count(host) as host_cnt, count(method) as method_cnt by url, response
| xyseries url response in ("200", "404", "500") host_cnt, method_cnt
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+--------+---------------+---------------+---------------+------------------+------------------+------------------+
| url    | host_cnt: 200 | host_cnt: 404 | host_cnt: 500 | method_cnt: 200  | method_cnt: 404  | method_cnt: 500  |
|--------+---------------+---------------+---------------+------------------+------------------+------------------|
| /page1 | 3             | 1             | null          | 3                | 1                | null             |
| /page2 | 5             | null          | 2             | 5                | null             | 2                |
+--------+---------------+---------------+---------------+------------------+------------------+------------------+
```

## Example 3: Custom separator

This example uses a custom separator between the data field name and pivot value in column names:

```ppl
source=weblogs
| stats count(host) as host_cnt by url, response
| xyseries sep="-" url response in ("200", "404") host_cnt
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+--------+--------------+--------------+
| url    | host_cnt-200 | host_cnt-404 |
|--------+--------------+--------------|
| /page1 | 3            | 1            |
| /page2 | 5            | null         |
+--------+--------------+--------------+
```

## Example 4: Format template

This example uses a format template to customize output column names. `$VAL$` is replaced with the pivot value and `$AGG$` is replaced with the data field name:

```ppl
source=weblogs
| stats count(host) as host_cnt by url, response
| xyseries format="$VAL$_$AGG$" url response in ("200", "404") host_cnt
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+--------+--------------+--------------+
| url    | 200_host_cnt | 404_host_cnt |
|--------+--------------+--------------|
| /page1 | 3            | 1            |
| /page2 | 5            | null         |
+--------+--------------+--------------+
```

## Example 5: Partial pivot values

When only a subset of values is specified in the `in` clause, rows with unmatched `<y-name-field>` values produce `null` for the corresponding `<x-field>` rows:

```ppl
source=accounts
| stats avg(balance) as avg_balance by gender, state
| xyseries state gender in ("F") avg_balance
```

The query returns the following results:

```text
fetched rows / total rows = 7/7
+-------+-----------------+
| state | avg_balance: F  |
|-------+-----------------|
| IL    | null            |
| IN    | 48086.0         |
| MD    | null            |
| PA    | 40540.0         |
| TN    | null            |
| VA    | 32838.0         |
| WA    | null            |
+-------+-----------------+
```

## Limitations

The `xyseries` command has the following limitations:

* Pivot values must be explicitly provided in the `in` clause. Dynamic pivot (deriving column names from data at runtime) is not supported.
* This command is only available when the Calcite engine is enabled.
