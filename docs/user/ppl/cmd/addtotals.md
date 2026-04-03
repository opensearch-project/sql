
# addtotals

The `addtotals` command computes the sum of numeric fields and can create both column totals (summary row) and row totals (new field). This command is useful for creating summary reports with subtotals or grand totals.

The command only processes numeric fields (integers, floats, doubles). Non-numeric fields are ignored regardless of whether they are explicitly specified in the field list.


## Syntax

The `addtotals` command has the following syntax:

```syntax
addtotals [field-list] [label=<string>] [labelfield=<field>] [row=<boolean>] [col=<boolean>] [fieldname=<field>]
```

## Parameters

The `addtotals` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field-list>` | Optional | A comma-separated list of numeric fields to add. By default, all numeric fields are added. |
| `row` | Optional | Calculates the total of each row and adds a new field to store the row total. Default is `true`. |
| `col` | Optional | Calculates the total of each column and adds a summary event at the end with the column totals. Default is `false`. |
| `labelfield` | Optional | The field in which the label is placed. If the field does not exist, it is created and the label is shown in the summary row (last row) of the new field. Applicable when `col=true`. |
| `label` | Optional | The text that appears in the summary row (last row) to identify the computed totals. When used with `labelfield`, this text is placed in the specified field in the summary row. Default is `Total`. Applicable when `col=true`. This parameter has no effect when the `labelfield` and `fieldname` parameters specify the same field name. |
| `fieldname` | Optional | The field used to store row totals. Applicable when `row=true`. |

## Example 1: Add column totals

The following query counts errors and warnings per service, then adds a column total row showing the grand totals:

```ppl
source=otellogs
| where severityText IN ('ERROR', 'WARN')
| eval error_count = IF(severityText = 'ERROR', 1, 0), warn_count = IF(severityText = 'WARN', 1, 0)
| stats sum(error_count) as errors, sum(warn_count) as warnings by `resource.attributes.service.name`
| sort `resource.attributes.service.name`
| fields `resource.attributes.service.name`, errors, warnings
| addtotals col=true labelfield='resource.attributes.service.name' label='Total'
```

The query returns the following results:

```text
fetched rows / total rows = 6/6
+----------------------------------+--------+----------+-------+
| resource.attributes.service.name | errors | warnings | Total |
|----------------------------------+--------+----------+-------|
| checkout                         | 2      | 0        | 2     |
| frontend-proxy                   | 1      | 2        | 3     |
| payment                          | 2      | 0        | 2     |
| product-catalog                  | 1      | 2        | 3     |
| recommendation                   | 1      | 0        | 1     |
| Total                            | 7      | 4        | null  |
+----------------------------------+--------+----------+-------+
```

## Example 2: Add row totals

The following query counts errors and warnings separately per service, then adds a row total showing the combined count of actionable issues per service:

```ppl
source=otellogs
| where severityText IN ('ERROR', 'WARN')
| eval error_count = IF(severityText = 'ERROR', 1, 0), warn_count = IF(severityText = 'WARN', 1, 0)
| stats sum(error_count) as errors, sum(warn_count) as warnings by `resource.attributes.service.name`
| sort `resource.attributes.service.name`
| fields `resource.attributes.service.name`, errors, warnings
| addtotals row=true fieldname='total_issues'
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+----------------------------------+--------+----------+--------------+
| resource.attributes.service.name | errors | warnings | total_issues |
|----------------------------------+--------+----------+--------------|
| checkout                         | 2      | 0        | 2            |
| frontend-proxy                   | 1      | 2        | 3            |
| payment                          | 2      | 0        | 2            |
| product-catalog                  | 1      | 2        | 3            |
| recommendation                   | 1      | 0        | 1            |
+----------------------------------+--------+----------+--------------+
```

## Example 3: Using all options

The following query uses the `addtotals` command with all options set, combining both row totals and column totals in a single report:

```ppl
source=otellogs
| where severityText IN ('ERROR', 'WARN')
| eval error_count = IF(severityText = 'ERROR', 1, 0), warn_count = IF(severityText = 'WARN', 1, 0)
| stats sum(error_count) as errors, sum(warn_count) as warnings by `resource.attributes.service.name`
| sort `resource.attributes.service.name`
| fields `resource.attributes.service.name`, errors, warnings
| addtotals errors, warnings row=true col=true fieldname='Row Total' label='Sum' labelfield='Column Total'
```

The query returns the following results:

```text
fetched rows / total rows = 6/6
+----------------------------------+--------+----------+-----------+--------------+
| resource.attributes.service.name | errors | warnings | Row Total | Column Total |
|----------------------------------+--------+----------+-----------+--------------|
| checkout                         | 2      | 0        | 2         | null         |
| frontend-proxy                   | 1      | 2        | 3         | null         |
| payment                          | 2      | 0        | 2         | null         |
| product-catalog                  | 1      | 2        | 3         | null         |
| recommendation                   | 1      | 0        | 1         | null         |
| null                             | 7      | 4        | null      | Sum          |
+----------------------------------+--------+----------+-----------+--------------+
```
