
# addcoltotals

The `addcoltotals` command computes the sum of each column and adds a summary row showing the total for each column. This command is equivalent to using `addtotals` with `row=false` and `col=true`, making it useful for creating summary reports with column totals.

The command only processes numeric fields (integers, floats, doubles). Non-numeric fields are ignored regardless of whether they are explicitly specified in the field list.


## Syntax

The `addcoltotals` command has the following syntax:

```syntax
addcoltotals [field-list] [label=<string>] [labelfield=<field>]
```

## Parameters

The `addcoltotals` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field-list>` | Optional | A comma-separated list of numeric fields to add. By default, all numeric fields are added. |
| `labelfield` | Optional | The field in which the label is placed. If the field does not exist, it is created and the label is shown in the summary row (last row) of the new field. |
| `label` | Optional | The text that appears in the summary row (last row) to identify the computed totals. When used with `labelfield`, this text is placed in the specified field in the summary row. Default is `Total`. |

## Example 1: Add column totals to a severity breakdown

The following query adds a total row to a severity breakdown, showing the grand total of all log entries:

```ppl
source=otellogs
| stats count() as log_count by severityText
| sort severityText
| addcoltotals labelfield='severityText'
```

The query returns the following results:

```text
fetched rows / total rows = 6/6
+-----------+--------------+
| log_count | severityText |
|-----------+--------------|
| 3         | DEBUG        |
| 5         | ERROR        |
| 2         | FATAL        |
| 6         | INFO         |
| 4         | WARN         |
| 20        | Total        |
+-----------+--------------+
```

## Example 2: Add column totals with a custom label

The following query adds totals to error counts per service with a custom summary label:

```ppl
source=otellogs
| where severityText IN ('ERROR', 'FATAL')
| stats count() as failures by `resource.attributes.service.name`
| sort `resource.attributes.service.name`
| addcoltotals failures label='Grand Total' labelfield='Summary'
```

The query returns the following results:

```text
fetched rows / total rows = 6/6
+----------+----------------------------------+-------------+
| failures | resource.attributes.service.name | Summary     |
|----------+----------------------------------+-------------|
| 2        | checkout                         | null        |
| 1        | frontend-proxy                   | null        |
| 2        | payment                          | null        |
| 1        | product-catalog                  | null        |
| 1        | recommendation                   | null        |
| 7        | null                             | Grand Total |
+----------+----------------------------------+-------------+
```

## Example 3: Using all options

The following query uses the `addcoltotals` command with all options set, totaling only the specified numeric fields and placing the summary label in a new column:

```ppl
source=otellogs
| where severityText IN ('ERROR', 'FATAL')
| eval error_count = IF(severityText = 'ERROR', 1, 0), fatal_count = IF(severityText = 'FATAL', 1, 0)
| stats sum(error_count) as errors, sum(fatal_count) as fatals by `resource.attributes.service.name`
| sort `resource.attributes.service.name`
| addcoltotals errors, fatals label='Sum' labelfield='Column Total'
```

The query returns the following results:

```text
fetched rows / total rows = 6/6
+--------+--------+----------------------------------+--------------+
| errors | fatals | resource.attributes.service.name | Column Total |
|--------+--------+----------------------------------+--------------|
| 2      | 0      | checkout                         | null         |
| 1      | 0      | frontend-proxy                   | null         |
| 1      | 1      | payment                          | null         |
| 0      | 1      | product-catalog                  | null         |
| 1      | 0      | recommendation                   | null         |
| 5      | 2      | null                             | Sum          |
+--------+--------+----------------------------------+--------------+
```
