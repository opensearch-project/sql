
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

### Example 1: Basic example

The following query places the label in an existing field:

```ppl
source=accounts 
| fields firstname, balance 
| head 3 
| addcoltotals labelfield='firstname'
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-----------+---------+
| firstname | balance |
|-----------+---------|
| Amber     | 39225   |
| Hattie    | 5686    |
| Nanette   | 32838   |
| Total     | 77749   |
+-----------+---------+
```

## Example 2: Adding column totals with a custom summary label

The following query adds totals after a `stats` command where the final summary event label is `Sum`. It also creates a new field specified by `labelfield` because this field does not exist in the data:

```ppl
source=accounts 
| stats count() by gender 
| addcoltotals `count()` label='Sum' labelfield='Total'
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+---------+--------+-------+
| count() | gender | Total |
|---------+--------+-------|
| 1       | F      | null  |
| 3       | M      | null  |
| 4       | null   | Sum   |
+---------+--------+-------+
```

## Example 3: Using all options

The following query uses the `addcoltotals` command with all options set:

```ppl
source=accounts 
| where age > 30 
| stats avg(balance) as avg_balance, count() as count by state 
| head 3 
| addcoltotals avg_balance, count  label='Sum' labelfield='Column Total'
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-------------+-------+-------+--------------+
| avg_balance | count | state | Column Total |
|-------------+-------+-------+--------------|
| 39225.0     | 1     | IL    | null         |
| 4180.0      | 1     | MD    | null         |
| 5686.0      | 1     | TN    | null         |
| 49091.0     | 3     | null  | Sum          |
+-------------+-------+-------+--------------+
```
