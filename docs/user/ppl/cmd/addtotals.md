
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

## Example 1: Basic example

The following query places the label in an existing field:

```ppl
source=accounts 
| head 3
| fields firstname, balance 
| addtotals col=true labelfield='firstname' label='Total'
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-----------+---------+-------+
| firstname | balance | Total |
|-----------+---------+-------|
| Amber     | 39225   | 39225 |
| Hattie    | 5686    | 5686  |
| Nanette   | 32838   | 32838 |
| Total     | 77749   | null  |
+-----------+---------+-------+
```    

## Example 2: Adding column totals with a custom summary label

The following query adds totals after a `stats` command, with the final summary event labeled `Sum`. It also creates a new field specified by `labelfield` because the field does not exist in the data:


```ppl
source=accounts
| fields   account_number, firstname , balance , age 
| addtotals col=true  row=false label='Sum' labelfield='Total'
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+----------------+-----------+---------+-----+-------+
| account_number | firstname | balance | age | Total |
|----------------+-----------+---------+-----+-------|
| 1              | Amber     | 39225   | 32  | null  |
| 6              | Hattie    | 5686    | 36  | null  |
| 13             | Nanette   | 32838   | 28  | null  |
| 18             | Dale      | 4180    | 33  | null  |
| 38             | null      | 81929   | 129 | Sum   |
+----------------+-----------+---------+-----+-------+
```

If you set `row=true` in the preceding example, both row totals and column totals try to use the same field name (`Total`), creating a conflict. When this happens, the summary row label displays as `null` instead of `Sum` because the field becomes numeric (for row totals) and cannot display string values:


```ppl
source=accounts
| fields   account_number, firstname , balance , age   
| addtotals col=true  row=true label='Sum' labelfield='Total'
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+----------------+-----------+---------+-----+-------+
| account_number | firstname | balance | age | Total |
|----------------+-----------+---------+-----+-------|
| 1              | Amber     | 39225   | 32  | 39258 |
| 6              | Hattie    | 5686    | 36  | 5728  |
| 13             | Nanette   | 32838   | 28  | 32879 |
| 18             | Dale      | 4180    | 33  | 4231  |
| 38             | null      | 81929   | 129 | null  |
+----------------+-----------+---------+-----+-------+
```

## Example 3: Using all options

The following query uses the `addtotals` command with all options set:

```ppl
source=accounts 
| where age > 30 
| stats avg(balance) as avg_balance, count() as count by state 
| head 3 
| addtotals avg_balance, count row=true col=true fieldname='Row Total' label='Sum' labelfield='Column Total'
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-------------+-------+-------+-----------+--------------+
| avg_balance | count | state | Row Total | Column Total |
|-------------+-------+-------+-----------+--------------|
| 39225.0     | 1     | IL    | 39226.0   | null         |
| 4180.0      | 1     | MD    | 4181.0    | null         |
| 5686.0      | 1     | TN    | 5687.0    | null         |
| 49091.0     | 3     | null  | null      | Sum          |
+-------------+-------+-------+-----------+--------------+
```