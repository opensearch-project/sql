# AddTotals


## Description

The `addtotals` command computes the sum of numeric fields and appends a row with the totals to the result. The command can also add row totals and add a field to store row totals. This is useful for creating summary reports with subtotals or grand totals. The `addtotals` command only sums numeric fields (integers, floats, doubles). Non-numeric fields in the field list are ignored even if it\'s specified in field-list or in the case of no field-list specified.

## Syntax

`addtotals [field-list] [label=<string>] [labelfield=<field>] [row=<boolean>] [col=<boolean>] [fieldname=<field>]`

- `field-list`: Optional. Comma-separated list of numeric fields to sum. If not specified, all numeric fields are summed.
- `row=<boolean>`: Optional. Calculates total of each row and add a new field with the total. Default is true.
- `col=<boolean>`: Optional. Calculates total of each column and add a new event at the end of all events with the total. Default is false.
- `labelfield=<field>`: Optional. Field name to place the label. If it specifies a non-existing field, adds the field and shows label at the summary event row at this field. This is applicable when col=true.
- `label=<string>`: Optional. Custom text for the totals row labelfield\'s label. Default is \"Total\". This is applicable when col=true. This does not have any effect when labelfield and fieldname parameter both have same value.
- `fieldname=<field>`: Optional. Calculates total of each row and add a new field to store this total. This is applicable when row=true.

## Example 1: Basic Example

The example shows placing the label in an existing field.

```ppl
source=accounts 
| head 3
|fields firstname, balance 
| addtotals col=true labelfield='firstname' label='Total'
```

Expected output:

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

## Example 2: Adding column totals and adding a summary event with label specified.

The example shows adding totals after a stats command where final summary event label is \'Sum\'. It also added new field specified by labelfield as it did not match existing field.

```ppl
source=accounts
| fields   account_number, firstname , balance , age 
| addtotals col=true  row=false label='Sum' labelfield='Total'
```

Expected output:

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

if row=true in above example, there will be conflict between column added for column totals and column added for row totals being same field \'Total\', in that case the output will have final event row label null instead of \'Sum\' because the column is number type and it cannot output String in number type column. 

```ppl
source=accounts
| fields   account_number, firstname , balance , age   
| addtotals col=true  row=true label='Sum' labelfield='Total'
```

Expected output:

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

## Example 3: With all options

The example shows using addtotals with all options set.

```ppl
source=accounts 
| where age > 30 
| stats avg(balance) as avg_balance, count() as count by state 
| head 3 
| addtotals avg_balance, count row=true col=true fieldname='Row Total' label='Sum' labelfield='Column Total'
```

Expected output:

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