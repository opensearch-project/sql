# AddTotals


## Description

The `addtotals` command computes the sum of numeric fields and appends a
row with the totals to the result. The command can also add row totals
and add a field to store row totals. This is useful for creating summary
reports with subtotals or grand totals. The `addtotals` command only
sums numeric fields (integers, floats, doubles). Non-numeric fields in
the field list are ignored even if it\'s specified in field-list or in
the case of no field-list specified.

## Syntax

`addtotals [field-list] [label=<string>] [labelfield=<field>] [row=<boolean>] [col=<boolean>] [fieldname=<field>]`

- `field-list`: Optional. Comma-separated list of numeric fields to sum.
  If not specified, all numeric fields are summed.
- `row=<boolean>`: Optional. Calculates total of each row and add a new
  field with the total. Default is true.
- `col=<boolean>`: Optional. Calculates total of each column and add a
  new event at the end of all events with the total. Default is false.
- `labelfield=<field>`: Optional. Field name to place the label. If it
  specifies a non-existing field, adds the field and shows label at the
  summary event row at this field. This is applicable when col=true.
- `label=<string>`: Optional. Custom text for the totals row
  labelfield\'s label. Default is \"Total\". This is applicable when
  col=true. This does not have any effect when labelfield and fieldname
  parameter both have same value.
- `fieldname=<field>`: Optional. Calculates total of each row and add a
  new field to store this total. This is applicable when row=true.

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

The example shows adding totals after a stats command where final
summary event label is \'Sum\' and row=true value was used by default
when not specified. It also added new field specified by labelfield as
it did not match existing field.

```ppl
source=accounts  
| addtotals col=true  row=false label='Sum' labelfield='Total'
```

Expected output:

```text
fetched rows / total rows = 5/5
+----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+-------+
| account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | Total |
|----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+-------|
| 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | null  |
| 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | null  |
| 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | null  |
| 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | null  |
| 38             | null      | null                 | 81929   | null   | null   | null     | null  | 129 | null                  | null     | Sum   |
+----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+-------+
```

if row=true, there will be conflict between column added for column
totals and column added for row totals being same field \'Total\', in
that case the output will have final event row label null instead of
\'Sum\' because the column is number type and it cannot output String in
number type column. 

```ppl
source=accounts  
| addtotals col=true  row=true label='Sum' labelfield='Total'
```

Expected output:

```text
fetched rows / total rows = 5/5
+----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+---------+
| account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname | Total   |
|----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+---------|
| 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     | 39258.0 |
| 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     | 5728.0  |
| 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    | 32879.0 |
| 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    | 4231.0  |
| 38             | null      | null                 | 81929   | null   | null   | null     | null  | 129 | null                  | null     | null    |
+----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+---------+
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