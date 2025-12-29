# transpose  

## Description  

The `transpose` command outputs the requested number of rows as columns, effectively transposing each result row into a corresponding column of field values.

## Syntax  

transpose [int] [column_name=<string>]

* number-of-rows: optional. The number of rows to transform into columns.  
* column_name: optional. The name of the first column to use when transposing rows. This column holds the field names.  
  
  
## Example 1: Transpose results   

This example shows transposing wihtout any parameters. It transforms 5 rows into columns as default is 5.
  
```ppl
source=accounts
| head 5 
| fields account_number, firstname,  lastname, balance 
| transpose
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------------+-------+--------+---------+-------+-------+
| column         | row 1 | row 2  | row 3   | row 4 | row 5 |
|----------------+-------+--------+---------+-------+-------|
| account_number | 1     | 6      | 13      | 18    | null  |
| firstname      | Amber | Hattie | Nanette | Dale  | null  |
| balance        | 39225 | 5686   | 32838   | 4180  | null  |
| lastname       | Duke  | Bond   | Bates   | Adams | null  |
+----------------+-------+--------+---------+-------+-------+
```
  
## Example 2: Tranpose results up to a provided number of rows.  

This example shows transposing wihtout any parameters. It transforms 4 rows into columns as default is 5.

```ppl
source=accounts
| head 5 
| fields  account_number, firstname,  lastname, balance 
| transpose 4
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------------+-------+--------+---------+-------+
| column         | row 1 | row 2  | row 3   | row 4 |
|----------------+-------+--------+---------+-------|
| account_number | 1     | 6      | 13      | 18    |
| firstname      | Amber | Hattie | Nanette | Dale  |
| balance        | 39225 | 5686   | 32838   | 4180  |
| lastname       | Duke  | Bond   | Bates   | Adams |
+----------------+-------+--------+---------+-------+
```

## Example 2: Tranpose results up to a provided number of rows and first column with specified column name.

This example shows transposing wihtout any parameters. It transforms 4 rows into columns as default is 5.

```ppl
source=accounts
| head 5 
| fields  account_number, firstname,  lastname, balance 
| transpose 4 column_name='column_names'
```

Expected output:

```text
fetched rows / total rows = 4/4
+----------------+-------+--------+---------+-------+
| column_names   | row 1 | row 2  | row 3   | row 4 |
|----------------+-------+--------+---------+-------|
| account_number | 1     | 6      | 13      | 18    |
| firstname      | Amber | Hattie | Nanette | Dale  |
| balance        | 39225 | 5686   | 32838   | 4180  |
| lastname       | Duke  | Bond   | Bates   | Adams |
+----------------+-------+--------+---------+-------+
```
  
## Limitations  

The `transpose` command transforms up to a number of rows specified and if not enough rows found, it shows those transposed rows as null columns.