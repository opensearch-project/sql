# sort


The `sort` command sorts all the search results by the specified fields.

## Syntax

Use the following syntax:

`sort [count] <[+|-] sort-field | sort-field [asc|a|desc|d]>...`
* `count`: optional. The number of results to return. Specifying a count of 0 or less than 0 returns all results. **Default:** 0.  
* `[+|-]`: optional. The plus [+] stands for ascending order and NULL/MISSING first and a minus [-] stands for descending order and NULL/MISSING last. **Default:** ascending order and NULL/MISSING first.  
* `[asc|a|desc|d]`: optional. asc/a stands for ascending order and NULL/MISSING first. desc/d stands for descending order and NULL/MISSING last. **Default:** ascending order and NULL/MISSING first.  
* `sort-field`: mandatory. The field used to sort. Can use `auto(field)`, `str(field)`, `ip(field)`, or `num(field)` to specify how to interpret field values.  
  
> **Note:**
> You cannot mix +/- and asc/desc in the same sort command. Choose one approach for all fields in a single sort command.
>
>

## Example 1: Sort by one field  

The following example PPL query shows how to use `sort` to sort all documents by age field in ascending order.
  
```ppl
source=accounts
| sort age
| fields account_number, age
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------------+-----+
| account_number | age |
|----------------+-----|
| 13             | 28  |
| 1              | 32  |
| 18             | 33  |
| 6              | 36  |
+----------------+-----+
```
  

## Example 2: Sort by one field return all the result  

The following example PPL query shows how to use `sort` to sort all documents by age field in ascending order and return all results.
  
```ppl
source=accounts
| sort 0 age
| fields account_number, age
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------------+-----+
| account_number | age |
|----------------+-----|
| 13             | 28  |
| 1              | 32  |
| 18             | 33  |
| 6              | 36  |
+----------------+-----+
```
  

## Example 3: Sort by one field in descending order (using -)  

The following example PPL query shows how to use `sort` to sort all documents by age field in descending order.
  
```ppl
source=accounts
| sort - age
| fields account_number, age
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------------+-----+
| account_number | age |
|----------------+-----|
| 6              | 36  |
| 18             | 33  |
| 1              | 32  |
| 13             | 28  |
+----------------+-----+
```
  

## Example 4: Sort by one field in descending order (using desc)  

The following example PPL query shows how to use `sort` to sort all documents by the age field in descending order using the desc keyword.
  
```ppl
source=accounts
| sort age desc
| fields account_number, age
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------------+-----+
| account_number | age |
|----------------+-----|
| 6              | 36  |
| 18             | 33  |
| 1              | 32  |
| 13             | 28  |
+----------------+-----+
```
  

## Example 5: Sort by multiple fields (using +/-)  

The following example PPL query shows how to use `sort` to sort all documents by gender field in ascending order and age field in descending order using +/- operators.
  
```ppl
source=accounts
| sort + gender, - age
| fields account_number, gender, age
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------------+--------+-----+
| account_number | gender | age |
|----------------+--------+-----|
| 13             | F      | 28  |
| 6              | M      | 36  |
| 18             | M      | 33  |
| 1              | M      | 32  |
+----------------+--------+-----+
```
  

## Example 6: Sort by multiple fields (using asc/desc)  

The following example PPL query shows how to use `sort` to sort all documents by the gender field in ascending order and age field in descending order using asc/desc keywords.
  
```ppl
source=accounts
| sort gender asc, age desc
| fields account_number, gender, age
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------------+--------+-----+
| account_number | gender | age |
|----------------+--------+-----|
| 13             | F      | 28  |
| 6              | M      | 36  |
| 18             | M      | 33  |
| 1              | M      | 32  |
+----------------+--------+-----+
```
  

## Example 7: Sort by field include null value  

The following example PPL query shows how to use `sort` to sort employer field by default option (ascending order and null first). The result shows that null value is in the first row.
  
```ppl
source=accounts
| sort employer
| fields employer
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------+
| employer |
|----------|
| null     |
| Netagy   |
| Pyrami   |
| Quility  |
+----------+
```
  

## Example 8: Specify the number of sorted documents to return  

The following example PPL query shows how to use `sort` to sort all documents and return 2 documents.
  
```ppl
source=accounts
| sort 2 age
| fields account_number, age
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+----------------+-----+
| account_number | age |
|----------------+-----|
| 13             | 28  |
| 1              | 32  |
+----------------+-----+
```
  

## Example 9: Sort with desc modifier  

The following example PPL query shows how to use `sort` to sort with the desc modifier to reverse sort order.
  
```ppl
source=accounts
| sort age desc
| fields account_number, age
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------------+-----+
| account_number | age |
|----------------+-----|
| 6              | 36  |
| 18             | 33  |
| 1              | 32  |
| 13             | 28  |
+----------------+-----+
```
  

## Example 10: Sort with specifying field type  

The following example PPL query shows how to use `sort` to sort with str() to sort numeric values lexicographically.
  
```ppl
source=accounts
| sort str(account_number)
| fields account_number
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------------+
| account_number |
|----------------|
| 1              |
| 13             |
| 18             |
| 6              |
+----------------+
```
  