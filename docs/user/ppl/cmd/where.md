# where


The `where` command filters the search results. The `where` command only returns the result when the bool-expression evaluates to true.

## Syntax

Use the following syntax:

`where <boolean-expression>`  
* `bool-expression`: optional. Any expression which could be evaluated to boolean value.  
  

## Example 1: Filter search results with condition  

The following example PPL query shows how to use `where` to fetch all the documents from the accounts index where account_number is 1 or gender is "F".
  
```ppl
source=accounts
| where account_number=1 or gender="F"
| fields account_number, gender
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+----------------+--------+
| account_number | gender |
|----------------+--------|
| 1              | M      |
| 13             | F      |
+----------------+--------+
```
  

## Example 2: Basic field Comparison  

The following example PPL query shows how to use `where` to filter accounts with balance greater than 30000.
  
```ppl
source=accounts
| where balance > 30000
| fields account_number, balance
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+----------------+---------+
| account_number | balance |
|----------------+---------|
| 1              | 39225   |
| 13             | 32838   |
+----------------+---------+
```
  

## Example 3: Pattern matching with LIKE  

Pattern Matching with Underscore (\_)
The following example PPL query demonstrates using LIKE with underscore (\_) to match a single character.
  
```ppl
source=accounts
| where LIKE(state, 'M_')
| fields account_number, state
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------------+-------+
| account_number | state |
|----------------+-------|
| 18             | MD    |
+----------------+-------+
```
  
Pattern Matching with Percent (%)
The following example PPL query demonstrates using LIKE with percent (%) to match multiple characters.
  
```ppl
source=accounts
| where LIKE(state, 'V%')
| fields account_number, state
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------------+-------+
| account_number | state |
|----------------+-------|
| 13             | VA    |
+----------------+-------+
```
  

## Example 4: Multiple conditions  

The following example PPL query shows how to combine multiple conditions using AND operator.
  
```ppl
source=accounts
| where age > 30 AND gender = 'M'
| fields account_number, age, gender
```
  
Expected output:
  
```text
fetched rows / total rows = 3/3
+----------------+-----+--------+
| account_number | age | gender |
|----------------+-----+--------|
| 1              | 32  | M      |
| 6              | 36  | M      |
| 18             | 33  | M      |
+----------------+-----+--------+
```
  

## Example 5: Using IN operator  

The following example PPL query demonstrates using IN operator to match multiple values.
  
```ppl
source=accounts
| where state IN ('IL', 'VA')
| fields account_number, state
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+----------------+-------+
| account_number | state |
|----------------+-------|
| 1              | IL    |
| 13             | VA    |
+----------------+-------+
```
  

## Example 6: NULL Checks  

The following example PPL query shows how to filter records with NULL values.
  
```ppl
source=accounts
| where ISNULL(employer)
| fields account_number, employer
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------------+----------+
| account_number | employer |
|----------------+----------|
| 18             | null     |
+----------------+----------+
```
  

## Example 7: Complex conditions  

The following example PPL query demonstrates combining multiple conditions with parentheses and logical operators.
  
```ppl
source=accounts
| where (balance > 40000 OR age > 35) AND gender = 'M'
| fields account_number, balance, age, gender
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------------+---------+-----+--------+
| account_number | balance | age | gender |
|----------------+---------+-----+--------|
| 6              | 5686    | 36  | M      |
+----------------+---------+-----+--------+
```
  

## Example 8: NOT conditions  

The following example PPL query shows how to use NOT operator to exclude matching records.
  
```ppl
source=accounts
| where NOT state = 'CA'
| fields account_number, state
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------------+-------+
| account_number | state |
|----------------+-------|
| 1              | IL    |
| 6              | TN    |
| 13             | VA    |
| 18             | MD    |
+----------------+-------+
```
  
