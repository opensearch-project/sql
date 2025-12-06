# replace  

## Description  

The `replace` replaces text in one or more fields in the search result. Supports literal string replacement and wildcard patterns using `*`.
## Syntax  

replace '\<pattern\>' WITH '\<replacement\>' [, '\<pattern\>' WITH '\<replacement\>']... IN \<field-name\>[, \<field-name\>]...
* pattern: mandatory. The text pattern you want to replace.  
* replacement: mandatory. The text you want to replace with.  
* field-name: mandatory. One or more field names where the replacement should occur.  
  
## Example 1: Replace text in one field  

This example shows replacing text in one field.
  
```ppl
source=accounts
| replace "IL" WITH "Illinois" IN state
| fields state
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------+
| state    |
|----------|
| Illinois |
| TN       |
| VA       |
| MD       |
+----------+
```
  
## Example 2: Replace text in multiple fields  

This example shows replacing text in multiple fields.
  
```ppl
source=accounts
| replace "IL" WITH "Illinois" IN state, address
| fields state, address
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------+----------------------+
| state    | address              |
|----------+----------------------|
| Illinois | 880 Holmes Lane      |
| TN       | 671 Bristol Street   |
| VA       | 789 Madison Street   |
| MD       | 467 Hutchinson Court |
+----------+----------------------+
```
  
## Example 3: Replace with other commands in a pipeline  

This example shows using replace with other commands in a query pipeline.
  
```ppl
source=accounts
| replace "IL" WITH "Illinois" IN state
| where age > 30
| fields state, age
```
  
Expected output:
  
```text
fetched rows / total rows = 3/3
+----------+-----+
| state    | age |
|----------+-----|
| Illinois | 32  |
| TN       | 36  |
| MD       | 33  |
+----------+-----+
```
  
## Example 4: Replace with multiple pattern/replacement pairs  

This example shows using multiple pattern/replacement pairs in a single replace command. The replacements are applied sequentially.
  
```ppl
source=accounts
| replace "IL" WITH "Illinois", "TN" WITH "Tennessee" IN state
| fields state
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+-----------+
| state     |
|-----------|
| Illinois  |
| Tennessee |
| VA        |
| MD        |
+-----------+
```
  
## Example 5: Pattern matching with LIKE and replace  

Since replace command only supports plain string literals, you can use LIKE command with replace for pattern matching needs.
  
```ppl
source=accounts
| where LIKE(address, '%Holmes%')
| replace "Holmes" WITH "HOLMES" IN address
| fields address, state, gender, age, city
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----------------+-------+--------+-----+--------+
| address         | state | gender | age | city   |
|-----------------+-------+--------+-----+--------|
| 880 HOLMES Lane | IL    | M      | 32  | Brogan |
+-----------------+-------+--------+-----+--------+
```
  
## Example 6: Wildcard suffix match  

Replace values that end with a specific pattern. The wildcard `*` matches any prefix.
  
```ppl
source=accounts
| replace "*IL" WITH "Illinois" IN state
| fields state
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------+
| state    |
|----------|
| Illinois |
| TN       |
| VA       |
| MD       |
+----------+
```
  
## Example 7: Wildcard prefix match  

Replace values that start with a specific pattern. The wildcard `*` matches any suffix.
  
```ppl
source=accounts
| replace "IL*" WITH "Illinois" IN state
| fields state
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------+
| state    |
|----------|
| Illinois |
| TN       |
| VA       |
| MD       |
+----------+
```
  
## Example 8: Wildcard capture and substitution  

Use wildcards in both pattern and replacement to capture and reuse matched portions. The number of wildcards must match in pattern and replacement.
  
```ppl
source=accounts
| replace "* Lane" WITH "Lane *" IN address
| fields address
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------------------+
| address              |
|----------------------|
| Lane 880 Holmes      |
| 671 Bristol Street   |
| 789 Madison Street   |
| 467 Hutchinson Court |
+----------------------+
```
  
## Example 9: Multiple wildcards for pattern transformation  

Use multiple wildcards to transform patterns. Each wildcard in the replacement substitutes the corresponding captured value.
  
```ppl
source=accounts
| replace "* *" WITH "*_*" IN address
| fields address
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------------------+
| address              |
|----------------------|
| 880_Holmes Lane      |
| 671_Bristol Street   |
| 789_Madison Street   |
| 467_Hutchinson Court |
+----------------------+
```
  
## Example 10: Wildcard with zero wildcards in replacement  

When replacement has zero wildcards, all matching values are replaced with the literal replacement string.
  
```ppl
source=accounts
| replace "*IL*" WITH "Illinois" IN state
| fields state
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------+
| state    |
|----------|
| Illinois |
| TN       |
| VA       |
| MD       |
+----------+
```
  
## Example 11: Matching literal asterisks  

Use `\*` to match literal asterisk characters (`\*` = literal asterisk, `\\` = literal backslash).
  
```ppl
source=accounts
| eval note = 'price: *sale*'
| replace 'price: \*sale\*' WITH 'DISCOUNTED' IN note
| fields note
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+------------+
| note       |
|------------|
| DISCOUNTED |
| DISCOUNTED |
| DISCOUNTED |
| DISCOUNTED |
+------------+
```
  
## Example 12: Wildcard with no replacement wildcards  

Use wildcards in pattern but none in replacement to create a fixed output.
  
```ppl
source=accounts
| eval test = 'prefix-value-suffix'
| replace 'prefix-*-suffix' WITH 'MATCHED' IN test
| fields test
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+---------+
| test    |
|---------|
| MATCHED |
| MATCHED |
| MATCHED |
| MATCHED |
+---------+
```
  
## Example 13: Escaped asterisks with wildcards  

Combine escaped asterisks (literal) with wildcards for complex patterns.
  
```ppl
source=accounts
| eval label = 'file123.txt'
| replace 'file*.*' WITH '\**.*' IN label
| fields label
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------+
| label    |
|----------|
| *123.txt |
| *123.txt |
| *123.txt |
| *123.txt |
+----------+
```
  
## Limitations  

* Wildcards: `*` matches zero or more characters (case-sensitive)  
* Replacement wildcards must match pattern wildcard count, or be zero  
* Escape sequences: `\*` (literal asterisk), `\\` (literal backslash)  