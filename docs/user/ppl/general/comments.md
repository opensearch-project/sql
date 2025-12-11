# Comments  

Comments are not evaluated texts. PPL supports both line comments and block comments.
## Line Comments  

Line comments begin with two slashes ( // ) and end with a new line.  
Example
  
```ppl
source=accounts
| top gender // finds most common gender of all the accounts
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+--------+-------+
| gender | count |
|--------+-------|
| M      | 3     |
| F      | 1     |
+--------+-------+
```
  
## Block Comments  

Block comments begin with a slash followed by an asterisk ( /\* ) and end with an asterisk followed by a slash ( \*/ ).  
Example
  
```ppl
source=accounts
| dedup 2 gender /* dedup the document with gender field keep 2 duplication */
| fields account_number, gender
```
  
Expected output:
  
```text
fetched rows / total rows = 3/3
+----------------+--------+
| account_number | gender |
|----------------+--------|
| 13             | F      |
| 1              | M      |
| 6              | M      |
+----------------+--------+
```
  