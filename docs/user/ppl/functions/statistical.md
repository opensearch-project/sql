# Statistical Functions  

## MAX  

### Description  

Usage: `max(x, y, ...)` returns the maximum value from all provided arguments. Strings are treated as greater than numbers, so if provided both strings and numbers, it will return the maximum string value (lexicographically ordered).

Note: This function is only available in the eval command context.

**Argument type:** Variable number of `INTEGER`/`LONG`/`FLOAT`/`DOUBLE`/`STRING` arguments  
**Return type:** Type of the selected argument  

### Example
  
```ppl
source=accounts
| eval max_val = MAX(age, 30)
| fields age, max_val
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+-----+---------+
| age | max_val |
|-----+---------|
| 32  | 32      |
| 36  | 36      |
| 28  | 30      |
| 33  | 33      |
+-----+---------+
```
  
```ppl
source=accounts
| eval result = MAX(firstname, 'John')
| fields firstname, result
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+-----------+---------+
| firstname | result  |
|-----------+---------|
| Amber     | John    |
| Hattie    | John    |
| Nanette   | Nanette |
| Dale      | John    |
+-----------+---------+
```
  
```ppl
source=accounts
| eval result = MAX(age, 35, 'John', firstname)
| fields age, firstname, result
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+-----+-----------+---------+
| age | firstname | result  |
|-----+-----------+---------|
| 32  | Amber     | John    |
| 36  | Hattie    | John    |
| 28  | Nanette   | Nanette |
| 33  | Dale      | John    |
+-----+-----------+---------+
```
  
## MIN  

### Description  

Usage: `min(x, y, ...)` returns the minimum value from all provided arguments. Strings are treated as greater than numbers, so if provided both strings and numbers, it will return the minimum numeric value.

Note: This function is only available in the eval command context.

**Argument type:** Variable number of `INTEGER`/`LONG`/`FLOAT`/`DOUBLE`/`STRING` arguments  
**Return type:** Type of the selected argument  

### Example
  
```ppl
source=accounts
| eval min_val = MIN(age, 30)
| fields age, min_val
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+-----+---------+
| age | min_val |
|-----+---------|
| 32  | 30      |
| 36  | 30      |
| 28  | 28      |
| 33  | 30      |
+-----+---------+
```
  
```ppl
source=accounts
| eval result = MIN(firstname, 'John')
| fields firstname, result
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+-----------+--------+
| firstname | result |
|-----------+--------|
| Amber     | Amber  |
| Hattie    | Hattie |
| Nanette   | John   |
| Dale      | Dale   |
+-----------+--------+
```
  
```ppl
source=accounts
| eval result = MIN(age, 35, firstname)
| fields age, firstname, result
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+-----+-----------+--------+
| age | firstname | result |
|-----+-----------+--------|
| 32  | Amber     | 32     |
| 36  | Hattie    | 35     |
| 28  | Nanette   | 28     |
| 33  | Dale      | 33     |
+-----+-----------+--------+
```
  
