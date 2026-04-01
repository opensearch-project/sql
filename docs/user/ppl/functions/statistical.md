# Statistical functions

The following statistical functions are supported in PPL.

## MAX

**Usage**: `MAX(x, y, ...)`

Returns the largest value among the provided arguments. When both strings and numbers are supplied, strings are considered greater than numbers, and the function returns the lexicographically greatest string. This function is available only in the `eval` command.

**Parameters**:

- `x, y, ...` (Required): Variable number of arguments of type `INTEGER`, `LONG`, `FLOAT`, `DOUBLE`, or `STRING`.

**Return type**: Type of the selected argument

### Example
  
```ppl
source=accounts
| eval max_val = MAX(age, 30)
| fields age, max_val
```
  
The query returns the following results:
  
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
  
The query returns the following results:
  
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
  
The query returns the following results:
  
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

**Usage**: `MIN(x, y, ...)`

Returns the smallest value among the provided arguments. When both strings and numbers are supplied, numbers are considered smaller than strings, and the function returns the minimum numeric value. This function is available only in the `eval` command.

**Parameters**:

- `x, y, ...` (Required): Variable number of arguments of type `INTEGER`, `LONG`, `FLOAT`, `DOUBLE`, or `STRING`.

**Return type**: Type of the selected argument

### Example
  
```ppl
source=accounts
| eval min_val = MIN(age, 30)
| fields age, min_val
```
  
The query returns the following results:
  
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
  
The query returns the following results:
  
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
  
The query returns the following results:
  
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
  
