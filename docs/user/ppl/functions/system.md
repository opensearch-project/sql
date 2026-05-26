# System functions

The following system functions are supported in PPL.

## TYPEOF

**Usage**: `TYPEOF(expr)`

Returns the data type of the given expression. This is useful for troubleshooting or dynamically constructing SQL queries.

**Parameters**:

- `expr` (Required): The expression to determine the data type for. Can be any data type.

**Return type**: `STRING`

### Example  
  
```ppl
source=people
| eval `typeof(date)` = typeof(DATE('2008-04-14')), `typeof(int)` = typeof(1), `typeof(now())` = typeof(now()), `typeof(column)` = typeof(accounts)
| fields `typeof(date)`, `typeof(int)`, `typeof(now())`, `typeof(column)`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------+-------------+---------------+----------------+
| typeof(date) | typeof(int) | typeof(now()) | typeof(column) |
|--------------+-------------+---------------+----------------|
| DATE         | INT         | TIMESTAMP     | STRUCT         |
+--------------+-------------+---------------+----------------+
```
  
