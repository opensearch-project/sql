# Condition Functions  
PPL functions use the search capabilities of the OpenSearch engine. However, these functions don't execute directly within the OpenSearch plugin's memory. Instead, they facilitate the global filtering of query results based on specific conditions, such as a `WHERE` or `HAVING` clause. 

The following sections describe the condition PPL functions.
## ISNULL  

### Description  

Usage: `isnull(field)` returns TRUE if field is NULL, FALSE otherwise.

The `isnull()` function is commonly used:
- In `eval` expressions to create conditional fields  
- With the `if()` function to provide default values  
- In `where` clauses to filter null records  
  
**Argument type:** All supported data types.
**Return type:** `BOOLEAN`  

### Example
  
```ppl
source=accounts
| eval result = isnull(employer)
| fields result, employer, firstname
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+--------+----------+-----------+
| result | employer | firstname |
|--------+----------+-----------|
| False  | Pyrami   | Amber     |
| False  | Netagy   | Hattie    |
| False  | Quility  | Nanette   |
| True   | null     | Dale      |
+--------+----------+-----------+
```
  
Using with if() to label records
  
```ppl
source=accounts
| eval status = if(isnull(employer), 'unemployed', 'employed')
| fields firstname, employer, status
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+-----------+----------+------------+
| firstname | employer | status     |
|-----------+----------+------------|
| Amber     | Pyrami   | employed   |
| Hattie    | Netagy   | employed   |
| Nanette   | Quility  | employed   |
| Dale      | null     | unemployed |
+-----------+----------+------------+
```
  
Filtering with where clause
  
```ppl
source=accounts
| where isnull(employer)
| fields account_number, firstname, employer
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------------+-----------+----------+
| account_number | firstname | employer |
|----------------+-----------+----------|
| 18             | Dale      | null     |
+----------------+-----------+----------+
```
  
## ISNOTNULL  

### Description  

Usage: `isnotnull(field)` returns TRUE if field is NOT NULL, FALSE otherwise. The `isnotnull(field)` function is the opposite of `isnull(field)`. Instead of checking for null values, it checks a specific field and returns `true` if the field contains data, that is, it is not null.

The `isnotnull()` function is commonly used:
- In `eval` expressions to create boolean flags  
- In `where` clauses to filter out null values  
- With the `if()` function for conditional logic  
- To validate data presence  
  
**Argument type:** All supported data types.  
**Return type:** `BOOLEAN`  
**Synonyms:** [ISPRESENT](#ispresent)  

### Example
  
```ppl
source=accounts
| eval has_employer = isnotnull(employer)
| fields firstname, employer, has_employer
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+-----------+----------+--------------+
| firstname | employer | has_employer |
|-----------+----------+--------------|
| Amber     | Pyrami   | True         |
| Hattie    | Netagy   | True         |
| Nanette   | Quility  | True         |
| Dale      | null     | False        |
+-----------+----------+--------------+
```
  
Filtering with where clause
  
```ppl
source=accounts
| where not isnotnull(employer)
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
  
Using with if() for validation messages
  
```ppl
source=accounts
| eval validation = if(isnotnull(employer), 'valid', 'missing employer')
| fields firstname, employer, validation
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+-----------+----------+------------------+
| firstname | employer | validation       |
|-----------+----------+------------------|
| Amber     | Pyrami   | valid            |
| Hattie    | Netagy   | valid            |
| Nanette   | Quility  | valid            |
| Dale      | null     | missing employer |
+-----------+----------+------------------+
```
  
## EXISTS  

[Since OpenSearch doesn't differentiate null and missing](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-exists-query.html), we can't provide functions like ismissing/isnotmissing to test if a field exists or not. But you can still use isnull/isnotnull for such purpose.
Example, the account 13 doesn't have email field
  
```ppl
source=accounts
| where isnull(email)
| fields account_number, email
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------------+-------+
| account_number | email |
|----------------+-------|
| 13             | null  |
+----------------+-------+
```
  
## IFNULL  

### Description  

Usage: `ifnull(field1, field2)` returns field2 if field1 is null.

**Argument type:** All supported data types (NOTE: if two parameters have different types, you will fail semantic check).  
**Return type:** `any`

### Example
  
```ppl
source=accounts
| eval result = ifnull(employer, 'default')
| fields result, employer, firstname
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+---------+----------+-----------+
| result  | employer | firstname |
|---------+----------+-----------|
| Pyrami  | Pyrami   | Amber     |
| Netagy  | Netagy   | Hattie    |
| Quility | Quility  | Nanette   |
| default | null     | Dale      |
+---------+----------+-----------+
```
  
### Nested IFNULL Pattern  

For OpenSearch versions prior to 3.1, COALESCE-like functionality can be achieved using nested IFNULL statements. This pattern is particularly useful in observability use cases where field names may vary across different data sources.
Usage: `ifnull(field1, ifnull(field2, ifnull(field3, default_value)))`
### Example
  
```ppl
source=accounts
| eval result = ifnull(employer, ifnull(firstname, ifnull(lastname, "unknown")))
| fields result, employer, firstname, lastname
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+---------+----------+-----------+----------+
| result  | employer | firstname | lastname |
|---------+----------+-----------+----------|
| Pyrami  | Pyrami   | Amber     | Duke     |
| Netagy  | Netagy   | Hattie    | Bond     |
| Quility | Quility  | Nanette   | Bates    |
| Dale    | null     | Dale      | Adams    |
+---------+----------+-----------+----------+
```
  
## NULLIF  

### Description  

Usage: `nullif(field1, field2)` returns null if two parameters are same, otherwise returns field1.

**Argument type:** All supported data types (NOTE: if two parameters have different types, you will fail semantic check).  
**Return type:** `any`

### Example
  
```ppl
source=accounts
| eval result = nullif(employer, 'Pyrami')
| fields result, employer, firstname
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+---------+----------+-----------+
| result  | employer | firstname |
|---------+----------+-----------|
| null    | Pyrami   | Amber     |
| Netagy  | Netagy   | Hattie    |
| Quility | Quility  | Nanette   |
| null    | null     | Dale      |
+---------+----------+-----------+
```
  
## IF  

### Description  

Usage: `if(condition, expr1, expr2)` returns expr1 if condition is true, otherwise returns expr2.

**Argument type:** All supported data types (NOTE: if expr1 and expr2 are different types, you will fail semantic check).  
**Return type:** `any`

### Example
  
```ppl
source=accounts
| eval result = if(true, firstname, lastname)
| fields result, firstname, lastname
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+---------+-----------+----------+
| result  | firstname | lastname |
|---------+-----------+----------|
| Amber   | Amber     | Duke     |
| Hattie  | Hattie    | Bond     |
| Nanette | Nanette   | Bates    |
| Dale    | Dale      | Adams    |
+---------+-----------+----------+
```
  
```ppl
source=accounts
| eval result = if(false, firstname, lastname)
| fields result, firstname, lastname
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+--------+-----------+----------+
| result | firstname | lastname |
|--------+-----------+----------|
| Duke   | Amber     | Duke     |
| Bond   | Hattie    | Bond     |
| Bates  | Nanette   | Bates    |
| Adams  | Dale      | Adams    |
+--------+-----------+----------+
```
  
```ppl
source=accounts
| eval is_vip = if(age > 30 AND isnotnull(employer), true, false)
| fields is_vip, firstname, lastname
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+--------+-----------+----------+
| is_vip | firstname | lastname |
|--------+-----------+----------|
| True   | Amber     | Duke     |
| True   | Hattie    | Bond     |
| False  | Nanette   | Bates    |
| False  | Dale      | Adams    |
+--------+-----------+----------+
```
  
## CASE  

### Description  

Usage: `case(condition1, expr1, condition2, expr2, ... conditionN, exprN else default)` returns expr1 if condition1 is true, or returns expr2 if condition2 is true, ... if no condition is true, then returns the value of ELSE clause. If the ELSE clause is not defined, returns NULL.

**Argument type:** All supported data types (NOTE: there is no comma before "else").  
**Return type:** `any`

### Limitations  

When each condition is a field comparison with a numeric literal and each result expression is a string literal, the query will be optimized as [range aggregations](https://docs.opensearch.org/latest/aggregations/bucket/range) if pushdown optimization is enabled. However, this optimization has the following limitations:
- Null values will not be grouped into any bucket of a range aggregation and will be ignored  
- The default ELSE clause will use the string literal `"null"` instead of actual NULL values  
  
### Example
  
```ppl
source=accounts
| eval result = case(age > 35, firstname, age < 30, lastname else employer)
| fields result, firstname, lastname, age, employer
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+--------+-----------+----------+-----+----------+
| result | firstname | lastname | age | employer |
|--------+-----------+----------+-----+----------|
| Pyrami | Amber     | Duke     | 32  | Pyrami   |
| Hattie | Hattie    | Bond     | 36  | Netagy   |
| Bates  | Nanette   | Bates    | 28  | Quility  |
| null   | Dale      | Adams    | 33  | null     |
+--------+-----------+----------+-----+----------+
```
  
```ppl
source=accounts
| eval result = case(age > 35, firstname, age < 30, lastname)
| fields result, firstname, lastname, age
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+--------+-----------+----------+-----+
| result | firstname | lastname | age |
|--------+-----------+----------+-----|
| null   | Amber     | Duke     | 32  |
| Hattie | Hattie    | Bond     | 36  |
| Bates  | Nanette   | Bates    | 28  |
| null   | Dale      | Adams    | 33  |
+--------+-----------+----------+-----+
```
  
```ppl
source=accounts
| where true = case(age > 35, false, age < 30, false else true)
| fields firstname, lastname, age
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+-----------+----------+-----+
| firstname | lastname | age |
|-----------+----------+-----|
| Amber     | Duke     | 32  |
| Dale      | Adams    | 33  |
+-----------+----------+-----+
```
  
## COALESCE  

### Description  

Usage: `coalesce(field1, field2, ...)` returns the first non-null, non-missing value in the argument list.

**Argument type:** All supported data types. Supports mixed data types with automatic type coercion.  
**Return type:** Determined by the least restrictive common type among all arguments, with fallback to string if no common type can be determined.
Behavior:
- Returns the first value that is not null and not missing (missing includes non-existent fields)  
- Empty strings ("") and whitespace strings (" ") are considered valid values  
- If all arguments are null or missing, returns null  
- Automatic type coercion is applied to match the determined return type  
- If type conversion fails, the value is converted to string representation  
- For best results, use arguments of the same data type to avoid unexpected type conversions  
  
Performance Considerations:
- Optimized for multiple field evaluation, more efficient than nested IFNULL patterns  
- Evaluates arguments sequentially, stopping at the first non-null value  
- Consider field order based on likelihood of containing values to minimize evaluation overhead  
  
Limitations:
- Type coercion may result in unexpected string conversions for incompatible types  
- Performance may degrade with very large numbers of arguments  
  
### Example
  
```ppl
source=accounts
| eval result = coalesce(employer, firstname, lastname)
| fields result, firstname, lastname, employer
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+---------+-----------+----------+----------+
| result  | firstname | lastname | employer |
|---------+-----------+----------+----------|
| Pyrami  | Amber     | Duke     | Pyrami   |
| Netagy  | Hattie    | Bond     | Netagy   |
| Quility | Nanette   | Bates    | Quility  |
| Dale    | Dale      | Adams    | null     |
+---------+-----------+----------+----------+
```
  
Empty String Handling Examples
  
```ppl
source=accounts
| eval empty_field = ""
| eval result = coalesce(empty_field, firstname)
| fields result, empty_field, firstname
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+--------+-------------+-----------+
| result | empty_field | firstname |
|--------+-------------+-----------|
|        |             | Amber     |
|        |             | Hattie    |
|        |             | Nanette   |
|        |             | Dale      |
+--------+-------------+-----------+
```
  
```ppl
source=accounts
| eval result = coalesce(" ", firstname)
| fields result, firstname
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+--------+-----------+
| result | firstname |
|--------+-----------|
|        | Amber     |
|        | Hattie    |
|        | Nanette   |
|        | Dale      |
+--------+-----------+
```
  
Mixed Data Types with Auto Coercion
  
```ppl
source=accounts
| eval result = coalesce(employer, balance, "fallback")
| fields result, employer, balance
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+---------+----------+---------+
| result  | employer | balance |
|---------+----------+---------|
| Pyrami  | Pyrami   | 39225   |
| Netagy  | Netagy   | 5686    |
| Quility | Quility  | 32838   |
| 4180    | null     | 4180    |
+---------+----------+---------+
```
  
Non-existent Field Handling
  
```ppl
source=accounts
| eval result = coalesce(nonexistent_field, firstname, "unknown")
| fields result, firstname
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+---------+-----------+
| result  | firstname |
|---------+-----------|
| Amber   | Amber     |
| Hattie  | Hattie    |
| Nanette | Nanette   |
| Dale    | Dale      |
+---------+-----------+
```
  
## ISPRESENT  

### Description  

Usage: `ispresent(field)` returns true if the field exists.

**Argument type:** All supported data types.  
**Return type:** `BOOLEAN`  
**Synonyms:** [ISNOTNULL](#isnotnull)

### Example
  
```ppl
source=accounts
| where ispresent(employer)
| fields employer, firstname
```
  
Expected output:
  
```text
fetched rows / total rows = 3/3
+----------+-----------+
| employer | firstname |
|----------+-----------|
| Pyrami   | Amber     |
| Netagy   | Hattie    |
| Quility  | Nanette   |
+----------+-----------+
```
  
## ISBLANK  

### Description  

Usage: `isblank(field)` returns true if the field is null, an empty string, or contains only white space.

**Argument type:** All supported data types.  
**Return type:** `BOOLEAN`

### Example
  
```ppl
source=accounts
| eval temp = ifnull(employer, '   ')
| eval `isblank(employer)` = isblank(employer), `isblank(temp)` = isblank(temp)
| fields `isblank(temp)`, temp, `isblank(employer)`, employer
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+---------------+---------+-------------------+----------+
| isblank(temp) | temp    | isblank(employer) | employer |
|---------------+---------+-------------------+----------|
| False         | Pyrami  | False             | Pyrami   |
| False         | Netagy  | False             | Netagy   |
| False         | Quility | False             | Quility  |
| True          |         | True              | null     |
+---------------+---------+-------------------+----------+
```
  
## ISEMPTY  

### Description  

Usage: `isempty(field)` returns true if the field is null or is an empty string.

**Argument type:** All supported data types.  
**Return type:** `BOOLEAN`

### Example
  
```ppl
source=accounts
| eval temp = ifnull(employer, '   ')
| eval `isempty(employer)` = isempty(employer), `isempty(temp)` = isempty(temp)
| fields `isempty(temp)`, temp, `isempty(employer)`, employer
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+---------------+---------+-------------------+----------+
| isempty(temp) | temp    | isempty(employer) | employer |
|---------------+---------+-------------------+----------|
| False         | Pyrami  | False             | Pyrami   |
| False         | Netagy  | False             | Netagy   |
| False         | Quility | False             | Quility  |
| False         |         | True              | null     |
+---------------+---------+-------------------+----------+
```
  
## EARLIEST  

### Description  

Usage: `earliest(relative_string, field)` returns true if the value of field is after the timestamp derived from relative_string relative to the current time. Otherwise, returns false.
relative_string: 
The relative string can be one of the following formats:
1. `"now"` or `"now()"`:  
  
   Uses the current system time.
2. Absolute format (`MM/dd/yyyy:HH:mm:ss` or `yyyy-MM-dd HH:mm:ss`):  
  
   Converts the string to a timestamp and compares it with the data.
3. Relative format: `(+|-)<time_integer><time_unit>[+<...>]@<snap_unit>`  
  
   Steps to specify a relative time:
   - **a. Time offset:** Indicate the offset from the current time using `+` or `-`.  
   - **b. Time amount:** Provide a numeric value followed by a time unit (`s`, `m`, `h`, `d`, `w`, `M`, `y`).  
   - **c. Snap to unit:** Optionally specify a snap unit with `@<unit>` to round the result down to the nearest unit (e.g., hour, day, month).  
  
   **Examples** (assuming current time is `2025-05-28 14:28:34`):
   - `-3d+2y` → `2027-05-25 14:28:34`  
   - `+1d@m` → `2025-05-29 14:28:00`  
   - `-3M+1y@M` → `2026-02-01 00:00:00`  
  
Read more details [here](https://github.com/opensearch-project/opensearch-spark/blob/main/docs/ppl-lang/functions/ppl-datetime.md#relative_timestamp)

**Argument type:** `relative_string`: `STRING`, `field`: `TIMESTAMP`  
**Return type:** `BOOLEAN`

### Example
  
```ppl
source=accounts
| eval now = utc_timestamp()
| eval a = earliest("now", now), b = earliest("-2d@d", now)
| fields a, b
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-------+------+
| a     | b    |
|-------+------|
| False | True |
+-------+------+
```
  
```ppl
source=nyc_taxi
| where earliest('07/01/2014:00:30:00', timestamp)
| stats COUNT() as cnt
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----+
| cnt |
|-----|
| 972 |
+-----+
```
  
## LATEST  

### Description  

Usage: `latest(relative_string, field)` returns true if the value of field is before the timestamp derived from relative_string relative to the current time. Otherwise, returns false.

**Argument type:** `relative_string`: `STRING`, `field`: `TIMESTAMP`  
**Return type:** `BOOLEAN`

### Example
  
```ppl
source=accounts
| eval now = utc_timestamp()
| eval a = latest("now", now), b = latest("+2d@d", now)
| fields a, b
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+------+------+
| a    | b    |
|------+------|
| True | True |
+------+------+
```
  
```ppl
source=nyc_taxi
| where latest('07/21/2014:04:00:00', timestamp)
| stats COUNT() as cnt
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----+
| cnt |
|-----|
| 969 |
+-----+
```
  
## REGEXP_MATCH  

### Description  

Usage: `regexp_match(string, pattern)` returns true if the regular expression pattern finds a match against any substring of the string value, otherwise returns false.
The function uses Java regular expression syntax for the pattern.

**Argument type:** `STRING`, `STRING`  
**Return type:** `BOOLEAN`

### Example
  
``` ppl ignore
source=logs | where regexp_match(message, 'ERROR|WARN|FATAL') | fields timestamp, message
```
  
```text
fetched rows / total rows = 3/100
+---------------------+------------------------------------------+
| timestamp           | message                                  |
|---------------------+------------------------------------------|
| 2024-01-15 10:23:45 | ERROR: Connection timeout to database   |
| 2024-01-15 10:24:12 | WARN: High memory usage detected        |
| 2024-01-15 10:25:33 | FATAL: System crashed unexpectedly      |
+---------------------+------------------------------------------+
```
  
``` ppl ignore
source=users | where regexp_match(email, '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}') | fields name, email
```
  
```text
fetched rows / total rows = 2/3
+-------+----------------------+
| name  | email                |
|-------+----------------------|
| John  | john@example.com     |
| Alice | alice@company.org    |
+-------+----------------------+
```
  
```ppl ignore
source=network | where regexp_match(ip_address, '^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$') AND NOT regexp_match(ip_address, '^(10\.|172\.(1[6-9]|2[0-9]|3[01])\.|192\.168\.)') | fields ip_address, status
```
  
```text
fetched rows / total rows = 2/10
+---------------+--------+
| ip_address    | status |
|---------------+--------|
| 8.8.8.8       | active |
| 1.1.1.1       | active |
+---------------+--------+
```
  
```ppl ignore
source=products | eval category = if(regexp_match(name, '(?i)(laptop|computer|desktop)'), 'Computing', if(regexp_match(name, '(?i)(phone|tablet|mobile)'), 'Mobile', 'Other')) | fields name, category
```
  
```text
fetched rows / total rows = 4/4
+------------------------+----------+
| name                   | category |
|------------------------+----------|
| Dell Laptop XPS        | Computing|
| iPhone 15 Pro          | Mobile   |
| Wireless Mouse         | Other    |
| Desktop Computer Tower | Computing|
+------------------------+----------+
```