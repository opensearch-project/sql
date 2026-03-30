# Conditional functions

PPL conditional functions enable global filtering of query results based on specific conditions, such as `WHERE` or `HAVING` clauses. These functions use the search capabilities of the OpenSearch engine but don't execute directly within the OpenSearch plugin's memory.
## ISNULL

**Usage**: `isnull(field)`

Returns `TRUE` if the field is `NULL`, `FALSE` otherwise.

The `field IS NULL` predicate syntax is also supported as a synonym.

The `isnull()` function is commonly used:
- In `eval` expressions to create conditional fields.
- With the `if()` function to provide default values.
- In `where` clauses to filter null records.

**Parameters**:

- `field` (Required): The field to check for null values.

**Return type**: `BOOLEAN`

#### Example
  
```ppl
source=accounts
| eval result = isnull(employer)
| fields result, employer, firstname
```
  
The query returns the following results:
  
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
  
The following example demonstrates using `isnull` with the `if` function to create conditional labels:
  
```ppl
source=accounts
| eval status = if(isnull(employer), 'unemployed', 'employed')
| fields firstname, employer, status
```
  
The query returns the following results:
  
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
  
The following example filters records using `isnull` in a `where` clause:

```ppl
source=accounts
| where isnull(employer)
| fields account_number, firstname, employer
```

The `IS NULL` predicate syntax can be used as an equivalent alternative:

```ppl
source=accounts
| where employer IS NULL
| fields account_number, firstname, employer
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------+-----------+----------+
| account_number | firstname | employer |
|----------------+-----------+----------|
| 18             | Dale      | null     |
+----------------+-----------+----------+
```
  
## ISNOTNULL

**Usage**: `isnotnull(field)`

Returns `TRUE` if the field is NOT `NULL`, `FALSE` otherwise.

The `field IS NOT NULL` predicate syntax is also supported as a synonym.

The `isnotnull()` function is commonly used:
- In `eval` expressions to create Boolean flags.
- In `where` clauses to filter out null values.
- With the `if()` function for conditional logic.
- To validate data presence.

**Synonyms**: [ISPRESENT](#ispresent)

**Parameters**:

- `field` (Required): The field to check for non-null values.

**Return type**: `BOOLEAN`

#### Example
  
```ppl
source=accounts
| eval has_employer = isnotnull(employer)
| fields firstname, employer, has_employer
```
  
The query returns the following results:
  
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
  
The following example shows how to filter records using `isnotnull` in a `where` clause:

```ppl
source=accounts
| where not isnotnull(employer)
| fields account_number, employer
```

The `IS NOT NULL` predicate syntax can be used as an equivalent alternative:

```ppl
source=accounts
| where employer IS NOT NULL
| fields account_number, employer
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------+----------+
| account_number | employer |
|----------------+----------|
| 18             | null     |
+----------------+----------+
```
  
The following example demonstrates using `isnotnull` with the `if` function to create validation messages:

```ppl
source=accounts
| eval validation = if(isnotnull(employer), 'valid', 'missing employer')
| fields firstname, employer, validation
```
  
The query returns the following results:
  
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

**Usage**: Use `isnull(field)` or `isnotnull(field)` to test field existence

Since OpenSearch doesn't differentiate between null and missing values, functions like `ismissing`/`isnotmissing` are not available. Use `isnull`/`isnotnull` to test field existence instead.

#### Example

The following example shows account 13, which doesn't contain an `email` field:
  
```ppl
source=accounts
| where isnull(email)
| fields account_number, email
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------+-------+
| account_number | email |
|----------------+-------|
| 13             | null  |
+----------------+-------+
```
  
## IFNULL

**Usage**: `ifnull(field1, field2)`

Returns `field2` if `field1` is `NULL`.

**Parameters**:

- `field1` (Required): The field to check for `NULL` values.
- `field2` (Required): The value to return if `field1` is `NULL`.

**Return type**: Any (matches input types)

#### Example
  
```ppl
source=accounts
| eval result = ifnull(employer, 'default')
| fields result, employer, firstname
```
  
The query returns the following results:
  
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
  
#### Nested ifnull pattern

For OpenSearch versions prior to 3.1, `coalesce`-like functionality can be achieved using nested `ifnull` statements. This pattern is particularly useful in observability use cases where field names may vary across different data sources.
Usage: `ifnull(field1, ifnull(field2, ifnull(field3, default_value)))`

#### Example
  
```ppl
source=accounts
| eval result = ifnull(employer, ifnull(firstname, ifnull(lastname, "unknown")))
| fields result, employer, firstname, lastname
```
  
The query returns the following results:
  
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

**Usage**: `nullif(field1, field2)`

Returns `NULL` if the two parameters are the same, otherwise returns `field1`.

**Parameters**:

- `field1` (Required): The field to return if different from `field2`.
- `field2` (Required): The value to compare against `field1`.

**Return type**: Any (matches `field1` type)

#### Example
  
```ppl
source=accounts
| eval result = nullif(employer, 'Pyrami')
| fields result, employer, firstname
```
  
The query returns the following results:
  
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

**Usage**: `if(condition, expr1, expr2)`

Returns `expr1` if the condition is `true`, otherwise returns `expr2`.

**Parameters**:

- `condition` (Required): The Boolean expression to evaluate.
- `expr1` (Required): The value to return if the condition is `true`.
- `expr2` (Required): The value to return if the condition is `false`.

**Return type**: Least restrictive common type of `expr1` and `expr2`

#### Example

The following example returns the first name when the condition is `true`:
  
```ppl
source=accounts
| eval result = if(true, firstname, lastname)
| fields result, firstname, lastname
```
  
The query returns the following results:
  
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

The following example returns the last name when the condition is `false`:

```ppl
source=accounts
| eval result = if(false, firstname, lastname)
| fields result, firstname, lastname
```
  
The query returns the following results:
  
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

The following example uses a complex condition to determine VIP status:

```ppl
source=accounts
| eval is_vip = if(age > 30 AND isnotnull(employer), true, false)
| fields is_vip, firstname, lastname
```


The query returns the following results:
  
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

**Usage**: `case(condition1, expr1, condition2, expr2, ... conditionN, exprN else default)`

Returns `expr1` if `condition1` is `true`, `expr2` if `condition2` is `true`, and so on. If no condition is `true`, returns the value of the `else` clause. If the `else` clause is not defined, returns `NULL`.

**Parameters**:

- `condition1, condition2, ..., conditionN` (Required): Boolean expressions to evaluate in sequence.
- `expr1, expr2, ..., exprN` (Required): Values to return when the corresponding condition is `true`.
- `default` (Optional): The value to return when no condition is `true`. If not specified, returns `NULL`.

**Return type**: Least restrictive common type of all result expressions

#### Limitations

When each condition is a field comparison against a numeric literal and each result expression is a string literal, the query is optimized as [range aggregations](https://docs.opensearch.org/latest/aggregations/bucket/range/) if pushdown optimization is enabled. However, this optimization has the following limitations:
- `NULL` values are not grouped into any bucket of a range aggregation and are ignored.
- The default `else` clauses use the string literal `"null"` instead of actual NULL values.  
  
#### Example

The following example demonstrates a case statement with an else clause:

```ppl
source=accounts
| eval result = case(age > 35, firstname, age < 30, lastname else employer)
| fields result, firstname, lastname, age, employer
```
  
The query returns the following results:
  
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

The following example demonstrates a case statement without an else clause:

```ppl
source=accounts
| eval result = case(age > 35, firstname, age < 30, lastname)
| fields result, firstname, lastname, age
```
  
The query returns the following results:
  
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

The following example uses case in a where clause to filter records:

```ppl
source=accounts
| where true = case(age > 35, false, age < 30, false else true)
| fields firstname, lastname, age
```
  
The query returns the following results:
  
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

**Usage**: `coalesce(field1, field2, ...)`

Returns the first non-null, non-missing value in the parameter list.

**Parameters**:

- `field1, field2, ...` (Required): Fields or expressions to evaluate for non-null values.

**Return type**: Least restrictive common type of all input parameters

**Behavior**:
- Returns the first value that is not `NULL` and not missing (missing includes non-existent fields).
- Empty strings (`""`) and whitespace strings (`" "`) are considered valid values.
- If all parameters are `NULL` or missing, returns `NULL`.
- Automatic type coercion is applied to match the determined return type.
- If type conversion fails, the value is converted to string representation.
- For best results, use parameters of the same data type to avoid unexpected type conversions.

**Performance considerations**:
- Optimized for multiple field evaluation, more efficient than nested `ifnull` patterns.
- Evaluates parameters sequentially, stopping at the first non-null value.
- Consider field order based on likelihood of containing values to minimize evaluation overhead.

**Limitations**:
- Type coercion may result in unexpected string conversions for incompatible types.
- Performance may degrade when using large numbers of arguments.

#### Example
  
```ppl
source=accounts
| eval result = coalesce(employer, firstname, lastname)
| fields result, firstname, lastname, employer
```
  
The query returns the following results:
  
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
  
#### Empty String Handling Examples
  
```ppl
source=accounts
| eval empty_field = ""
| eval result = coalesce(empty_field, firstname)
| fields result, empty_field, firstname
```
  
The query returns the following results:
  
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
  
The query returns the following results:
  
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
  
#### Mixed Data Types with Auto Coercion
  
```ppl
source=accounts
| eval result = coalesce(employer, balance, "fallback")
| fields result, employer, balance
```
  
The query returns the following results:
  
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
  
#### Non-existent Field Handling
  
```ppl
source=accounts
| eval result = coalesce(nonexistent_field, firstname, "unknown")
| fields result, firstname
```
  
The query returns the following results:
  
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

**Usage**: `ispresent(field)`

Returns `TRUE` if the field exists, `FALSE` otherwise.

**Parameters**:

- `field` (Required): The field to check for existence.

**Return type**: `BOOLEAN`

**Synonyms**: [ISNOTNULL](#isnotnull)

#### Example
  
```ppl
source=accounts
| where ispresent(employer)
| fields employer, firstname
```
  
The query returns the following results:
  
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

**Usage**: `isblank(field)`

Returns `TRUE` if the field is `NULL`, an empty string, or contains only white space.

**Parameters**:

- `field` (Required): The field to check for blank values.

**Return type**: `BOOLEAN`

#### Example
  
```ppl
source=accounts
| eval temp = ifnull(employer, '   ')
| eval `isblank(employer)` = isblank(employer), `isblank(temp)` = isblank(temp)
| fields `isblank(temp)`, temp, `isblank(employer)`, employer
```
  
The query returns the following results:
  
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

**Usage**: `isempty(field)`

Returns `TRUE` if the field is `NULL` or is an empty string.

**Parameters**:

- `field` (Required): The field to check for empty values.

**Return type**: `BOOLEAN`

#### Example
  
```ppl
source=accounts
| eval temp = ifnull(employer, '   ')
| eval `isempty(employer)` = isempty(employer), `isempty(temp)` = isempty(temp)
| fields `isempty(temp)`, temp, `isempty(employer)`, employer
```
  
The query returns the following results:
  
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

**Usage**: `earliest(relative_string, field)`

Returns `TRUE` if the field value is after the timestamp derived from `relative_string` relative to the current time, `FALSE` otherwise.

**Parameters**:

- `relative_string` (Required): The reference time specification in one of the supported formats.
- `field` (Required): The timestamp field to compare against the reference time.

**Return type**: `BOOLEAN`

**Relative string formats**:
1. `"now"` or `"now()"`: Uses the current system time.
2. Absolute format (`MM/dd/yyyy:HH:mm:ss` or `yyyy-MM-dd HH:mm:ss`): Converts the string to a timestamp and compares it against the field value.
3. Relative format: `(+|-)<time_integer><time_unit>[+<...>]@<snap_unit>`

**Steps to specify a relative time**:
- **Time offset**: Indicate the offset from the current time using `+` or `-`.
- **Time amount**: Provide a numeric value followed by a time unit (`s`, `m`, `h`, `d`, `w`, `M`, `y`).
- **Snap to unit**: Optionally, specify a snap unit using `@<unit>` to round the result down to the nearest unit (for example, hour, day, month).

**Examples** (assuming current time is `2025-05-28 14:28:34`):
- `-3d+2y` → `2027-05-25 14:28:34`.
- `+1d@m` → `2025-05-29 14:28:00`.
- `-3M+1y@M` → `2026-02-01 00:00:00`.

#### Example

The following example compares timestamps against current time and relative time:
  
```ppl
source=accounts
| eval now = utc_timestamp()
| eval a = earliest("now", now), b = earliest("-2d@d", now)
| fields a, b
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------+------+
| a     | b    |
|-------+------|
| False | True |
+-------+------+
```

The following example filters records using an absolute time format:

```ppl
source=nyc_taxi
| where earliest('07/01/2014:00:30:00', timestamp)
| stats COUNT() as cnt
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----+
| cnt |
|-----|
| 972 |
+-----+
```
  
## LATEST

**Usage**: `latest(relative_string, field)`

Returns `TRUE` if the field value is before the timestamp derived from `relative_string` relative to the current time, `FALSE` otherwise.

**Parameters**:

- `relative_string` (Required): The reference time specification in one of the supported formats.
- `field` (Required): The timestamp field to compare against the reference time.

**Return type**: `BOOLEAN`

#### Example

The following example compares timestamps using the latest function:
  
```ppl
source=accounts
| eval now = utc_timestamp()
| eval a = latest("now", now), b = latest("+2d@d", now)
| fields a, b
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+------+------+
| a    | b    |
|------+------|
| True | True |
+------+------+
```

The following example filters records using latest with an absolute time format:

```ppl
source=nyc_taxi
| where latest('07/21/2014:04:00:00', timestamp)
| stats COUNT() as cnt
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----+
| cnt |
|-----|
| 969 |
+-----+
```
  
## CONTAINS

### Description

Usage: `field contains 'substring'` returns TRUE if the field value contains the given substring (case-insensitive), FALSE otherwise.

The `contains` operator is a CloudWatch-style comparison operator that performs case-insensitive substring matching. It is sugar for an `ilike` comparison with `%substring%` wildcards.

Syntax: `<field> contains '<string_literal>'`

- The left-hand side must be a field reference.
- The right-hand side must be a string literal. Using a field reference on the right-hand side will raise a semantic error.
- Matching is case-insensitive.

**Argument type:** `STRING`
**Return type:** `BOOLEAN`

### Example

The following example filters accounts using a substring match to find names containing 'mbe':

```ppl
source=accounts
| where firstname contains 'mbe'
| fields firstname, age
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+-----------+-----+
| firstname | age |
|-----------+-----|
| Amber     | 32  |
+-----------+-----+
```

The following queries are all equivalent due to case-insensitive matching:

```ppl ignore
source=accounts | where firstname contains 'mbe'
source=accounts | where firstname CONTAINS 'MBE'
source=accounts | where firstname Contains 'Mbe'
```

The following example combines substring filtering with other conditions:

```ppl
source=accounts
| where employer contains 'ami' AND age > 30
| fields firstname, employer, age
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+-----------+----------+-----+
| firstname | employer | age |
|-----------+----------+-----|
| Amber     | Pyrami   | 32  |
+-----------+----------+-----+
```

## REGEXP_MATCH

**Usage**: `regexp_match(string, pattern)`

Returns `TRUE` if the regular expression pattern finds a match against any substring of the string value, otherwise returns `FALSE`. The function uses Java regular expression syntax for the pattern.

**Parameters**:

- `string` (Required): The string to search within.
- `pattern` (Required): The regular expression pattern to match against.

**Return type**: `BOOLEAN`

#### Example

The following example filters log messages using a regex pattern:
  
```ppl
source=logs
| where regexp_match(message, 'ERROR|WARN|FATAL')
| fields timestamp, message
```

  
| timestamp | message |
| --- | --- |
| 2024-01-15 10:23:45 | ERROR: Connection timeout to database |
| 2024-01-15 10:24:12 | WARN: High memory usage detected |
| 2024-01-15 10:25:33 | FATAL: System crashed unexpectedly |

The following example uses regex to validate email addresses:
  
```ppl
source=users
| where regexp_match(email, '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
| fields name, email
```

  
| name | email |
| --- | --- |
| John | john@example.com |
| Alice | alice@company.org |

The following example filters for valid public IP addresses using regex:
  
```ppl
source=network
| where regexp_match(ip_address, '^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$') AND NOT regexp_match(ip_address, '^(10\.|172\.(1[6-9]|2[0-9]|3[01])\.|192\.168\.)')
| fields ip_address, status
```

  
| ip_address | status |
| --- | --- |
| 8.8.8.8 | active |
| 1.1.1.1 | active |

The following example uses regex for product categorization with case-insensitive matching:
  
```ppl
source=products
| eval category = if(regexp_match(name, '(?i)(laptop|computer|desktop)'), 'Computing', if(regexp_match(name, '(?i)(phone|tablet|mobile)'), 'Mobile', 'Other'))
| fields name, category
```

  
| name | category |
| --- | --- |
| Dell Laptop XPS | Computing |
| iPhone 15 Pro | Mobile |
| Wireless Mouse | Other |