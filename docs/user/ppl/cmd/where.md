
# where

The `where` command filters the search results. It only returns results that match the specified conditions.

## Syntax

The `where` command has the following syntax:

```syntax
where <boolean-expression>
```

## Parameters

The `where` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<boolean-expression>` | Required | The condition used to filter the results. Only rows in which this condition evaluates to `true` are returned. |

## Example 1: Filter by numeric values

The following query returns accounts in which `balance` is greater than `30000`:

```ppl
source=accounts
| where balance > 30000
| fields account_number, balance
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+----------------+---------+
| account_number | balance |
|----------------+---------|
| 1              | 39225   |
| 13             | 32838   |
+----------------+---------+
```

## Example 2: Filter using combined criteria

The following query combines multiple conditions using an `AND` operator:

```ppl
source=accounts
| where age > 30 AND gender = 'M'
| fields account_number, age, gender
```

The query returns the following results:

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


## Example 3: Filter with multiple possible values

The following query fetches all the documents from the `accounts` index in which `account_number` is `1` or `gender` is `F`:

```ppl
source=accounts
| where account_number=1 or gender="F"
| fields account_number, gender
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+----------------+--------+
| account_number | gender |
|----------------+--------|
| 1              | M      |
| 13             | F      |
+----------------+--------+
```
  

## Example 4: Filter by text patterns 

The `LIKE` operator enables pattern matching on string fields using wildcards.

### Matching a single character

The following query uses an underscore (`_`) to match a single character:

```ppl
source=accounts
| where LIKE(state, 'M_')
| fields account_number, state
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+----------------+-------+
| account_number | state |
|----------------+-------|
| 18             | MD    |
+----------------+-------+
```

### Matching multiple characters

The following query uses a percent sign (`%`) to match multiple characters:

```ppl
source=accounts
| where LIKE(state, 'V%')
| fields account_number, state
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+----------------+-------+
| account_number | state |
|----------------+-------|
| 13             | VA    |
+----------------+-------+
```

## Example 5: Filter by excluding specific values  

The following query uses a `NOT` operator to exclude matching records:
  
```ppl
source=accounts
| where NOT state = 'CA'
| fields account_number, state
```
  
The query returns the following results:
  
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
  

## Example 6: Filter using value lists  

The following query uses an `IN` operator to match multiple values:
  
```ppl
source=accounts
| where state IN ('IL', 'VA')
| fields account_number, state
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+----------------+-------+
| account_number | state |
|----------------+-------|
| 1              | IL    |
| 13             | VA    |
+----------------+-------+
```
  

## Example 7: Filter records with missing data  

The following query returns records in which the `employer` field is `null`:
  
```ppl
source=accounts
| where ISNULL(employer)
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
  

## Example 8: Filter using grouped conditions  

The following query combines multiple conditions using parentheses and logical operators:
  
```ppl
source=accounts
| where (balance > 40000 OR age > 35) AND gender = 'M'
| fields account_number, balance, age, gender
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------------+---------+-----+--------+
| account_number | balance | age | gender |
|----------------+---------+-----+--------|
| 6              | 5686    | 36  | M      |
+----------------+---------+-----+--------+
```
  