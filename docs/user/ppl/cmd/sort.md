
# sort

The `sort` command sorts the search results by the specified fields.

## Syntax

The `sort` command supports two syntax notations. You must use one notation consistently within a single `sort` command.

### Prefix notation

The `sort` command has the following syntax in prefix notation:

```syntax
sort [<count>] [+|-] <field> [, [+|-] <field>]...
```

### Suffix notation

The `sort` command has the following syntax in suffix notation:

```syntax
sort [<count>] <field> [asc|desc|a|d] [, <field> [asc|desc|a|d]]...
```

## Parameters

The `sort` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field>` | Required | The field used to sort. Use `auto(field)`, `str(field)`, `ip(field)`, or `num(field)` to specify how to interpret field values. Multiple fields can be specified as a comma-separated list. |
| `<count>` | Optional | The number of results to return. A value of `0` or less returns all results. Default is `0`. |
| `[+|-]` | Optional | **Prefix notation only.** The plus sign (`+`) specifies ascending order, and the minus sign (`-`) specifies descending order. Default is ascending order. |
| `[asc|desc|a|d]` | Optional | **Suffix notation only.** Specifies the sort order: `asc`/`a` for ascending, `desc`/`d` for descending. Default is ascending order. |

## Example 1: Sort by one field

The following query sorts all documents by the `age` field in ascending order. By default, the sort command returns all results, which is equivalent to specifying `sort 0 age`:

```ppl
source=accounts
| sort age
| fields account_number, age
```

The query returns the following results:

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


## Example 2: Sort by one field in descending order

The following query sorts all documents by the `age` field in descending order. You can use either prefix notation (`- age`) or suffix notation (`age desc`):

```ppl
source=accounts
| sort - age
| fields account_number, age
```

This query is equivalent to the following query:

```ppl
source=accounts
| sort age desc
| fields account_number, age
```

The query returns the following results:

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


## Example 3: Sort by multiple fields in prefix notation

The following query uses prefix notation to sort all documents by the `gender` field in ascending order and the `age` field in descending order:
  
```ppl
source=accounts
| sort + gender, - age
| fields account_number, gender, age
```
  
The query returns the following results:
  
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
  

## Example 4: Sort by multiple fields in suffix notation

The following query uses suffix notation to sort all documents by the `gender` field in ascending order and the `age` field in descending order:
  
```ppl
source=accounts
| sort gender asc, age desc
| fields account_number, gender, age
```
  
The query returns the following results:
  
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
  

## Example 5: Sort fields with null values

The default ascending order lists null values first. The following query sorts the `employer` field in the default order:
  
```ppl
source=accounts
| sort employer
| fields employer
```
  
The query returns the following results:
  
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
  

## Example 6: Specify the number of sorted documents to return  

The following query sorts all documents and returns two documents:
  
```ppl
source=accounts
| sort 2 age
| fields account_number, age
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+----------------+-----+
| account_number | age |
|----------------+-----|
| 13             | 28  |
| 1              | 32  |
+----------------+-----+
```
  

## Example 7: Sort by specifying field type

The following query uses the `sort` command with `str()` to sort numeric values lexicographically:

```ppl
source=accounts
| sort str(account_number)
| fields account_number
```
  
The query returns the following results:
  
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
  