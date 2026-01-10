
# append

The `append` command appends the results of a subsearch as additional rows to the end of the input search results (the main search).

The command aligns columns that have the same field names and types. For columns that exist in only the main search or subsearch, `NULL` values are inserted into the missing fields for the respective rows.

## Syntax

The `append` command has the following syntax:

```syntax
append <subsearch>
```

## Parameters

The `append` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<subsearch>` | Required | Executes PPL commands as a secondary search. |  

## Example 1: Append rows from a count aggregation to existing search results

The following query appends rows from `count by gender` to `sum by gender, state`:
  
```ppl
source=accounts | stats sum(age) by gender, state | sort -`sum(age)` | head 5 | append [ source=accounts | stats count(age) by gender ]
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 6/6
+----------+--------+-------+------------+
| sum(age) | gender | state | count(age) |
|----------+--------+-------+------------|
| 36       | M      | TN    | null       |
| 33       | M      | MD    | null       |
| 32       | M      | IL    | null       |
| 28       | F      | VA    | null       |
| null     | F      | null  | 1          |
| null     | M      | null  | 3          |
+----------+--------+-------+------------+
```
  

## Example 2: Append rows with merged column names

The following query appends rows from `sum by gender` to `sum by gender, state`, merging columns that have the same field name and type:
  
```ppl
source=accounts | stats sum(age) as sum by gender, state | sort -sum | head 5 | append [ source=accounts | stats sum(age) as sum by gender ]
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 6/6
+-----+--------+-------+
| sum | gender | state |
|-----+--------+-------|
| 36  | M      | TN    |
| 33  | M      | MD    |
| 32  | M      | IL    |
| 28  | F      | VA    |
| 28  | F      | null  |
| 101 | M      | null  |
+-----+--------+-------+
```

## Limitations

The `append` command has the following limitations:

* **Schema compatibility**: When fields with the same name exist in both the main search and the subsearch but have incompatible types, the query fails with an error. To avoid type conflicts, ensure that fields with the same name share the same data type. Alternatively, use different field names. You can rename the conflicting fields using `eval` or select non-conflicting columns using `fields`.