
# appendpipe

The `appendpipe` command appends the results of a subpipeline to the search results. Unlike a subsearch, the subpipeline is not executed first; it runs only when the search reaches the `appendpipe` command.

The command aligns columns that have the same field names and types. For columns that exist in only the main search or subpipeline, `NULL` values are inserted into the missing fields for the respective rows.

## Syntax

The `appendpipe` command has the following syntax:

```syntax
appendpipe [<subpipeline>]
```

## Parameters

The `appendpipe` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<subpipeline>` | Required | A list of commands applied to the search results produced by the commands that precede the `appendpipe` command. |
  

## Example 1: Append rows from a total count to existing search results  

This example appends rows from `total by gender` to `sum by gender, state`, merging columns that have the same field name and type:
  
```ppl
source=accounts
| stats sum(age) as part by gender, state
| sort -part
| head 5
| appendpipe [ stats sum(part) as total by gender ]
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 6/6
+------+--------+-------+-------+
| part | gender | state | total |
|------+--------+-------+-------|
| 36   | M      | TN    | null  |
| 33   | M      | MD    | null  |
| 32   | M      | IL    | null  |
| 28   | F      | VA    | null  |
| null | F      | null  | 28    |
| null | M      | null  | 101   |
+------+--------+-------+-------+
```
  

## Example 2: Append rows with merged column names  

This example appends rows from `count by gender` to `sum by gender, state`:
  
```ppl
source=accounts
| stats sum(age) as total by gender, state
| sort -total
| head 5
| appendpipe [ stats sum(total) as total by gender ]
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 6/6
+----------+--------+-------+
| total    | gender | state |
|----------+--------+-------|
| 36       | M      | TN    |
| 33       | M      | MD    |
| 32       | M      | IL    |
| 28       | F      | VA    |
| 28       | F      | null  |
| 101      | M      | null  |
+----------+--------+-------+
```
  

## Limitations

The `appendpipe` command has the following limitations:

* **Schema compatibility**: When fields with the same name exist in both the main search and the subpipeline but have incompatible types, the query fails with an error. To avoid type conflicts, ensure that fields with the same name share the same data type. Alternatively, use different field names. You can rename the conflicting fields using `eval` or select non-conflicting columns using `fields`.
