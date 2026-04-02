
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
  

## Example 1: Append a total row to aggregated results  

The following query counts logs by severity level, then appends a total row. This is useful for building summary reports that include both breakdowns and totals:
  
```ppl
source=otellogs
| stats count() as log_count by severityText
| sort - log_count
| head 3
| appendpipe [ stats sum(log_count) as total ]
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------+--------------+-------+
| log_count | severityText | total |
|-----------+--------------+-------|
| 6         | INFO         | null  |
| 5         | ERROR        | null  |
| 4         | WARN         | null  |
| null      | null         | 15    |
+-----------+--------------+-------+
```
  

## Example 2: Append summary statistics to detail rows  

The following query shows error counts per service, then appends the overall average error count across all services:
  
```ppl
source=otellogs
| where severityText IN ('ERROR', 'FATAL')
| stats count() as error_count by `resource.attributes.service.name`
| sort - error_count
| appendpipe [ stats avg(error_count) as avg_errors ]
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 6/6
+-------------+----------------------------------+------------+
| error_count | resource.attributes.service.name | avg_errors |
|-------------+----------------------------------+------------|
| 2           | checkout                         | null       |
| 2           | payment                          | null       |
| 1           | frontend-proxy                   | null       |
| 1           | product-catalog                  | null       |
| 1           | recommendation                   | null       |
| null        | null                             | 1.4        |
+-------------+----------------------------------+------------+
```
  
