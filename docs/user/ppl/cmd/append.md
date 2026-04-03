
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

## Example 1: Append error and warning counts side by side

The following query shows error counts per service, then appends warning counts from a separate query. This lets you compare error and warning rates across services:
  
```ppl
source=otellogs
| where severityText = 'ERROR'
| stats count() as error_count by `resource.attributes.service.name`
| sort - error_count
| append [ source=otellogs | where severityText = 'WARN' | stats count() as warn_count by `resource.attributes.service.name` ]
| sort `resource.attributes.service.name`
| fields `resource.attributes.service.name`, error_count, warn_count
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 7/7
+----------------------------------+-------------+------------+
| resource.attributes.service.name | error_count | warn_count |
|----------------------------------+-------------+------------|
| checkout                         | 2           | null       |
| frontend-proxy                   | 1           | null       |
| frontend-proxy                   | null        | 2          |
| payment                          | 2           | null       |
| product-catalog                  | 1           | null       |
| product-catalog                  | null        | 2          |
| recommendation                   | 1           | null       |
+----------------------------------+-------------+------------+
```
  

## Example 2: Append summary rows to detail rows

The following query shows severity levels by count, then appends the total count across all levels:
  
```ppl
source=otellogs
| stats count() as log_count by severityText
| sort - log_count
| append [ source=otellogs | stats count() as log_count | eval severityText = 'ALL' ]
| fields severityText, log_count
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 5/5
+--------------+-----------+
| severityText | log_count |
|--------------+-----------|
| DEBUG        | 3         |
| ERROR        | 7         |
| INFO         | 6         |
| WARN         | 4         |
| ALL          | 20        |
+--------------+-----------+
```

## Limitations

The `append` command has the following limitations:

* **Schema compatibility**: When fields with the same name exist in both the main search and the subsearch but have incompatible types, the query fails with an error. To avoid type conflicts, ensure that fields with the same name share the same data type. Alternatively, use different field names. You can rename the conflicting fields using `eval` or select non-conflicting columns using `fields`.
