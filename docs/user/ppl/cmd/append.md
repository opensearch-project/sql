
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

## Example 1: Append error and fatal counts side by side

The following query shows error counts per service, then appends fatal counts from a separate query. This lets you compare error and fatal rates across services:
  
```ppl
source=otellogs
| where severityText = 'ERROR'
| stats count() as error_count by `resource.attributes.service.name`
| sort - error_count
| append [ source=otellogs | where severityText = 'FATAL' | stats count() as fatal_count by `resource.attributes.service.name` ]
| sort `resource.attributes.service.name`
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 7/7
+-------------+----------------------------------+-------------+
| error_count | resource.attributes.service.name | fatal_count |
|-------------+----------------------------------+-------------|
| 1           | api-gateway                      | null        |
| 1           | auth-service                     | null        |
| 1           | cart-service                     | null        |
| null        | inventory-service                | 1           |
| 1           | payment-service                  | null        |
| null        | payment-service                  | 1           |
| 1           | user-service                     | null        |
+-------------+----------------------------------+-------------+
```
  

## Example 2: Append summary rows to detail rows

The following query shows the top 3 severity levels by count, then appends the total count across all levels:
  
```ppl
source=otellogs
| stats count() as log_count by severityText
| sort - log_count
| head 3
| append [ source=otellogs | stats count() as log_count | eval severityText = 'ALL' ]
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------+--------------+
| log_count | severityText |
|-----------+--------------|
| 6         | INFO         |
| 5         | ERROR        |
| 4         | WARN         |
| 20        | ALL          |
+-----------+--------------+
```

## Limitations

The `append` command has the following limitations:

* **Schema compatibility**: When fields with the same name exist in both the main search and the subsearch but have incompatible types, the query fails with an error. To avoid type conflicts, ensure that fields with the same name share the same data type. Alternatively, use different field names. You can rename the conflicting fields using `eval` or select non-conflicting columns using `fields`.
