
# rename

The `rename` command renames one or more fields in the search results.

The `rename` command handles non-existent fields as follows:

* **Renaming a non-existent field to a non-existent field**: No change occurs to the search results.
* **Renaming a non-existent field to an existing field**: The existing target field is removed from the search results.
* **Renaming an existing field to an existing field**: The existing target field is removed and the source field is renamed to the target.

> **Note**: The `rename` command is not rewritten to [query domain-specific language (DSL)](https://docs.opensearch.org/latest/query-dsl/). It is only executed on the coordinating node.

## Syntax

The `rename` command has the following syntax:

```syntax
rename <source-field> AS <target-field>[, <source-field> AS <target-field>]...
```

## Parameters

The `rename` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<source-field>` | Required | The name of the field you want to rename. Supports wildcard patterns using `*`. |
| `<target-field>` | Required | The name you want to rename to. Must contain the same number of wildcards as the source. |

## Example 1: Rename a field  

The following query renames one field:
  
```ppl
source=otellogs
| rename severityText as severity
| fields severity
| head 4
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+----------+
| severity |
|----------|
| INFO     |
| INFO     |
| WARN     |
| ERROR    |
+----------+
```
  

## Example 2: Rename multiple fields  

The following query renames multiple fields:
  
```ppl
source=otellogs
| rename severityText as severity, `resource.attributes.service.name` as service
| fields severity, service
| head 4
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+----------+-----------------+
| severity | service         |
|----------+-----------------|
| INFO     | frontend        |
| INFO     | cart            |
| WARN     | product-catalog |
| ERROR    | payment         |
+----------+-----------------+
```
  

## Example 3: Rename fields using wildcards  

The following query renames multiple fields using a wildcard pattern. Both `severityText` and `severityNumber` match `severity*` and are renamed to `sev*`:
  
```ppl
source=otellogs
| rename severity* as sev*
| fields sevText, sevNumber
| head 4
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+---------+-----------+
| sevText | sevNumber |
|---------+-----------|
| INFO    | 9         |
| INFO    | 9         |
| WARN    | 13        |
| ERROR   | 17        |
+---------+-----------+
```
  

## Example 4: Rename fields using multiple wildcard patterns  

The following query renames multiple fields using multiple wildcard patterns:
  
```ppl
source=otellogs
| rename severity* as sev*, `@*` as otel_*
| fields sevText, sevNumber, otel_timestamp
| head 4
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+---------+-----------+---------------------+
| sevText | sevNumber | otel_timestamp      |
|---------+-----------+---------------------|
| INFO    | 9         | 2024-02-01 09:10:00 |
| INFO    | 9         | 2024-02-01 09:11:00 |
| WARN    | 13        | 2024-02-01 09:12:00 |
| ERROR   | 17        | 2024-02-01 09:13:00 |
+---------+-----------+---------------------+
```
  

## Example 5: Rename an existing field to another existing field  

The following query renames an existing field to another existing field. The target field is removed and the source field is renamed to the target:
  
```ppl
source=otellogs
| rename severityText as body
| fields body
| head 4
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-------+
| body  |
|-------|
| INFO  |
| INFO  |
| WARN  |
| ERROR |
+-------+
```
  

## Limitations

The `rename` command has the following limitations:

* Literal asterisk (`*`) characters in field names cannot be replaced because the asterisk is used for wildcard matching.
