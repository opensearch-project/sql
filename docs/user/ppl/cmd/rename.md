
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
rename <source-field> AS <target-field>["," <source-field> AS <target-field>]...
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
source=accounts
| rename account_number as an
| fields an
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+----+
| an |
|----|
| 1  |
| 6  |
| 13 |
| 18 |
+----+
```
  

## Example 2: Rename multiple fields  

The following query renames multiple fields:
  
```ppl
source=accounts
| rename account_number as an, employer as emp
| fields an, emp
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+----+---------+
| an | emp     |
|----+---------|
| 1  | Pyrami  |
| 6  | Netagy  |
| 13 | Quility |
| 18 | null    |
+----+---------+
```
  

## Example 3: Rename fields using wildcards  

The following query renames multiple fields using wildcard patterns:
  
```ppl
source=accounts
| rename *name as *_name
| fields first_name, last_name
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+------------+-----------+
| first_name | last_name |
|------------+-----------|
| Amber      | Duke      |
| Hattie     | Bond      |
| Nanette    | Bates     |
| Dale       | Adams     |
+------------+-----------+
```
  

## Example 4: Rename fields using multiple wildcard patterns  

The following query renames multiple fields using multiple wildcard patterns:
  
```ppl
source=accounts
| rename *name as *_name, *_number as *number
| fields first_name, last_name, accountnumber
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+------------+-----------+---------------+
| first_name | last_name | accountnumber |
|------------+-----------+---------------|
| Amber      | Duke      | 1             |
| Hattie     | Bond      | 6             |
| Nanette    | Bates     | 13            |
| Dale       | Adams     | 18            |
+------------+-----------+---------------+
```
  

## Example 5: Rename an existing field to another existing field  

The following query renames an existing field to another existing field. The target field is removed and the source field is renamed to the target field:
  
```ppl
source=accounts
| rename firstname as age
| fields age
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+---------+
| age     |
|---------|
| Amber   |
| Hattie  |
| Nanette |
| Dale    |
+---------+
```
  

## Limitations

The `rename` command has the following limitations:

* Literal asterisk (`*`) characters in field names cannot be replaced because the asterisk is used for wildcard matching.