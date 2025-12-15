# rename


The `rename` command renames one or more fields in the search results.

## Syntax

Use the following syntax:

`rename <source-field> AS <target-field>["," <source-field> AS <target-field>]...`  
* `source-field`: mandatory. The name of the field you want to rename. Supports wildcard patterns using `*`.  
* `target-field`: mandatory. The name you want to rename to. Must have same number of wildcards as the source.  
  

## Behavior  

The rename command handles non-existent fields as follows:
* **Renaming a non-existent field to a non-existent field**: No change occurs to the search results.  
* **Renaming a non-existent field to an existing field**: The existing target field is removed from the search results.  
* **Renaming an existing field to an existing field**: The existing target field is removed and the source field is renamed to the target.  
  

## Example 1: Rename one field  

The following example PPL query shows how to use `rename` to rename one field.
  
```ppl
source=accounts
| rename account_number as an
| fields an
```
  
Expected output:
  
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

The following example PPL query shows how to use `rename` to rename multiple fields.
  
```ppl
source=accounts
| rename account_number as an, employer as emp
| fields an, emp
```
  
Expected output:
  
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
  

## Example 3: Rename with wildcards  

The following example PPL query shows how to use `rename` to rename multiple fields using wildcard patterns.
  
```ppl
source=accounts
| rename *name as *_name
| fields first_name, last_name
```
  
Expected output:
  
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
  

## Example 4: Rename with multiple wildcard patterns  

The following example PPL query shows how to use `rename` to rename multiple fields using multiple wildcard patterns.
  
```ppl
source=accounts
| rename *name as *_name, *_number as *number
| fields first_name, last_name, accountnumber
```
  
Expected output:
  
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
  

## Example 5: Rename existing field to existing field  

The following example PPL query shows how to use `rename` to rename an existing field to an existing field. The target field gets removed and the source field is renamed to the target field.
  
```ppl
source=accounts
| rename firstname as age
| fields age
```
  
Expected output:
  
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

The `rename` command is not rewritten to [query domain-specific language (DSL)](https://opensearch.org/docs/latest/query-dsl/index/). It is only run on the coordinating node.
Literal asterisk (*) characters in field names cannot be replaced as asterisk is used for wildcard matching.