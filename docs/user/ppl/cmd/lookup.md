
# lookup

The `lookup` command enriches search data by adding or replacing values from a lookup index (dimension table). It allows you to extend fields in your index with values from a dimension table, appending or replacing values when the lookup condition matches. Compared with the `join` command, `lookup` is better suited for enriching source data with a static dataset.

## Syntax

The `lookup` command has the following syntax:

```syntax
lookup <lookupIndex> (<lookupMappingField> [as <sourceMappingField>])... [(replace | append) (<inputField> [as <outputField>])...]
```

The following are examples of the `lookup` command syntax:

```syntax
source = table1 | lookup table2 id
source = table1 | lookup table2 id, name
source = table1 | lookup table2 id as cid, name
source = table1 | lookup table2 id as cid, name replace dept as department
source = table1 | lookup table2 id as cid, name replace dept as department, city as location
source = table1 | lookup table2 id as cid, name append dept as department
source = table1 | lookup table2 id as cid, name append dept as department, city as location
```

## Parameters

The `lookup` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<lookupIndex>` | Required | The name of the lookup index (dimension table). |
| `<lookupMappingField>` | Required | A key in the lookup index used for matching, similar to a join key in the right table. Specify multiple fields as a comma-separated list. |
| `<sourceMappingField>` | Optional | A key from the source data (left side) used for matching, similar to a join key in the left table. Default is `lookupMappingField`. |
| `<inputField>` | Optional | A field in the lookup index whose matched values are applied to the results (output). Specify multiple fields as a comma-separated list. If not specified, all fields except `lookupMappingField` from the lookup index are applied to the results. |
| `<outputField>` | Optional | The name of the field in the results (output) in which matched values are placed. Specify multiple fields as a comma-separated list. If the `outputField` specifies an existing field in the source query, its values are replaced or appended with matched values from the `inputField`. If the field specified in the `outputField` is not an existing field, a new field is added to the results when using `replace`, or the operation fails when using `append`. |
| `(replace | append)` | Optional | Specifies how matched values are applied to the output. `replace` overwrites existing values with matched values from the lookup index. `append` fills only missing values in the results with matched values from the lookup index. Default is `replace`. |
  
## Example 1: Replace existing values  

The following query uses the `lookup` command with the `replace` strategy to overwrite existing values:  
  
```ppl ignore
source = worker
  | LOOKUP work_information uid AS id REPLACE department
  | fields id, name, occupation, country, salary, department
```
  
The query returns the following results:

```text
+------+-------+------------+---------+--------+------------+
| id   | name  | occupation | country | salary | department |
|------+-------+------------+---------+--------+------------|
| 1000 | Jake  | Engineer   | England | 100000 | IT         |
| 1001 | Hello | Artist     | USA     | 70000  | null       |
| 1002 | John  | Doctor     | Canada  | 120000 | DATA       |
| 1003 | David | Doctor     | null    | 120000 | HR         |
| 1004 | David | null       | Canada  | 0      | null       |
| 1005 | Jane  | Scientist  | Canada  | 90000  | DATA       |
+------+-------+------------+---------+--------+------------+
```
  

## Example 2: Append missing values  

The following query uses the `lookup` command with the `append` strategy to append missing values only:
  
```ppl ignore
source = worker
  | LOOKUP work_information uid AS id APPEND department
  | fields id, name, occupation, country, salary, department
```
  

## Example 3: No input field specified  

The following query uses the `lookup` command without specifying an `inputField`, which adds all fields from the lookup index to the results:
  
```ppl ignore
  source = worker
  | LOOKUP work_information uid AS id, name
  | fields id, name, occupation, country, salary, department
```
  
The query returns the following results:

```text
+------+-------+---------+--------+------------+------------+
| id   | name  | country | salary | department | occupation |
|------+-------+---------+--------+------------+------------|
| 1000 | Jake  | England | 100000 | IT         | Engineer   |
| 1001 | Hello | USA     | 70000  | null       | null       |
| 1002 | John  | Canada  | 120000 | DATA       | Scientist  |
| 1003 | David | null    | 120000 | HR         | Doctor     |
| 1004 | David | Canada  | 0      | null       | null       |
| 1005 | Jane  | Canada  | 90000  | DATA       | Engineer   |
+------+-------+---------+--------+------------+------------+
```
  
## Example 4: Add matched values to a new field

The following query places matched values into a new field specified by `outputField`:
  
```ppl ignore
  source = worker
  | LOOKUP work_information name REPLACE occupation AS new_col
  | fields id, name, occupation, country, salary, new_col
```
  
The query returns the following results:

```text
+------+-------+------------+---------+--------+-----------+
| id   | name  | occupation | country | salary | new_col   |
|------+-------+------------+---------+--------+-----------|
| 1003 | David | Doctor     | null    | 120000 | Doctor    |
| 1004 | David | null       | Canada  | 0      | Doctor    |
| 1001 | Hello | Artist     | USA     | 70000  | null      |
| 1000 | Jake  | Engineer   | England | 100000 | Engineer  |
| 1005 | Jane  | Scientist  | Canada  | 90000  | Engineer  |
| 1002 | John  | Doctor     | Canada  | 120000 | Scientist |
+------+-------+------------+---------+--------+-----------+
```