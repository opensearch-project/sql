# lookup

The `lookup` command enriches search data by adding or replacing values from a lookup index (dimension table). It allows you to extend fields in your index with values from a dimension table, appending or replacing values when the lookup condition matches. Compared with the `join` command, `lookup` is better suited for enriching source data with a static dataset.

## Syntax

The `lookup` command has the following syntax:

```sql
lookup <lookupIndex> (<lookupMappingField> [as <sourceMappingField>])... [(replace | append) (<inputField> [as <outputField>])...]
```

The following are examples of the `lookup` command syntax:

```sql
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
  
```bash ignore
curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_ppl -d '{
  "query" : """
  source = worker
  | LOOKUP work_information uid AS id REPLACE department
  | fields id, name, occupation, country, salary, department
  """
}'
```
  
Result set
  
```json
{
  "schema": [
    {
      "name": "id",
      "type": "integer"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "occupation",
      "type": "string"
    },
    {
      "name": "country",
      "type": "string"
    },
    {
      "name": "salary",
      "type": "integer"
    },
    {
      "name": "department",
      "type": "string"
    }
  ],
  "datarows": [
    [
      1000,
      "Jake",
      "Engineer",
      "England",
      100000,
      "IT"
    ],
    [
      1001,
      "Hello",
      "Artist",
      "USA",
      70000,
      null
    ],
    [
      1002,
      "John",
      "Doctor",
      "Canada",
      120000,
      "DATA"
    ],
    [
      1003,
      "David",
      "Doctor",
      null,
      120000,
      "HR"
    ],
    [
      1004,
      "David",
      null,
      "Canada",
      0,
      null
    ],
    [
      1005,
      "Jane",
      "Scientist",
      "Canada",
      90000,
      "DATA"
    ]
  ],
  "total": 6,
  "size": 6
}
```
  

## Example 2: Append strategy  

The following example PPL query shows how to use `lookup` with the APPEND strategy to fill missing values only.
  
```bash ignore
curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_ppl -d '{
  "query" : """
  source = worker
  | LOOKUP work_information uid AS id APPEND department
  | fields id, name, occupation, country, salary, department
  """
}'
```
  

## Example 3: No inputField specified  

The following example PPL query shows how to use `lookup` without specifying inputField, which applies all fields from the lookup index.
  
```bash ignore
curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_ppl -d '{
  "query" : """
  source = worker
  | LOOKUP work_information uid AS id, name
  | fields id, name, occupation, country, salary, department
  """
}'
```
  
Result set
  
```json
{
  "schema": [
    {
      "name": "id",
      "type": "integer"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "country",
      "type": "string"
    },
    {
      "name": "salary",
      "type": "integer"
    },
    {
      "name": "department",
      "type": "string"
    },
    {
      "name": "occupation",
      "type": "string"
    }
  ],
  "datarows": [
    [
      1000,
      "Jake",
      "England",
      100000,
      "IT",
      "Engineer"
    ],
    [
      1001,
      "Hello",
      "USA",
      70000,
      null,
      null
    ],
    [
      1002,
      "John",
      "Canada",
      120000,
      "DATA",
      "Scientist"
    ],
    [
      1003,
      "David",
      null,
      120000,
      "HR",
      "Doctor"
    ],
    [
      1004,
      "David",
      "Canada",
      0,
      null,
      null
    ],
    [
      1005,
      "Jane",
      "Canada",
      90000,
      "DATA",
      "Engineer"
    ]
  ],
  "total": 6,
  "size": 6
}
```
  

## Example 4: OutputField as a new field  

The following example PPL query shows how to use `lookup` with outputField as a new field name.
  
```bash ignore
curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_ppl -d '{
  "query" : """
  source = worker
  | LOOKUP work_information name REPLACE occupation AS new_col
  | fields id, name, occupation, country, salary, new_col
  """
}'
```
  
Result set
  
```json
{
  "schema": [
    {
      "name": "id",
      "type": "integer"
    },
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "occupation",
      "type": "string"
    },
    {
      "name": "country",
      "type": "string"
    },
    {
      "name": "salary",
      "type": "integer"
    },
    {
      "name": "new_col",
      "type": "string"
    }
  ],
  "datarows": [
    [
      1003,
      "David",
      "Doctor",
      null,
      120000,
      "Doctor"
    ],
    [
      1004,
      "David",
      null,
      "Canada",
      0,
      "Doctor"
    ],
    [
      1001,
      "Hello",
      "Artist",
      "USA",
      70000,
      null
    ],
    [
      1000,
      "Jake",
      "Engineer",
      "England",
      100000,
      "Engineer"
    ],
    [
      1005,
      "Jane",
      "Scientist",
      "Canada",
      90000,
      "Engineer"
    ],
    [
      1002,
      "John",
      "Doctor",
      "Canada",
      120000,
      "Scientist"
    ]
  ],
  "total": 6,
  "size": 6
}
```