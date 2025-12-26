# expand

The `expand` command transforms a single document with a nested array field into multiple documents, each containing one element of the array. All other fields in the original document are duplicated across the resulting documents.

The `expand` command operates in the following way:

* It generates one row per element in the specified array field.
* The specified array field is converted into individual rows.
* If an alias is provided, the expanded values appear under the alias instead of the original field name.
* If the specified field is an empty array, the row is retained with the expanded field set to `null`.

## Syntax

The `expand` command has the following syntax:

```sql
expand <field> [as alias]
```

## Parameters

The `expand` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field>` | Required | The field to be expanded. Only nested arrays are supported. |
| `<alias>` | Optional | The name to use in place of the original field name. |  
  

## Example 1: Expand address field with an alias  

The following query expands the `address` field and renames it to `addr`:
  
```json
{"name":"abbas","age":24,"address":[{"city":"New york city","state":"NY","moveInDate":{"dateAndTime":"19840412T090742.000Z"}}]}
{"name":"chen","age":32,"address":[{"city":"Miami","state":"Florida","moveInDate":{"dateAndTime":"19010811T040333.000Z"}},{"city":"los angeles","state":"CA","moveInDate":{"dateAndTime":"20230503T080742.000Z"}}]}

```
  
The following query expand the address field and rename it to addr:
  
```ppl
source=migration
| expand address as addr
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-------+-----+-------------------------------------------------------------------------------------------+
| name  | age | addr                                                                                      |
|-------+-----+-------------------------------------------------------------------------------------------|
| abbas | 24  | {"city":"New york city","state":"NY","moveInDate":{"dateAndTime":"19840412T090742.000Z"}} |
| chen  | 32  | {"city":"Miami","state":"Florida","moveInDate":{"dateAndTime":"19010811T040333.000Z"}}    |
| chen  | 32  | {"city":"los angeles","state":"CA","moveInDate":{"dateAndTime":"20230503T080742.000Z"}}   |
+-------+-----+-------------------------------------------------------------------------------------------+
```
  

## Limitations  

The `expand` command has the following limitations:

* The `expand` command only supports nested arrays. Primitive fields storing arrays are not supported. For example, a string field storing an array of strings cannot be expanded.
