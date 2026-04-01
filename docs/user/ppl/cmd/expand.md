
# expand

The `expand` command transforms a single document with a nested array field into multiple documents, each containing one element of the array. All other fields in the original document are duplicated across the resulting documents.

The `expand` command operates in the following way:

* It generates one row per element in the specified array field.
* The specified array field is converted into individual rows.
* If an alias is provided, the expanded values appear under the alias instead of the original field name.
* If the specified field is an empty array, the row is retained with the expanded field set to `null`.

## Syntax

The `expand` command has the following syntax:

```syntax
expand <field> [as alias]
```

## Parameters

The `expand` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field>` | Required | The field to be expanded. Only nested arrays are supported. |
| `<alias>` | Optional | The name to use in place of the original field name. |  
  

## Example: Expand an array field  

The following query creates an array of severity text and service name, then expands it into separate rows:
  
```ppl
source=otellogs
| where severityText = 'FATAL'
| eval tags = array(severityText, body)
| expand tags as tag
| fields severityText, tag
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+--------------+---------------------------------------------------------------------------------+
| severityText | tag                                                                             |
|--------------+---------------------------------------------------------------------------------|
| FATAL        | FATAL                                                                           |
| FATAL        | Out of memory: Java heap space - shutting down pod payment-service-7d4b8c-xk2q9 |
| FATAL        | FATAL                                                                           |
| FATAL        | Database primary node unreachable: connection refused to db-primary-01:5432     |
+--------------+---------------------------------------------------------------------------------+
```
  

## Limitations

The `expand` command has the following limitations:

* The `expand` command only supports nested arrays. Primitive fields storing arrays are not supported. For example, a string field storing an array of strings cannot be expanded.
