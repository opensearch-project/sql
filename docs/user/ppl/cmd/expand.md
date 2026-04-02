
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
  

## Example: Expand a collected list of services into individual rows  

The following query first collects all service names per severity level into an array using `stats list()`, then expands each array element into its own row. This is useful when you need to go from an aggregated view back to individual rows:
  
```ppl
source=otellogs
| where severityText = 'FATAL'
| stats list(`resource.attributes.service.name`) as services by severityText
| expand services as service
| fields severityText, service
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+--------------+-----------------+
| severityText | service         |
|--------------+-----------------|
| FATAL        | payment         |
| FATAL        | product-catalog |
+--------------+-----------------+
```
  

## Limitations

The `expand` command has the following limitations:

* The `expand` command only supports nested arrays. Primitive fields storing arrays are not supported. For example, a string field storing an array of strings cannot be expanded.
