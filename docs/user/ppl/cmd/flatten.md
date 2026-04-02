
# flatten

The `flatten` command converts a struct or object field into individual fields within a document.

The resulting flattened fields are ordered lexicographically by their original key names. For example, if a struct contains the keys `b`, `c`, and `Z`, the flattened fields are ordered as `Z`, `b`, `c`.

> **Important**: `flatten` should not be applied to arrays. To expand an array field into multiple rows, use the `expand` command. Note that arrays can be stored in non-array fields in OpenSearch; when flattening a field that contains a nested array, only the first element of the array is flattened.

## Syntax

The `flatten` command has the following syntax:

```syntax
flatten <field> [as (<alias-list>)]
```

## Parameters

The `flatten` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field>` | Required | The field to be flattened. Only object and nested fields are supported. |
| `<alias-list>` | Optional | A list of names to use instead of the original key names, separated by commas. If specifying more than one alias, enclose the list in parentheses. The number of aliases must match the number of keys in the struct, and the aliases must follow the lexicographical order of the corresponding original keys. |  
  

## Example: Flatten the instrumentation scope object  

The following query flattens the `instrumentationScope` nested object into individual fields, useful for analyzing which OTel SDK versions are in use:
  
```ppl
source=otellogs
| where NOT ISNULL(instrumentationScope.name)
| flatten instrumentationScope
| fields severityText, name, version
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+--------------+----------------------+---------+
| severityText | name                 | version |
|--------------+----------------------+---------|
| INFO         | opentelemetry-js     | 1.21.0  |
| INFO         | opentelemetry-dotnet | 1.7.0   |
| WARN         | opentelemetry-go     | 1.24.0  |
| ERROR        | opentelemetry-js     | 1.21.0  |
+--------------+----------------------+---------+
```
  

## Limitations

The `flatten` command has the following limitations:

* The `flatten` command may not function as expected if the fields to be flattened are not visible. For example, in the query `source=my-index | fields message | flatten message`, the `flatten message` command fails to execute as expected because some flattened fields, such as `message.info` and `message.author`, are hidden after the `fields message` command. As an alternative, use `source=my-index | flatten message`.
