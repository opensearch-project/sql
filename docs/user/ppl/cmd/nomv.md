
# nomv

The `nomv` command converts a multivalue (array) field into a single-value string field by joining all array elements with newline characters (`\n`). This operation is performed in-place, replacing the original field with its joined string representation.

> **Note**: The field must be an array type. For scalar fields, use the `array()` function to create an array first.

## Syntax

The `nomv` command has the following syntax:

```syntax
nomv <field>
```

## Parameters

The `nomv` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field>` | Required | The name of the field whose multivalue content should be converted to a single-value string. |

## Example 1: Convert a collected list to a single-value string

The following query collects all service names that reported errors into an array, then converts it to a string:

```ppl
source=otellogs
| where severityText = 'ERROR'
| stats list(`resource.attributes.service.name`) as affected_services by severityText
| nomv affected_services
| fields severityText, affected_services
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+--------------+-------------------+
| severityText | affected_services |
|--------------+-------------------|
| ERROR        | payment           |
|              | checkout          |
|              | payment           |
|              | frontend-proxy    |
|              | recommendation    |
|              | product-catalog   |
|              | checkout          |
+--------------+-------------------+
```

## Limitations

The `nomv` command has the following limitations:

- The `nomv` command is only available when the Calcite query engine is enabled.
- The newline delimiter (`\n`) is fixed and cannot be customized. For custom delimiters, use the `mvjoin` function directly in an `eval` expression.
- `NULL` values within the array are automatically filtered out and do not appear in the output.

## Related commands

- [`mvcombine`](mvcombine.md) -- Combines multiple rows into a single row with multivalue fields
- [`mvexpand`](mvexpand.md) -- Expands multivalue fields into separate rows
