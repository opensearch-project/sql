# nomv

## Description

The `nomv` command converts a multivalue (array) field into a single-value string field by joining all array elements with newline characters (`\n`). This operation is performed in-place, replacing the original field with its joined string representation.

`nomv` is a transforming command: it modifies the specified field without changing the number of rows in the result set.

### Key behaviors

- The field must be **ARRAY type**. For scalar fields, use the `array()` function to create an array first. 

---

## Syntax

```syntax
nomv <field>
```

### Arguments

- **field** (required)
  The name of the field whose multivalue content should be converted to a single-value string.

---

## Example 1: Convert a collected list to a multiline display

The following query collects all service names that reported errors into an array, then converts it to a multiline string for display in a report:

```ppl
source=otellogs
| where severityText = 'ERROR'
| stats list(`resource.attributes.service.name`) as affected_services by severityText
| nomv affected_services
| fields severityText, affected_services
```

Expected output:
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

---

## Notes

- The `nomv` command is only available when the Calcite query engine is enabled.
- This command is particularly useful when you need to export or display multivalue fields as single strings.
- The newline delimiter (`\n`) is fixed and cannot be customized. For custom delimiters, use the `mvjoin` function directly in an eval expression.
- NULL values within the array are automatically filtered out when converting the array to a string, so they do not appear in the output or contribute empty lines.

## Related commands

- [`mvcombine`](mvcombine.md) -- Combines multiple rows into a single row with multivalue fields
- [`mvexpand`](mvexpand.md) -- Expands multivalue fields into separate rows
