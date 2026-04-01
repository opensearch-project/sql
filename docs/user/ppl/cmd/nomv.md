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

## Example 1: Convert array to multiline string

The following query creates an array from the severity text and service name, then converts it to a multiline string for display:

```ppl
source=otellogs
| where severityText = 'FATAL'
| eval summary = array(severityText, `resource.attributes.service.name`)
| nomv summary
| fields severityText, summary
| head 1
```

Expected output:
```text
fetched rows / total rows = 1/1
+--------------+-----------------+
| severityText | summary         |
|--------------+-----------------|
| FATAL        | FATAL           |
|              | payment-service |
+--------------+-----------------+
```

## Example 2: nomv with an eval-created field

```ppl
source=otellogs
| where severityText = 'ERROR'
| eval details = array(severityText, body)
| nomv details
| fields `resource.attributes.service.name`, details
| head 1
```

Expected output:
```text
fetched rows / total rows = 1/1
+----------------------------------+---------------------------------------------------------------------+
| resource.attributes.service.name | details                                                             |
|----------------------------------+---------------------------------------------------------------------|
| payment-service                  | ERROR                                                               |
|                                  | Payment failed: connection timeout to payment gateway after 30000ms |
+----------------------------------+---------------------------------------------------------------------+
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
