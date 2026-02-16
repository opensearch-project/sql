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

## Example 1: Basic nomv usage

```ppl
source=accounts
| where account_number=1
| eval names = array(firstname, lastname)
| nomv names
| fields account_number, names
```

Expected output:
```text
fetched rows / total rows = 1/1
+----------------+-------+
| account_number | names |
|----------------+-------|
| 1              | Amber |
|                | Duke  |
+----------------+-------+
```

## Example 2: nomv with an eval-created field

```ppl
source=accounts
| where account_number=1
| eval location = array(city, state)
| nomv location
| fields account_number, location
```

Expected output:
```text
fetched rows / total rows = 1/1
+----------------+----------+
| account_number | location |
|----------------+----------|
| 1              | Brogan   |
|                | IL       |
+----------------+----------+
```

---

## Notes

- The `nomv` command is only available when the Calcite query engine is enabled.
- This command is particularly useful when you need to export or display multivalue fields as single strings.
- The newline delimiter (`\n`) is fixed and cannot be customized. For custom delimiters, use the `mvjoin` function directly in an eval expression.
- NULL values within the array are automatically filtered out when converting the array to a string, so they do not appear in the output or contribute empty lines.

## Related commands

- `mvjoin()` -- Function used by nomv internally to join array elements with a custom delimiter
