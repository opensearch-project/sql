# nomv

## Description

The `nomv` command converts a multivalue (array) field into a single-value string field by joining all array elements with newline characters (`\n`). This operation is performed in-place, replacing the original field with its joined string representation.

`nomv` is a transforming command: it modifies the specified field without changing the number of rows in the result set.

### Key behaviors

- The field must be a **direct field reference** of **ARRAY type**. For scalar fields, use the `array()` function to create an array first. 
- The specified field is **replaced** with a string containing all array elements joined by newline (`\n`) characters.
- **NULL values within the array are automatically filtered out** before joining.
- If the field doesn't exist, an error is returned.
- The operation uses Calcite's ARRAY_JOIN function internally (same underlying implementation as mvjoin).

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

## Example 3: Error when field does not exist

```ppl
source=accounts
| nomv does_not_exist
```

Expected output:
```text
{'reason': 'Invalid Query', 'details': 'Field [does_not_exist] not found.', 'type': 'IllegalArgumentException'}
Error: Query returned no data
```

---

## Notes

- The `nomv` command is only available when the Calcite query engine is enabled.
- This command is particularly useful when you need to export or display multivalue fields as single strings.
- The newline delimiter (`\n`) is fixed and cannot be customized. For custom delimiters, use the `mvjoin` function directly in an eval expression.
- NULL values are automatically filtered out during the join operation, so they do not contribute empty strings to the output.

## Related commands

- `mvjoin()` -- Function used by nomv internally to join array elements with a custom delimiter
- [`eval`](eval.md) -- Create computed fields using the `array()` and `mvjoin()` functions