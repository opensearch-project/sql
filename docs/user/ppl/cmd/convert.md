# Convert Command

The `convert` command applies conversion functions to transform field values into different data types and formats.

## Syntax

```
... | convert <convert-function>(<field-list>) [AS <field>] [<convert-function>(<field-list>) [AS <field>]]...
```

## Conversion Functions

### Numeric Conversions

#### `auto(field)`
Automatically converts fields to numbers using intelligent conversion:
- Converts numeric strings to double precision numbers
- Removes commas from numeric strings before conversion
- Extracts leading numbers from strings starting with digits
- Supports special values like `NaN`
- Returns `null` for values that cannot be converted to a number

**Examples:**
```sql
source=accounts | convert auto(balance)
```
- `"39,225"` → `39225.0`
- `"1,,234"` → `1234.0` (handles consecutive commas)
- `"2,12.0 sec"` → `2.0`
- `"45.67 kg"` → `45.67`
- `"1e5"` → `100000.0` (scientific notation)
- `"NaN"` → `NaN`
- `"hello"` → `null`
- `"AAAA2.000"` → `null` (doesn't start with digit)

#### `num(field)`
Extracts leading numbers from strings. Handles commas and units intelligently:
- For strings without letters: removes commas as thousands separators
- For strings with letters: extracts leading number, stops at letters or commas
- Supports special value `NaN`
- Returns `null` for non-convertible values

**Examples:**
```sql
source=accounts | convert num(age)
```
- `"1,234"` → `1234.0`
- `"1,,234"` → `1234.0` (handles consecutive commas)
- `"32"` → `32.0`
- `"212 sec"` → `212.0`
- `"2,12.0 sec"` → `2.0`
- `"1e5"` → `100000.0` (scientific notation)
- `"NaN"` → `NaN`
- `"no numbers"` → `null`

#### `rmcomma(field)`
Removes commas from field values and attempts to convert to a number. Returns `null` if the value contains letters.

**Examples:**
```sql
source=accounts | convert rmcomma(balance)
```
- `"1,234"` → `1234.0`
- `"1,,234"` → `1234.0` (handles consecutive commas)
- `"1,234.56"` → `1234.56`
- `"34,54,45"` → `345445.0`
- `"abc"` → `null`
- `"AAA3454,45"` → `null`

#### `rmunit(field)`
Extracts leading numeric values from strings. Stops at the first non-numeric character (including commas).

**Examples:**
```sql
source=metrics | convert rmunit(duration)
```
- `"123 dollars"` → `123.0`
- `"45.67 kg"` → `45.67`
- `"2.000 sec"` → `2.0`
- `"34,54,45"` → `34.0` (stops at first comma)
- `"no numbers"` → `null`
- `"AAAA2\\ sec"` → `null` (doesn't start with digit)

### Utility Functions

#### `none(field)`
No-op function that preserves the original field value. Used for excluding specific fields from wildcard conversions.

**Example:**
```sql
source=accounts | convert none(account_id)
```

## Parameters

- `<convert-function>`: One of the conversion functions listed above
- `<field-list>`: Field name(s) to convert
- `AS <field>`: (Optional) Create new field with converted value, preserving original

## Examples

### Basic Conversion
```sql
source=accounts | convert auto(balance)
```

### Multiple Conversions
```sql
source=data | convert auto(balance), num(age), rmcomma(description)
```

### Using AS Clause
```sql
source=accounts | convert auto(balance) AS balance_num | fields account_number, balance_num
```

### Complex Example
```sql
source=sales | convert auto(revenue) AS revenue_clean, rmunit(duration) AS duration_seconds | stats sum(revenue_clean) by product
```

## Notes

- All conversion functions (`auto()`, `num()`, `rmunit()`, `rmcomma()`) return `null` for values that cannot be converted to a number
- All numeric conversion functions return double precision numbers to support use in aggregations like `avg()`, `sum()`, etc.
- **Display Format**: All converted numbers display with decimal notation (e.g., `1234.0`, `1234.56`)
- The `auto()` function is the most comprehensive and handles mixed data formats
- Use `AS` clause to preserve original fields while creating converted versions
- Multiple conversions can be applied in a single command

## Limitations

The `convert` command can only work with `plugins.calcite.enabled=true`. 

When Calcite is disabled, attempting to use convert functions will result in an "unsupported function" error.
