# Convert Command

The `convert` command applies conversion functions to transform field values into different data types and formats.

## Syntax

```
... | convert <convert-function>(<field-list>) [AS <field>] [<convert-function>(<field-list>) [AS <field>]]...
```

## Conversion Functions

### Numeric Conversions

#### `auto(field)`
Automatically converts fields to numbers using comprehensive best-fit heuristics. Combines the functionality of `rmcomma()`, `rmunit()`, and `num()` functions:
- Removes commas from numeric strings
- Extracts leading numbers from mixed alphanumeric text
- Converts clean numeric values to appropriate numeric types

**Examples:**
```sql
source=accounts | convert auto(balance)
```
- `"39,225"` → `39225`
- `"1,234 dollars"` → `1234`
- `"45.67 kg"` → `45.67`

#### `num(field)`
Converts field values to numbers. Only works with clean numeric strings.

**Example:**
```sql
source=accounts | convert num(age)
```
- `"32"` → `32`
- `"1,234"` → `null` (fails with commas)

#### `rmcomma(field)`
Removes commas from field values, returning the cleaned string.

**Example:**
```sql
source=accounts | convert rmcomma(balance)
```
- `"39,225.50"` → `"39225.50"`

#### `rmunit(field)`
Extracts leading numeric values and removes trailing text/units.

**Example:**
```sql
source=metrics | convert rmunit(duration)
```
- `"212 seconds"` → `212`
- `"45.67 kg"` → `45.67`

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

- Conversion functions return `null` for values that cannot be converted
- The `auto()` function is the most comprehensive and handles mixed data formats
- Use `AS` clause to preserve original fields while creating converted versions
- Multiple conversions can be applied in a single command

## Limitations

The `convert` command can only work with `plugins.calcite.enabled=true`. 

When Calcite is disabled, attempting to use convert functions will result in an "unsupported function" error.
