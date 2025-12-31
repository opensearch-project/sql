## convert

**Description**

The `convert` command converts fields in the search results to different data types. This is useful for data type transformations, especially when working with string representations of numbers or removing formatting from fields.

**Syntax**

```sql
convert <conversion-function>(<field>) [AS <alias>] [, <conversion-function>(<field>) [AS <alias>]]...
```

* `conversion-function`: The conversion function to apply (see below for available functions)
* `field`: The field name to convert
* `alias`: (Optional) An alias for the converted field

**Conversion Functions**

### auto(field)

Automatically converts a string field to a number. This function attempts to parse the string value as a numeric type.

**Example**:
```sql
source=accounts | convert auto(balance) | fields balance
```

### num(field)

Explicitly converts a string field to a number. Similar to `auto()`, but makes the intent more explicit in the query.

**Example**:
```sql
source=accounts | convert num(revenue) | fields revenue
```

### rmcomma(field)

Removes commas from a string field. This is useful when dealing with formatted numbers like "1,000,000".

**Example**:
```sql
source=accounts | convert rmcomma(amount) | fields amount
```

### rmunit(field)

Removes measurement units from a string field. This helps extract numeric values from fields containing units like "100KB" or "50ms".

**Example**:
```sql
source=logs | convert rmunit(response_size) | fields response_size
```

### none(field)

Pass-through function that doesn't perform any conversion. This can be useful for explicitly documenting that a field should not be converted.

**Example**:
```sql
source=accounts | convert none(account_id) | fields account_id
```

**Usage Examples**

### Example 1: Basic conversion

Convert a balance field from string to number:

```sql
source=accounts | convert auto(balance) | fields account_number, balance
```

### Example 2: Conversion with alias

Convert balance and give it a new name:

```sql
source=accounts | convert auto(balance) AS balance_num | fields account_number, balance_num
```

### Example 3: Multiple conversions

Convert multiple fields at once:

```sql
source=accounts | convert auto(balance), num(age), rmcomma(revenue) | fields balance, age, revenue
```

### Example 4: Convert then filter

Convert a field and then use it in a where clause:

```sql
source=accounts | convert auto(balance) | where balance > 10000 | fields account_number, balance
```

### Example 5: Convert then aggregate

Convert a field before using it in aggregation:

```sql
source=accounts | convert auto(balance) | stats avg(balance), sum(balance) by state
```

### Example 6: Remove formatting before analysis

Remove commas from formatted numbers:

```sql
source=sales | convert rmcomma(annual_revenue) | stats sum(annual_revenue) by region
```

**Limitations**

* The `convert` command requires Calcite engine to be enabled (`plugins.calcite.enabled=true`)
* Conversion functions only work on fields that can be logically converted to the target type
* Failed conversions may result in null values or errors depending on the input data

**Related Commands**

* [eval](eval.md) - Create new fields with calculated values
* [fields](fields.md) - Select which fields to display
* [where](where.md) - Filter results based on conditions
