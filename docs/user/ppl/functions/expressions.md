# Expressions

Expressions, particularly value expressions, return a scalar value. Expressions have different types and forms. For example, there are literal values as atomic expressions, as well as arithmetic, predicate, and function expressions built on top of them. You can use expressions in different clauses, such as arithmetic expressions in the `Filter` or `Stats` commands.

## Arithmetic operators

Arithmetic expressions are formed by combining numeric literals and binary arithmetic operators. The following operators are available:
1. `+`: Addition
2. `-`: Subtraction
3. `*`: Multiplication
4. `/`: Division. When [`plugins.ppl.syntax.legacy.preferred`](../admin/settings.md) is `true` (default), integer operands follow the legacy truncating result. When the setting is `false`, the operands are promoted to floating-point, preserving the fractional part. Division by zero returns `NULL`.
5. `%`: Modulo. This operator can only be used with integers and returns the remainder of the division.

### Precedence

You can use parentheses to control the precedence of arithmetic operators. Otherwise, operators with higher precedence are performed first.

### Type conversion

The system performs implicit type conversion when determining which operator to use. For example, adding an integer to a real number matches the signature `+(double,double)`, which results in a real number. The same type conversion rules apply to function calls.

### Examples

The following are examples of different types of arithmetic expressions:
  
```ppl
source=accounts
| where age > (25 + 5)
| fields age
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+-----+
| age |
|-----|
| 32  |
| 36  |
| 33  |
+-----+
```
  
## Predicate operators

Predicate operators are expressions that evaluate to `true` or `false`.

Comparisons for `MISSING` and `NULL` values follow these rules:

- `MISSING` values only equal other `MISSING` values and are less than all other values.
- `NULL` values equal other `NULL` values, are greater than `MISSING` values, but less than all other values.

### Operators
  
| Name | Description |
| --- | --- |
| `>` | Greater than |
| `>=` | Greater than or equal to |
| `<` | Less than |
| `!=` | Not equal to |
| `<=` | Less than or equal to |
| `=` | Equal to |
| `==` | Equal to (alternative syntax) |
| `LIKE` | Simple pattern matching |
| `IN` | Value list membership test |
| `AND` | Logical AND |
| `OR` | Logical OR |
| `XOR` | Logical XOR |
| `NOT` | Logical NOT |
  
You can compare date and time values. When comparing different date and time types (for example, `DATE` and `TIME`), both values are converted to `DATETIME`.

The following conversion rules are applied:
- A `TIME` value is combined with today's date.
- A `DATE` value is interpreted as midnight on that date.

### Examples

The following examples demonstrate how to use predicate operators in PPL queries.

#### Basic predicate operators

The following is an example of comparison operators:
  
```ppl
source=accounts
| where age > 33
| fields age
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----+
| age |
|-----|
| 36  |
+-----+
```
  
The `==` operator can be used as an alternative to `=` for equality comparisons.
  
```ppl
source=accounts
| where age == 32
| fields age
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----+
| age |
|-----|
| 32  |
+-----+
```
  
> **Note**: Both `=` and `==` perform the same equality comparison. You can use either based on your preference.
#### IN  

The `IN` operator tests whether a field value is in the specified list of values.
  
```ppl
source=accounts
| where age in (32, 33)
| fields age
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+-----+
| age |
|-----|
| 32  |
| 33  |
+-----+
```
  
#### OR  

The `OR` operator performs a logical OR operation between two Boolean expressions.
  
```ppl
source=accounts
| where age = 32 OR age = 33
| fields age
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+-----+
| age |
|-----|
| 32  |
| 33  |
+-----+
```
  
#### NOT  

The `NOT` operator performs a logical NOT operation, negating a Boolean expression.
  
```ppl
source=accounts
| where not age in (32, 33)
| fields age
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+-----+
| age |
|-----|
| 36  |
| 28  |
+-----+
```
  