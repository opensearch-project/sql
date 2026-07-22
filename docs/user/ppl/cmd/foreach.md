# foreach

The `foreach` command runs a templated `eval` expression for each field in a field list, each element of a multivalue (array) field, or each element of a JSON array. It eliminates repetitive `eval` statements when the same computation applies to many fields or to every element of a collection.

## Syntax

The `foreach` command has the following syntax:

```syntax
foreach [mode=<mode>] [<option>=<value>]... <target>... "[" eval <field-template> = <expression> ["," <field-template> = <expression>]... "]"
```

## Parameters

The `foreach` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `mode` | Optional | How the target is interpreted: `multifield` (iterate over matching fields), `multivalue` (iterate over the elements of an array field), `json_array` (parse a JSON array string and iterate over its elements), or `auto_collections` (detect `multivalue` or `json_array` automatically). Defaults to `multifield`, or `json_array` when the target is a `json_array(...)` function call. |
| `<target>` | Required for most modes | In `multifield` mode, one or more field names or wildcard patterns (for example, `value_*`). In collection modes, exactly one array field, JSON array string, or `json_array(...)` call. May be omitted in `auto_collections` mode, which then picks the first array field. |
| `fieldstr` | Optional | Renames the `<<FIELD>>` placeholder in `multifield` mode. |
| `matchstr` | Optional | Renames the `<<MATCHSTR>>` placeholder in `multifield` mode. |
| `itemstr` | Optional | Renames the `<<ITEM>>` placeholder in collection modes. |
| `iterstr` | Optional | Renames the `<<ITER>>` placeholder in collection modes. |

### Placeholders

Inside the bracketed `eval`, placeholders refer to the current iteration:

| Placeholder | Mode | Meaning |
| --- | --- | --- |
| `<<FIELD>>` | `multifield` | The name of the current field. Usable in both the target field template and the expression. |
| `<<MATCHSTR>>` | `multifield` | The part of the field name matched by wildcards in the pattern. |
| `<<ITEM>>` | collection modes | The current element of the collection. |
| `<<ITER>>` | collection modes | The zero-based index of the current element. |

Placeholders renamed via `itemstr`/`iterstr` may be written without the `<<...>>` markers in the expression.

## Notes

The following considerations apply when using the `foreach` command:

* In collection modes, the bracketed `eval` acts as an accumulator: each target field must already exist (typically initialized with a preceding `eval`), and the expressions are applied once per element. Multiple assignments run from left to right, so a later assignment in the same iteration sees an earlier assignment's updated value.
* In `json_array` mode the element type is inferred: a `json_array(...)` call or JSON string literal is inspected at plan time; for a field holding JSON text, elements are treated as numbers when `<<ITEM>>` is used in arithmetic and as strings otherwise. Plan-time arrays with mixed string/number elements are rejected. When numeric use is inferred for field-backed JSON text, elements that cannot be converted to numbers evaluate to `null`.
* Placeholders are also substituted inside string literals. For example, `eval <<FIELD>> = '<<FIELD>>'` replaces each selected field value with that field's name.
* `multivalue` mode applied to a non-array value and `json_array` mode applied to a native array are no-ops. A field whose mapping is scalar cannot be identified as multivalue at plan time even when a document stores several values, so `multivalue` mode also no-ops for that mapping.

## Example 1: Apply the same calculation to multiple fields

The following query doubles two fields using one templated eval:

```ppl
source=accounts
| foreach age balance [ eval <<FIELD>>_double = <<FIELD>> * 2 ]
| fields age, age_double, balance, balance_double
| head 2
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+-----+------------+---------+----------------+
| age | age_double | balance | balance_double |
|-----+------------+---------+----------------|
| 32  | 64         | 39225   | 78450          |
| 36  | 72         | 5686    | 11372          |
+-----+------------+---------+----------------+
```

## Example 2: Wildcard field patterns with `<<MATCHSTR>>`

The following query copies every field ending in `name`, naming each copy after the wildcard match:

```ppl
source=accounts
| foreach *name [ eval copy_<<MATCHSTR>> = <<FIELD>> ]
| fields firstname, copy_first, lastname, copy_last
| head 2
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+-----------+------------+----------+-----------+
| firstname | copy_first | lastname | copy_last |
|-----------+------------+----------+-----------|
| Amber     | Amber      | Duke     | Duke      |
| Hattie    | Hattie     | Bond     | Bond      |
+-----------+------------+----------+-----------+
```

## Example 3: Sum the elements of an array field

The following query iterates over an array and accumulates a total:

```ppl
source=accounts
| eval nums = array(1, 2, 3), total = 0
| foreach mode=multivalue nums [ eval total = total + <<ITEM>> ]
| fields total
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+-------+
| total |
|-------|
| 6     |
+-------+
```

## Example 4: Use the element index with a custom placeholder name

The following query sums the indexes (0 + 1 + 2) instead of the values, renaming `<<ITER>>` to `IDX`:

```ppl
source=accounts
| eval nums = array(10, 20, 30), total = 0
| foreach mode=multivalue iterstr=IDX nums [ eval total = total + IDX ]
| fields total
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+-------+
| total |
|-------|
| 3     |
+-------+
```

## Example 5: Iterate over a JSON array string

The following query parses a JSON array literal and accumulates its elements:

```ppl
source=accounts
| eval total = 0
| foreach mode=json_array '[1,2,3]' [ eval total = total + <<ITEM>> ]
| fields total
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+-------+
| total |
|-------|
| 6.0   |
+-------+
```

`json_array` mode also accepts a field whose value is JSON array text, which is the common case for raw logs that embed JSON.

## Example 6: Automatic collection detection

The following query lets `auto_collections` mode detect that the target is an array field:

```ppl
source=accounts
| eval nums = array(1, 2, 3), total = 0
| foreach mode=auto_collections nums [ eval total = total + <<ITEM>> ]
| fields total
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+-------+
| total |
|-------|
| 6     |
+-------+
```

## Related commands

- [eval](eval.md) --- evaluate an expression and append the result to search results
- [fieldformat](fieldformat.md) --- format field values for display
