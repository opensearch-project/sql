# mvexpand

## Description
The `mvexpand` command expands each value in a multivalue (array) field into a separate row, similar to Splunk's `mvexpand` command. For each document, every element in the specified array field is returned as a new row.


## Syntax
```
mvexpand <field> [limit=<int>]
```

- `<field>`: The multivalue (array) field to expand. (Required)
- `limit`: Maximum number of values per document to expand. (Optional)

## Notes about these doctests
- The examples below generate deterministic multivalue fields using `eval` + `array()` so doctests are stable.
- All examples run against a single source index (`people`) and use `head 1` to keep output predictable.


### Output field naming
After `mvexpand`, the expanded value remains under the same field name (for example, `tags` or `ids`).
If the array contains objects, you can reference subfields (for example, `skills.name`).


## Examples

### Example 1: Basic Expansion (single document)
Input document (case "basic") contains three tag values.

PPL query:
```ppl
source=people
| eval tags = array('error', 'warning', 'info')
| fields tags
| head 1
| mvexpand tags
| fields tags
```

Expected output:
```text
fetched rows / total rows = 3/3
+---------+
| tags    |
|---------|
| error   |
| warning |
| info    |
+---------+
```

### Example 2: Expansion with Limit
Input document (case "ids") contains an array of integers; expand and apply limit.

PPL query:
```ppl
source=people
| eval ids = array(1, 2, 3, 4, 5)
| fields ids
| head 1
| mvexpand ids limit=3
| fields ids
```

Expected output:
```text
fetched rows / total rows = 3/3
+-----+
| ids |
|-----|
| 1   |
| 2   |
| 3   |
+-----+
```

### Example 3: Empty Expansion
This example demonstrates that mvexpand produces no rows when there are no matching input rows.

PPL query:
```ppl
source=people
| eval tags = array('dummy')
| where false
| fields tags
| head 1
| mvexpand tags
| fields tags
```

Expected output:
```text
fetched rows / total rows = 0/0
+------+
| tags |
|------|
+------+
```

### Example 4: Single-value array (case "single")
Single-element array should expand to one row.

PPL query:
```ppl
source=people
| eval tags = array('error')
| fields tags
| head 1
| mvexpand tags
| fields tags
```

Expected output:
```text
fetched rows / total rows = 1/1
+-------+
| tags  |
|-------|
| error |
+-------+
```

### Example 5: Missing Field
If the field is missing in the document (case "missing"), no rows are produced.

PPL query:
```ppl
source=people
| eval some_field = 'x'
| fields some_field
| head 1
| mvexpand tags
| fields tags
```

Expected output:
```text
fetched rows / total rows = 0/0
+------+
| tags |
|------|
+------+
```