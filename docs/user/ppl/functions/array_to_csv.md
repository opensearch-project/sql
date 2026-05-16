# ARRAY_TO_CSV

**Usage**: `array_to_csv(array, [delimiter])`

Converts an array to a comma-separated values (CSV) string representation. All array elements are converted to their string representation and joined using the specified delimiter.

**Parameters**:

- `array` (Required): An array expression of any supported data type to convert to CSV.
- `delimiter` (Optional): The string to use as a separator between array elements. Defaults to comma (",").

**Return type**: `STRING`

## Example

The following example converts an array to CSV with the default comma delimiter:

```ppl
source=people
| eval result = array_to_csv(array('apple', 'banana', 'cherry'))
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+-------------------------+
| result                  |
|-------------------------|
| apple,banana,cherry     |
+-------------------------+
```

The following example uses a custom delimiter:

```ppl
source=people
| eval result = array_to_csv(array('a', 'b', 'c'), '|')
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| a|b|c   |
+---------+
```

The following example converts a numeric array:

```ppl
source=people
| eval result = array_to_csv(array(1, 2, 3, 4))
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| 1,2,3,4 |
+---------+
```

The following example handles mixed data types:

```ppl
source=people
| eval result = array_to_csv(array('text', 123, 45.67), ';')
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+----------------+
| result         |
|----------------|
| text;123;45.67 |
+----------------+
```

The following example uses multi-character delimiters:

```ppl
source=people
| eval result = array_to_csv(array('apple', 'banana', 'cherry'), ' -> ')
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+---------------------------+
| result                    |
|---------------------------|
| apple -> banana -> cherry |
+---------------------------+
```

The following example uses arrays created from real database fields:

```ppl
source=accounts
| eval names_array = array(firstname, lastname)
| eval full_name = array_to_csv(names_array, ' ')
| fields firstname, lastname, full_name
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+------------+-----------+--------------+
| firstname  | lastname  | full_name    |
|------------|-----------|--------------|
| Amber      | Duke      | Amber Duke   |
+------------+-----------+--------------+
```

The following example shows behavior with empty arrays:

```ppl
source=people
| eval result = array_to_csv(array())
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
|        |
+--------+
```

The following example shows behavior with single element arrays:

```ppl
source=people
| eval result = array_to_csv(array('single'))
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| single |
+--------+
```

The following example demonstrates null element handling:

```ppl
source=people
| eval result = array_to_csv(array('a', null, 'c'))
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| a,,c   |
+--------+
```

## Limitations

- The function converts all array elements to their string representation
- Null array elements are represented as empty strings in the output
- If the input array is null, the function returns null
- If the delimiter is null, it defaults to comma (",")