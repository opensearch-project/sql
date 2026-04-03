# Collection functions

Collection functions create, manipulate, and analyze arrays and multivalue fields in data. These functions are essential for working with complex data structures and performing operations such as filtering, transforming, and analyzing array elements.

The following collection functions are supported in PPL.

## ARRAY

**Usage**: `array(value1, value2, value3...)`

Creates an array containing the input values. Mixed types are automatically converted to the least restrictive type. For example, `array(1, "demo")` returns `["1", "demo"]` where the integer is converted to a string.

**Parameters**:

- `value1` (Required): A value of any type to include in the array.
- `value2`, `value3` (Optional): Additional values of any type to include in the array.

**Return type**: `ARRAY`

#### Example

The following example creates an array with numeric values:
  
```ppl
source=people
| eval array = array(1, 2, 3)
| fields array
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------+
| array   |
|---------|
| [1,2,3] |
+---------+
```

The following example demonstrates mixed-type conversion:
  
```ppl
source=people
| eval array = array(1, "demo")
| fields array
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------+
| array    |
|----------|
| [1,demo] |
+----------+
```
  
## ARRAY_LENGTH

**Usage**: `array_length(array)`

Returns the length of the input `array`.

**Parameters**:

- `array` (Required): The array for which to return the length.

**Return type**: `INTEGER`

#### Example

```ppl
source=people
| eval array = array(1, 2, 3)
| eval length = array_length(array)
| fields length
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------+
| length |
|--------|
| 3      |
+--------+
```
  
## FORALL

**Usage**: `forall(array, function)`

Checks whether all elements in the array satisfy the lambda function condition. The lambda function must accept a single input parameter and return a Boolean value.

**Parameters**:

- `array` (Required): The array to check.
- `function` (Required): A lambda function that returns a Boolean value and accepts a single input parameter.

**Return type**: `BOOLEAN`

#### Example

```ppl
source=people
| eval array = array(1, 2, 3), result = forall(array, x -> x > 0)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| True   |
+--------+
```
  
## EXISTS

**Usage**: `exists(array, function)`

Checks whether at least one element in the array satisfies the lambda function condition. The lambda function must accept a single input parameter and return a Boolean value.

**Parameters**:

- `array` (Required): The array to check.
- `function` (Required): A lambda function that returns a Boolean value and accepts a single input parameter.

**Return type**: `BOOLEAN`

#### Example

```ppl
source=people
| eval array = array(-1, -2, 3), result = exists(array, x -> x > 0)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| True   |
+--------+
```
  
## FILTER

**Usage**: `filter(array, function)`

Filters the elements in the array using a lambda function. The lambda function must accept a single input parameter and return a Boolean value.

**Parameters**:

- `array` (Required): The array to filter.
- `function` (Required): A lambda function that returns a Boolean value and accepts a single input parameter.

**Return type**: `ARRAY`

#### Example

```ppl
source=people
| eval array = array(1, -2, 3), result = filter(array, x -> x > 0)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| [1,3]  |
+--------+
```
  
## TRANSFORM

**Usage**: `transform(array, function)`

Transforms the elements of the `array` one by one using a lambda function. The lambda function can accept one or two inputs. If the lambda function accepts two parameters, the second parameter is the index of the element in the `array`.

**Parameters**:

- `array` (Required): The array to transform.
- `function` (Required): A lambda function that accepts one or two input parameters and returns a transformed value.

**Return type**: `ARRAY`

#### Example

The following example transforms each element by adding 2:

<!-- TODO: To be fixed with https://github.com/opensearch-project/sql/issues/4972 -->
```ppl ignore
source=people
| eval array = array(1, -2, 3), result = transform(array, x -> x + 2)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| [3,0,5] |
+---------+
```

The following example uses both element value and index in the transformation:

<!-- TODO: To be fixed with https://github.com/opensearch-project/sql/issues/4972 -->
```ppl ignore
source=people
| eval array = array(1, -2, 3), result = transform(array, (x, i) -> x + i)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+----------+
| result   |
|----------|
| [1,-1,5] |
+----------+
```
  
## REDUCE

**Usage**: `reduce(array, acc_base, function, <reduce_function>)`

Uses a lambda function to iterate through all elements and interact with the accumulator base value. The lambda function accepts two parameters: the accumulator and the array element. When an optional `reduce_function` is provided, it is applied to the final accumulator value. The reduce function accepts the accumulator as a single parameter.

**Parameters**:

- `array` (Required): The array to reduce.
- `acc_base` (Required): The initial accumulator value.
- `function` (Required): A lambda function that accepts accumulator and array element as parameters.
- `reduce_function` (Optional): A lambda function to apply to the final accumulator value.

**Return type**: Same as accumulator type (determined by `acc_base` and `reduce_function`)

#### Example

The following example reduces an array by summing all elements with an initial value:

```ppl
source=people
| eval array = array(1, -2, 3), result = reduce(array, 10, (acc, x) -> acc + x)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| 12     |
+--------+
```

The following example uses an additional reduce function to transform the final result:
  
```ppl
source=people
| eval array = array(1, -2, 3), result = reduce(array, 10, (acc, x) -> acc + x, acc -> acc * 10)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| 120    |
+--------+
```
  
## MVJOIN

**Usage**: `mvjoin(array, delimiter)`

Joins string array elements into a single string, separated by the specified delimiter. `NULL` elements are excluded from the output. Only string arrays are supported.

**Parameters**:

- `array` (Required): An array of strings to join.
- `delimiter` (Required): The string to use as a separator between array elements.

**Return type**: `STRING`

#### Example

The following example joins an array of strings with a comma delimiter:

```ppl
source=people
| eval result = mvjoin(array('a', 'b', 'c'), ',')
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| a,b,c  |
+--------+
```

The following example joins field values into a single string:
  
```ppl
source=accounts
| eval names_array = array(firstname, lastname)
| eval result = mvjoin(names_array, ', ')
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-------------+
| result      |
|-------------|
| Amber, Duke |
+-------------+
```
  
## MVAPPEND

**Usage**: `mvappend(value1, value2, value3...)`

Appends all elements from parameters to create an array. Flattens array parameters and collects all individual elements. Always returns an array or `NULL` for consistent type behavior.

**Parameters**:

- `value1` (Required): A value of any type to append to the array.
- `value2` (Optional): Additional values of any type to append to the array.
- `...` (Optional): Any number of additional values.

**Return type**: `ARRAY`

#### Example

The following example appends multiple values to create an array:

```ppl
source=people
| eval result = mvappend(1, 1, 3)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| [1,1,3] |
+---------+
```

The following example demonstrates array flattening:
  
```ppl
source=people
| eval result = mvappend(1, array(2, 3))
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| [1,2,3] |
+---------+
```

The following example shows nested `mvappend` calls:
  
```ppl
source=people
| eval result = mvappend(mvappend(1, 2), 3)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| [1,2,3] |
+---------+
```

The following example creates an array from a single value:
  
```ppl
source=people
| eval result = mvappend(42)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| [42]   |
+--------+
```

The following example demonstrates `NULL` value filtering:
  
```ppl
source=people
| eval result = mvappend(nullif(1, 1), 2)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| [2]    |
+--------+
```

The following example shows behavior with only `NULL` values:
  
```ppl
source=people
| eval result = mvappend(nullif(1, 1))
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| null   |
+--------+
```

The following example concatenates multiple arrays:
  
```ppl
source=people
| eval arr1 = array(1, 2), arr2 = array(3, 4), result = mvappend(arr1, arr2)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------+
| result    |
|-----------|
| [1,2,3,4] |
+-----------+
```

The following example appends field values:
  
```ppl
source=accounts
| eval result = mvappend(firstname, lastname)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------+
| result       |
|--------------|
| [Amber,Duke] |
+--------------+
```

The following example demonstrates mixed data types:
  
```ppl
source=people
| eval result = mvappend(1, 'text', 2.5)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------+
| result       |
|--------------|
| [1,text,2.5] |
+--------------+
```
  
## SPLIT

**Usage**: `split(str, delimiter)`

Splits the string values on the delimiter and returns the string values as a multivalue field (array). Use an empty string (`""`) to split the original string into one value per character. If the delimiter is not found, the function returns an array containing the original string. If the input string is empty, the function returns an empty array.

**Parameters**:

- `str` (Required): The string to split.
- `delimiter` (Required): The string to use as a delimiter for splitting.

**Return type**: `ARRAY`

#### Example

The following example splits a string using a semicolon delimiter:

```ppl
source=people
| eval test = 'buttercup;rarity;tenderhoof;dash', result = split(test, ';')
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+------------------------------------+
| result                             |
|------------------------------------|
| [buttercup,rarity,tenderhoof,dash] |
+------------------------------------+
```

The following example uses a multi-character delimiter:

```ppl
source=people
| eval test = '1a2b3c4def567890', result = split(test, 'def')
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+------------------+
| result           |
|------------------|
| [1a2b3c4,567890] |
+------------------+
```

The following example splits a string into individual characters using an empty delimiter:

```ppl
source=people
| eval test = 'abcd', result = split(test, '')
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+-----------+
| result    |
|-----------|
| [a,b,c,d] |
+-----------+
```

The following example splits using a double-colon delimiter:

```ppl
source=people
| eval test = 'name::value', result = split(test, '::')
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+--------------+
| result       |
|--------------|
| [name,value] |
+--------------+
```

The following example shows behavior when the delimiter is not found:

```ppl
source=people
| eval test = 'hello', result = split(test, ',')
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| [hello] |
+---------+
```
  
## MVDEDUP

**Usage**: `mvdedup(array)`

Removes duplicate values from a multivalue array while preserving the order of the first occurrence. `NULL` elements are filtered out. Returns a deduplicated array, or `NULL` if the input is `NULL`.

**Parameters**:

- `array` (Required): The array from which to remove duplicates.

**Return type**: `ARRAY`

#### Example

The following example removes duplicate numbers while preserving order:

```ppl
source=people
| eval array = array(1, 2, 2, 3, 1, 4), result = mvdedup(array)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------+
| result    |
|-----------|
| [1,2,3,4] |
+-----------+
```

The following example deduplicates string values:
  
```ppl
source=people
| eval array = array('z', 'a', 'z', 'b', 'a', 'c'), result = mvdedup(array)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+-----------+
| result    |
|-----------|
| [z,a,b,c] |
+-----------+
```

The following example shows behavior with an empty array:
  
```ppl
source=people
| eval array = array(), result = mvdedup(array)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| []     |
+--------+
```

## MVFIND

**Usage**: `mvfind(array, regex)`

Searches a multivalue array and returns the `0`-based index of the first element that matches the regular expression. Returns `NULL` if no match is found.

**Parameters**:

- `array` (Required): The array to search.
- `regex` (Required): The regular expression pattern to match against array elements.

**Return type**: `INTEGER` (or `NULL` if no match found)

#### Example

The following example searches for the first element that matches a regular expression:

```ppl
source=people
| eval array = array('apple', 'banana', 'apricot'), result = mvfind(array, 'ban.*')
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| 1      |
+--------+
```

The following example shows behavior when no match is found:

```ppl
source=people
| eval array = array('cat', 'dog', 'bird'), result = mvfind(array, 'fish')
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| null   |
+--------+
```

The following example uses a regex pattern with character classes:

```ppl
source=people
| eval array = array('error123', 'info', 'error456'), result = mvfind(array, 'error[0-9]+')
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| 0      |
+--------+
```

The following example demonstrates case-insensitive matching:

```ppl
source=people
| eval array = array('Apple', 'Banana', 'Cherry'), result = mvfind(array, '(?i)banana')
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| 1      |
+--------+
```

## MVINDEX

**Usage**: `mvindex(array, start, [end])`

Returns a subset of the multivalue array using the start and optional end index values. Indexes are `0`-based (the first element is at index `0`). Supports negative indexing where `-1` refers to the last element. When only start is provided, the function returns a single element. When both start and end are provided, the function returns an array of elements from start to end (inclusive).

**Parameters**:

- `array` (Required): The array from which to extract elements.
- `start` (Required): The starting index (`0`-based).
- `end` (Optional): The ending index (`0`-based, inclusive).

**Return type**: Single element type when only `start` is provided; `ARRAY` when both `start` and `end` are provided

#### Example

The following example gets a single element at index 1:

```ppl
source=people
| eval array = array('a', 'b', 'c', 'd', 'e'), result = mvindex(array, 1)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| b      |
+--------+
```

The following example uses negative indexing to get the last element:
  
```ppl
source=people
| eval array = array('a', 'b', 'c', 'd', 'e'), result = mvindex(array, -1)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| e      |
+--------+
```

The following example extracts a range of elements:
  
```ppl
source=people
| eval array = array(1, 2, 3, 4, 5), result = mvindex(array, 1, 3)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| [2,3,4] |
+---------+
```

The following example uses negative indexing for a range:
  
```ppl
source=people
| eval array = array(1, 2, 3, 4, 5), result = mvindex(array, -3, -1)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| [3,4,5] |
+---------+
```

The following example extracts elements from the beginning of an array:
  
```ppl
source=people
| eval array = array('alex', 'celestino', 'claudia', 'david'), result = mvindex(array, 0, 2)
| fields result
| head 1
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+--------------------------+
| result                   |
|--------------------------|
| [alex,celestino,claudia] |
+--------------------------+
```

## MVMAP

**Usage**: `mvmap(array, expression)`

Iterates over each element of a multivalue array, applies the expression to each element, and returns a multivalue array containing the transformed results. The field name in the expression is implicitly bound to each element value.

**Parameters**:

- `array` (Required): The array to map over.
- `expression` (Required): The expression to apply to each element.

**Return type**: `ARRAY`

#### Example

The following example applies a mathematical operation to each element of an array:

<!-- TODO: To be fixed with https://github.com/opensearch-project/sql/issues/4972 -->
```ppl ignore
source=people
| eval array = array(1, 2, 3), result = mvmap(array, array * 10)
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+------------+
| result     |
|------------|
| [10,20,30] |
+------------+
```

The following example applies a different mathematical operation:

<!-- TODO: To be fixed with https://github.com/opensearch-project/sql/issues/4972 -->
```ppl ignore
source=people
| eval array = array(1, 2, 3), result = mvmap(array, array + 5)
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| [6,7,8] |
+---------+
```

> **Note**: For nested expressions such as `mvmap(mvindex(arr, 1, 3), arr * 2)`, the field name (`arr`) is extracted from the first argument and must match the field referenced in the expression.

The following example shows how the expression can reference other single-value fields:

<!-- TODO: To be fixed with https://github.com/opensearch-project/sql/issues/4972 -->
```ppl ignore
source=people
| eval array = array(1, 2, 3), multiplier = 10, result = mvmap(array, array * multiplier)
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+------------+
| result     |
|------------|
| [10,20,30] |
+------------+
```


## MVZIP

**Usage**: `mvzip(mv_left, mv_right, [delim])`

Combines the values in two multivalue arrays by pairing corresponding elements and joining them into strings. The delimiter specifies the character or string used to join the two values. This is similar to the Python zip command.

The values are combined by pairing the first value of `mv_left` with the first value of `mv_right`, then the second with the second, and so on. Each pair is concatenated into a string using the delimiter. The function stops at the length of the shorter array.

The delimiter is optional. When specified, it must be enclosed in quotation marks. The default delimiter is a comma.

Returns `NULL` if either input is `NULL`. Returns an empty array if either input array is empty.

**Parameters**:

- `mv_left` (Required): The first array to combine.
- `mv_right` (Required): The second array to combine.
- `delim` (Optional): The delimiter to use for joining pairs. Defaults to comma.

**Return type**: `ARRAY`

#### Example

The following example combines host and port arrays with a colon delimiter:

```ppl
source=people
| eval hosts = array('host1', 'host2'), ports = array('80', '443'), nserver = mvzip(hosts, ports, ':')
| fields nserver
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+----------------------+
| nserver              |
|----------------------|
| [host1:80,host2:443] |
+----------------------+
```

The following example uses a pipe delimiter with equal-length arrays:

```ppl
source=people
| eval arr1 = array('a', 'b', 'c'), arr2 = array('x', 'y', 'z'), result = mvzip(arr1, arr2, '|')
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+---------------+
| result        |
|---------------|
| [a|x,b|y,c|z] |
+---------------+
```

The following example demonstrates behavior with arrays of different lengths:

```ppl
source=people
| eval arr1 = array('1', '2', '3'), arr2 = array('a', 'b'), result = mvzip(arr1, arr2, '-')
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+-----------+
| result    |
|-----------|
| [1-a,2-b] |
+-----------+
```

The following example shows nested mvzip calls:

```ppl
source=people
| eval arr1 = array('a', 'b', 'c'), arr2 = array('x', 'y', 'z'), arr3 = array('1', '2', '3'), result = mvzip(mvzip(arr1, arr2, '-'), arr3, ':')
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+---------------------+
| result              |
|---------------------|
| [a-x:1,b-y:2,c-z:3] |
+---------------------+
```

The following example shows behavior with an empty array:

```ppl
source=people
| eval arr1 = array('a', 'b'), arr2 = array(), result = mvzip(arr1, arr2)
| fields result
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| []     |
+--------+
```
