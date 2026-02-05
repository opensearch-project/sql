# PPL Collection Functions  

## ARRAY  

### Description  

Usage: `array(value1, value2, value3...)` create an array with input values. Currently we don't allow mixture types. We will infer a least restricted type, for example `array(1, "demo")` -> ["1", "demo"]
**Argument type:** `value1: ANY, value2: ANY, ...`
**Return type:** `ARRAY`
### Example
  
```ppl
source=people
| eval array = array(1, 2, 3)
| fields array
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+---------+
| array   |
|---------|
| [1,2,3] |
+---------+
```
  
```ppl
source=people
| eval array = array(1, "demo")
| fields array
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------+
| array    |
|----------|
| [1,demo] |
+----------+
```
  
## ARRAY_LENGTH  

### Description  

Usage: `array_length(array)` returns the length of input array.
**Argument type:** `array:ARRAY`
**Return type:** `INTEGER`
### Example
  
```ppl
source=people
| eval array = array(1, 2, 3)
| eval length = array_length(array)
| fields length
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------+
| length |
|--------|
| 3      |
+--------+
```
  
## FORALL  

### Description  

Usage: `forall(array, function)` check whether all element inside array can meet the lambda function. The function should also return boolean. The lambda function accepts one single input.
**Argument type:** `array:ARRAY, function:LAMBDA`
**Return type:** `BOOLEAN`
### Example
  
```ppl
source=people
| eval array = array(1, 2, 3), result = forall(array, x -> x > 0)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| True   |
+--------+
```
  
## EXISTS  

### Description  

Usage: `exists(array, function)` check whether existing one of element inside array can meet the lambda function. The function should also return boolean. The lambda function accepts one single input.
**Argument type:** `array:ARRAY, function:LAMBDA`
**Return type:** `BOOLEAN`
### Example
  
```ppl
source=people
| eval array = array(-1, -2, 3), result = exists(array, x -> x > 0)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| True   |
+--------+
```
  
## FILTER  

### Description  

Usage: `filter(array, function)` filter the element in the array by the lambda function. The function should return boolean. The lambda function accepts one single input.
**Argument type:** `array:ARRAY, function:LAMBDA`
**Return type:** `ARRAY`
### Example
  
```ppl
source=people
| eval array = array(1, -2, 3), result = filter(array, x -> x > 0)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| [1,3]  |
+--------+
```
  
## TRANSFORM  

### Description  

Usage: `transform(array, function)` transform the element of array one by one using lambda. The lambda function can accept one single input or two input. If the lambda accepts two argument, the second one is the index of element in array.
**Argument type:** `array:ARRAY, function:LAMBDA`
**Return type:** `ARRAY`
### Example

<!-- TODO: To be fixed with https://github.com/opensearch-project/sql/issues/4972 -->
```ppl ignore
source=people
| eval array = array(1, -2, 3), result = transform(array, x -> x + 2)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| [3,0,5] |
+---------+
```
<!-- TODO: To be fixed with https://github.com/opensearch-project/sql/issues/4972 -->
```ppl ignore
source=people
| eval array = array(1, -2, 3), result = transform(array, (x, i) -> x + i)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+----------+
| result   |
|----------|
| [1,-1,5] |
+----------+
```
  
## REDUCE  

### Description  

Usage: `reduce(array, acc_base, function, <reduce_function>)` use lambda function to go through all element and interact with acc_base. The lambda function accept two argument accumulator and array element. If add one more reduce_function, will apply reduce_function to accumulator finally. The reduce function accept accumulator as the one argument.
**Argument type:** `array:ARRAY, acc_base:ANY, function:LAMBDA, reduce_function:LAMBDA`
**Return type:** `ANY`
### Example
  
```ppl
source=people
| eval array = array(1, -2, 3), result = reduce(array, 10, (acc, x) -> acc + x)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| 12     |
+--------+
```
  
```ppl
source=people
| eval array = array(1, -2, 3), result = reduce(array, 10, (acc, x) -> acc + x, acc -> acc * 10)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| 120    |
+--------+
```
  
## MVJOIN  

### Description  

Usage: `mvjoin(array, delimiter)` joins string array elements into a single string, separated by the specified delimiter. NULL elements are excluded from the output. Only string arrays are supported. 
**Argument type:** `array: ARRAY of STRING, delimiter: STRING`
**Return type:** `STRING`
### Example
  
```ppl
source=people
| eval result = mvjoin(array('a', 'b', 'c'), ',')
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| a,b,c  |
+--------+
```
  
```ppl
source=accounts
| eval names_array = array(firstname, lastname)
| eval result = mvjoin(names_array, ', ')
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-------------+
| result      |
|-------------|
| Amber, Duke |
+-------------+
```
  
## MVAPPEND  

### Description  

Usage: `mvappend(value1, value2, value3...)` appends all elements from arguments to create an array. Flattens array arguments and collects all individual elements. Always returns an array or null for consistent type behavior.
**Argument type:** `value1: ANY, value2: ANY, ...`
**Return type:** `ARRAY`
### Example
  
```ppl
source=people
| eval result = mvappend(1, 1, 3)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| [1,1,3] |
+---------+
```
  
```ppl
source=people
| eval result = mvappend(1, array(2, 3))
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| [1,2,3] |
+---------+
```
  
```ppl
source=people
| eval result = mvappend(mvappend(1, 2), 3)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| [1,2,3] |
+---------+
```
  
```ppl
source=people
| eval result = mvappend(42)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| [42]   |
+--------+
```
  
```ppl
source=people
| eval result = mvappend(nullif(1, 1), 2)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| [2]    |
+--------+
```
  
```ppl
source=people
| eval result = mvappend(nullif(1, 1))
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| null   |
+--------+
```
  
```ppl
source=people
| eval arr1 = array(1, 2), arr2 = array(3, 4), result = mvappend(arr1, arr2)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----------+
| result    |
|-----------|
| [1,2,3,4] |
+-----------+
```
  
```ppl
source=accounts
| eval result = mvappend(firstname, lastname)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------------+
| result       |
|--------------|
| [Amber,Duke] |
+--------------+
```
  
```ppl
source=people
| eval result = mvappend(1, 'text', 2.5)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------------+
| result       |
|--------------|
| [1,text,2.5] |
+--------------+
```
  
## SPLIT  

### Description  

Usage: `split(str, delimiter)` splits the string values on the delimiter and returns the string values as a multivalue field (array). Use an empty string ("") to split the original string into one value per character. If the delimiter is not found, returns an array containing the original string. If the input string is empty, returns an empty array.

**Argument type:** `str: STRING, delimiter: STRING`

**Return type:** `ARRAY of STRING`

### Example

```ppl
source=people
| eval test = 'buttercup;rarity;tenderhoof;dash', result = split(test, ';')
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+------------------------------------+
| result                             |
|------------------------------------|
| [buttercup,rarity,tenderhoof,dash] |
+------------------------------------+
```

```ppl
source=people
| eval test = '1a2b3c4def567890', result = split(test, 'def')
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+------------------+
| result           |
|------------------|
| [1a2b3c4,567890] |
+------------------+
```

```ppl
source=people
| eval test = 'abcd', result = split(test, '')
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+-----------+
| result    |
|-----------|
| [a,b,c,d] |
+-----------+
```

```ppl
source=people
| eval test = 'name::value', result = split(test, '::')
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------------+
| result       |
|--------------|
| [name,value] |
+--------------+
```

```ppl
source=people
| eval test = 'hello', result = split(test, ',')
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| [hello] |
+---------+
```
  
## MVDEDUP  

### Description  

Usage: `mvdedup(array)` removes duplicate values from a multivalue array while preserving the order of first occurrence. NULL elements are filtered out. Returns an array with duplicates removed, or null if the input is null.
**Argument type:** `array: ARRAY`
**Return type:** `ARRAY`
### Example
  
```ppl
source=people
| eval array = array(1, 2, 2, 3, 1, 4), result = mvdedup(array)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----------+
| result    |
|-----------|
| [1,2,3,4] |
+-----------+
```
  
```ppl
source=people
| eval array = array('z', 'a', 'z', 'b', 'a', 'c'), result = mvdedup(array)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+-----------+
| result    |
|-----------|
| [z,a,b,c] |
+-----------+
```
  
```ppl
source=people
| eval array = array(), result = mvdedup(array)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| []     |
+--------+
```

## MVFIND

### Description

Usage: mvfind(array, regex) searches a multivalue array and returns the 0-based index of the first element that matches the regular expression. Returns NULL if no match is found.
Argument type: array: ARRAY, regex: STRING
Return type: INTEGER (nullable)
Example

```ppl
source=people
| eval array = array('apple', 'banana', 'apricot'), result = mvfind(array, 'ban.*')
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| 1      |
+--------+
```

```ppl
source=people
| eval array = array('cat', 'dog', 'bird'), result = mvfind(array, 'fish')
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| null   |
+--------+
```

```ppl
source=people
| eval array = array('error123', 'info', 'error456'), result = mvfind(array, 'error[0-9]+')
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| 0      |
+--------+
```

```ppl
source=people
| eval array = array('Apple', 'Banana', 'Cherry'), result = mvfind(array, '(?i)banana')
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| 1      |
+--------+
```

## MVINDEX  

### Description  

Usage: `mvindex(array, start, [end])` returns a subset of the multivalue array using the start and optional end index values. Indexes are 0-based (first element is at index 0). Supports negative indexing where -1 refers to the last element. When only start is provided, returns a single element. When both start and end are provided, returns an array of elements from start to end (inclusive).
**Argument type:** `array: ARRAY, start: INTEGER, end: INTEGER (optional)`
**Return type:** `ANY (single element) or ARRAY (range)`
### Example
  
```ppl
source=people
| eval array = array('a', 'b', 'c', 'd', 'e'), result = mvindex(array, 1)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| b      |
+--------+
```
  
```ppl
source=people
| eval array = array('a', 'b', 'c', 'd', 'e'), result = mvindex(array, -1)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| e      |
+--------+
```
  
```ppl
source=people
| eval array = array(1, 2, 3, 4, 5), result = mvindex(array, 1, 3)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| [2,3,4] |
+---------+
```
  
```ppl
source=people
| eval array = array(1, 2, 3, 4, 5), result = mvindex(array, -3, -1)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| [3,4,5] |
+---------+
```
  
```ppl
source=people
| eval array = array('alex', 'celestino', 'claudia', 'david'), result = mvindex(array, 0, 2)
| fields result
| head 1
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+--------------------------+
| result                   |
|--------------------------|
| [alex,celestino,claudia] |
+--------------------------+
```

## MVMAP

### Description

Usage: mvmap(array, expression) iterates over each element of a multivalue array, applies the expression to each element, and returns a multivalue array with the transformed results. The field name in the expression is implicitly bound to each element value.
Argument type: array: ARRAY, expression: EXPRESSION
Return type: ARRAY
Example
<!-- TODO: To be fixed with https://github.com/opensearch-project/sql/issues/4972 -->
```ppl ignore
source=people
| eval array = array(1, 2, 3), result = mvmap(array, array * 10)
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+------------+
| result     |
|------------|
| [10,20,30] |
+------------+
```
<!-- TODO: To be fixed with https://github.com/opensearch-project/sql/issues/4972 -->
```ppl ignore
source=people
| eval array = array(1, 2, 3), result = mvmap(array, array + 5)
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| [6,7,8] |
+---------+
```

Note: For nested expressions like ``mvmap(mvindex(arr, 1, 3), arr * 2)``, the field name (``arr``) is extracted from the first argument and must match the field referenced in the expression.

The expression can also reference other single-value fields:
<!-- TODO: To be fixed with https://github.com/opensearch-project/sql/issues/4972 -->
```ppl ignore
source=people
| eval array = array(1, 2, 3), multiplier = 10, result = mvmap(array, array * multiplier)
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+------------+
| result     |
|------------|
| [10,20,30] |
+------------+
```


## MVZIP

### Description

Usage: `mvzip(mv_left, mv_right, [delim])` combines the values in two multivalue arrays by pairing corresponding elements and joining them into strings. The delimiter is used to specify a delimiting character to join the two values. This is similar to the Python zip command.

The values are stitched together combining the first value of mv_left with the first value of mv_right, then the second with the second, and so on. Each pair is concatenated into a string using the delimiter. The function stops at the length of the shorter array.

The delimiter is optional. When specified, it must be enclosed in quotation marks. The default delimiter is a comma ( , ).

Returns null if either input is null. Returns an empty array if either input array is empty.

**Argument type:** `mv_left: ARRAY, mv_right: ARRAY, delim: STRING (optional)`
**Return type:** `ARRAY of STRING`
### Example

```ppl
source=people
| eval hosts = array('host1', 'host2'), ports = array('80', '443'), nserver = mvzip(hosts, ports, ':')
| fields nserver
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+----------------------+
| nserver              |
|----------------------|
| [host1:80,host2:443] |
+----------------------+
```

```ppl
source=people
| eval arr1 = array('a', 'b', 'c'), arr2 = array('x', 'y', 'z'), result = mvzip(arr1, arr2, '|')
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------------+
| result        |
|---------------|
| [a|x,b|y,c|z] |
+---------------+
```

```ppl
source=people
| eval arr1 = array('1', '2', '3'), arr2 = array('a', 'b'), result = mvzip(arr1, arr2, '-')
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+-----------+
| result    |
|-----------|
| [1-a,2-b] |
+-----------+
```

```ppl
source=people
| eval arr1 = array('a', 'b', 'c'), arr2 = array('x', 'y', 'z'), arr3 = array('1', '2', '3'), result = mvzip(mvzip(arr1, arr2, '-'), arr3, ':')
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------------------+
| result              |
|---------------------|
| [a-x:1,b-y:2,c-z:3] |
+---------------------+
```

```ppl
source=people
| eval arr1 = array('a', 'b'), arr2 = array(), result = mvzip(arr1, arr2)
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| []     |
+--------+
```
