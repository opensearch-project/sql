===========================
PPL Collection Functions
===========================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1

ARRAY
-----

Description
>>>>>>>>>>>

Usage: ``array(value1, value2, value3...)`` create an array with input values. Currently we don't allow mixture types. We will infer a least restricted type, for example ``array(1, "demo")`` -> ["1", "demo"]

Argument type: value1: ANY, value2: ANY, ...

Return type: ARRAY

Example::

    os> source=people | eval array = array(1, 2, 3) | fields array | head 1
    fetched rows / total rows = 1/1
    +---------+
    | array   |
    |---------|
    | [1,2,3] |
    +---------+

    os> source=people | eval array = array(1, "demo") | fields array | head 1
    fetched rows / total rows = 1/1
    +----------+
    | array    |
    |----------|
    | [1,demo] |
    +----------+

ARRAY_LENGTH
------------

Description
>>>>>>>>>>>

Usage: ``array_length(array)`` returns the length of input array.

Argument type: array:ARRAY

Return type: INTEGER

Example::

    os> source=people | eval array = array(1, 2, 3) | eval length = array_length(array) | fields length | head 1
    fetched rows / total rows = 1/1
    +--------+
    | length |
    |--------|
    | 3      |
    +--------+

FORALL
------

Description
>>>>>>>>>>>

Usage: ``forall(array, function)`` check whether all element inside array can meet the lambda function. The function should also return boolean. The lambda function accepts one single input.

Argument type: array:ARRAY, function:LAMBDA

Return type: BOOLEAN

Example::

    os> source=people | eval array = array(1, 2, 3), result = forall(array, x -> x > 0)  | fields result | head 1
    fetched rows / total rows = 1/1
    +--------+
    | result |
    |--------|
    | True   |
    +--------+

EXISTS
------

Description
>>>>>>>>>>>

Usage: ``exists(array, function)`` check whether existing one of element inside array can meet the lambda function. The function should also return boolean. The lambda function accepts one single input.

Argument type: array:ARRAY, function:LAMBDA

Return type: BOOLEAN

Example::

    os> source=people | eval array = array(-1, -2, 3), result = exists(array, x -> x > 0)  | fields result | head 1
    fetched rows / total rows = 1/1
    +--------+
    | result |
    |--------|
    | True   |
    +--------+

FILTER
------

Description
>>>>>>>>>>>

Usage: ``filter(array, function)`` filter the element in the array by the lambda function. The function should return boolean. The lambda function accepts one single input.

Argument type: array:ARRAY, function:LAMBDA

Return type: ARRAY

Example::

    os> source=people | eval array = array(1, -2, 3), result = filter(array, x -> x > 0)  | fields result | head 1
    fetched rows / total rows = 1/1
    +--------+
    | result |
    |--------|
    | [1,3]  |
    +--------+

TRANSFORM
---------

Description
>>>>>>>>>>>

Usage: ``transform(array, function)`` transform the element of array one by one using lambda. The lambda function can accept one single input or two input. If the lambda accepts two argument, the second one is the index of element in array.

Argument type: array:ARRAY, function:LAMBDA

Return type: ARRAY

Example::

    os> source=people | eval array = array(1, -2, 3), result = transform(array, x -> x + 2)  | fields result | head 1
    fetched rows / total rows = 1/1
    +---------+
    | result  |
    |---------|
    | [3,0,5] |
    +---------+

    os> source=people | eval array = array(1, -2, 3), result = transform(array, (x, i) -> x + i)  | fields result | head 1
    fetched rows / total rows = 1/1
    +----------+
    | result   |
    |----------|
    | [1,-1,5] |
    +----------+

REDUCE
------

Description
>>>>>>>>>>>

Usage: ``reduce(array, acc_base, function, <reduce_function>)`` use lambda function to go through all element and interact with acc_base. The lambda function accept two argument accumulator and array element. If add one more reduce_function, will apply reduce_function to accumulator finally. The reduce function accept accumulator as the one argument.

Argument type: array:ARRAY, acc_base:ANY, function:LAMBDA, reduce_function:LAMBDA

Return type: ANY

Example::

    os> source=people | eval array = array(1, -2, 3), result = reduce(array, 10, (acc, x) -> acc + x) | fields result | head 1
    fetched rows / total rows = 1/1
    +--------+
    | result |
    |--------|
    | 12     |
    +--------+

    os> source=people | eval array = array(1, -2, 3), result = reduce(array, 10, (acc, x) -> acc + x, acc -> acc * 10) | fields result | head 1
    fetched rows / total rows = 1/1
    +--------+
    | result |
    |--------|
    | 120    |
    +--------+

MVJOIN
------

Description
>>>>>>>>>>>

Usage: mvjoin(array, delimiter) joins string array elements into a single string, separated by the specified delimiter. NULL elements are excluded from the output. Only string arrays are supported. 

Argument type: array: ARRAY of STRING, delimiter: STRING

Return type: STRING

Example::

    os> source=people | eval result = mvjoin(array('a', 'b', 'c'), ',') | fields result | head 1
    fetched rows / total rows = 1/1
    +--------+
    | result |
    |--------|
    | a,b,c  |
    +--------+

    os> source=accounts | eval names_array = array(firstname, lastname) | eval result = mvjoin(names_array, ', ') | fields result | head 1
    fetched rows / total rows = 1/1
    +-------------+
    | result      |
    |-------------|
    | Amber, Duke |
    +-------------+

MVAPPEND
--------

Description
>>>>>>>>>>>

Usage: mvappend(value1, value2, value3...) appends all elements from arguments to create an array. Flattens array arguments and collects all individual elements. Always returns an array or null for consistent type behavior.

Argument type: value1: ANY, value2: ANY, ...

Return type: ARRAY

Example::

    os> source=people | eval result = mvappend(1, 1, 3) | fields result | head 1
    fetched rows / total rows = 1/1
    +---------+
    | result  |
    |---------|
    | [1,1,3] |
    +---------+

    os> source=people | eval result = mvappend(1, array(2, 3)) | fields result | head 1
    fetched rows / total rows = 1/1
    +---------+
    | result  |
    |---------|
    | [1,2,3] |
    +---------+

    os> source=people | eval result = mvappend(mvappend(1, 2), 3) | fields result | head 1
    fetched rows / total rows = 1/1
    +---------+
    | result  |
    |---------|
    | [1,2,3] |
    +---------+

    os> source=people | eval result = mvappend(42) | fields result | head 1
    fetched rows / total rows = 1/1
    +--------+
    | result |
    |--------|
    | [42]   |
    +--------+

    os> source=people | eval result = mvappend(nullif(1, 1), 2) | fields result | head 1
    fetched rows / total rows = 1/1
    +--------+
    | result |
    |--------|
    | [2]    |
    +--------+

    os> source=people | eval result = mvappend(nullif(1, 1)) | fields result | head 1
    fetched rows / total rows = 1/1
    +--------+
    | result |
    |--------|
    | null   |
    +--------+

    os> source=people | eval arr1 = array(1, 2), arr2 = array(3, 4), result = mvappend(arr1, arr2) | fields result | head 1
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    |-----------|
    | [1,2,3,4] |
    +-----------+

    os> source=accounts | eval result = mvappend(firstname, lastname) | fields result | head 1
    fetched rows / total rows = 1/1
    +--------------+
    | result       |
    |--------------|
    | [Amber,Duke] |
    +--------------+

    os> source=people | eval result = mvappend(1, 'text', 2.5) | fields result | head 1
    fetched rows / total rows = 1/1
    +--------------+
    | result       |
    |--------------|
    | [1,text,2.5] |
    +--------------+

MVDEDUP
-------

Description
>>>>>>>>>>>

Usage: mvdedup(array) removes duplicate values from a multivalue array while preserving the order of first occurrence. NULL elements are filtered out. Returns an array with duplicates removed, or null if the input is null.

Argument type: array: ARRAY

Return type: ARRAY

Example::

    os> source=people | eval array = array(1, 2, 2, 3, 1, 4), result = mvdedup(array) | fields result | head 1
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    |-----------|
    | [1,2,3,4] |
    +-----------+

    os> source=people | eval array = array('z', 'a', 'z', 'b', 'a', 'c'), result = mvdedup(array) | fields result | head 1
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    |-----------|
    | [z,a,b,c] |
    +-----------+

    os> source=people | eval array = array(), result = mvdedup(array) | fields result | head 1
    fetched rows / total rows = 1/1
    +--------+
    | result |
    |--------|
    | []     |
    +--------+

MVINDEX
-------

Description
>>>>>>>>>>>

Usage: mvindex(array, start, [end]) returns a subset of the multivalue array using the start and optional end index values. Indexes are 0-based (first element is at index 0). Supports negative indexing where -1 refers to the last element. When only start is provided, returns a single element. When both start and end are provided, returns an array of elements from start to end (inclusive).

Argument type: array: ARRAY, start: INTEGER, end: INTEGER (optional)

Return type: ANY (single element) or ARRAY (range)

Example::

    os> source=people | eval array = array('a', 'b', 'c', 'd', 'e'), result = mvindex(array, 1) | fields result | head 1
    fetched rows / total rows = 1/1
    +--------+
    | result |
    |--------|
    | b      |
    +--------+

    os> source=people | eval array = array('a', 'b', 'c', 'd', 'e'), result = mvindex(array, -1) | fields result | head 1
    fetched rows / total rows = 1/1
    +--------+
    | result |
    |--------|
    | e      |
    +--------+

    os> source=people | eval array = array(1, 2, 3, 4, 5), result = mvindex(array, 1, 3) | fields result | head 1
    fetched rows / total rows = 1/1
    +---------+
    | result  |
    |---------|
    | [2,3,4] |
    +---------+

    os> source=people | eval array = array(1, 2, 3, 4, 5), result = mvindex(array, -3, -1) | fields result | head 1
    fetched rows / total rows = 1/1
    +---------+
    | result  |
    |---------|
    | [3,4,5] |
    +---------+

    os> source=people | eval array = array('alex', 'celestino', 'claudia', 'david'), result = mvindex(array, 0, 2) | fields result | head 1
    fetched rows / total rows = 1/1
    +--------------------------+
    | result                   |
    |--------------------------|
    | [alex,celestino,claudia] |
    +--------------------------+

