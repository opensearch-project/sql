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

Version: 3.1.0

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

Version: 3.1.0

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

Version: 3.1.0

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

Version: 3.1.0

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

Version: 3.1.0

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

Version: 3.1.0

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

Version: 3.1.0

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

Version: 3.3.0

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

Version: 3.4.0

Usage: mvappend(value1, value2, value3...) appends all elements from arguments to create an array. Flattens array arguments and collects all individual elements. Always returns an array or null for consistent type behavior.

Argument type: value1: ANY, value2: ANY, ...

Return type: ARRAY

Example::

    os> source=people | eval result = mvappend(1, 2, 3) | fields result | head 1
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
