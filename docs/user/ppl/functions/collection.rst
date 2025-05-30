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

    PPL> source=people | eval array = array(1, 2, 3) | fields array | head 1
    fetched rows / total rows = 1/1
    +----------------------------------+
    | array                            |
    |----------------------------------|
    | [1, 2, 3]                        |
    +----------------------------------+

    PPL> source=people | eval array = array(1, "demo") | fields array | head 1
    fetched rows / total rows = 1/1
    +----------------------------------+
    | array                            |
    |----------------------------------|
    | ["1", "demo"]                    |
    +----------------------------------+

ARRAY_LENGTH
------------

Description
>>>>>>>>>>>

Version: 3.1.0

Usage: ``array_length(array)`` returns the length of input array.

Argument type: array:ARRAY

Return type: INTEGER

Example::

    PPL> source=people | eval array = array(1, 2, 3) | eval length = array_length(array) | fields length | head 1
    fetched rows / total rows = 1/1
    +---------------+
    | length        |
    |---------------|
    | 4             |
    +---------------+

FORALL
------

Description
>>>>>>>>>>>

Version: 3.1.0

Usage: ``forall(array, function)`` check whether all element inside array can meet the lambda function. The function should also return boolean. The lambda function accepts one single input.

Argument type: array:ARRAY, function:LAMBDA

Return type: BOOLEAN

Example::

    PPL> source=people | eval array = array(1, 2, 3), result = forall(array, x -> x > 0)  | fields result | head 1
    fetched rows / total rows = 1/1
    +---------+
    | result  |
    |---------|
    | true    |
    +---------+

EXISTS
------

Description
>>>>>>>>>>>

Version: 3.1.0

Usage: ``exists(array, function)`` check whether existing one of element inside array can meet the lambda function. The function should also return boolean. The lambda function accepts one single input.

Argument type: array:ARRAY, function:LAMBDA

Return type: BOOLEAN

Example::

    PPL> source=people | eval array = array(-1, -2, 3), result = exists(array, x -> x > 0)  | fields result | head 1
    fetched rows / total rows = 1/1
    +---------+
    | result  |
    |---------|
    | true    |
    +---------+

FILTER
------

Description
>>>>>>>>>>>

Version: 3.1.0

Usage: ``filter(array, function)`` filter the element in the array by the lambda function. The function should return boolean. The lambda function accepts one single input.

Argument type: array:ARRAY, function:LAMBDA

Return type: ARRAY

Example::

    PPL> source=people | eval array = array(1, -2, 3), result = filter(array, x -> x > 0)  | fields result | head 1
    fetched rows / total rows = 1/1
    +---------+
    | result  |
    |---------|
    | [1, 3]  |
    +---------+

TRANSFORM
---------

Description
>>>>>>>>>>>

Version: 3.1.0

Usage: ``transform(array, function)`` transform the element of array one by one using lambda. The lambda function can accept one single input or two input. If the lambda accepts two argument, the second one is the index of element in array.

Argument type: array:ARRAY, function:LAMBDA

Return type: ARRAY

Example::

    PPL> source=people | eval array = array(1, -2, 3), result = transform(array, x -> x + 2)  | fields result | head 1
    fetched rows / total rows = 1/1
    +------------+
    | result     |
    |------------|
    | [3, 0, 5]  |
    +------------+ 

    PPL> source=people | eval array = array(1, -2, 3), result = transform(array, (x, i) -> x + i)  | fields result | head 1
    fetched rows / total rows = 1/1
    +------------+
    | result     |
    |------------|
    | [1, -1, 5] |
    +------------+ 

REDUCE
------

Description
>>>>>>>>>>>

Version: 3.1.0

Usage: ``reduce(array, acc_base, function, <reduce_function>)`` use lambda function to go through all element and interact with acc_base. The lambda function accept two argument accumulator and array element. If add one more reduce_function, will apply reduce_function to accumulator finally. The reduce function accept accumulator as the one argument.

Argument type: array:ARRAY, acc_base:ANY, function:LAMBDA, reduce_function:LAMBDA

Return type: ANY

Example::

    PPL> source=people | eval array = array(1, -2, 3), result = reduce(array, 10, (acc, x) -> acc + x) | fields result | head 1
    fetched rows / total rows = 1/1
    +------------+
    | result     |
    |------------|
    | 8          |
    +------------+ 

    PPL> source=people | eval array = array(1, -2, 3), result = reduce(array, 10, (acc, x) -> acc + x, acc -> acc * 10) | fields result | head 1
    fetched rows / total rows = 1/1
    +------------+
    | result     |
    |------------|
    | 80         |
    +------------+ 