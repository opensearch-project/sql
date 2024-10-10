===========
Expressions
===========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 3


Introduction
============

Expressions, particularly value expressions, are those which return a scalar value. Expressions have different types and forms. For example, there are literal values as atom expression and arithmetic, predicate and function expression built on top of them. And also expressions can be used in different clauses, such as using arithmetic expression in ``Filter``, ``Stats`` command.

Arithmetic Operators
====================

Description
-----------

Operators
`````````

Arithmetic expression is an expression formed by numeric literals and binary arithmetic operators as follows:

1. ``+``: Add.
2. ``-``: Subtract.
3. ``*``: Multiply.
4. ``/``: Divide. For integers, the result is an integer with fractional part discarded.
5. ``%``: Modulo. This can be used with integers only with remainder of the division as result.

Precedence
``````````

Parentheses can be used to control the precedence of arithmetic operators. Otherwise, operators of higher precedence is performed first.

Type Conversion
```````````````

Implicit type conversion is performed when looking up operator signature. For example, an integer ``+`` a real number matches signature ``+(double,double)`` which results in a real number. This rule also applies to function call discussed below.

Examples
--------

Here is an example for different type of arithmetic expressions::

    os> source=accounts | where age > (25 + 5) | fields age ;
    fetched rows / total rows = 3/3
    +-----+
    | age |
    |-----|
    | 32  |
    | 36  |
    | 33  |
    +-----+

Predicate Operators
===================

Description
-----------

Predicate operator is an expression that evaluated to be ture. The MISSING and NULL value comparison has following the rule. MISSING value only equal to MISSING value and less than all the other values. NULL value equals to NULL value, large than MISSING value, but less than all the other values.

Operators
`````````

+----------------+----------------------------------------+
| name           | description                            |
+----------------+----------------------------------------+
| >              | Greater than operator                  |
+----------------+----------------------------------------+
| >=             | Greater than or equal operator         |
+----------------+----------------------------------------+
| <              | Less than operator                     |
+----------------+----------------------------------------+
| !=             | Not equal operator                     |
+----------------+----------------------------------------+
| <=             | Less than or equal operator            |
+----------------+----------------------------------------+
| =              | Equal operator                         |
+----------------+----------------------------------------+
| LIKE           | Simple Pattern matching                |
+----------------+----------------------------------------+
| IN             | NULL value test                        |
+----------------+----------------------------------------+
| AND            | AND operator                           |
+----------------+----------------------------------------+
| OR             | OR operator                            |
+----------------+----------------------------------------+
| XOR            | XOR operator                           |
+----------------+----------------------------------------+
| NOT            | NOT NULL value test                    |
+----------------+----------------------------------------+

It is possible to compare datetimes. When comparing different datetime types, for example `DATE` and `TIME`, both converted to `DATETIME`.
The following rule is applied on coversion: a `TIME` applied to today's date; `DATE` is interpreted at midnight.

Examples
--------

Basic Predicate Operator
````````````````````````

Here is an example for comparison operators::

    os> source=accounts | where age > 33 | fields age ;
    fetched rows / total rows = 1/1
    +-----+
    | age |
    |-----|
    | 36  |
    +-----+


IN
``

IN operator test field in value lists::

    os> source=accounts | where age in (32, 33) | fields age ;
    fetched rows / total rows = 2/2
    +-----+
    | age |
    |-----|
    | 32  |
    | 33  |
    +-----+


OR
``

OR operator ::

    os> source=accounts | where age = 32 OR age = 33 | fields age ;
    fetched rows / total rows = 2/2
    +-----+
    | age |
    |-----|
    | 32  |
    | 33  |
    +-----+


NOT
```

NOT operator ::

    os> source=accounts | where not age in (32, 33) | fields age ;
    fetched rows / total rows = 2/2
    +-----+
    | age |
    |-----|
    | 36  |
    | 28  |
    +-----+

