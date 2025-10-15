=============
where
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``where`` command filters the search result. The ``where`` command only return the result when bool-expression evaluated to true.

Syntax
============
where <boolean-expression>

* bool-expression: optional. Any expression which could be evaluated to boolean value.

Example 1: Filter result set with condition
===========================================

This example shows fetching all the documents from the accounts index where account_number is 1 or gender is "F".

PPL query::

    os> source=accounts | where account_number=1 or gender="F" | fields account_number, gender;
    fetched rows / total rows = 2/2
    +----------------+--------+
    | account_number | gender |
    |----------------+--------|
    | 1              | M      |
    | 13             | F      |
    +----------------+--------+

