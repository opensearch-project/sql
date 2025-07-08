=============
reverse
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Using ``reverse`` command to reverse the order of search results. The reverse command returns results in the opposite order from how they would normally be displayed, but does not affect which results are returned by the search.


Syntax
============
reverse


* No parameters: The reverse command takes no arguments or options.


Example 1: Basic reverse operation
==================================

The example shows reversing the order of all documents.

PPL query::

    os> source=accounts | fields account_number, age | reverse;
    fetched rows / total rows = 4/4
    +----------------+-----+
    | account_number | age |
    |----------------+-----|
    | 6              | 36  |
    | 18             | 33  |
    | 1              | 32  |
    | 13             | 28  |
    +----------------+-----+


Example 2: Reverse with sort
============================

The example shows reversing results after sorting by age in ascending order, effectively giving descending order.

PPL query::

    os> source=accounts | sort age | fields account_number, age | reverse;
    fetched rows / total rows = 4/4
    +----------------+-----+
    | account_number | age |
    |----------------+-----|
    | 6              | 36  |
    | 18             | 33  |
    | 1              | 32  |
    | 13             | 28  |
    +----------------+-----+


Example 3: Reverse with head
============================

The example shows using reverse with head to get the last 2 records from the original order.

PPL query::

    os> source=accounts | reverse | head 2 | fields account_number, age;
    fetched rows / total rows = 2/2
    +----------------+-----+
    | account_number | age |
    |----------------+-----|
    | 6              | 36  |
    | 18             | 33  |
    +----------------+-----+


Example 4: Double reverse
=========================

The example shows that applying reverse twice returns to the original order.

PPL query::

    os> source=accounts | reverse | reverse | fields account_number, age;
    fetched rows / total rows = 4/4
    +----------------+-----+
    | account_number | age |
    |----------------+-----|
    | 13             | 28  |
    | 1              | 32  |
    | 18             | 33  |
    | 6              | 36  |
    +----------------+-----+


Example 5: Reverse with complex pipeline
=======================================

The example shows reverse working with filtering and field selection.

PPL query::

    os> source=accounts | where age > 30 | fields account_number, age | reverse;
    fetched rows / total rows = 3/3
    +----------------+-----+
    | account_number | age |
    |----------------+-----|
    | 6              | 36  |
    | 18             | 33  |
    | 1              | 32  |
    +----------------+-----+
