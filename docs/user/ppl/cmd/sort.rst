=============
sort
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
===========
| The ``sort`` command sorts all the search results by the specified fields.

Syntax
======
sort [count] <[+|-] sort-field>... [asc|a|desc|d]


* count: optional. The number of results to return. Specifying a count of 0 or less than 0 returns all results. **Default:** 0.
* [+|-]: optional. The plus [+] stands for ascending order and NULL/MISSING first and a minus [-] stands for descending order and NULL/MISSING last. **Default:** ascending order and NULL/MISSING first.
* sort-field: mandatory. The field used to sort. Can use ``auto(field)``, ``str(field)``, ``ip(field)``, or ``num(field)`` to specify how to interpret field values.
* [asc|a|desc|d]: optional. asc/a keeps the sort order as specified. desc/d reverses the sort results. If multiple fields are specified with desc/d, reverses order of the first field then for all duplicate values of the first field, reverses the order of the values of the second field and so on. **Default:** asc.


Example 1: Sort by one field
============================

This example shows sorting all documents by age field in ascending order.

PPL query::

    os> source=accounts | sort age | fields account_number, age;
    fetched rows / total rows = 4/4
    +----------------+-----+
    | account_number | age |
    |----------------+-----|
    | 13             | 28  |
    | 1              | 32  |
    | 18             | 33  |
    | 6              | 36  |
    +----------------+-----+


Example 2: Sort by one field return all the result
==================================================

This example shows sorting all documents by age field in ascending order and returning all results.

PPL query::

    os> source=accounts | sort 0 age | fields account_number, age;
    fetched rows / total rows = 4/4
    +----------------+-----+
    | account_number | age |
    |----------------+-----|
    | 13             | 28  |
    | 1              | 32  |
    | 18             | 33  |
    | 6              | 36  |
    +----------------+-----+


Example 3: Sort by one field in descending order
================================================

This example shows sorting all documents by age field in descending order.

PPL query::

    os> source=accounts | sort - age | fields account_number, age;
    fetched rows / total rows = 4/4
    +----------------+-----+
    | account_number | age |
    |----------------+-----|
    | 6              | 36  |
    | 18             | 33  |
    | 1              | 32  |
    | 13             | 28  |
    +----------------+-----+

Example 4: Sort by multiple field
=================================

This example shows sorting all documents by gender field in ascending order and age field in descending order.

PPL query::

    os> source=accounts | sort + gender, - age | fields account_number, gender, age;
    fetched rows / total rows = 4/4
    +----------------+--------+-----+
    | account_number | gender | age |
    |----------------+--------+-----|
    | 13             | F      | 28  |
    | 6              | M      | 36  |
    | 18             | M      | 33  |
    | 1              | M      | 32  |
    +----------------+--------+-----+

Example 5: Sort by field include null value
===========================================

This example shows sorting employer field by default option (ascending order and null first). The result shows that null value is in the first row.

PPL query::

    os> source=accounts | sort employer | fields employer;
    fetched rows / total rows = 4/4
    +----------+
    | employer |
    |----------|
    | null     |
    | Netagy   |
    | Pyrami   |
    | Quility  |
    +----------+

Example 6: Specify the number of sorted documents to return
===========================================================

This example shows sorting all documents and returning 2 documents.

PPL query::

    os> source=accounts | sort 2 age | fields account_number, age;
    fetched rows / total rows = 2/2
    +----------------+-----+
    | account_number | age |
    |----------------+-----|
    | 13             | 28  |
    | 1              | 32  |
    +----------------+-----+

Example 7: Sort with desc modifier
==================================

This example shows sorting with the desc modifier to reverse sort order.

PPL query::

    os> source=accounts | sort age desc | fields account_number, age;
    fetched rows / total rows = 4/4
    +----------------+-----+
    | account_number | age |
    |----------------+-----|
    | 6              | 36  |
    | 18             | 33  |
    | 1              | 32  |
    | 13             | 28  |
    +----------------+-----+

Example 8: Sort by multiple fields with desc modifier
=====================================================

This example shows sorting by multiple fields using desc, which reverses the sort order for all specified fields. Gender is reversed from ascending to descending, and the descending age sort is reversed to ascending within each gender group.

PPL query::

    os> source=accounts | sort gender, -age desc | fields account_number, gender, age;
    fetched rows / total rows = 4/4
    +----------------+--------+-----+
    | account_number | gender | age |
    |----------------+--------+-----|
    | 1              | M      | 32  |
    | 18             | M      | 33  |
    | 6              | M      | 36  |
    | 13             | F      | 28  |
    +----------------+--------+-----+


Example 9: Sort with specifying field type
==========================================

This example shows sorting with str() to sort numeric values lexicographically.

PPL query::

    os> source=accounts | sort str(account_number) | fields account_number;
    fetched rows / total rows = 4/4
    +----------------+
    | account_number |
    |----------------|
    | 1              |
    | 13             |
    | 18             |
    | 6              |
    +----------------+