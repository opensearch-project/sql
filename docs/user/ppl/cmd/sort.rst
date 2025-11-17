====
sort
====

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
===========
| The ``sort`` command sorts all the search results by the specified fields.

Syntax
============
sort [count] <[+|-] sort-field | sort-field [asc|a|desc|d]>...


* count: optional. The number of results to return. Specifying a count of 0 or less than 0 returns all results. **Default:** 0.
* [+|-]: optional. The plus [+] stands for ascending order and NULL/MISSING first and a minus [-] stands for descending order and NULL/MISSING last. **Default:** ascending order and NULL/MISSING first.
* [asc|a|desc|d]: optional. asc/a stands for ascending order and NULL/MISSING first. desc/d stands for descending order and NULL/MISSING last. **Default:** ascending order and NULL/MISSING first.
* sort-field: mandatory. The field used to sort. Can use ``auto(field)``, ``str(field)``, ``ip(field)``, or ``num(field)`` to specify how to interpret field values.

.. note::
   You cannot mix +/- and asc/desc in the same sort command. Choose one approach for all fields in a single sort command.


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


Example 3: Sort by one field in descending order (using -)
==========================================================

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

Example 4: Sort by one field in descending order (using desc)
==============================================================

This example shows sorting all the document by the age field in descending order using the desc keyword.

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

Example 5: Sort by multiple fields (using +/-)
==============================================

This example shows sorting all documents by gender field in ascending order and age field in descending order using +/- operators.

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

Example 6: Sort by multiple fields (using asc/desc)
====================================================

This example shows sorting all the document by the gender field in ascending order and age field in descending order using asc/desc keywords.

PPL query::

    os> source=accounts | sort gender asc, age desc | fields account_number, gender, age;
    fetched rows / total rows = 4/4
    +----------------+--------+-----+
    | account_number | gender | age |
    |----------------+--------+-----|
    | 13             | F      | 28  |
    | 6              | M      | 36  |
    | 18             | M      | 33  |
    | 1              | M      | 32  |
    +----------------+--------+-----+

Example 7: Sort by field include null value
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

Example 8: Specify the number of sorted documents to return
============================================================

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

Example 9: Sort with desc modifier
===================================

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

Example 10: Sort with specifying field type
==================================

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