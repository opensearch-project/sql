=============
sort
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Using ``sort`` command to sorts all the search result by the specified fields.


Syntax
============
sort [count] <[+|-] sort-field | sort-field [asc|a|desc|d]>...


* count (Since 3.3): optional. The number of results to return. **Default:** returns all results. Specifying a count of 0 or less than 0 also returns all results.
* [+|-]: optional. The plus [+] stands for ascending order and NULL/MISSING first and a minus [-] stands for descending order and NULL/MISSING last. **Default:** ascending order and NULL/MISSING first.
* [asc|a|desc|d]: optional. asc/a stands for ascending order and NULL/MISSING first. desc/d stands for descending order and NULL/MISSING last. **Default:** ascending order and NULL/MISSING first.
* sort-field: mandatory. The field used to sort. Can use ``auto(field)``, ``str(field)``, ``ip(field)``, or ``num(field)`` to specify how to interpret field values.

.. note::
   You cannot mix +/- and asc/desc in the same sort command. Choose one approach for all fields in a single sort command.


Example 1: Sort by one field
=============================

The example show sort all the document with age field in ascending order.

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

The example show sort all the document with age field in ascending order.

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

The example show sort all the document with age field in descending order using the - operator.

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

The example show sort all the document with age field in descending order using the desc keyword.

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

The example show sort all the document with gender field in ascending order and age field in descending using +/- operators.

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

The example show sort all the document with gender field in ascending order and age field in descending using asc/desc keywords.

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

The example shows sorting the employer field by the default option (ascending order and null first), the result shows that the null value is in the first row.

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

The example shows sorting all the document and returning 2 documents.

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

The example shows sorting with the desc modifier to reverse sort order.

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

The example shows sorting with str() to sort numeric values lexicographically.

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