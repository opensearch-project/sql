=============
where
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``where`` command bool-expression to filter the search result. The ``where`` command only return the result when bool-expression evaluated to true.


Syntax
============
where <boolean-expression>

* bool-expression: optional. any expression which could be evaluated to boolean value.

Examples
========

**Example 1: Filter result set with condition**

The example show fetch all the document from accounts index with .

PPL query::

    os> source=accounts | where account_number=1 or gender="F" | fields account_number, gender;
    fetched rows / total rows = 2/2
    +----------------+--------+
    | account_number | gender |
    |----------------+--------|
    | 1              | M      |
    | 13             | F      |
    +----------------+--------+

**Example 2: Basic Field Comparison**

The example shows how to filter accounts with balance greater than 40000.

PPL query::

    os> source=accounts | where balance > 40000 | fields account_number, balance;
    fetched rows / total rows = 2/4
    +----------------+---------+
    | account_number | balance |
    |----------------+---------|
    | 1              | 45000   |
    | 6              | 42000   |
    +----------------+---------+

**Example 3: Pattern Matching with LIKE**

Pattern Matching with Underscore (_)

The example demonstrates using LIKE with underscore (_) to match a single character.

PPL query::

    os> source=accounts | where LIKE(state, 'N_') | fields account_number, state;
    fetched rows / total rows = 2/6
    +----------------+-------+
    | account_number | state |
    |----------------+-------|
    | 4              | NY    |
    | 7              | NJ    |
    +----------------+-------+

Pattern Matching with Percent (%)

The example demonstrates using LIKE with percent (%) to match multiple characters.

PPL query::

    os> source=accounts | where LIKE(state, 'CA%') | fields account_number, state;
    fetched rows / total rows = 2/6
    +----------------+-------+
    | account_number | state |
    |----------------+-------|
    | 2              | CA    |
    | 8              | CAL   |
    +----------------+-------+

**Example 4: Multiple Conditions**

The example shows how to combine multiple conditions using AND operator.

PPL query::

    os> source=accounts | where age > 30 AND gender = 'F' | fields account_number, age, gender;
    fetched rows / total rows = 2/5
    +----------------+-----+--------+
    | account_number | age | gender |
    |----------------+-----+--------|
    | 13             | 32  | F      |
    | 24             | 36  | F      |
    +----------------+-----+--------+

**Example 5: Using IN Operator**

The example demonstrates using IN operator to match multiple values.

PPL query::

    os> source=accounts | where state IN ('CA', 'NY') | fields account_number, state;
    fetched rows / total rows = 3/6
    +----------------+-------+
    | account_number | state |
    |----------------+-------|
    | 2              | CA    |
    | 4              | NY    |
    | 8              | CA    |
    +----------------+-------+

**Example 6: NULL Checks**

The example shows how to filter records with NULL values.

PPL query::

   os> source=accounts | where ISNULL(employer) | fields account_number, employer;
   fetched rows / total rows = 2/8
   +----------------+----------+
   | account_number | employer |
   |----------------+----------|
   | 7              | null     |
   | 15             | null     |
   +----------------+----------+

**Example 7: Complex Conditions**

The example demonstrates combining multiple conditions with parentheses and logical operators.

PPL query::

    os> source=accounts | where (balance > 40000 OR age > 35) AND gender = 'M' | fields account_number, balance, age, gender;
    fetched rows / total rows = 2/7
    +----------------+---------+-----+--------+
    | account_number | balance | age | gender |
    |----------------+---------+-----+--------|
    | 1              | 45000   | 38  | M      |
    | 16             | 38000   | 39  | M      |
    +----------------+---------+-----+--------+


**Example 8: NOT Conditions**

The example shows how to use NOT operator to exclude matching records.

PPL query::

    os> source=accounts | where NOT state = 'CA' | fields account_number, state;
    fetched rows / total rows = 4/6
    +----------------+-------+
    | account_number | state |
    |----------------+-------|
    | 3              | TX    |
    | 4              | NY    |
    | 11             | FL    |
    | 19             | WA    |
    +----------------+-------+

**Example 9: Field-to-Field Comparison**

The example demonstrates comparing two fields from the same record.

PPL query::

    os> source=accounts | where employer_id = manager_id | fields account_number, employer_id, manager_id;
    fetched rows / total rows = 1/5
    +----------------+-------------+------------+
    | account_number | employer_id | manager_id |
    |----------------+-------------+------------|
    | 4              | 101         | 101        |
    +----------------+-------------+------------+