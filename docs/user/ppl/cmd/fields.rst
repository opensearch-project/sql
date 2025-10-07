=============
fields
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
Using ``field`` command to keep or remove fields from the search result.

Enhanced field features are available when the Calcite engine is enabled with 3.3+ version. When Calcite is disabled, only basic comma-delimited field selection is supported.

Syntax
============
field [+|-] <field-list>

* index: optional. if the plus (+) is used, only the fields specified in the field list will be keep. if the minus (-) is used, all the fields specified in the field list will be removed. **Default** +
* field list: mandatory. comma-delimited keep or remove fields.


Basic Examples
==============

Example 1: Select specified fields from result
----------------------------------------------

The example show fetch account_number, firstname and lastname fields from search results.

PPL query::

    os> source=accounts | fields account_number, firstname, lastname;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------+
    | account_number | firstname | lastname |
    |----------------+-----------+----------|
    | 1              | Amber     | Duke     |
    | 6              | Hattie    | Bond     |
    | 13             | Nanette   | Bates    |
    | 18             | Dale      | Adams    |
    +----------------+-----------+----------+

Example 2: Remove specified fields from result
----------------------------------------------

The example show fetch remove account_number field from search results.

PPL query::

    os> source=accounts | fields account_number, firstname, lastname | fields - account_number ;
    fetched rows / total rows = 4/4
    +-----------+----------+
    | firstname | lastname |
    |-----------+----------|
    | Amber     | Duke     |
    | Hattie    | Bond     |
    | Nanette   | Bates    |
    | Dale      | Adams    |
    +-----------+----------+

Enhanced Features (Version 3.3.0)
===========================================

All features in this section require the Calcite engine to be enabled. When Calcite is disabled, only basic comma-delimited field selection is supported.

Example 3: Space-delimited field selection
-------------------------------------------

Fields can be specified using spaces instead of commas, providing a more concise syntax.

**Syntax**: ``fields field1 field2 field3``

PPL query::

    os> source=accounts | fields firstname lastname age;
    fetched rows / total rows = 4/4
    +-----------+----------+-----+
    | firstname | lastname | age |
    |-----------+----------+-----|
    | Amber     | Duke     | 32  |
    | Hattie    | Bond     | 36  |
    | Nanette   | Bates    | 28  |
    | Dale      | Adams    | 33  |
    +-----------+----------+-----+

Example 4: Prefix wildcard pattern
-----------------------------------

Select fields starting with a pattern using prefix wildcards.

PPL query::

    os> source=accounts | fields account*;
    fetched rows / total rows = 4/4
    +----------------+
    | account_number |
    |----------------|
    | 1              |
    | 6              |
    | 13             |
    | 18             |
    +----------------+

Example 5: Suffix wildcard pattern
-----------------------------------

Select fields ending with a pattern using suffix wildcards.

PPL query::

    os> source=accounts | fields *name;
    fetched rows / total rows = 4/4
    +-----------+----------+
    | firstname | lastname |
    |-----------+----------|
    | Amber     | Duke     |
    | Hattie    | Bond     |
    | Nanette   | Bates    |
    | Dale      | Adams    |
    +-----------+----------+

Example 6: Contains wildcard pattern
------------------------------------

Select fields containing a pattern using contains wildcards.

PPL query::

    os> source=accounts | fields *a* | head 1;
    fetched rows / total rows = 1/1
    +----------------+-----------+-----------------+---------+-------+-----+----------------------+----------+
    | account_number | firstname | address         | balance | state | age | email                | lastname |
    |----------------+-----------+-----------------+---------+-------+-----+----------------------+----------|
    | 1              | Amber     | 880 Holmes Lane | 39225   | IL    | 32  | amberduke@pyrami.com | Duke     |
    +----------------+-----------+-----------------+---------+-------+-----+----------------------+----------+

Example 7: Mixed delimiter syntax
----------------------------------

Combine spaces and commas for flexible field specification.

PPL query::

    os> source=accounts | fields firstname, account* *name;
    fetched rows / total rows = 4/4
    +-----------+----------------+----------+
    | firstname | account_number | lastname |
    |-----------+----------------+----------|
    | Amber     | 1              | Duke     |
    | Hattie    | 6              | Bond     |
    | Nanette   | 13             | Bates    |
    | Dale      | 18             | Adams    |
    +-----------+----------------+----------+

Example 8: Field deduplication
-------------------------------

Automatically prevents duplicate columns when wildcards expand to already specified fields.

PPL query::

    os> source=accounts | fields firstname, *name;
    fetched rows / total rows = 4/4
    +-----------+----------+
    | firstname | lastname |
    |-----------+----------|
    | Amber     | Duke     |
    | Hattie    | Bond     |
    | Nanette   | Bates    |
    | Dale      | Adams    |
    +-----------+----------+

Note: Even though ``firstname`` is explicitly specified and would also match ``*name``, it appears only once due to automatic deduplication.

Example 9: Full wildcard selection
-----------------------------------

Select all available fields using ``*`` or ```*```. This selects all fields defined in the index schema, including fields that may contain null values.

PPL query::

    os> source=accounts | fields `*` | head 1;
    fetched rows / total rows = 1/1
    +----------------+-----------+-----------------+---------+--------+--------+----------+-------+-----+----------------------+----------+
    | account_number | firstname | address         | balance | gender | city   | employer | state | age | email                | lastname |
    |----------------+-----------+-----------------+---------+--------+--------+----------+-------+-----+----------------------+----------|
    | 1              | Amber     | 880 Holmes Lane | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com | Duke     |
    +----------------+-----------+-----------------+---------+--------+--------+----------+-------+-----+----------------------+----------+

Note: The ``*`` wildcard selects fields based on the index schema, not on data content. Fields with null values are included in the result set. Use backticks ```*``` if the plain ``*`` doesn't return all expected fields.

Example 10: Wildcard exclusion
-------------------------------

Remove fields using wildcard patterns with the minus (-) operator.

PPL query::

    os> source=accounts | fields - *name;
    fetched rows / total rows = 4/4
    +----------------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+
    | account_number | address              | balance | gender | city   | employer | state | age | email                 |
    |----------------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------|
    | 1              | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  |
    | 6              | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com |
    | 13             | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  |
    | 18             | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   |
    +----------------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+

Requirements
============
- **Calcite Engine**: All enhanced features require the Calcite engine to be enabled
- **Backward Compatibility**: Basic comma-delimited syntax continues to work when Calcite is disabled
- **Error Handling**: Attempting to use enhanced features without Calcite will result in an ``UnsupportedOperationException``

See Also
========
- `table <table.rst>`_ - Alias command with identical functionality
