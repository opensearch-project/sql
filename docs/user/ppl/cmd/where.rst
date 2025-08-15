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

Example 1: Filter result set with condition
===========================================

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

Example 2: Filter with wildcard patterns
========================================

Both equals (``=``) and not-equals (``!=``) operators support wildcard pattern matching using wildcard characters when a string value contains wildcards.

**Important**: Wildcard patterns MUST be enclosed in quotes (single or double) because the ``*`` and ``?`` characters are not valid in unquoted literals according to PPL grammar.

**Supported wildcards:**

- ``*`` matches zero or more characters (OpenSearch native)
- ``?`` matches exactly one character (OpenSearch native)
- For SQL-style wildcards (``%``, ``_``), use the ``LIKE`` operator instead

**Field type restrictions:**

- **Only works properly on keyword fields** - OpenSearch wildcard queries are designed for keyword fields
- **Text fields**: Wildcard queries do NOT work on analyzed text fields. You must use the ``.keyword`` subfield (e.g., ``firstname.keyword = "Am*"``) for proper wildcard matching
- **Numeric, IP, date, boolean fields**: These use script-based matching which is less efficient but functional

PPL query::

    // Correct: Wildcard pattern is quoted
    os> source=accounts | where firstname="Am*" | fields firstname, lastname;
    fetched rows / total rows = 1/1
    +-----------+----------+
    | firstname | lastname |
    |-----------|----------|
    | Amber     | Duke     |
    +-----------+----------+

    // Correct: Numeric field with wildcard pattern quoted (works via script query)
    os> source=accounts | where account_number="1*" | fields account_number, firstname;
    fetched rows / total rows = 2/2
    +----------------+-----------+
    | account_number | firstname |
    |----------------+-----------|
    | 1              | Amber     |
    | 18             | Dale      |
    +----------------+-----------+

    os> source=accounts | where firstname!="Am*" | fields firstname, lastname;
    fetched rows / total rows = 3/3
    +-----------+----------+
    | firstname | lastname |
    |-----------|----------|
    | Hattie    | Bond     |
    | Nanette   | Bates    |
    | Dale      | Adams    |
    +-----------+----------+

**Common Errors:**

    // WRONG: Unquoted wildcard pattern - will cause syntax error
    os> source=accounts | where account_number=1*
    Error: SyntaxCheckException - [<EOF>] is not a valid term at '1*'
    
    // WRONG: Unquoted wildcard pattern with text
    os> source=accounts | where firstname=Am*
    Error: SyntaxCheckException - [<EOF>] is not a valid term at 'Am*'
    
    // CORRECT: Always quote wildcard patterns
    os> source=accounts | where account_number="1*"
    os> source=accounts | where firstname="Am*"

