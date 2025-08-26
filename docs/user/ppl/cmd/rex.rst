=============
rex
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``rex`` command extracts fields from a raw text field using regular expression named capture groups.

Version
=======
3.3.0

Syntax
============
rex [mode=<mode>] field=<field> <pattern> [max_match=<int>] [offset_field=<string>]

* field: mandatory. The field must be a string field to extract data from.
* pattern: mandatory string. The regular expression pattern with named capture groups used to extract new fields. Pattern must contain at least one named capture group using ``(?<name>pattern)`` syntax.
* mode: optional. Either ``extract`` (default) or ``sed``. In extract mode, creates new fields from named capture groups. In sed mode, performs text substitution on the field.

  **Sed mode syntax:**
  
  - **Substitution:** ``s/pattern/replacement/[flags]``
    
    - ``s/pattern/replacement/`` - Replace first occurrence
    - ``s/pattern/replacement/g`` - Replace all occurrences (global)
    - ``s/pattern/replacement/n`` - Replace only the nth occurrence (where n is a number)
    - Backreferences: Use ``\1``, ``\2``, etc. to reference captured groups in replacement
    
  - **Transliteration:** ``y/from_chars/to_chars/`` - Character-by-character mapping

* max_match: optional integer (default=1). Maximum number of matches to extract. If greater than 1, extracted fields become arrays.
* offset_field: optional string. Field name to store the character offset positions of matches.

Example 1: Basic Field Extraction
==================================

Extract username and domain from email addresses using named capture groups.

PPL query::

    os> source=accounts | rex field=email "(?<username>[^@]+)@(?<domain>[^.]+)" | fields email, username, domain | head 3 ;
    fetched rows / total rows = 3/3
    +--------------------------+--------------+---------+
    | email                    | username     | domain  |
    |--------------------------+--------------+---------|
    | amberduke@pyrami.com     | amberduke    | pyrami  |
    | hattiebond@netagy.com    | hattiebond   | netagy  |
    | nanettebates@quility.com | nanettebates | quility |
    +--------------------------+--------------+---------+


Example 2: Extract with Filtering
==================================

The rex command automatically filters out events that don't match the pattern.

PPL query::

    os> source=accounts | rex field=address "(?<streetnum>\\d+)\\s+(?<streetname>.+)" | fields address, streetnum, streetname | head 3 ;
    fetched rows / total rows = 3/3
    +------------------+-----------+----------------+
    | address          | streetnum | streetname     |
    |------------------|-----------|----------------|
    | 880 Holmes Lane  | 880       | Holmes Lane    |
    | 671 Bristol Street | 671     | Bristol Street |
    | 789 Madison Street | 789     | Madison Street |
    +------------------+-----------+----------------+


Example 3: Multiple Matches with max_match
===========================================

Extract multiple words from address field using max_match parameter.

PPL query::

    os> source=accounts | rex field=address "(?<words>[A-Za-z]+)" max_match=3 | fields address, words | head 3 ;
    fetched rows / total rows = 3/3
    +--------------------+-------------------+
    | address            | words             |
    |--------------------|-------------------|
    | 880 Holmes Lane    | [Holmes, Lane]    |
    | 671 Bristol Street | [Bristol, Street] |
    | 789 Madison Street | [Madison, Street] |
    +--------------------+-------------------+


Example 4: Text Replacement with mode=sed
==========================================

Replace email domains using sed mode for text substitution.

PPL query::

    os> source=accounts | rex field=email mode=sed "s/@.*/@company.com/" | fields email | head 3 ;
    fetched rows / total rows = 3/3
    +---------------------------+
    | email                     |
    |---------------------------|
    | amberduke@company.com     |
    | hattiebond@company.com    |
    | nanettebates@company.com  |
    +---------------------------+


Example 5: Using offset_field
==============================

Track the character positions where matches occur.

PPL query::

    os> source=accounts | rex field=email "(?<username>[^@]+)@(?<domain>[^.]+)" offset_field=matchpos | fields email, username, domain, matchpos | head 3 ;
    fetched rows / total rows = 3/3
    +--------------------------+--------------+---------+-----------------------------+
    | email                    | username     | domain  | matchpos                    |
    |--------------------------+--------------+---------+-----------------------------|
    | amberduke@pyrami.com     | amberduke    | pyrami  | username=0-8,domain=10-15   |
    | hattiebond@netagy.com    | hattiebond   | netagy  | username=0-9,domain=11-16   |
    | nanettebates@quility.com | nanettebates | quility | username=0-11,domain=13-19  |
    +--------------------------+--------------+---------+-----------------------------+


Example 6: Complex Email Pattern
=================================

Extract comprehensive email components including top-level domain.

PPL query::

    os> source=accounts | rex field=email "(?<user>[a-zA-Z0-9._%+-]+)@(?<domain>[a-zA-Z0-9.-]+)\\.(?<tld>[a-zA-Z]{2,})" | fields email, user, domain, tld | head 3 ;
    fetched rows / total rows = 3/3
    +--------------------------+--------------+---------+-----+
    | email                    | user         | domain  | tld |
    |--------------------------+--------------+---------+-----|
    | amberduke@pyrami.com     | amberduke    | pyrami  | com |
    | hattiebond@netagy.com    | hattiebond   | netagy  | com |
    | nanettebates@quility.com | nanettebates | quility | com |
    +--------------------------+--------------+---------+-----+


Example 7: Chaining Multiple rex Commands
==========================================

Extract initial letters from both first and last names.

PPL query::

    os> source=accounts | rex field=firstname "(?<firstinitial>^.)" | rex field=lastname "(?<lastinitial>^.)" | fields firstname, lastname, firstinitial, lastinitial | head 3 ;
    fetched rows / total rows = 3/3
    +-----------+----------+--------------+-------------+
    | firstname | lastname | firstinitial | lastinitial |
    |-----------|----------|--------------|-------------|
    | Amber     | Duke     | A            | D           |
    | Hattie    | Bond     | H            | B           |
    | Nanette   | Bates    | N            | B           |
    +-----------+----------+--------------+-------------+


Comparison with Related Commands
================================

============================= ============ ============
Feature                        rex          parse
============================= ============ ============
Pattern Type                   Java Regex   Java Regex
Named Groups Required          Yes          Yes
Filtering by Match             Yes          Yes  
Multiple Matches               Yes          No
Text Substitution              Yes          No
Offset Tracking                Yes          No
Underscores in Group Names     No           No
============================= ============ ============


Limitations
===========

There are several important limitations with the rex command:

**Named Capture Group Naming:**

- Named capture groups cannot contain underscores due to Java regex limitations
- Group names must start with a letter and contain only letters and digits
- Use ``(?<username>...)`` not ``(?<user_name>...)``

**Pattern Requirements:**

- Pattern must contain at least one named capture group
- Regular capture groups ``(...)`` without names are not allowed
