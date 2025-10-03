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
* mode: optional. Either ``extract`` (default) or ``sed``.

  - **extract mode** (default): Creates new fields from regular expression named capture groups. This is the standard field extraction behavior.
  - **sed mode**: Performs text substitution on the field using sed-style patterns:

    - ``s/pattern/replacement/`` - Replace first occurrence
    - ``s/pattern/replacement/g`` - Replace all occurrences (global)
    - ``s/pattern/replacement/n`` - Replace only the nth occurrence (where n is a number)
    - ``y/from_chars/to_chars/`` - Character-by-character transliteration
    - Backreferences: ``\1``, ``\2``, etc. reference captured groups in replacement

* max_match: optional integer (default=1). Maximum number of matches to extract. If greater than 1, extracted fields become arrays. The value 0 means unlimited matches, but is automatically capped to the configured limit (default: 10, configurable via ``plugins.ppl.rex.max_match.limit``).
* offset_field: optional string. Field name to store the character offset positions of matches. Only available in extract mode.

Example 1: Basic Field Extraction
==================================

Extract username and domain from email addresses using named capture groups. Both extracted fields are returned as string type.

PPL query::

    os> source=accounts | rex field=email "(?<username>[^@]+)@(?<domain>[^.]+)" | fields email, username, domain | head 2 ;
    fetched rows / total rows = 2/2
    +-----------------------+------------+--------+
    | email                 | username   | domain |
    |-----------------------+------------+--------|
    | amberduke@pyrami.com  | amberduke  | pyrami |
    | hattiebond@netagy.com | hattiebond | netagy |
    +-----------------------+------------+--------+


Example 2: Handling Non-matching Patterns
==========================================

The rex command returns all events, setting extracted fields to null for non-matching patterns. Extracted fields would be string type when matches are found.

PPL query::

    os> source=accounts | rex field=email "(?<user>[^@]+)@(?<domain>gmail\\.com)" | fields email, user, domain | head 2 ;
    fetched rows / total rows = 2/2
    +-----------------------+------+--------+
    | email                 | user | domain |
    |-----------------------+------+--------|
    | amberduke@pyrami.com  | null | null   |
    | hattiebond@netagy.com | null | null   |
    +-----------------------+------+--------+


Example 3: Multiple Matches with max_match
===========================================

Extract multiple words from address field using max_match parameter. The extracted field is returned as an array type containing string elements.

PPL query::

    os> source=accounts | rex field=address "(?<words>[A-Za-z]+)" max_match=2 | fields address, words | head 3 ;
    fetched rows / total rows = 3/3
    +--------------------+------------------+
    | address            | words            |
    |--------------------+------------------|
    | 880 Holmes Lane    | [Holmes,Lane]    |
    | 671 Bristol Street | [Bristol,Street] |
    | 789 Madison Street | [Madison,Street] |
    +--------------------+------------------+


Example 4: Text Replacement with mode=sed
==========================================

Replace email domains using sed mode for text substitution. The extracted field is returned as string type.

PPL query::

    os> source=accounts | rex field=email mode=sed "s/@.*/@company.com/" | fields email | head 2 ;
    fetched rows / total rows = 2/2
    +------------------------+
    | email                  |
    |------------------------|
    | amberduke@company.com  |
    | hattiebond@company.com |
    +------------------------+


Example 5: Using offset_field
==============================

Track the character positions where matches occur. Extracted fields are string type, and the offset_field is also string type.

PPL query::

    os> source=accounts | rex field=email "(?<username>[^@]+)@(?<domain>[^.]+)" offset_field=matchpos | fields email, username, domain, matchpos | head 2 ;
    fetched rows / total rows = 2/2
    +-----------------------+------------+--------+---------------------------+
    | email                 | username   | domain | matchpos                  |
    |-----------------------+------------+--------+---------------------------|
    | amberduke@pyrami.com  | amberduke  | pyrami | domain=10-15&username=0-8 |
    | hattiebond@netagy.com | hattiebond | netagy | domain=11-16&username=0-9 |
    +-----------------------+------------+--------+---------------------------+


Example 6: Complex Email Pattern
=================================

Extract comprehensive email components including top-level domain. All extracted fields are returned as string type.

PPL query::

    os> source=accounts | rex field=email "(?<user>[a-zA-Z0-9._%+-]+)@(?<domain>[a-zA-Z0-9.-]+)\\.(?<tld>[a-zA-Z]{2,})" | fields email, user, domain, tld | head 2 ;
    fetched rows / total rows = 2/2
    +-----------------------+------------+--------+-----+
    | email                 | user       | domain | tld |
    |-----------------------+------------+--------+-----|
    | amberduke@pyrami.com  | amberduke  | pyrami | com |
    | hattiebond@netagy.com | hattiebond | netagy | com |
    +-----------------------+------------+--------+-----+


Example 7: Chaining Multiple rex Commands
==========================================

Extract initial letters from both first and last names. All extracted fields are returned as string type.

PPL query::

    os> source=accounts | rex field=firstname "(?<firstinitial>^.)" | rex field=lastname "(?<lastinitial>^.)" | fields firstname, lastname, firstinitial, lastinitial | head 3 ;
    fetched rows / total rows = 3/3
    +-----------+----------+--------------+-------------+
    | firstname | lastname | firstinitial | lastinitial |
    |-----------+----------+--------------+-------------|
    | Amber     | Duke     | A            | D           |
    | Hattie    | Bond     | H            | B           |
    | Nanette   | Bates    | N            | B           |
    +-----------+----------+--------------+-------------+


Example 8: Named Capture Group Limitations
============================================

Demonstrates naming restrictions for capture groups. Group names cannot contain underscores due to Java regex limitations.

Invalid PPL query with underscores::

    os> source=accounts | rex field=email "(?<user_name>[^@]+)@(?<email_domain>[^.]+)" | fields email, user_name, email_domain ;
    {'reason': 'Invalid Query', 'details': 'Underscores are not permitted in Java Regex capture group names', 'type': 'IllegalArgumentException'}
    Error: Query returned no data

Correct PPL query without underscores::

    os> source=accounts | rex field=email "(?<username>[^@]+)@(?<emaildomain>[^.]+)" | fields email, username, emaildomain | head 2 ;
    fetched rows / total rows = 2/2
    +-----------------------+------------+-------------+
    | email                 | username   | emaildomain |
    |-----------------------+------------+-------------|
    | amberduke@pyrami.com  | amberduke  | pyrami      |
    | hattiebond@netagy.com | hattiebond | netagy      |
    +-----------------------+------------+-------------+


Example 9: Max Match Limit Protection
======================================

Demonstrates the max_match limit protection mechanism. When max_match=0 (unlimited) is specified, the system automatically caps it to prevent memory exhaustion.

PPL query with max_match=0 automatically capped to default limit of 10::

    os> source=accounts | rex field=address "(?<digit>\\d*)" max_match=0 | eval digit_count=array_length(digit) | fields address, digit_count | head 1 ;
    fetched rows / total rows = 1/1
    +-----------------+-------------+
    | address         | digit_count |
    |-----------------+-------------|
    | 880 Holmes Lane | 10          |
    +-----------------+-------------+

PPL query exceeding the configured limit results in an error::

    os> source=accounts | rex field=address "(?<digit>\\d*)" max_match=100 | fields address, digit | head 1 ;
    {'reason': 'Invalid Query', 'details': 'Rex command max_match value (100) exceeds the configured limit (10). Consider using a smaller max_match value or adjust the plugins.ppl.rex.max_match.limit setting.', 'type': 'IllegalArgumentException'}
    Error: Query returned no data


Comparison with Related Commands
================================

============================= ============ ============
Feature                        rex          parse
============================= ============ ============
Pattern Type                   Java Regex   Java Regex
Named Groups Required          Yes          Yes
Multiple Named Groups          Yes          No
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
- For detailed Java regex pattern syntax and usage, refer to the `official Java Pattern documentation <https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html>`_

**Pattern Requirements:**

- Pattern must contain at least one named capture group
- Regular capture groups ``(...)`` without names are not allowed

**Max Match Limit:**

- The ``max_match`` parameter is subject to a configurable system limit to prevent memory exhaustion
- When ``max_match=0`` (unlimited) is specified, it is automatically capped at the configured limit (default: 10)
- User-specified values exceeding the configured limit will result in an error
- Users can adjust the limit via the ``plugins.ppl.rex.max_match.limit`` cluster setting. Setting this limit to a large value is not recommended as it can lead to excessive memory consumption, especially with patterns that match empty strings (e.g., ``\d*``, ``\w*``)