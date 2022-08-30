=============
parse
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``parse`` command parses a text field using a specified method and appends the results to the search result.


Syntax
============
parse [method=<parse-method>] <field> <pattern>

* parse-method: optional. Possible values are ``regex``, ``punct``, ``grok``, default value is ``regex``.
* field: mandatory. The field must be a text field.
* pattern: mandatory string. The pattern used by parse method to extract new fields from the given text field. If a new field name already exists, it will replace the original field.

Parse Methods
=============

regex
-----

Use Java regex engine to match the whole text field of each document with the regular expression defined by ``pattern``. Each named capture group in the expression will become a new ``STRING`` field.

punct
-----

Remove alphanumeric and whitespace characters (``[a-zA-Z\d\s]``) in the text field of each document to form a new field. The name of the new field is defined by ``pattern``.

grok
-----

Use grok patterns to match the text field of each document to extract new fields.

Example 1: Create the new field
===============================

The example shows how to create new field ``host`` for each document. ``host`` will be the host name after ``@`` in ``email`` field. Parsing a null field will return an empty string.

PPL query::

    os> source=accounts | parse email '.+@(?<host>.+)' | fields email, host ;
    fetched rows / total rows = 4/4
    +-----------------------+------------+
    | email                 | host       |
    |-----------------------+------------|
    | amberduke@pyrami.com  | pyrami.com |
    | hattiebond@netagy.com | netagy.com |
    | null                  |            |
    | daleadams@boink.com   | boink.com  |
    +-----------------------+------------+


Example 2: Override the existing field
======================================

The example shows how to override the existing ``address`` field with street number removed.

PPL query::

    os> source=accounts | parse address '\d+ (?<address>.+)' | fields address ;
    fetched rows / total rows = 4/4
    +------------------+
    | address          |
    |------------------|
    | Holmes Lane      |
    | Bristol Street   |
    | Madison Street   |
    | Hutchinson Court |
    +------------------+

Example 3: Filter and sort by casted parsed field
=================================================

The example shows how to sort street numbers that are higher than 500 in ``address`` field.

PPL query::

    os> source=accounts | parse address '(?<streetNumber>\d+) (?<street>.+)' | where cast(streetNumber as int) > 500 | sort num(streetNumber) | fields streetNumber, street ;
    fetched rows / total rows = 3/3
    +----------------+----------------+
    | streetNumber   | street         |
    |----------------+----------------|
    | 671            | Bristol Street |
    | 789            | Madison Street |
    | 880            | Holmes Lane    |
    +----------------+----------------+

Example 4: Filter out punctuations with punct
=============================================

The example shows how to use ``punct`` as parse method to 

PPL query::

    os> source=accounts | parse method=punct email 'email_puncts' | fields email, email_puncts ;
    fetched rows / total rows = 4/4
    +-----------------------+----------------+
    | email                 | email_puncts   |
    |-----------------------+----------------|
    | amberduke@pyrami.com  | @.             |
    | hattiebond@netagy.com | @.             |
    | null                  |                |
    | daleadams@boink.com   | @.             |
    +-----------------------+----------------+

Limitation
==========

There are a few limitations with parse command:

- Fields defined by parse cannot be parsed again.

  The following command will not work::

    source=accounts | parse address '\d+ (?<street>.+)' | parse street '\w+ (?<road>\w+)' ;

- Fields defined by parse cannot be overridden with other commands.

  ``where`` will not match any documents since ``street`` cannot be overridden::

    source=accounts | parse address '\d+ (?<street>.+)' | eval street='1' | where street='1' ;

- The text field used by parse cannot be overridden.

  ``street`` will not be successfully parsed since ``address`` is overridden::

    source=accounts | parse address '\d+ (?<street>.+)' | eval address='1' ;

- Fields defined by parse cannot be filtered/sorted after using them in ``stats`` command.

  ``where`` in the following command will not work::

    source=accounts | parse email '.+@(?<host>.+)' | stats avg(age) by host | where host='pyrami.com' ;
