=============
parse
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``parse`` command parses a text field using a regular expression and append the result to the search result.


Syntax
============
parse <field> <regular-expression>

* field: mandatory. The field must be a text field.
* regular-expression: mandatory. The regular expression used to extract new fields from given text field. If a new field name already exists, it will replace the original field.

Regular Expression
==================

The regular expression is used to match the whole text field of each document with Java regex engine. Each named capture group in the expression will become a new ``STRING`` field.

Example 1: Create the new field
===============================

The example shows how to create new field ``host`` for each document. ``host`` will be the host name after ``@`` in ``email`` field.

PPL query::

    os> source=accounts | parse email '.+@(?<host>.+)' | fields email, host ;
    fetched rows / total rows = 4/4
    +--------------------------+-------------+
    | email                    | host        |
    |--------------------------+-------------|
    | amberduke@pyrami.com     | pyrami.com  |
    | hattiebond@netagy.com    | netagy.com  |
    | nanettebates@quility.com | quility.com |
    | daleadams@boink.com      | boink.com   |
    +--------------------------+-------------+


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


Limitation
==========

There are a few limitations with parse command:

- Fields defined by parse cannot be parsed again
- Fields defined by parse cannot be overridden with other commands
- The text field used by parse cannot be overridden
- Fields defined by parse cannot be filtered/sorted after using them in ``stats`` command
