=============
grok
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``grok`` command parses a text field with a grok pattern and appends the results to the search result.


Syntax
============
grok <field> <pattern>

* field: mandatory. The field must be a text field.
* pattern: mandatory string. The grok pattern used to extract new fields from the given text field. If a new field name already exists, it will replace the original field.

Grok Pattern
============

The grok pattern is used to match the text field of each document to extract new fields.

Example 1: Create the new field
===============================

The example shows how to create new field ``host`` for each document. ``host`` will be the host name after ``@`` in ``email`` field. Parsing a null field will return an empty string.

PPL query::

    os> source=accounts | grok email '.+@%{HOSTNAME:host}' | fields email, host ;
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

    os> source=accounts | grok address '%{NUMBER} %{GREEDYDATA:address}' | fields address ;
    fetched rows / total rows = 4/4
    +------------------+
    | address          |
    |------------------|
    | Holmes Lane      |
    | Bristol Street   |
    | Madison Street   |
    | Hutchinson Court |
    +------------------+

Example 3: Using grok to parse logs
===================================

The example shows how to use grok to parse raw logs.

PPL query::

    os> source=accounts | eval raw='145.128.75.121 - - [29/Aug/2022:13:26:44 -0700] "GET /deliverables HTTP/2.0" 501 2721' | grok raw '%{COMMONAPACHELOG}' | head 1 | fields COMMONAPACHELOG, timestamp, response, bytes ;
    fetched rows / total rows = 1/1
    +---------------------------------------------------------------------------------------+----------------------------+------------+---------+
    | COMMONAPACHELOG                                                                       | timestamp                  | response   | bytes   |
    |---------------------------------------------------------------------------------------+----------------------------+------------+---------|
    | 145.128.75.121 - - [29/Aug/2022:13:26:44 -0700] "GET /deliverables HTTP/2.0" 501 2721 | 29/Aug/2022:13:26:44 -0700 | 501        | 2721    |
    +---------------------------------------------------------------------------------------+----------------------------+------------+---------+

Limitations
===========

The grok command has the same limitations as the parse command, see `parse limitations <./parse.rst#Limitations>`_ for details.
