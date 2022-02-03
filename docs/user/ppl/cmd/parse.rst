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

The regular expression is used to match the whole text field of each document with Java regex engine. Each captured group in the expression will become a new field with type to be ``STRING`` by default. The field type can be casted using the following syntax for a captured group: ``(?<fieldName[fieldType]>regex)``. List of available field types:

+------------------------------+
| Field types (case sensitive) |
+==============================+
| STRING                       |
+------------------------------+
| BYTE                         |
+------------------------------+
| SHORT                        |
+------------------------------+
| INTEGER                      |
+------------------------------+
| LONG                         |
+------------------------------+
| FLOAT                        |
+------------------------------+
| BOOLEAN                      |
+------------------------------+
| TIME                         |
+------------------------------+
| DATE                         |
+------------------------------+
| TIMESTAMP                    |
+------------------------------+
| DATETIME                     |
+------------------------------+


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


Example 2: Cast new field to other data types
=============================================

The example shows how to extract the street number from ``address`` and cast to integer.

PPL query::

    os> source=accounts | parse address '(?<streetNumberINTEGER>\d+).*' | fields address, streetNumber ;
    fetched rows / total rows = 4/4
    +----------------------+--------------+
    | address              | streetNumber |
    |----------------------|--------------|
    | 880 Holmes Lane      | 880          |
    | 671 Bristol Street   | 671          |
    | 789 Madison Street   | 789          |
    | 467 Hutchinson Court | 467          |
    +----------------------+--------------+


Limitation
==========

The number of results fetched by ``source`` command is limited by `plugins.query.size_limit setting <../admin/settings.rst#plugins-query-size-limit>`_. The parse command (including subsequent commands) will only operate on the results returned.
