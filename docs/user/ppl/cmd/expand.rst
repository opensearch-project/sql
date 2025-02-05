=============
expand
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Description
============

The ``expand`` command expands a field that contains an array of values to produce a seperate row for each value in the
array. If the field does not contain an array, the row is not modified.

Syntax
============

``expand <field>``

* ``field``: reference to the field to flatten.

Example 1: Expand a field
=========================

PPL query::

    os> source=expand | expand team | fields city, team.name
    fetched rows / total rows = 7/7
    +--------------+-------------------+
    | city         | team.name         |
    |--------------+-------------------|
    | Seattle      | Seattle Seahawks  |
    | Seattle      | Seattle Kraken    |
    | Vancouver    | Vancouver Canucks |
    | Vancouver    | BC Lions          |
    | San Antonio  | San Antonio Spurs |
    | Null Team    | null              |
    | Missing Team | null              |
    +--------------+-------------------+

Example 2: Expand a nested field
=================================

PPL query::

    os> source=expand | where city = 'San Antonio' | expand team.title | fields team.name, team.title
    fetched rows / total rows = 5/5
    +-------------------+------------+
    | team.name         | team.title |
    |-------------------+------------|
    | San Antonio Spurs | 1999       |
    | San Antonio Spurs | 2003       |
    | San Antonio Spurs | 2005       |  
    | San Antonio Spurs | 2007       |
    | San Antonio Spurs | 2014       |
    +-------------------+------------+

Example 3: Expand multiple fields
==================================

PPL query::

    os> source=expand | expand team | expand team.title | fields team.name, team.title
    fetched rows / total rows = 16/16
    +-------------------+------------+
    | team.name         | team.title |
    |-------------------+------------|
    | Seattle Seahawks  | 2014       |
    | Seattle Kraken    | null       |
    | Vancouver Canucks | null       |
    | BC Lions          | 1964       |
    | BC Lions          | 1985       |
    | BC Lions          | 1994       |
    | BC Lions          | 2000       |
    | BC Lions          | 2006       |
    | BC Lions          | 2011       |
    | San Antonio Spurs | 1999       |
    | San Antonio Spurs | 2003       |
    | San Antonio Spurs | 2005       |
    | San Antonio Spurs | 2007       |
    | San Antonio Spurs | 2014       |
    | null              | null       |
    | null              | null       |
    +-------------------+------------+

Example 4: Expand and flatten
=============================

TODO #3016: Test once flatten merged.