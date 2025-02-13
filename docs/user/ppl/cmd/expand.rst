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

    os> source=expand_flatten | expand teams | fields city, teams.name
    fetched rows / total rows = 7/7
    +--------------+-------------------+
    | city         | teams.name        |
    |--------------+-------------------|
    | Seattle      | Seattle Seahawks  |
    | Seattle      | Seattle Kraken    |
    | Vancouver    | Vancouver Canucks |
    | Vancouver    | BC Lions          |
    | San Antonio  | San Antonio Spurs |
    | Null City    | null              |
    | Missing City | null              |
    +--------------+-------------------+

Example 2: Expand a nested field
=================================

PPL query::

    os> source=expand_flatten | where city = 'San Antonio' | expand teams.title | fields teams.name, teams.title
    fetched rows / total rows = 5/5
    +-------------------+-------------+
    | teams.name        | teams.title |
    |-------------------+-------------|
    | San Antonio Spurs | 1999        |
    | San Antonio Spurs | 2003        |
    | San Antonio Spurs | 2005        |
    | San Antonio Spurs | 2007        |
    | San Antonio Spurs | 2014        |
    +-------------------+-------------+

Example 3: Expand multiple fields
==================================

PPL query::

    os> source=expand_flatten | expand teams | expand teams.title | fields teams.name, teams.title
    fetched rows / total rows = 16/16
    +-------------------+-------------+
    | teams.name        | teams.title |
    |-------------------+-------------|
    | Seattle Seahawks  | 2014        |
    | Seattle Kraken    | null        |
    | Vancouver Canucks | null        |
    | BC Lions          | 1964        |
    | BC Lions          | 1985        |
    | BC Lions          | 1994        |
    | BC Lions          | 2000        |
    | BC Lions          | 2006        |
    | BC Lions          | 2011        |
    | San Antonio Spurs | 1999        |
    | San Antonio Spurs | 2003        |
    | San Antonio Spurs | 2005        |
    | San Antonio Spurs | 2007        |
    | San Antonio Spurs | 2014        |
    | null              | null        |
    | null              | null        |
    +-------------------+-------------+

Example 4: Expand and flatten a field
=====================================

PPL query::

    os> source=expand_flatten | expand teams | flatten teams | fields name, title
    fetched rows / total rows = 7/7
    +-------------------+---------------------------------+
    | name              | title                           |
    |-------------------+---------------------------------|
    | Seattle Seahawks  | 2014                            |
    | Seattle Kraken    | null                            |
    | Vancouver Canucks | null                            |
    | BC Lions          | [1964,1985,1994,2000,2006,2011] |
    | San Antonio Spurs | [1999,2003,2005,2007,2014]      |
    | null              | null                            |
    | null              | null                            |
    +-------------------+---------------------------------+
