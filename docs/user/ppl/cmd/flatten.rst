=============
flatten
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Description
============

Using ``flatten`` command to flatten an `object`. New fields are added to the search results corresponding
to each of the object's fields, while the object field itself is removed from the search results.

Syntax
============

`flatten field`

* `field`: reference to the `object` field to flatten.

Example 1: Flatten an object field
==================================

PPL query::

    os> source=cities | flatten location | fields name, province, state, country, coordinates
    fetched rows / total rows = 2/2
    +-----------+------------------+------------+---------------+-----------------------------------------------+
    | name      | province         | state      | country       | coordinates                                   |
    |-----------+------------------+------------+---------------+-----------------------------------------------|
    | Seattle   | null             | Washington | United States | {'latitude': 47.6061, 'longitude': -122.3328} |
    | Vancouver | British Columbia | null       | Canada        | {'latitude': 49.2827, 'longitude': -123.1207} |
    +-----------+------------------+------------+---------------+-----------------------------------------------+

Example 2: Flatten multiple object fields
=========================================

PPL query::

    os> source=cities | flatten location | flatten coordinates | fields name, province, state, country, latitude, longitude
    fetched rows / total rows = 2/2
    +-----------+------------------+------------+---------------+----------+-----------+
    | name      | province         | state      | country       | latitude | longitude |
    |-----------+------------------+------------+---------------+----------+-----------|
    | Seattle   | null             | Washington | United States | 47.6061  | -122.3328 |
    | Vancouver | British Columbia | null       | Canada        | 49.2827  | -123.1207 |
    +-----------+------------------+------------+---------------+----------+-----------+

Example 3: Flatten a nested object field
========================================

PPL query::

    os> source=cities | flatten location.coordinates | fields name, location
    fetched rows / total rows = 2/2
    +-----------+----------------------------------------------------------------------------------------------------+
    | name      | location                                                                                           |
    |-----------+----------------------------------------------------------------------------------------------------|
    | Seattle   | {'state': 'Washington', 'country': 'United States', 'latitude': 47.6061, 'longitude': -122.3328}   |
    | Vancouver | {'province': 'British Columbia', 'country': 'Canada', 'latitude': 49.2827, 'longitude': -123.1207} |
    +-----------+----------------------------------------------------------------------------------------------------+

