=============
flatten
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Description
============

The ``flatten`` command flattens an `object`'s fields. New fields are added to the search results corresponding
to each of the object's fields; if the specified `object` is null or missing, the search results are not modified.

Syntax
============

`flatten <field>`

* `field`: reference to the `object` field to flatten.

Example 1: Flatten an object field
==================================

PPL query::

    os> source=cities | flatten location | fields name, country, province, coordinates, state
    fetched rows / total rows = 4/4
    +------------------+---------------+------------------+-----------------------------------------------+------------+
    | name             | country       | province         | coordinates                                   | state      |
    |------------------+---------------+------------------+-----------------------------------------------+------------|
    | Seattle          | United States | null             | {'latitude': 47.6061, 'longitude': -122.3328} | Washington |
    | Vancouver        | Canada        | British Columbia | {'latitude': 49.2827, 'longitude': -123.1207} | null       |
    | Null Location    | null          | null             | null                                          | null       |
    | Null Coordinates | Australia     | null             | null                                          | Victoria   |
    +------------------+---------------+------------------+-----------------------------------------------+------------+

Example 2: Flatten multiple object fields
=========================================

PPL query::

    os> source=cities | flatten location | flatten coordinates | fields name, country, province, state, latitude, longitude
    fetched rows / total rows = 4/4
    +------------------+---------------+------------------+------------+----------+-----------+
    | name             | country       | province         | state      | latitude | longitude |
    |------------------+---------------+------------------+------------+----------+-----------|
    | Seattle          | United States | null             | Washington | 47.6061  | -122.3328 |
    | Vancouver        | Canada        | British Columbia | null       | 49.2827  | -123.1207 |
    | Null Location    | null          | null             | null       | null     | null      |
    | Null Coordinates | Australia     | null             | Victoria   | null     | null      |
    +------------------+---------------+------------------+------------+----------+-----------+

Example 3: Flatten a nested object field
========================================

PPL query::

    os> source=cities | flatten location.coordinates | fields name, location
    fetched rows / total rows = 4/4
    +------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | name             | location                                                                                                                                                         |
    |------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------|
    | Seattle          | {'state': 'Washington', 'country': 'United States', 'coordinates': {'latitude': 47.6061, 'longitude': -122.3328}, 'latitude': 47.6061, 'longitude': -122.3328}   |
    | Vancouver        | {'province': 'British Columbia', 'country': 'Canada', 'coordinates': {'latitude': 49.2827, 'longitude': -123.1207}, 'latitude': 49.2827, 'longitude': -123.1207} |
    | Null Location    | null                                                                                                                                                             |
    | Null Coordinates | {'state': 'Victoria', 'country': 'Australia'}                                                                                                                    |
    +------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------+

