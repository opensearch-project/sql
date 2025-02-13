=============
flatten
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Description
============

The ``flatten`` command flattens an object's field by adding a new field to the search results corresponding
to each of the object's fields. If the specified object is null or missing, the search results are not modified.

Syntax
============

``flatten <field>``

* ``field``: reference to the object field to flatten.

Example 1: Flatten an object field
==================================

PPL query::

    os> source=flatten | flatten location | fields name, country, province, coordinates, state
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

    os> source=flatten | flatten location | flatten coordinates | fields name, location, latitude, longitude
    fetched rows / total rows = 4/4
    +------------------+---------------------------------------------------------------------------------------------------------------------+----------+-----------+
    | name             | location                                                                                                            | latitude | longitude |
    |------------------+---------------------------------------------------------------------------------------------------------------------+----------+-----------|
    | Seattle          | {'state': 'Washington', 'country': 'United States', 'coordinates': {'latitude': 47.6061, 'longitude': -122.3328}}   | 47.6061  | -122.3328 |
    | Vancouver        | {'province': 'British Columbia', 'country': 'Canada', 'coordinates': {'latitude': 49.2827, 'longitude': -123.1207}} | 49.2827  | -123.1207 |
    | Null Location    | null                                                                                                                | null     | null      |
    | Null Coordinates | {'state': 'Victoria', 'country': 'Australia'}                                                                       | null     | null      |
    +------------------+---------------------------------------------------------------------------------------------------------------------+----------+-----------+

Example 3: Flatten a nested object field
========================================

PPL query::

    os> source=flatten | flatten location.coordinates | fields name, location
    fetched rows / total rows = 4/4
    +------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | name             | location                                                                                                                                                         |
    |------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------|
    | Seattle          | {'state': 'Washington', 'country': 'United States', 'coordinates': {'latitude': 47.6061, 'longitude': -122.3328}, 'latitude': 47.6061, 'longitude': -122.3328}   |
    | Vancouver        | {'province': 'British Columbia', 'country': 'Canada', 'coordinates': {'latitude': 49.2827, 'longitude': -123.1207}, 'latitude': 49.2827, 'longitude': -123.1207} |
    | Null Location    | null                                                                                                                                                             |
    | Null Coordinates | {'state': 'Victoria', 'country': 'Australia'}                                                                                                                    |
    +------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Example 4: Flatten and expand
=============================

TODO #3016: Add test once flatten merged.