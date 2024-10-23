================
IP Functions
================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1

CIDR
------

Description
>>>>>>>>>>>

Usage: cidr(address, range) returns true if IP address is within the IP address range in CIDR notation. Supports both IPv4 and IPv6 addresses.

Argument type: STRING, STRING

Return type: BOOLEAN

Example::

    os> source=devices | where cidr(address, "198.51.100.0/24")
    fetched rows / total rows = 2/2
    +----------------+----------------+
    | name           | address        |
    |----------------+----------------+
    | John's Macbook | 198.51.100.2   |
    | Iain's PC      | 198.51.100.254 |
    +----------------+----------------+

