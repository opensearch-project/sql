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

Usage: cidr(address, range) returns whether the given IP address is within the specified IP address range. Supports both IPv4 and IPv6 addresses.

Argument type: STRING, STRING

Return type: BOOLEAN

Example::

    os> source=weblogs | where cidr(address, "199.120.110.0/24") | fields host, method, url
    fetched rows / total rows = 1/1
    +----------------+--------+----------------------------------------------+
    | host           | method | url                                          |
    |----------------+--------+----------------------------------------------+
    | 199.120.110.21 | GET    | /shuttle/missions/sts-73/mission-sts-73.html |
    +----------------+--------+----------------------------------------------+

