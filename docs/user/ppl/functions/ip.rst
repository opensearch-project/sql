====================
IP Address Functions
====================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1

CIDRMATCH
---------

Description
>>>>>>>>>>>

Usage: cidrmatch(ip, cidr) returns whether the given IP address is within the specified CIDR IP address range. Supports both IPv4 and IPv6 addresses.

Argument type: STRING, STRING

Return type: BOOLEAN

Example::

    os> source=weblogs | where cidrmatch(host, "199.120.110.0/24") | fields host ;
    fetched rows / total rows = 1/1
    +----------------+
    | host           |
    |----------------|
    | 199.120.110.21 |
    +----------------+
