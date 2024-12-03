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

Usage: `cidrmatch(ip, cidr)` checks if `ip` is within the specified `cidr` range.

Argument type: STRING, STRING

Return type: BOOLEAN

Example:

    os> source=weblogs | where cidrmatch(host, '199.120.110.0/24') | fields host
    fetched rows / total rows = 1/1
    +----------------+
    | host           |
    |----------------|
    | 199.120.110.21 |
    +----------------+

Note:
 - `ip` can be an IPv4 or an IPv6 address
 - `cidr` can be an IPv4 or an IPv6 block
 - `ip` and `cidr` must be either both IPv4 or both IPv6
 - `ip` and `cidr` must both be valid and non-empty/non-null

