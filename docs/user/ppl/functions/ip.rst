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

    os> source=weblogs | where cidrmatch(host, '1.0.0.0/24') | fields host, url
    fetched rows / total rows = 2/2
    +----------------+--------------------|
    | host           | url                |
    +----------------+--------------------+
    | 1.2.3.4        | /history/voyager1/ |
    | 1.2.3.5        | /history/voyager2/ |
    +----------------+--------------------|

Note:
 - `ip` can be an IPv4 or an IPv6 address
 - `cidr` can be an IPv4 or an IPv6 block
 - `ip` and `cidr` must be either both IPv4 or both IPv6
 - `ip` and `cidr` must both be valid and non-empty/non-null

