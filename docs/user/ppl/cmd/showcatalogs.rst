=============
show catalogs
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Using ``show catalogs`` command to query catalogs configured in the PPL engine. ``show catalogs`` command could be only used as the first command in the PPL query.


Syntax
============
show catalogs


Example 1: Fetch all PROMETHEUS catalogs
=================================

The example fetches all the catalogs configured.

PPL query for all PROMETHEUS CATALOGS::

    os> show catalogs | where CONNECTOR_TYPE='PROMETHEUS';
    fetched rows / total rows = 1/1
    +----------------+------------------+
    | CATALOG_NAME   | CONNECTOR_TYPE   |
    |----------------+------------------|
    | my_prometheus  | PROMETHEUS       |
    +----------------+------------------+

