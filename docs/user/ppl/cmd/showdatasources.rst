================
show datasources
================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Use the ``show datasources`` command to query datasources configured in the PPL engine. The ``show datasources`` command can only be used as the first command in the PPL query.


Syntax
============
show datasources


Example 1: Fetch all PROMETHEUS datasources
===========================================

This example shows fetching all the datasources of type prometheus.

PPL query for all PROMETHEUS DATASOURCES::

    os> show datasources | where CONNECTOR_TYPE='PROMETHEUS';
    fetched rows / total rows = 1/1
    +-----------------+----------------+
    | DATASOURCE_NAME | CONNECTOR_TYPE |
    |-----------------+----------------|
    | my_prometheus   | PROMETHEUS     |
    +-----------------+----------------+


Limitations
===========
The ``show datasources`` command can only work with ``plugins.calcite.enabled=false``.
