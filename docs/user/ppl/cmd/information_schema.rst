=========================================
Metadata queries using information_schema
=========================================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Use ``information_schema`` in source command to query tables information under a datasource.
In the current state, ``information_schema`` only support metadata of tables.
This schema will be extended for views, columns and other metadata info in future.


Syntax
============
source = datasource.information_schema.tables;

Example 1: Fetch tables in prometheus datasource.
==============================================

The examples fetches tables in the prometheus datasource.

PPL query for fetching PROMETHEUS TABLES with where clause::

    os> source = my_prometheus.information_schema.tables | where TABLE_NAME='prometheus_http_requests_total'
    fetched rows / total rows = 1/1
    +---------------+--------------+--------------------------------+------------+------+---------------------------+
    | TABLE_CATALOG | TABLE_SCHEMA | TABLE_NAME                     | TABLE_TYPE | UNIT | REMARKS                   |
    |---------------+--------------+--------------------------------+------------+------+---------------------------|
    | my_prometheus | default      | prometheus_http_requests_total | counter    |      | Counter of HTTP requests. |
    +---------------+--------------+--------------------------------+------------+------+---------------------------+


Example 2: Search tables in prometheus datasource.
=================================================

The examples searches tables in the prometheus datasource.

PPL query for searching PROMETHEUS TABLES::

    os> source = my_prometheus.information_schema.tables | where LIKE(TABLE_NAME, "%http%");
    fetched rows / total rows = 6/6
     +---------------+--------------+--------------------------------------------+------------+------+----------------------------------------------------+
    | TABLE_CATALOG | TABLE_SCHEMA | TABLE_NAME                                 | TABLE_TYPE | UNIT | REMARKS                                            |
    |---------------+--------------+--------------------------------------------+------------+------+----------------------------------------------------|
    | my_prometheus | default      | prometheus_http_requests_total             | counter    |      | Counter of HTTP requests.                          |
    | my_prometheus | default      | promhttp_metric_handler_requests_in_flight | gauge      |      | Current number of scrapes being served.            |
    | my_prometheus | default      | prometheus_http_request_duration_seconds   | histogram  |      | Histogram of latencies for HTTP requests.          |
    | my_prometheus | default      | prometheus_sd_http_failures_total          | counter    |      | Number of HTTP service discovery refresh failures. |
    | my_prometheus | default      | promhttp_metric_handler_requests_total     | counter    |      | Total number of scrapes by HTTP status code.       |
    | my_prometheus | default      | prometheus_http_response_size_bytes        | histogram  |      | Histogram of response size for HTTP requests.      |
    +---------------+--------------+--------------------------------------------+------------+------+----------------------------------------------------+
