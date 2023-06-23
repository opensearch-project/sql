.. highlight:: sh

====================
Prometheus Connector
====================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1


Introduction
============

This page covers prometheus connector properties for dataSource configuration
and the nuances associated with prometheus connector.


Prometheus Connector Properties in DataSource Configuration
========================================================
Prometheus Connector Properties.

* ``prometheus.uri`` [Required].
    * This parameters provides the URI information to connect to a prometheus instance.
* ``prometheus.auth.type`` [Optional]
    * This parameters provides the authentication type information.
    * Prometheus connector currently supports ``basicauth`` and ``awssigv4`` authentication mechanisms.
    * If prometheus.auth.type is basicauth, following are required parameters.
        * ``prometheus.auth.username`` and ``prometheus.auth.password``.
    * If prometheus.auth.type is awssigv4, following are required parameters.
        * ``prometheus.auth.region``, ``prometheus.auth.access_key`` and ``prometheus.auth.secret_key``

Example prometheus dataSource configuration with different authentications
=======================================================================

No Auth ::

    [{
        "name" : "my_prometheus",
        "connector": "prometheus",
        "properties" : {
            "prometheus.uri" : "http://localhost:9090"
        }
    }]

Basic Auth ::

    [{
        "name" : "my_prometheus",
        "connector": "prometheus",
        "properties" : {
            "prometheus.uri" : "http://localhost:9090",
            "prometheus.auth.type" : "basicauth",
            "prometheus.auth.username" : "admin",
            "prometheus.auth.password" : "admin"
        }
    }]

AWSSigV4 Auth::

    [{
        "name" : "my_prometheus",
        "connector": "prometheus",
        "properties" : {
            "prometheus.uri" : "http://localhost:8080",
            "prometheus.auth.type" : "awssigv4",
            "prometheus.auth.region" : "us-east-1",
            "prometheus.auth.access_key" : "{{accessKey}}"
            "prometheus.auth.secret_key" : "{{secretKey}}"
        }
    }]

PPL Query support for prometheus connector
==========================================

Metric as a Table
---------------------------
Each connector has to abstract the underlying datasource constructs into a table as part of the interface contract with the PPL query engine.
Prometheus connector abstracts each metric as a table and the columns of this table are ``@value``, ``@timestamp``, ``label1``, ``label2``---.
``@value`` represents metric measurement and ``@timestamp`` represents the timestamp at which the metric is collected. labels are tags associated with metric queried.
For eg: ``handler``, ``code``, ``instance``, ``code`` are the labels associated with ``prometheus_http_requests_total`` metric. With this abstraction, we can query prometheus
data using PPL syntax similar to opensearch indices.

Sample Example::

    > source = my_prometheus.prometheus_http_requests_total;

    +------------+------------------------+--------------------------------+---------------+-------------+-------------+
    | @value     | @timestamp             |   handler                      | code          | instance    | job         |
    |------------+------------------------+--------------------------------+---------------+-------------+-------------|
    | 5          | "2022-11-03 07:18:14"  | "/-/ready"                     | 200           | 192.15.1.1  | prometheus  |
    | 3          | "2022-11-03 07:18:24"  | "/-/ready"                     | 200           | 192.15.1.1  | prometheus  |
    | 7          | "2022-11-03 07:18:34"  | "/-/ready"                     | 200           | 192.15.1.1  | prometheus  |
    | 2          | "2022-11-03 07:18:44"  | "/-/ready"                     | 400           | 192.15.2.1  | prometheus  |
    | 9          | "2022-11-03 07:18:54"  | "/-/promql"                    | 400           | 192.15.2.1  | prometheus  |
    | 11         | "2022-11-03 07:18:64"  |"/-/metrics"                    | 500           | 192.15.2.1  | prometheus  |
    +------------+------------------------+--------------------------------+---------------+-------------+-------------+



Default time range and resolution
---------------------------------
Since time range and resolution are required parameters for query apis and these parameters are determined in the following  manner from the PPL commands.
* Time range is determined through filter clause on ``@timestamp``. If there is no such filter clause, time range will be set to 1h with endtime set to now().
* In case of stats, resolution is determined by ``span(@timestamp,15s)`` expression. For normal select queries, resolution is auto determined from the time range set.

Prometheus Connector Limitations
--------------------------------
* Only one aggregation is supported in stats command.
* Span Expression is compulsory in stats command.
* AVG, MAX, MIN, SUM, COUNT are the only aggregations supported in prometheus connector.
* Where clause only supports EQUALS(=) operation on metric dimensions and Comparative(> , < , >= , <=) Operations on @timestamp attribute.

Example queries
---------------

1. Metric Selection Query::

    > source = my_prometheus.prometheus_http_requests_total
     +------------+------------------------+--------------------------------+---------------+-------------+-------------+
    | @value     | @timestamp             |   handler                      | code          | instance    | job         |
    |------------+------------------------+--------------------------------+---------------+-------------+-------------|
    | 5          | "2022-11-03 07:18:14"  | "/-/ready"                     | 200           | 192.15.1.1  | prometheus  |
    | 3          | "2022-11-03 07:18:24"  | "/-/ready"                     | 200           | 192.15.1.1  | prometheus  |
    | 7          | "2022-11-03 07:18:34"  | "/-/ready"                     | 200           | 192.15.1.1  | prometheus  |
    | 2          | "2022-11-03 07:18:44"  | "/-/ready"                     | 400           | 192.15.2.1  | prometheus  |
    | 9          | "2022-11-03 07:18:54"  | "/-/promql"                    | 400           | 192.15.2.1  | prometheus  |
    | 11         | "2022-11-03 07:18:64"  |"/-/metrics"                    | 500           | 192.15.2.1  | prometheus  |
    +------------+------------------------+--------------------------------+---------------+-------------+-------------+

2. Metric Selecting Query with specific dimensions::

    > source = my_prometheus.prometheus_http_requests_total | where handler='/-/ready' and code='200'
    +------------+------------------------+--------------------------------+---------------+-------------+-------------+
    | @value     | @timestamp             |   handler                      | code          | instance    | job         |
    |------------+------------------------+--------------------------------+---------------+-------------+-------------|
    | 5          | "2022-11-03 07:18:14"  | "/-/ready"                     | 200           | 192.15.1.1  | prometheus  |
    | 3          | "2022-11-03 07:18:24"  | "/-/ready"                     | 200           | 192.15.1.1  | prometheus  |
    | 7          | "2022-11-03 07:18:34"  | "/-/ready"                     | 200           | 192.15.1.1  | prometheus  |
    | 2          | "2022-11-03 07:18:44"  | "/-/ready"                     | 200           | 192.15.2.1  | prometheus  |
    | 9          | "2022-11-03 07:18:54"  | "/-/ready"                     | 200           | 192.15.2.1  | prometheus  |
    | 11         | "2022-11-03 07:18:64"  | "/-/ready"                     | 200           | 192.15.2.1  | prometheus  |
    +------------+------------------------+--------------------------------+---------------+-------------+-------------+

3. Average aggregation on a metric::

    > source = my_prometheus.prometheus_http_requests_total | stats avg(@value) by span(@timestamp,15s)
    +------------+------------------------+
    | avg(@value)| span(@timestamp,15s)   |
    |------------+------------------------+
    | 5          | "2022-11-03 07:18:14"  |
    | 3          | "2022-11-03 07:18:24"  |
    | 7          | "2022-11-03 07:18:34"  |
    | 2          | "2022-11-03 07:18:44"  |
    | 9          | "2022-11-03 07:18:54"  |
    | 11         | "2022-11-03 07:18:64"  |
    +------------+------------------------+

4. Average aggregation grouped by dimensions::

    > source = my_prometheus.prometheus_http_requests_total | stats avg(@value) by span(@timestamp,15s), handler, code
    +------------+------------------------+--------------------------------+---------------+
    | avg(@value)| span(@timestamp,15s)   |   handler                      | code          |
    |------------+------------------------+--------------------------------+---------------+
    | 5          | "2022-11-03 07:18:14"  | "/-/ready"                     | 200           |
    | 3          | "2022-11-03 07:18:24"  | "/-/ready"                     | 200           |
    | 7          | "2022-11-03 07:18:34"  | "/-/ready"                     | 200           |
    | 2          | "2022-11-03 07:18:44"  | "/-/ready"                     | 400           |
    | 9          | "2022-11-03 07:18:54"  | "/-/promql"                    | 400           |
    | 11         | "2022-11-03 07:18:64"  | "/-/metrics"                   | 500           |
    +------------+------------------------+--------------------------------+---------------+

5. Count aggregation query::

    > source = my_prometheus.prometheus_http_requests_total | stats count() by span(@timestamp,15s), handler, code
    +------------+------------------------+--------------------------------+---------------+
    | count()    | span(@timestamp,15s)   |   handler                      | code          |
    |------------+------------------------+--------------------------------+---------------+
    | 5          | "2022-11-03 07:18:14"  | "/-/ready"                     | 200           |
    | 3          | "2022-11-03 07:18:24"  | "/-/ready"                     | 200           |
    | 7          | "2022-11-03 07:18:34"  | "/-/ready"                     | 200           |
    | 2          | "2022-11-03 07:18:44"  | "/-/ready"                     | 400           |
    | 9          | "2022-11-03 07:18:54"  | "/-/promql"                    | 400           |
    | 11         | "2022-11-03 07:18:64"  | "/-/metrics"                   | 500           |
    +------------+------------------------+--------------------------------+---------------+

PromQL Support for prometheus Connector
==========================================

`query_range` Table Function
----------------------------
Prometheus connector offers `query_range` table function. This table function can be used to query metrics in a specific time range using promQL.
The function takes inputs similar to parameters mentioned for query range api mentioned here: https://prometheus.io/docs/prometheus/latest/querying/api/
Arguments should be either passed by name or positionArguments should be either passed by name or position.
`source=my_prometheus.query_range('prometheus_http_requests_total', 1686694425, 1686700130, 14)`
or
`source=my_prometheus.query_range(query='prometheus_http_requests_total', starttime=1686694425, endtime=1686700130, step=14)`
Example::

    > source=my_prometheus.query_range('prometheus_http_requests_total', 1686694425, 1686700130, 14)
     +------------+------------------------+--------------------------------+---------------+-------------+-------------+
    | @value     | @timestamp             |   handler                      | code          | instance    | job         |
    |------------+------------------------+--------------------------------+---------------+-------------+-------------|
    | 5          | "2022-11-03 07:18:14"  | "/-/ready"                     | 200           | 192.15.1.1  | prometheus  |
    | 3          | "2022-11-03 07:18:24"  | "/-/ready"                     | 200           | 192.15.1.1  | prometheus  |
    | 7          | "2022-11-03 07:18:34"  | "/-/ready"                     | 200           | 192.15.1.1  | prometheus  |
    | 2          | "2022-11-03 07:18:44"  | "/-/ready"                     | 400           | 192.15.2.1  | prometheus  |
    | 9          | "2022-11-03 07:18:54"  | "/-/promql"                    | 400           | 192.15.2.1  | prometheus  |
    | 11         | "2022-11-03 07:18:64"  |"/-/metrics"                    | 500           | 192.15.2.1  | prometheus  |
    +------------+------------------------+--------------------------------+---------------+-------------+-------------+
