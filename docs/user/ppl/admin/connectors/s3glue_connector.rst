.. highlight:: sh

====================
S3Glue Connector
====================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1


Introduction
============

s3Glue connector provides a way to query s3 files using glue as metadata store and spark as execution engine.
This page covers s3Glue datasource configuration and also how to query and s3Glue datasource.

Required resources for s3 Glue Connector
===================================
* ``EMRServerless Spark Execution Engine Config Setting``:  Since we execute s3Glue queries on top of spark execution engine, we require this configuration.
  More details: `ExecutionEngine Config <../../../interfaces/asyncqueryinterface.rst#id2>`_
* ``S3``: This is where the data lies.
* ``Glue`` Metadata store: Glue takes care of table metadata.
* ``Opensearch IndexStore``: Index for s3 data lies in opensearch and also acts as temporary buffer for query results.

We currently only support emr-serverless as spark execution engine and Glue as metadata store. we will add more support in future.

Glue Connector Properties.

* ``resultIndex`` is a new parameter specific to glue connector. Stores the results of queries executed on the data source. If unavailable, it defaults to .query_execution_result.
* ``glue.auth.type`` [Required]
    * This parameters provides the authentication type information required for execution engine to connect to glue.
    * S3 Glue connector currently only supports ``iam_role`` authentication and the below parameters is required.
        * ``glue.auth.role_arn``
* ``glue.indexstore.opensearch.*`` [Required]
    * This parameters provides the Opensearch domain host information for glue connector. This opensearch instance is used for writing index data back and also
    * ``glue.indexstore.opensearch.uri`` [Required]
    * ``glue.indexstore.opensearch.auth`` [Required]
        * Accepted values include ["noauth", "basicauth", "awssigv4"]
        * Basic Auth required ``glue.indexstore.opensearch.auth.username`` and ``glue.indexstore.opensearch.auth.password``
        * AWSSigV4 Auth requires ``glue.indexstore.opensearch.auth.region``  and ``glue.auth.role_arn``
    * ``glue.indexstore.opensearch.region`` [Required for awssigv4 auth]

Sample Glue dataSource configuration
========================================

Glue datasource configuration::

    [{
        "name" : "my_glue",
        "connector": "s3glue",
        "properties" : {
                "glue.auth.type": "iam_role",
                "glue.auth.role_arn": "role_arn",
                "glue.indexstore.opensearch.uri": "http://localhost:9200",
                "glue.indexstore.opensearch.auth" :"basicauth",
                "glue.indexstore.opensearch.auth.username" :"username"
                "glue.indexstore.opensearch.auth.password" :"password"
        },
        "resultIndex": "query_execution_result"
    }]

    [{
        "name" : "my_glue",
        "connector": "s3glue",
        "properties" : {
                "glue.auth.type": "iam_role",
                "glue.auth.role_arn": "role_arn",
                "glue.indexstore.opensearch.uri": "http://adsasdf.amazonopensearch.com:9200",
                "glue.indexstore.opensearch.auth" :"awssigv4",
                "glue.indexstore.opensearch.auth.region" :"awssigv4",
        },
        "resultIndex": "query_execution_result"
    }]

Sample s3Glue datasource queries APIS
=====================================

Sample Queries

* Select Query : ``select * from mys3.default.http_logs limit 1"``
* Create Covering Index Query: ``create index clientip_year on my_glue.default.http_logs (clientip, year) WITH (auto_refresh=true)``
* Create Skipping Index: ``create skipping index on mys3.default.http_logs (status VALUE_SET)``

These queries would work only top of async queries. Documentation: `Async Query APIs <../../../interfaces/asyncqueryinterface.rst>`_

Documentation for Index Queries: https://github.com/opensearch-project/opensearch-spark/blob/main/docs/index.md