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
* S3: This is where the data lies.
* Spark Execution Engine: Query Execution happens on spark.
* Glue Metadata store: Glue takes care of table metadata.
* Opensearch: Index for s3 data lies in opensearch and also acts as temporary buffer for query results.

We currently only support emr-serverless as spark execution engine and Glue as metadata store. we will add more support in future.

Glue Connector Properties in DataSource Configuration
========================================================
Glue Connector Properties.

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
        }
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
        }
    }]

Sample s3Glue datasource queries
================================
<To Be Added>


