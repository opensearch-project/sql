.. highlight:: sh

====================
Security Lake Connector
====================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1


Introduction
============

Security Lake connector provides a way to query Security Lake tables.

Required resources for Security Lake Connector
========================================
* ``EMRServerless Spark Execution Engine Config Setting``:  Since we execute s3Glue queries on top of spark execution engine, we require this configuration.
  More details: `ExecutionEngine Config <../../../interfaces/asyncqueryinterface.rst#id2>`_
* ``S3``: This is where the data lies.
* ``Glue``: Metadata store: Glue takes care of table metadata.
* ``Lake Formation``: AWS service that performs authorization on Security Lake tables
* ``Security Lake``: AWS service that orchestrates creation of S3 files, Glue tables, and Lake Formation permissions.
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
* ``glue.lakeformation.session_tag`` [Required]
    * What session tag to use when assuming the data source role.

Sample Glue dataSource configuration
========================================

Glue datasource configuration::

    [{
        "name" : "my_sl",
        "connector": "security_lake",
        "properties" : {
                "glue.auth.type": "iam_role",
                "glue.auth.role_arn": "role_arn",
                "glue.indexstore.opensearch.uri": "http://adsasdf.amazonopensearch.com:9200",
                "glue.indexstore.opensearch.auth" :"awssigv4",
                "glue.indexstore.opensearch.auth.region" :"us-east-1",
                "glue.lakeformation.session_tag": "sesson_tag"
        },
        "resultIndex": "query_execution_result"
    }]

Sample Security Lake datasource queries APIS
=====================================

Sample Queries

* Select Query : ``select * from mysl.amazon_security_lake_glue_db_eu_west_1.amazon_security_lake_table_eu_west_1_vpc_flow_2_0 limit 1``
* Create Covering Index Query: ``create index srcip_time on mysl.amazon_security_lake_glue_db_eu_west_1.amazon_security_lake_table_eu_west_1_vpc_flow_2_0 (src_endpoint.ip, time) WITH (auto_refresh=true)``

These queries would work only top of async queries. Documentation: `Async Query APIs <../../../interfaces/asyncqueryinterface.rst>`_

Documentation for Index Queries: https://github.com/opensearch-project/opensearch-spark/blob/main/docs/index.md
