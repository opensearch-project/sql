.. highlight:: sh

=======================
Async Query Interface Endpoints
=======================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1


Introduction
============

For supporting `S3Glue <../ppl/admin/connectors/s3glue_connector.rst>`_  datasource connector, we have introduced a new execution engine on top of Spark.
All the queries to be executed on spark execution engine can only be submitted via Async Query APIs. Below sections will list all the new APIs introduced.


Required Spark Execution Engine Config for Async Query APIs
===========================================================
Currently, we only support AWS EMRServerless as SPARK execution engine. The details of execution engine should be configured under
``plugins.query.executionengine.spark.config`` in cluster settings. The value should be a stringified json comprising of ``applicationId``, ``executionRoleARN``,``region``, ``sparkSubmitParameter``.
Sample Setting Value ::

    plugins.query.executionengine.spark.config:
    '{  "applicationId":"xxxxx",
        "executionRoleARN":"arn:aws:iam::***********:role/emr-job-execution-role",
        "region":"eu-west-1",
        "sparkSubmitParameter": "--conf spark.dynamicAllocation.enabled=false"
    }'
If this setting is not configured during bootstrap, Async Query APIs will be disabled and it requires a cluster restart to enable them back again.
We make use of default aws credentials chain to make calls to the emr serverless application and also make sure the default credentials
have pass role permissions for emr-job-execution-role mentioned in the engine configuration.

*  ``applicationId``, ``executionRoleARN`` and ``region`` are required parameters.
*  ``sparkSubmitParameter`` is an optional parameter. It can take the form ``--conf A=1 --conf B=2 ...``.


Async Query Creation API
======================================
If security plugin is enabled, this API can only be invoked by users with permission ``cluster:admin/opensearch/ql/async_query/create``.

HTTP URI: ``_plugins/_async_query``

HTTP VERB: ``POST``

Sample Request::

    curl --location 'http://localhost:9200/_plugins/_async_query' \
    --header 'Content-Type: application/json' \
    --data '{
        "datasource" : "my_glue",
        "lang" : "sql",
        "query" : "select * from my_glue.default.http_logs limit 10"
    }'

Sample Response::

    {
      "queryId": "00fd796ut1a7eg0q"
    }

Execute query in session
------------------------

if plugins.query.executionengine.spark.session.enabled is set to true, session based execution is enabled. Under the hood, all queries submitted to the same session will be executed in the same SparkContext. Session is auto closed if not query submission in 10 minutes.

Async query response include ``sessionId`` indicate the query is executed in session.

Sample Request::

    curl --location 'http://localhost:9200/_plugins/_async_query' \
    --header 'Content-Type: application/json' \
    --data '{
        "datasource" : "my_glue",
        "lang" : "sql",
        "query" : "select * from my_glue.default.http_logs limit 10"
    }'

Sample Response::

    {
      "queryId": "HlbM61kX6MDkAktO",
      "sessionId": "1Giy65ZnzNlmsPAm"
    }

User could reuse the session by using ``sessionId`` query parameters.

Sample Request::

    curl --location 'http://localhost:9200/_plugins/_async_query' \
    --header 'Content-Type: application/json' \
    --data '{
        "datasource" : "my_glue",
        "lang" : "sql",
        "query" : "select * from my_glue.default.http_logs limit 10",
        "sessionId" : "1Giy65ZnzNlmsPAm"
    }'

Sample Response::

    {
      "queryId": "7GC4mHhftiTejvxN",
      "sessionId": "1Giy65ZnzNlmsPAm"
    }


Async Query Result API
======================================
If security plugin is enabled, this API can only be invoked by users with permission ``cluster:admin/opensearch/ql/async_query/result``.
Async Query Creation and Result Query permissions are orthogonal, so any user with result api permissions and queryId can query the corresponding query results irrespective of the user who created the async query.

HTTP URI: ``_plugins/_async_query/{queryId}``

HTTP VERB: ``GET``

Sample Request BODY::

    curl --location --request GET 'http://localhost:9200/_plugins/_async_query/00fd796ut1a7eg0q' \
    --header 'Content-Type: application/json' \

Sample Response if the Query is in Progress ::

    {"status":"RUNNING"}

Sample Response If the Query is successful ::

    {
        "status": "SUCCESS",
        "schema": [
            {
                "name": "indexed_col_name",
                "type": "string"
            },
            {
                "name": "data_type",
                "type": "string"
            },
            {
                "name": "skip_type",
                "type": "string"
            }
        ],
        "datarows": [
            [
                "status",
                "int",
                "VALUE_SET"
            ]
        ],
        "total": 1,
        "size": 1
    }


Async Query Cancellation API
======================================
If security plugin is enabled, this API can only be invoked by users with permission ``cluster:admin/opensearch/ql/jobs/delete``.

HTTP URI: ``_plugins/_async_query/{queryId}``

HTTP VERB: ``DELETE``

Sample Request Body ::

    curl --location --request DELETE 'http://localhost:9200/_plugins/_async_query/00fdalrvgkbh2g0q' \
    --header 'Content-Type: application/json' \

