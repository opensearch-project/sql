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
Currently, the system supports only AWS EMRServerless as the SPARK execution engine. Configuration details for the execution engine should be specified under ``plugins.query.executionengine.spark.config`` in the opensearch.yml or cluster settings. The configuration value is expected to be a JSON string that includes ``applicationId``, ``executionRoleARN``, ``region``, and ``sparkSubmitParameter``.

Sample Setting Value in opensearch.yml
--------------------

.. code-block:: yaml

    "plugins.query.executionengine.spark.config: '{\"applicationId\":\"xxxxx\",\"executionRoleARN\":\"arn:aws:iam::xxxxx:role/emr-job-execution-role\",\"region\":\"us-west-2\", \"sparkSubmitParameters\": \"--conf spark.dynamicAllocation.enabled=false\"}'"

Caution
-------

Users must exercise caution when transitioning to a new application or region, as changes to these parameters may lead to failures in retrieving results from previous asynchronous query jobs.

The system utilizes the default AWS credentials chain for calls to the EMR serverless application. It is critical to ensure that the default credentials have the necessary permissions to assume the role required for EMR job execution, as delineated in the engine configuration.

Requirements
-------------

- **Required Parameters**: ``applicationId``, ``executionRoleARN``, and ``region`` must be provided.
- **Optional Parameter**: ``sparkSubmitParameter`` is optional and can be formatted as ``--conf A=1 --conf B=2 ...``.

AWS CloudWatch metrics configuration
-------------

Starting with Flint 0.1.1, users can utilize AWS CloudWatch as an external metrics sink while configuring their own metric sources. Below is an example of a console request for setting this up:

.. code-block:: json

    PUT _cluster/settings
    {
      "persistent": {
        "plugins.query.executionengine.spark.config": "{\"applicationId\":\"xxxxx\",\"executionRoleARN\":\"arn:aws:iam::xxxxx:role/emr-job-execution-role\",\"region\":\"us-east-1\",\"sparkSubmitParameters\":\"--conf spark.dynamicAllocation.enabled=false --conf spark.metrics.conf.*.sink.cloudwatch.class=org.apache.spark.metrics.sink.CloudWatchSink --conf spark.metrics.conf.*.sink.cloudwatch.namespace=OpenSearchSQLSpark --conf spark.metrics.conf.*.sink.cloudwatch.regex=(opensearch|numberAllExecutors).* --conf spark.metrics.conf.*.source.cloudwatch.class=org.apache.spark.metrics.source.FlintMetricSource \"}"
      }
    }

For a comprehensive list of Spark configuration options related to metrics, please refer to the Spark documentation on monitoring:

- Spark Monitoring Documentation: https://spark.apache.org/docs/latest/monitoring.html#metrics

Additionally, for details on setting up CloudWatch metric sink and Flint metric source, consult the OpenSearch Spark project:

- OpenSearch Spark GitHub Repository: https://github.com/opensearch-project/opensearch-spark

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

Limitation: Flint index creation statement with auto_refresh = true can not be cancelled. User could submit ALTER statement to stop auto refresh query.
- flint index creation statement include, CREATE SKIPPING INDEX / CREATE INDEX / CREATE MATERIALIZED VIEW

HTTP URI: ``_plugins/_async_query/{queryId}``

HTTP VERB: ``DELETE``

Sample Request Body ::

    curl --location --request DELETE 'http://localhost:9200/_plugins/_async_query/00fdalrvgkbh2g0q' \
    --header 'Content-Type: application/json' \

