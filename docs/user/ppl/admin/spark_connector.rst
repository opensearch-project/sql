.. highlight:: sh

====================
Spark Connector
====================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1


Introduction
============

This page covers spark connector properties for dataSource configuration
and the nuances associated with spark connector.


Spark Connector Properties in DataSource Configuration
========================================================
Spark Connector Properties.

* ``spark.connector`` [Required].
    * This parameters provides the spark client information for connection.
* ``spark.sql.application`` [Optional].
    * This parameters provides the spark sql application jar. Default value is ``s3://spark-datasource/sql-job.jar``.
* ``emr.cluster`` [Required].
    * This parameters provides the emr cluster id information.
* ``emr.auth.type`` [Required]
    * This parameters provides the authentication type information.
    * Spark emr connector currently supports ``awssigv4`` authentication mechanism and following parameters are required.
        * ``emr.auth.region``, ``emr.auth.access_key`` and ``emr.auth.secret_key``
* ``spark.datasource.flint.*`` [Optional]
    * This parameters provides the Opensearch domain host information for flint integration.
    * ``spark.datasource.flint.integration`` [Optional]
        * Default value for integration jar is ``s3://spark-datasource/flint-spark-integration-assembly-0.1.0-SNAPSHOT.jar``.
    * ``spark.datasource.flint.host`` [Optional]
        * Default value for host is ``localhost``.
    * ``spark.datasource.flint.port`` [Optional]
        * Default value for port is ``9200``.
    * ``spark.datasource.flint.scheme`` [Optional]
        * Default value for scheme is ``http``.
    * ``spark.datasource.flint.auth`` [Optional]
        * Default value for auth is ``false``.
    * ``spark.datasource.flint.region`` [Optional]
        * Default value for auth is ``us-west-2``.

Example spark dataSource configuration
========================================

AWSSigV4 Auth::

    [{
        "name" : "my_spark",
        "connector": "spark",
        "properties" : {
            "spark.connector": "emr",
            "emr.cluster" : "{{clusterId}}",
            "emr.auth.type" : "awssigv4",
            "emr.auth.region" : "us-east-1",
            "emr.auth.access_key" : "{{accessKey}}"
            "emr.auth.secret_key" : "{{secretKey}}"
            "spark.datasource.flint.host" : "{{opensearchHost}}",
            "spark.datasource.flint.port" : "{{opensearchPort}}",
            "spark.datasource.flint.scheme" : "{{opensearchScheme}}",
            "spark.datasource.flint.auth" : "{{opensearchAuth}}",
            "spark.datasource.flint.region" : "{{opensearchRegion}}",
        }
    }]


Spark SQL Support
==================

`sql` Function
----------------------------
Spark connector offers `sql` function. This function can be used to run spark sql query.
The function takes spark sql query as input. Argument should be either passed by name or positionArguments should be either passed by name or position.
`source=my_spark.sql('select 1')`
or
`source=my_spark.sql(query='select 1')`
Example::

    > source=my_spark.sql('select 1')
    +---+
    | 1 |
    |---+
    | 1 |
    +---+

