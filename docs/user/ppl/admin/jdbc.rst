.. highlight:: sh

==============
JDBC Connector
==============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1


Introduction
============

This page covers JDBC connector properties for dataSource configuration and the nuances associated with JDBC connector.


JDBC Connector Properties in DataSource Configuration
=====================================================
JDBC Connector Properties.

* ``url`` [Required].
    * This parameters provides the URL to connect to a database instance provided endpoint.
* ``driver`` [Required].
    * This parameters provides the Driver to connect to a database instance provided endpoint. The only supported ``org.apache.hive.jdbc.HiveDriver``
* ``username`` [Optional].
    * This username for basicauth.
* ``password`` [Optional].
    * This password for basicauth.

Example dataSource configuration with basic authentications
===========================================================

No Auth ::

    [{
        "name" : "myspark",
        "connector": "jdbc",
        "properties" : {
            "url" : "jdbc:hive2://localhost:10000/default",
            "driver" : "org.apache.hive.jdbc.HiveDriver"
        }
    }]

Basic Auth ::

    [{
        "name" : "myspark",
        "connector": "jdbc",
        "properties" : {
            "url" : "jdbc:hive2://localhost:10000/default",
            "driver" : "org.apache.hive.jdbc.HiveDriver",
            "username" : "username",
            "password" : "password"
        }
    }]

PPL supported for jdbc connector
================================

JDBC Table Function
-------------------
JDBC datasource could execute direct SQL against the target database. The SQL must be supported by target database.

Sample Example::

    os> source = myspark.jdbc('SHOW DATABASES');
    fetched rows / total rows = 1/1
    +-------------+
    | namespace   |
    |-------------|
    | default     |
    +-------------+

