.. highlight:: sh

===================
Datasource Settings
===================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1

Introduction
============

The concept of ``datasource`` is introduced to support the federation of SQL/PPL query engine to multiple data stores.
This helps PPL users to leverage data from multiple data stores and derive correlation and insights.
Datasource definition provides the information to connect to a data store and also gives a name to them to refer in PPL commands.

Refer below sections for quick setup.

* `Datasource configuration APIs`_
* `Master Key config for encrypting credential information`_


Definitions of datasource and connector
====================================
* Connector is a component that adapts the query engine to a datastore. For example, Prometheus connector would adapt and help execute the queries to run on Prometheus datastore. connector name is enough in the datasource definition json.
* Datasource is a construct to define how to connect to a data store and which connector to adapt by query engine.

Example Prometheus Datasource Definition ::

    {
        "name" : "my_prometheus",
        "connector": "prometheus",
        "properties" : {
            "prometheus.uri" : "http://localhost:8080",
            "prometheus.auth.type" : "basicauth",
            "prometheus.auth.username" : "admin",
            "prometheus.auth.password" : "admin"
        },
        "allowedRoles" : ["prometheus_access"]
    }
Datasource configuration Restrictions.

* ``name``, ``connector``, ``properties`` are required fields in the datasource configuration.
* In case of secure domains, ``allowedRoles`` can be used to specify the opensearch roles allowed to access the datasource via PPL/SQL.
* If ``allowedRoles`` are not specified for a datasource, only users with ``all_access`` could access the datasource in case of secure domains.
* In case of security disabled domains, authorization is disbaled.
* All the datasource names should be unique and match the following regex[``[@*A-Za-z]+?[*a-zA-Z_\-0-9]*``].
* Allowed Connectors.
    * ``prometheus`` [More details: `Prometheus Connector <connectors/prometheus_connector.rst>`_]
* All the allowed config parameters in ``properties`` are defined in individual connector pages mentioned above.

Datasource configuration APIs
======================================
Datasource configuration can be managed using below REST APIs. All the examples below are for OpenSearch domains enabled with secure domain.
we can remove authorization and other details in case of security disabled domains.

* Datasource Creation POST API ("_plugins/_query/_datasources") ::

    POST https://localhost:9200/_plugins/_query/_datasources
    content-type: application/json
    Authorization: Basic {{username}} {{password}}

    {
        "name" : "my_prometheus",
        "connector": "prometheus",
        "properties" : {
            "prometheus.uri" : "http://localhost:8080",
            "prometheus.auth.type" : "basicauth",
            "prometheus.auth.username" : "admin",
            "prometheus.auth.password" : "admin"
        },
        "allowedRoles" : ["prometheus_access"]
    }

* Datasource modification PUT API ("_plugins/_query/_datasources") ::

    PUT https://localhost:9200/_plugins/_query/_datasources
    content-type: application/json
    Authorization: Basic {{username}} {{password}}

    {
        "name" : "my_prometheus",
        "connector": "prometheus",
        "properties" : {
            "prometheus.uri" : "http://localhost:8080",
            "prometheus.auth.type" : "basicauth",
            "prometheus.auth.username" : "admin",
            "prometheus.auth.password" : "admin"
        },
        "allowedRoles" : ["prometheus_access"]
    }

* Datasource modification PATCH API ("_plugins/_query/_datasources") ::

    PATCH https://localhost:9200/_plugins/_query/_datasources
    content-type: application/json
    Authorization: Basic {{username}} {{password}}

    {
        "name" : "my_prometheus",
        "allowedRoles" : ["all_access"]
    }

  **Name is required and must exist. Connector cannot be modified and will be ignored.**

* Datasource Read GET API("_plugins/_query/_datasources/{{dataSourceName}}" ::

    GET https://localhost:9200/_plugins/_query/_datasources/my_prometheus
    content-type: application/json
    Authorization: Basic {{username}} {{password}}

  **Authentication Information won't be vended out in GET API's response.**

* Datasource Deletion DELETE API("_plugins/_query/_datasources/{{dataSourceName}}") ::

    DELETE https://localhost:9200/_plugins/_query/_datasources/my_prometheus
    content-type: application/json
    Authorization: Basic {{username}} {{password}}

Authorization of datasource configuration APIs
==============================================
Each of the datasource configuration management apis are controlled by following actions respectively.

* cluster:admin/opensearch/datasources/create [Create POST API]
* cluster:admin/opensearch/datasources/read   [Get GET API]
* cluster:admin/opensearch/datasources/update [Update PUT API]
* cluster:admin/opensearch/datasources/patch [Update PATCH API]
* cluster:admin/opensearch/datasources/delete [Delete DELETE API]

Only users mapped with roles having above actions are authorized to execute datasource management apis.

Master Key config for encrypting credential information
========================================================
* When users provide credentials for a data source, the system encrypts and securely stores them in the metadata index. System uses "AES/GCM/NoPadding" symmetric encryption algorithm.
* Master key is a required config and users can set this up by configuring the `plugins.query.datasources.encryption.masterkey` setting in the opensearch.yml file.
* The master key must be 16, 24, or 32 characters long.
* Sample Bash Script to generate a 24 character master key ::

    #!/bin/bash
    # Generate a 24-character key
    master_key=$(openssl rand -hex 12)
    echo "Master Key: $master_key"
* Sample python script to generate a 24 character master key ::

    import random
    import string

    # Generate a 24-character random master key
    master_key = ''.join(random.choices(string.ascii_letters + string.digits, k=24))

    # Print the master key
    print("Generated master key:", master_key)

Datasource URI Hosts Deny Lists Config
========================================================
* In the OpenSearch configuration file (opensearch.yml), the parameter "plugins.query.datasources.uri.hosts.denylist" can be utilized to control the permitted host ips within the datasource URI configuration.
* By default, the value is set to empty list, which allows any domain to be accepted.
* For instance, if you set the value to `127.0.0.0/8`, ppl plugins will deny all the query requests where the datasource URI resolves to the ip range from `127.0.0.0` to `127.255.255.255`


Using a datasource in PPL command
====================================
Datasource is referred in source command as show in the code block below.
Based on the abstraction designed by the connector,
one can refer the corresponding entity as table in the source command.
For example in prometheus connector, each metric is abstracted as a table.
so we can refer a metric and apply stats over it in the following way.

Example source command with prometheus datasource ::

    >> source = my_prometheus.prometheus_http_requests_total | stats avg(@value) by job;


Authorization of PPL commands on datasources
============================================
In case of secure opensearch domains, only admins and users with roles mentioned in datasource configuration are allowed to make queries.
For example: with below datasource configuration, only admins and users with prometheus_access role can run queries on my_prometheus datasource. ::

    {
        "name" : "my_prometheus",
        "connector": "prometheus",
        "properties" : {
            "prometheus.uri" : "http://localhost:8080",
            "prometheus.auth.type" : "basicauth",
            "prometheus.auth.username" : "admin",
            "prometheus.auth.password" : "admin"
        },
        "allowedRoles" : ["prometheus_access"]
    }


Moving from keystore datasource configuration
=============================================
* In versions prior to 2.7, the plugins.query.federation.datasources.config key store setting was used to configure datasources, but it has been deprecated and will be removed in version 3.0.
* To port previously configured datasources from the keystore, users can use the `create datasource` REST API mentioned in the above section.
