.. highlight:: sh

=================
Catalog Settings
=================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1

Introduction
============

The concept of ``catalog`` is introduced to support the federation of SQL/PPL query engine to multiple data sources.
This helps PPL users to leverage data from multiple data sources and derive correlation and insights.
Catalog definition provides the information to connect to a datasource and also gives a name to them to refer in PPL commands.


Definitions of catalog and connector
====================================
* Connector is a component that adapts the query engine to a datasource. For example, Prometheus connector would adapt and help execute the queries to run on Prometheus data source. connector name is enough in the catalog definition json.
* Catalog is a construct to define how to connect to a datasource and which connector to adapt by query engine.

Example Prometheus Catalog Definition ::

    [{
        "name" : "prometheus",
        "connector": "prometheus",
        "uri" : "http://localhost:9090",
        "authentication" : {
            "type" : "basicauth",
            "username" : "admin",
            "password" : "admin"
        }
    }]
Catalog configuration Restrictions.

* ``name``, ``uri``, ``connector`` are required fields in the catalog configuration.
* All the catalog names should be unique.
* Catalog names should match with the regex of an identifier[``[@*A-Za-z]+?[*a-zA-Z_\-0-9]*``].
* ``prometheus`` is the only connector allowed.

Configuring catalog in OpenSearch
====================================

* Catalogs are configured in opensearch keystore as secure settings under ``plugins.query.federation.catalog.config`` key as they contain credential info.
* A json file containing array of catalog configurations should be injected into keystore with the above mentioned key. sample json file can be seen in the above section.


[**To be run on all the nodes in the cluster**] Command to add catalog.json file to OpenSearch Keystore ::

    >> bin/opensearch-keystore add-file plugins.query.federation.catalog.config catalog.json

Catalogs can be configured during opensearch start up or can be updated while the opensearch is running.
If we update catalog configuration during runtime, the following api should be triggered to update the query engine with the latest changes.

[**Required only if we update keystore settings during runtime**] Secure Settings refresh api::

    >> curl --request POST \
      --url http://{{opensearch-domain}}:9200/_nodes/reload_secure_settings \
      --data '{"secure_settings_password":"{{keystore-password}}"}'


Using a catalog in PPL command
====================================
Catalog is referred in source command as show in the code block below.
Based on the abstraction designed by the connector,
one can refer the corresponding entity as table in the source command.
For example in prometheus connector, each metric is abstracted as a table.
so we can refer a metric and apply stats over it in the following way.

Example source command with prometheus catalog ::

    >> source = prometheus.prometheus_http_requests_total | stats avg(@value) by job;


Limitations of catalog
====================================
* Catalog settings are global and all PPL users are allowed to fetch data from all the defined catalogs.
* In each catalog, PPL users can access all the data available with the credentials provided in the catalog definition.
* With the current release, Basic and AWSSigV4 are the only authentication mechanisms supported with the underlying data sources.



