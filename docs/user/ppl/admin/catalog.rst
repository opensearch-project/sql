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

The concept of ``datasource`` is introduced to support the federation of SQL/PPL query engine to multiple data sources.
This helps PPL users to leverage data from multiple data sources and derive correlation and insights.
Catalog definition provides the information to connect to a datasource and also gives a name to them to refer in PPL commands.


Definitions of datasource and connector
====================================
* Connector is a component that adapts the query engine to a datasource. For example, Prometheus connector would adapt and help execute the queries to run on Prometheus data source. connector name is enough in the datasource definition json.
* Catalog is a construct to define how to connect to a datasource and which connector to adapt by query engine.

Example Prometheus Catalog Definition ::

    [{
        "name" : "my_prometheus",
        "connector": "prometheus",
        "properties" : {
            "prometheus.uri" : "http://localhost:8080",
            "prometheus.auth.type" : "basicauth",
            "prometheus.auth.username" : "admin",
            "prometheus.auth.password" : "admin"
        }
    }]
Catalog configuration Restrictions.

* ``name``, ``connector``, ``properties`` are required fields in the datasource configuration.
* All the datasource names should be unique and match the following regex[``[@*A-Za-z]+?[*a-zA-Z_\-0-9]*``].
* Allowed Connectors.
    * ``prometheus`` [More details: `Prometheus Connector <prometheus_connector.rst>`_]
* All the allowed config parameters in ``properties`` are defined in individual connector pages mentioned above.

Configuring datasource in OpenSearch
====================================

* Catalogs are configured in opensearch keystore as secure settings under ``plugins.query.federation.datasource.config`` key as they contain credential info.
* A json file containing array of datasource configurations should be injected into keystore with the above mentioned key. sample json file can be seen in the above section.


[**To be run on all the nodes in the cluster**] Command to add datasource.json file to OpenSearch Keystore ::

    >> bin/opensearch-keystore add-file plugins.query.federation.datasource.config datasource.json

Catalogs can be configured during opensearch start up or can be updated while the opensearch is running.
If we update datasource configuration during runtime, the following api should be triggered to update the query engine with the latest changes.

[**Required only if we update keystore settings during runtime**] Secure Settings refresh api::

    >> curl --request POST \
      --url http://{{opensearch-domain}}:9200/_nodes/reload_secure_settings \
      --data '{"secure_settings_password":"{{keystore-password}}"}'


Using a datasource in PPL command
====================================
Catalog is referred in source command as show in the code block below.
Based on the abstraction designed by the connector,
one can refer the corresponding entity as table in the source command.
For example in prometheus connector, each metric is abstracted as a table.
so we can refer a metric and apply stats over it in the following way.

Example source command with prometheus datasource ::

    >> source = my_prometheus.prometheus_http_requests_total | stats avg(@value) by job;


Limitations of datasource
====================================
Catalog settings are global and users with PPL access are allowed to fetch data from all the defined datasources.
PPL access can be controlled using roles.(More details: `Security Settings <security.rst>`_)