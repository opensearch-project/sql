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


Definitions of datasource and connector
====================================
* Connector is a component that adapts the query engine to a datastore. For example, Prometheus connector would adapt and help execute the queries to run on Prometheus datastore. connector name is enough in the datasource definition json.
* Datasource is a construct to define how to connect to a data store and which connector to adapt by query engine.

Example Prometheus Datasource Definition ::

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
Datasource configuration Restrictions.

* ``name``, ``connector``, ``properties`` are required fields in the datasource configuration.
* All the datasource names should be unique and match the following regex[``[@*A-Za-z]+?[*a-zA-Z_\-0-9]*``].
* Allowed Connectors.
    * ``prometheus`` [More details: `Prometheus Connector <prometheus_connector.rst>`_]
* All the allowed config parameters in ``properties`` are defined in individual connector pages mentioned above.

Configuring a datasource in OpenSearch
======================================

* Datasources are configured in opensearch keystore as secure settings under ``plugins.query.federation.datasources.config`` key as they contain credential info.
* A json file containing array of datasource configurations should be injected into keystore with the above mentioned key. sample json file can be seen in the above section.


[**To be run on all the nodes in the cluster**] Command to add datasources.json file to OpenSearch Keystore ::

    >> bin/opensearch-keystore add-file plugins.query.federation.datasource.config datasources.json

Datasources can be configured during opensearch start up or can be updated while the opensearch is running.
If we update a datasource configuration during runtime, the following api should be triggered to update the query engine with the latest changes.

[**Required only if we update keystore settings during runtime**] Secure Settings refresh api::

    >> curl --request POST \
      --url http://{{opensearch-domain}}:9200/_nodes/reload_secure_settings \
      --data '{"secure_settings_password":"{{keystore-password}}"}'


Using a datasource in PPL command
====================================
Datasource is referred in source command as show in the code block below.
Based on the abstraction designed by the connector,
one can refer the corresponding entity as table in the source command.
For example in prometheus connector, each metric is abstracted as a table.
so we can refer a metric and apply stats over it in the following way.

Example source command with prometheus datasource ::

    >> source = my_prometheus.prometheus_http_requests_total | stats avg(@value) by job;


Limitations of datasource
====================================
Datasource settings are global and users with PPL access are allowed to fetch data from all the defined datasources.
PPL access can be controlled using roles.(More details: `Security Settings <security.rst>`_)