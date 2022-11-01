.. highlight:: sh

====================
Prometheus Connector
====================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1


Introduction
============

This page covers prometheus connector properties for catalog configuration
and the nuances associated with prometheus connector.


Prometheus Connector Properties in Catalog Configuration
========================================================
Prometheus Connector Properties.

* ``prometheus.uri`` [Required].
    * This parameters provides the URI information to connect to a prometheus instance.
* ``prometheus.auth.type`` [Optional]
    * This parameters provides the authentication type information.
    * Prometheus connector currently supports ``basicauth`` and ``awssigv4`` authentication mechanisms.
    * If prometheus.auth.type is basicauth, following are required parameters.
        * ``prometheus.auth.username`` and ``prometheus.auth.password``.
    * If prometheus.auth.type is awssigv4, following are required parameters.
        * ``prometheus.auth.region``, ``prometheus.auth.access_key`` and ``prometheus.auth.secret_key``

Example prometheus catalog configuration with different authentications
=======================================================================

No Auth ::

    [{
        "name" : "my_prometheus",
        "connector": "prometheus",
        "properties" : {
            "prometheus.uri" : "http://localhost:9090"
        }
    }]

Basic Auth ::

    [{
        "name" : "my_prometheus",
        "connector": "prometheus",
        "properties" : {
            "prometheus.uri" : "http://localhost:9090",
            "prometheus.auth.type" : "basicauth",
            "prometheus.auth.username" : "admin",
            "prometheus.auth.password" : "admin"
        }
    }]

AWSSigV4 Auth::

    [{
        "name" : "my_prometheus",
        "connector": "prometheus",
        "properties" : {
            "prometheus.uri" : "http://localhost:8080",
            "prometheus.auth.type" : "awssigv4",
            "prometheus.auth.region" : "us-east-1",
            "prometheus.auth.access_key" : "{{accessKey}}"
            "prometheus.auth.secret_key" : "{{secretKey}}"
        }
    }]

