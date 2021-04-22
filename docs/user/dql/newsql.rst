.. highlight:: sh

==============
New SQL Engine
==============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Introduction
============

To use the SQL features present in documentation correctly, you need to enable our new SQL query engine by the following command::

    sh$ curl -sS -H 'Content-Type: application/json' \
    ... -X PUT localhost:9200/_opensearch/_sql/settings \
    ... -d '{"transient" : {"opensearch.sql.engine.new.enabled" : "true"}}'
    {
      "acknowledged": true,
      "persistent": {},
      "transient": {
        "opensearch": {
          "sql": {
            "engine": {
              "new": {
                "enabled": "true"
              }
            }
          }
        }
      }
    }
