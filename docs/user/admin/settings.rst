.. highlight:: sh

===============
Plugin Settings
===============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1


Introduction
============

When OpenSearch bootstraps, SQL plugin will register a few settings in OpenSearch cluster settings. Most of the settings are able to change dynamically so you can control the behavior of SQL plugin without need to bounce your cluster. You can update the settings by sending requests to either ``_cluster/settings`` or ``_plugins/_query/settings`` endpoint, though the examples are sending to the latter.

Breaking Change
===============
opendistro.sql.engine.new.enabled
---------------------------------
The opendistro.sql.engine.new.enabled setting is deprecated and will be removed then. From OpenSearch 1.0, the new engine is always enabled.

opendistro.sql.query.analysis.enabled
-------------------------------------
The opendistro.sql.query.analysis.enabled setting is deprecated and will be removed then. From OpenSearch 1.0, the query analysis in legacy engine is disabled.

opendistro.sql.query.analysis.semantic.suggestion
-------------------------------------------------
The opendistro.sql.query.analysis.semantic.suggestion setting is deprecated and will be removed then. From OpenSearch 1.0, the query analysis suggestion in legacy engine is disabled.

opendistro.sql.query.analysis.semantic.threshold
------------------------------------------------
The opendistro.sql.query.analysis.semantic.threshold setting is deprecated and will be removed then. From OpenSearch 1.0, the query analysis threshold in legacy engine is disabled.

opendistro.sql.query.response.format
------------------------------------
The opendistro.sql.query.response.format setting is deprecated and will be removed then. From OpenSearch 1.0, the query response format is default to JDBC format. `You can change the format by using query parameters<../interfaces/protocol.rst>`_.

opendistro.sql.cursor.enabled
-----------------------------
The opendistro.sql.cursor.enabled setting is deprecated and will be removed then. From OpenSearch 1.0, the cursor feature is enabled by default.

opendistro.sql.cursor.fetch_size
--------------------------------
The opendistro.sql.cursor.fetch_size setting is deprecated and will be removed then. From OpenSearch 1.0, the fetch_size in query body will decide whether create the cursor context. No cursor will be created if the fetch_size = 0.

plugins.sql.enabled
======================

Description
-----------

You can disable SQL plugin to reject all coming requests.

1. The default value is true.
2. This setting is node scope.
3. This setting can be updated dynamically.


Example 1
---------

You can update the setting with a new value like this.

SQL query::

	>> curl -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_query/settings -d '{
	  "transient" : {
	    "plugins.sql.enabled" : "false"
	  }
	}'

Result set::

	{
	  "acknowledged" : true,
	  "persistent" : { },
	  "transient" : {
	    "plugins" : {
	      "sql" : {
	        "enabled" : "false"
	      }
	    }
	  }
	}

Note: the legacy settings of ``opendistro.sql.enabled`` is deprecated, it will fallback to the new settings if you request an update with the legacy name.

Example 2
---------

Query result after the setting updated is like:

SQL query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_sql -d '{
	  "query" : "SELECT * FROM accounts"
	}'

Result set::

	{
	  "error" : {
	    "reason" : "Invalid SQL query",
	    "details" : "Either plugins.sql.enabled or rest.action.multi.allow_explicit_index setting is false",
	    "type" : "SQLFeatureDisabledException"
	  },
	  "status" : 400
	}

plugins.sql.slowlog
============================

Description
-----------

You can configure the time limit (seconds) for slow query which would be logged as 'Slow query: elapsed=xxx (ms)' in opensearch.log.

1. The default value is 2.
2. This setting is node scope.
3. This setting can be updated dynamically.


Example
-------

You can update the setting with a new value like this.

SQL query::

	>> curl -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_query/settings -d '{
	  "transient" : {
	    "plugins.query.slowlog" : "10"
	  }
	}'

Result set::

	{
	  "acknowledged" : true,
	  "persistent" : { },
	  "transient" : {
	    "plugins" : {
	      "query" : {
	        "slowlog" : "10"
	      }
	    }
	  }
	}

Note: the legacy settings of ``opendistro.sql.slowlog`` is deprecated, it will fallback to the new settings if you request an update with the legacy name.

plugins.sql.cursor.keep_alive
================================

Description
-----------

User can set this value to indicate how long the cursor context should be kept open. Cursor contexts are resource heavy, and a lower value should be used if possible.

1. The default value is 1m.
2. This setting is node scope.
3. This setting can be updated dynamically.


Example
-------

You can update the setting with a new value like this.

SQL query::

	>> curl -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_query/settings -d '{
	  "transient" : {
	    "plugins.sql.cursor.keep_alive" : "5m"
	  }
	}'

Result set::

	{
	  "acknowledged" : true,
	  "persistent" : { },
	  "transient" : {
	    "plugins" : {
	      "sql" : {
	        "cursor" : {
	          "keep_alive" : "5m"
	        }
	      }
	    }
	  }
	}

Note: the legacy settings of ``opendistro.sql.cursor.keep_alive`` is deprecated, it will fallback to the new settings if you request an update with the legacy name.

plugins.query.size_limit
===========================

Description
-----------

The new engine fetches a default size of index from OpenSearch set by this setting, the default value is 200. You can change the value to any value not greater than the max result window value in index level (10000 by default), here is an example::

	>> curl -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_query/settings -d '{
	  "transient" : {
	    "plugins.query.size_limit" : 500
	  }
	}'

Result set::

    {
      "acknowledged" : true,
      "persistent" : { },
      "transient" : {
        "plugins" : {
          "query" : {
            "size_limit" : "500"
          }
        }
      }
    }

Note: the legacy settings of ``opendistro.query.size_limit`` is deprecated, it will fallback to the new settings if you request an update with the legacy name.

plugins.query.memory_limit
==========================

Description
-----------

You can set heap memory usage limit for the query engine. When query running, it will detected whether the heap memory usage under the limit, if not, it will terminated the current query. The default value is: 85%. Here is an example::

	>> curl -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_query/settings -d '{
	  "transient" : {
	    "plugins.query.memory_limit" : "80%"
	  }
	}'

Result set::

    {
      "acknowledged": true,
      "persistent": {
        "plugins": {
          "query": {
            "memory_limit": "80%"
          }
        }
      },
      "transient": {}
    }

Note: the legacy settings of ``opendistro.ppl.query.memory_limit`` is deprecated, it will fallback to the new settings if you request an update with the legacy name.


plugins.sql.delete.enabled
======================

Description
-----------

By default, DELETE clause disabled. You can enable DELETE clause by this setting.

1. The default value is false.
2. This setting is node scope.
3. This setting can be updated dynamically.


Example 1
---------

You can update the setting with a new value like this.

SQL query::

    sh$ curl -sS -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_query/settings \
    ... -d '{"transient":{"plugins.sql.delete.enabled":"false"}}'
    {
      "acknowledged": true,
      "persistent": {},
      "transient": {
        "plugins": {
          "sql": {
            "delete": {
              "enabled": "false"
            }
          }
        }
      }
    }

Example 2
---------

Query result after the setting updated is like:

SQL query::

    sh$ curl -sS -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_sql \
    ... -d '{"query" : "DELETE * FROM accounts"}'
    {
      "error": {
        "reason": "Invalid SQL query",
        "details": "DELETE clause is disabled by default and will be deprecated. Using the plugins.sql.delete.enabled setting to enable it",
        "type": "SQLFeatureDisabledException"
      },
      "status": 400
    }

