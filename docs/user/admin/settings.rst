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

When OpenSearch bootstraps, SQL plugin will register a few settings in OpenSearch cluster settings. Most of the settings are able to change dynamically so you can control the behavior of SQL plugin without need to bounce your cluster.


opensearch.sql.enabled
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

	>> curl -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_sql/settings -d '{
	  "transient" : {
	    "opensearch.sql.enabled" : "false"
	  }
	}'

Result set::

	{
	  "acknowledged" : true,
	  "persistent" : { },
	  "transient" : {
	    "opensearch" : {
	      "sql" : {
	        "enabled" : "false"
	      }
	    }
	  }
	}

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
	    "details" : "Either opensearch.sql.enabled or rest.action.multi.allow_explicit_index setting is false",
	    "type" : "SQLFeatureDisabledException"
	  },
	  "status" : 400
	}

opensearch.sql.query.slowlog
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

	>> curl -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_sql/settings -d '{
	  "transient" : {
	    "opensearch.sql.query.slowlog" : "10"
	  }
	}'

Result set::

	{
	  "acknowledged" : true,
	  "persistent" : { },
	  "transient" : {
	    "opensearch" : {
	      "sql" : {
	        "query" : {
	          "slowlog" : "10"
	        }
	      }
	    }
	  }
	}

opensearch.sql.cursor.keep_alive
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

	>> curl -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_sql/settings -d '{
	  "transient" : {
	    "opensearch.sql.cursor.keep_alive" : "5m"
	  }
	}'

Result set::

	{
	  "acknowledged" : true,
	  "persistent" : { },
	  "transient" : {
	    "opensearch" : {
	      "sql" : {
	        "cursor" : {
	          "keep_alive" : "5m"
	        }
	      }
	    }
	  }
	}

opensearch.query.size_limit
===========================

Description
-----------

The new engine fetches a default size of index from OpenSearch set by this setting, the default value is 200. You can change the value to any value not greater than the max result window value in index level (10000 by default), here is an example::

	>> curl -H 'Content-Type: application/json' -X PUT localhost:9200/_cluster/settings -d '{
	  "transient" : {
	    "opensearch.query.size_limit" : 500
	  }
	}'

Result set::

    {
      "acknowledged" : true,
      "persistent" : { },
      "transient" : {
        "opensearch" : {
          "query" : {
            "size_limit" : "500"
          }
        }
      }
    }

opensearch.sql.delete.enabled
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

    sh$ curl -sS -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_sql/settings \
    ... -d '{"transient":{"opensearch.sql.delete.enabled":"false"}}'
    {
      "acknowledged": true,
      "persistent": {},
      "transient": {
        "opensearch": {
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
        "details": "DELETE clause is disabled by default and will be deprecated. Using the opensearch.sql.delete.enabled setting to enable it",
        "type": "SQLFeatureDisabledException"
      },
      "status": 400
    }
