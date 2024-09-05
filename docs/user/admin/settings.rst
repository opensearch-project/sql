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

The new engine fetches a default size of index from OpenSearch set by this setting, the default value equals to max result window in index level (10000 by default). You can change the value to any value not greater than the max result window value in index level (`index.max_result_window`), here is an example::

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


plugins.query.executionengine.spark.session.limit
==================================================

Description
-----------

Each cluster can have maximum 10 sessions running in parallel by default. You can increase limit by this setting.

1. The default value is 10.
2. This setting is node scope.
3. This setting can be updated dynamically.

You can update the setting with a new value like this.

SQL query::

    sh$ curl -sS -H 'Content-Type: application/json' -X PUT localhost:9200/_cluster/settings \
    ... -d '{"transient":{"plugins.query.executionengine.spark.session.limit":200}}'
    {
      "acknowledged": true,
      "persistent": {},
      "transient": {
        "plugins": {
          "query": {
            "executionengine": {
              "spark": {
                "session": {
                  "limit": "200"
                }
              }
            }
          }
        }
      }
    }


plugins.query.executionengine.spark.refresh_job.limit
=====================================================

Description
-----------

Each cluster can have maximum 5 refresh job running concurrently. You can increase limit by this setting.

1. The default value is 5.
2. This setting is node scope.
3. This setting can be updated dynamically.

You can update the setting with a new value like this.

SQL query::

    sh$ curl -sS -H 'Content-Type: application/json' -X PUT localhost:9200/_cluster/settings \
    ... -d '{"transient":{"plugins.query.executionengine.spark.refresh_job.limit":200}}'
    {
      "acknowledged": true,
      "persistent": {},
      "transient": {
        "plugins": {
          "query": {
            "executionengine": {
              "spark": {
                "refresh_job": {
                  "limit": "200"
                }
              }
            }
          }
        }
      }
    }


plugins.query.datasources.limit
===============================

Description
-----------

Each cluster can have maximum 20 datasources. You can increase limit by this setting.

1. The default value is 20.
2. This setting is node scope.
3. This setting can be updated dynamically.

You can update the setting with a new value like this.

SQL query::

    sh$ curl -sS -H 'Content-Type: application/json' -X PUT localhost:9200/_cluster/settings \
    ... -d '{"transient":{"plugins.query.datasources.limit":25}}'
    {
      "acknowledged": true,
      "persistent": {},
      "transient": {
        "plugins": {
          "query": {
            "datasources": {
              "limit": "25"
            }
          }
        }
      }
    }


plugins.query.executionengine.spark.session_inactivity_timeout_millis
=====================================================================

Description
-----------

This setting determines the duration after which a session is considered stale if there has been no update. The default
timeout is 3 minutes (180,000 milliseconds).

1. Default Value: 180000 (milliseconds)
2. Scope: Node-level
3. Dynamic Update: Yes, this setting can be updated dynamically.

To change the session inactivity timeout to 10 minutes for example, use the following command:

SQL query::

    sh$ curl -sS -H 'Content-Type: application/json' -X PUT localhost:9200/_cluster/settings \
    ... -d '{"transient":{"plugins.query.executionengine.spark.session_inactivity_timeout_millis":600000}}'
    {
        "acknowledged": true,
        "persistent": {},
        "transient": {
            "plugins": {
                "query": {
                    "executionengine": {
                        "spark": {
                            "session_inactivity_timeout_millis": "600000"
                        }
                    }
                }
            }
        }
    }


plugins.query.executionengine.spark.auto_index_management.enabled
=================================================================

Description
-----------
This setting controls the automatic management of request and result indices for each data source. When enabled, it
deletes outdated index documents.

* Default State: Enabled (true)
* Purpose: Manages auto index management for request and result indices.

To disable auto index management, use the following command:

SQL query::

    sh$ curl -sS -H 'Content-Type: application/json' -X PUT localhost:9200/_cluster/settings \
    ... -d '{"transient":{"plugins.query.executionengine.spark.auto_index_management.enabled":false}}'
    {
        "acknowledged": true,
        "persistent": {},
        "transient": {
            "plugins": {
                "query": {
                    "executionengine": {
                        "spark": {
                            "auto_index_management": {
                                "enabled": "false"
                            }
                        }
                    }
                }
            }
        }
    }


plugins.query.executionengine.spark.session.index.ttl
=====================================================

Description
-----------
This setting defines the time-to-live (TTL) for request indices when plugins.query.executionengine.spark.auto_index_management.enabled
is true. By default, request indices older than 14 days are deleted.

* Default Value: 30 days

To change the TTL to 60 days for example, execute the following command:

SQL query::

    sh$ curl -sS -H 'Content-Type: application/json' -X PUT localhost:9200/_cluster/settings \
    ... -d '{"transient":{"plugins.query.executionengine.spark.session.index.ttl":"60d"}}'
    {
        "acknowledged": true,
        "persistent": {},
        "transient": {
            "plugins": {
                "query": {
                    "executionengine": {
                        "spark": {
                            "session": {
                                "index": {
                                    "ttl": "60d"
                                }
                            }
                        }
                    }
                }
            }
        }
    }


plugins.query.executionengine.spark.result.index.ttl
====================================================

Description
-----------
This setting specifies the TTL for result indices when plugins.query.executionengine.spark.auto_index_management.enabled
is set to true. The default setting is to delete result indices older than 60 days.

* Default Value: 60 days

To modify the TTL to 30 days for example, use this command:

SQL query::

    sh$ curl -sS -H 'Content-Type: application/json' -X PUT localhost:9200/_cluster/settings \
    ... -d '{"transient":{"plugins.query.executionengine.spark.result.index.ttl":"30d"}}'
    {
        "acknowledged": true,
        "persistent": {},
        "transient": {
            "plugins": {
                "query": {
                    "executionengine": {
                        "spark": {
                            "result": {
                                "index": {
                                    "ttl": "30d"
                                }
                            }
                        }
                    }
                }
            }
        }
    }

plugins.query.executionengine.async_query.enabled
=================================================

Description
-----------
You can disable submit async query to reject all coming requests.

1. The default value is true.
2. This setting is node scope.
3. This setting can be updated dynamically.

Request::

    sh$ curl -sS -H 'Content-Type: application/json' -X PUT localhost:9200/_cluster/settings \
    ... -d '{"transient":{"plugins.query.executionengine.async_query.enabled":"false"}}'
    {
        "acknowledged": true,
        "persistent": {},
        "transient": {
            "plugins": {
                "query": {
                    "executionengine": {
                        "async_query": {
                            "enabled": "false"
                        }
                    }
                }
            }
        }
    }

plugins.query.executionengine.async_query.external_scheduler.enabled
=====================================================================

Description
-----------
This setting controls whether the external scheduler is enabled for async queries.

* Default Value: true
* Scope: Node-level
* Dynamic Update: Yes, this setting can be updated dynamically. 

To disable the external scheduler, use the following command:

Request ::

    sh$ curl -sS -H 'Content-Type: application/json' -X PUT localhost:9200/_cluster/settings \
    ... -d '{"transient":{"plugins.query.executionengine.async_query.external_scheduler.enabled":"false"}}'
    {
        "acknowledged": true,
        "persistent": {},
        "transient": {
            "plugins": {
                "query": {
                    "executionengine": {
                        "async_query": {
                            "external_scheduler": {
                                "enabled": "false"
                            }
                        }
                    }
                }
            }
        }
    }

plugins.query.executionengine.async_query.external_scheduler.interval
=====================================================================

Description
-----------
This setting defines the interval at which the external scheduler applies for auto refresh queries. It optimizes Spark applications by allowing them to automatically decide whether to use the Spark scheduler or the external scheduler.

* Default Value: None (must be explicitly set)
* Format: A string representing a time duration follows Spark `CalendarInterval <https://spark.apache.org/docs/latest/api/java/org/apache/spark/unsafe/types/CalendarInterval.html>`__ format (e.g., ``10 minutes`` for 10 minutes, ``1 hour`` for 1 hour).

To modify the interval to 10 minutes for example, use this command:

Request ::

    sh$ curl -sS -H 'Content-Type: application/json' -X PUT localhost:9200/_cluster/settings \
    ... -d '{"transient":{"plugins.query.executionengine.async_query.external_scheduler.interval":"10 minutes"}}'
    {
        "acknowledged": true,
        "persistent": {},
        "transient": {
            "plugins": {
                "query": {
                    "executionengine": {
                        "async_query": {
                            "external_scheduler": {
                                "interval": "10 minutes"
                            }
                        }
                    }
                }
            }
        }
    }

plugins.query.executionengine.spark.streamingjobs.housekeeper.interval
======================================================================

Description
-----------
This setting specifies the interval at which the streaming job housekeeper runs to clean up streaming jobs associated with deleted and disabled data sources.
The default configuration executes this cleanup every 15 minutes.

* Default Value: 15 minutes

To modify the TTL to 30 minutes for example, use this command:

Request ::

    sh$ curl -sS -H 'Content-Type: application/json' -X PUT localhost:9200/_cluster/settings \
    ... -d '{"transient":{"plugins.query.executionengine.spark.streamingjobs.housekeeper.interval":"30m"}}'
    {
    "acknowledged": true,
    "persistent": {},
    "transient": {
        "plugins": {
            "query": {
                "executionengine": {
                    "spark": {
                        "streamingjobs": {
                            "housekeeper": {
                                "interval": "30m"
                            }
                        }
                    }
                }
            }
        }
      }
    }

plugins.query.datasources.enabled
=================================

Description
-----------

This setting controls whether datasources are enabled.

1. The default value is true
2. This setting is node scope
3. This setting can be updated dynamically

Update Settings Request::

    sh$ curl -sS -H 'Content-Type: application/json' -X PUT 'localhost:9200/_cluster/settings?pretty' \
    ... -d '{"transient":{"plugins.query.datasources.enabled":"false"}}'
    {
      "acknowledged": true,
      "persistent": {},
      "transient": {
        "plugins": {
          "query": {
            "datasources": {
              "enabled": "false"
            }
          }
        }
      }
    }

When Attempting to Call Data Source APIs::

    sh$ curl -sS -H 'Content-Type: application/json' -X GET 'localhost:9200/_plugins/_query/_datasources'
    {
      "status": 400,
      "error": {
        "type": "OpenSearchStatusException",
        "reason": "Invalid Request",
        "details": "plugins.query.datasources.enabled setting is false"
      }
    }

When Attempting to List Data Source::

    sh$ curl -sS -H 'Content-Type: application/json' -X POST 'localhost:9200/_plugins/_ppl' \
    ... -d '{"query":"show datasources"}'
    {
      "schema": [
        {
          "name": "DATASOURCE_NAME",
          "type": "string"
        },
        {
          "name": "CONNECTOR_TYPE",
          "type": "string"
        }
      ],
      "datarows": [],
      "total": 0,
      "size": 0
    }

To Re-enable Data Sources:::

    sh$ curl -sS -H 'Content-Type: application/json' -X PUT 'localhost:9200/_cluster/settings?pretty' \
    ... -d '{"transient":{"plugins.query.datasources.enabled":"true"}}'
    {
      "acknowledged": true,
      "persistent": {},
      "transient": {
        "plugins": {
          "query": {
            "datasources": {
              "enabled": "true"
            }
          }
        }
      }
    }

