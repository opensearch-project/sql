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

plugins.sql.enabled
======================

Version
-------
1.0

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

Version
-------
1.0

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

plugins.sql.cursor.keep_alive
================================

Version
-------
1.0

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

plugins.query.size_limit
========================

Version
-------
1.0

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

plugins.query.pruning.enabled (Experimental)
============================================

Version
-------
3.9

Description
-----------

Prunes a wildcard index expression down to the concrete indices that can hold data in the query's time range, so fewer indices and shards are touched. The primary use currently is to avoid exhausting the open point-in-time (PIT) context limit when a query would otherwise open a reader context over many indices. Enabled by default.

There are two ways the range can reach the pruner:

1. From the filter pushed down to the search, which must be a range on ``@timestamp``. This runs when the search request is built, after the index expression has been resolved, so it reduces the shards read and the PIT but not the mapping merge.
2. From the ``start_time``/``end_time`` request parameters (see below), which run while the table is still being resolved, before the expression is expanded. This reduces the mapping merge as well, so a field the out-of-range indices map differently no longer contributes a conflict. Specific to the Calcite engine; the parameters are accepted and ignored otherwise.

Anything else is left untouched, and any failure while probing the cluster falls back to querying the full expression. Weigh these limitations before turning it off:

1. An index whose shards are all unavailable is pruned rather than reported, because ``_field_caps`` does not surface per-index failures. Such a query returns fewer rows instead of an error.
2. Pruning fixes the list of index names, so an index created or deleted between pruning and PIT creation, by a rollover or retention policy for instance, is missed or fails the query. The interval between the two is short, so this is unlikely in practice.
3. An expression that matches an alias or a data stream is never pruned, because a filtered alias contributes a filter and routing that are resolved from the expression itself and so would be silently dropped.
4. Pruning probes the cluster with the ``indices:admin/resolve/index`` and ``indices:data/read/field_caps*`` actions, both granted by the ``ppl_full_access`` role of the security plugin since 3.9. A principal lacking either permission falls back to querying the full expression silently, so pruning simply never takes effect.

Pruning is also skipped when it would not reduce the read, that is when no index is excluded. The query then uses the original wildcard expression and reads exactly the same indices.

**Request-level time bounds.** ``start_time`` and ``end_time`` declare the window the request is asking about, so the engine has it before it resolves the queried index expression. They are inclusive, and accept OpenSearch date math (``now-7d``) and absolute timestamps alike. The bounds are handed to the probe as sent rather than reinterpreted, so a relative range is resolved once, by OpenSearch. Absolute bounds are parsed as ``strict_date_optional_time``, epoch milliseconds, or ``yyyy-MM-dd HH:mm:ss.SSS``; a field declaring some other custom format is not pruned on, since the bound fails to parse and pruning then declines. ``time_field`` names the field they constrain, defaulting to ``@timestamp`` -- a caller whose index pattern is configured on another field has to say so, or nothing is pruned. Bounds that cannot be used are ignored rather than failing the query.

Their scope is the whole request: every source the query reads is narrowed, subsearches included, as with Splunk's time range picker and OpenSearch SQL's own PPL ``earliest``/``latest`` at request level.

They are not a filter, and they are not free of effect either. Pruning drops whole indices, so a query whose text already constrains the same field to the same window returns exactly the same rows -- that is the intended use, and how a client appending its own ``where`` should send them. A query whose text does not carry that constraint returns fewer rows: documents outside the window still count inside a retained index, while an index wholly outside it contributes nothing. Send bounds only for a window the query itself already restricts.

An index that does not map ``time_field`` holds nothing in any window, so it is pruned like one whose values fall outside the range. That is consistent with the pushed-down-filter path, and with the request-level range these parameters describe, but it is part of why they belong only on a query whose text already constrains the same field: such an index's rows go with it.

Planning-time pruning is specific to the Calcite engine's own query path. The unified query path does not read these parameters; they are accepted and ignored there.

Request body::

    {
      "query" : "source=logs-* | stats count() by span(@timestamp, 1h)",
      "time_field" : "@timestamp",
      "start_time" : "now-7d",
      "end_time" : "now"
    }

Disable it with::

	>> curl -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_query/settings -d '{
	  "transient" : {
	    "plugins.query.pruning.enabled" : false
	  }
	}'

Result set::

    {
      "acknowledged" : true,
      "persistent" : { },
      "transient" : {
        "plugins" : {
          "query" : {
            "pruning" : {
              "enabled" : "false"
            }
          }
        }
      }
    }

Settings:

1. The default value is true.
2. This setting is node scope.
3. This setting can be updated dynamically.

plugins.query.max_expression_depth
==================================

Version
-------
3.8

Description
-----------

This setting bounds the maximum nesting depth of an expression while a query is parsed into its abstract syntax tree, keeping parsing bounded for very large or deeply nested expressions. A query exceeding the limit is rejected with a 400 error. The default value is 1000. Set it to 0 to disable the limit (unlimited). Here is an example::

	>> curl -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_query/settings -d '{
	  "transient" : {
	    "plugins.query.max_expression_depth" : 500
	  }
	}'

Result set::

    {
      "acknowledged" : true,
      "persistent" : { },
      "transient" : {
        "plugins" : {
          "query" : {
            "max_expression_depth" : "500"
          }
        }
      }
    }

plugins.query.partial_result.on_mapping_conflict.enabled [Experimental]
=======================================================================

Version
-------
Since 3.9

Description
-----------

This setting is experimental; its name, values, and default may change in a future release. Controls how an aggregation behaves when its group-by field is mapped inconsistently across the queried indices -- for example ``keyword`` in some indices of a wildcard pattern and ``text`` (without a ``.keyword`` sub-field) in others. Such a field collapses to ``text``-without-``.keyword`` across the pattern, which has no doc values, so the aggregation cannot be pushed down natively and instead runs as a per-document script over ``_source`` -- correct, but a full scan of every document.

When this setting is ``false`` (the default), that complete-but-slow result is returned. When set to ``true``, the aggregation is pushed down over only the subset of indices where the field is aggregatable, and the response carries a ``PARTIAL_RESULT`` warning naming the excluded indices and the remedy (map the field as ``keyword`` everywhere). The result is therefore **partial** -- documents in the excluded indices are not counted -- so the setting is off by default and only takes effect for response formats that can surface the warning (the JSON format; CSV/raw/visualization responses fall through to the complete result rather than silently dropping data).

The behavior can also be overridden per request with the ``partial_result`` boolean field in the query body, which takes precedence over this cluster setting. Here is an example enabling it at the cluster level::

	>> curl -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_query/settings -d '{
	  "transient" : {
	    "plugins.query.partial_result.on_mapping_conflict.enabled" : true
	  }
	}'

Result set::

    {
      "acknowledged" : true,
      "persistent" : { },
      "transient" : {
        "plugins" : {
          "query" : {
            "partial_result" : {
              "on_mapping_conflict" : {
                "enabled" : "true"
              }
            }
          }
        }
      }
    }

Per-request override example, opting a single query into a partial result regardless of the cluster setting::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_ppl -d '{
	  "query" : "source=logs-* | stats count() by service",
	  "partial_result" : true
	}'

plugins.query.buckets
=====================

Version
-------
3.4

Description
-----------

This configuration indicates how many aggregation buckets will return in a single response. The default value equals to ``plugins.query.size_limit``.
You can change the value to any value not greater than the maximum number of aggregation buckets allowed in a single response (`search.max_buckets`), here is an example::

	>> curl -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_query/settings -d '{
	  "transient" : {
	    "plugins.query.buckets" : 1000
	  }
	}'

Result set::

    {
      "acknowledged" : true,
      "persistent" : { },
      "transient" : {
        "plugins" : {
          "query" : {
            "buckets" : "1000"
          }
        }
      }
    }

Limitations
-----------
The number of aggregation buckets is fixed to ``1000`` in v2. ``plugins.query.buckets`` can only effect the number of aggregation buckets when calcite enabled.

plugins.query.memory_limit
==========================

Version
-------
1.0

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

Thread Pool Settings
====================

Version
-------
3.4

The SQL plugin is integrated with the `OpenSearch Thread Pool Settings <https://docs.opensearch.org/latest/install-and-configure/configuring-opensearch/thread-pool-settings/>`_.
There are two thread pools which can be configured on cluster setup via `settings.yml`::

    thread_pool:
      sql-worker:
        size: 30
        queue_size: 100
      sql_background_io:
        size: 30
        queue_size: 1000

The ``sql-worker`` pool corresponds to compute resources related to running queries, such as compute-heavy evaluations on result sets.
This directly maps to the number of queries that can be run concurrently.
This is the primary pool you interact with externally.
``sql_background_io`` is a low-footprint pool for IO requests the plugin makes,
and can be used to limit indirect load that SQL places on your cluster for Calcite-enabled operations.
A ``sql-worker`` thread may spawn multiple background threads.

plugins.query.executionengine.spark.session.limit
==================================================

Version
-------
2.12

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

Version
-------
2.12

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

Version
-------
2.12

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

Version
-------
2.12

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

Version
-------
2.12

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

Version
-------
2.12

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

Version
-------
2.12

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

Version
-------
2.12

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

Version
-------
2.17

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

Version
-------
2.17

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

Version
-------
2.13

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

Version
-------
2.16

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

plugins.query.field_type_tolerance
==================================

Version
-------
2.19

Description
-----------

This setting controls whether preserve arrays. If this setting is set to false, then an array is reduced
to the first non array value of any level of nesting.

1. The default value is true (preserve arrays)
2. This setting is node scope
3. This setting can be updated dynamically

Querying a field containing array values will return the full array values::

    os> SELECT accounts FROM people;
    fetched rows / total rows = 1/1
    +-----------------------+
    | accounts              |
    +-----------------------+
    | [{'id': 1},{'id': 2}] |
    +-----------------------+

Disable field type tolerance::

    >> curl -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_query/settings -d '{
	    "transient" : {
	      "plugins.query.field_type_tolerance" : false
	    }
	  }'

When field type tolerance is disabled, arrays are collapsed to the first non array value::

    os> SELECT accounts FROM people;
    fetched rows / total rows = 1/1
    +-----------+
    | accounts  |
    +-----------+
    | {'id': 1} |
    +-----------+

Reenable field type tolerance::

    >> curl -H 'Content-Type: application/json' -X PUT localhost:9200/_plugins/_query/settings -d '{
	    "transient" : {
	      "plugins.query.field_type_tolerance" : true
	    }
	  }'

Limitations:
------------
OpenSearch does not natively support the ARRAY data type but does allow multi-value fields implicitly. The
SQL/PPL plugin adheres strictly to the data type semantics defined in index mappings. When parsing OpenSearch
responses, it expects data to match the declared type and does not account for data in array format. If the
plugins.query.field_type_tolerance setting is enabled, the SQL/PPL plugin will handle array datasets by returning
scalar data types, allowing basic queries (e.g., SELECT * FROM tbl WHERE condition). However, using multi-value
fields in expressions or functions will result in exceptions. If this setting is disabled or absent, only the
first element of an array is returned, preserving the default behavior.

plugins.calcite.enabled
=======================

Version
-------
3.0

Description
-----------

You can enable Calcite as new query optimizer and execution engine to all coming requests.

1. The default value is false in 3.0, 3.1 and 3.2.
2. The default value is true since 3.3.0.
3. This setting is node scope.
4. This setting can be updated dynamically.

Check `introduce v3 engine <../../../dev/intro-v3-engine.md>`_ for more details.
Check `join doc <../../ppl/cmd/join.rst>`_ for example.

plugins.calcite.fallback.allowed
================================

Version
-------
3.1

Description
-----------

If Calcite is enabled, you can use this setting to decide whether to allow fallback to v2 engine for some queries which are not supported by v3 engine.

1. The default value is false since 3.2.0.
2. This setting is node scope.
3. This setting can be updated dynamically.

plugins.calcite.pushdown.enabled
================================

Version
-------
3.1

Description
-----------

If Calcite is enabled, you can use this setting to decide whether to enable the operator pushdown optimization for v3 engine.

1. The default value is true since 3.0.0.
2. This setting is node scope.
3. This setting can be updated dynamically.

plugins.calcite.pushdown.rowcount.estimation.factor
===================================================

Version
-------
3.1

Description
-----------

If Calcite pushdown optimization is enabled, this setting is used to estimate the row count of the query plan. The value is a factor to multiply the row count of the table scan to get the estimated row count.

1. The default value is 0.9 since 3.1.0.
2. This setting is node scope.
3. This setting can be updated dynamically.

plugins.calcite.all_join_types.allowed
======================================

Version
-------
3.3

Description
-----------

Join types ``inner``, ``left``, ``outer`` (alias of ``left``), ``semi`` and ``anti`` are supported by default. ``right``, ``full``, ``cross`` are performance sensitive join types which are disabled by default. Set config ``plugins.calcite.all_join_types.allowed = true`` to enable.

1. The default value is false since 3.3.0.
2. This setting is node scope.
3. This setting can be updated dynamically.
