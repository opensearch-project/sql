.. highlight:: sh

=================
Plugin Monitoring
=================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1


Introduction
============

By a stats endpoint, you are able to collect metrics for the plugin within the interval. Note that only node level statistics collecting is implemented for now. In other words, you only get the metrics for the node you're accessing. Cluster level statistics have yet to be implemented.

Node Stats
==========

Description
-----------

The meaning of fields in the response is as follows:

+----------------------------+---------------------------------------------------------------+
|                  Field name|                                                    Description|
+============================+===============================================================+
|               request_total|                                         Total count of request|
+----------------------------+---------------------------------------------------------------+
|               request_count|                     Total count of request within the interval|
+----------------------------+---------------------------------------------------------------+
|default_cursor_request_total|                           Total count of simple cursor request|
+----------------------------+---------------------------------------------------------------+
|default_cursor_request_count|       Total count of simple cursor request within the interval|
+----------------------------+---------------------------------------------------------------+
| failed_request_count_syserr|Count of failed request due to system error within the interval|
+----------------------------+---------------------------------------------------------------+
| failed_request_count_cuserr| Count of failed request due to bad request within the interval|
+----------------------------+---------------------------------------------------------------+
|     failed_request_count_cb| Indicate if plugin is being circuit broken within the interval|
+----------------------------+---------------------------------------------------------------+
|    sql_query_time_in_millis|     Total time spent executing successful SQL queries since   |
|                            |     node start, in milliseconds. Explain and cursor close are |
|                            |     excluded                                                  |
+----------------------------+---------------------------------------------------------------+
|       sql_query_timed_count|  Total count of SQL queries included in                       |
|                            |  sql_query_time_in_millis. Average latency over an interval   |
|                            |  is the change in time divided by the change in this count.   |
|                            |  Both values are read together, so a response never includes  |
|                            |  a query's time without its count. Both reset to 0 on node    |
|                            |  restart                                                      |
+----------------------------+---------------------------------------------------------------+


Example
-------

SQL query::

	>> curl -H 'Content-Type: application/json' -X GET localhost:9200/_plugins/_sql/stats

Result set::

	{
	  "failed_request_count_cb" : 0,
	  "default_cursor_request_count" : 10,
	  "default_cursor_request_total" : 3,
	  "failed_request_count_cuserr" : 0,
	  "circuit_breaker" : 0,
	  "request_total" : 70,
	  "request_count" : 0,
	  "failed_request_count_syserr" : 0
	}

