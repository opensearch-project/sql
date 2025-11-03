=========================
WAF Dashboard PPL Queries
=========================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Description
===========

WAF PPL queries analyze web traffic patterns, security events, and rule effectiveness. These queries demonstrate common dashboard patterns for AWS WAF log analysis.

Request Analysis
================

Total Requests
--------------

Basic count aggregation for all WAF requests.

PPL query::

    os> source=waf_logs | stats count();
    fetched rows / total rows = 1/1
    +----------+
    | count()  |
    |----------|
    | 3        |
    +----------+

Request History
---------------

Request patterns over time by action (ALLOW/BLOCK).

PPL query::

    os> source=waf_logs | STATS count() as Count by span(timestamp, 30d), action | SORT - Count;
    fetched rows / total rows = 2/2
    +-------+---------------------+--------+
    | Count | span(timestamp,30d) | action |
    |-------|---------------------|--------|
    | 2     | 1731456000000       | BLOCK  |
    | 1     | 1731456000000       | ALLOW  |
    +-------+---------------------+--------+

WebACL Analysis
===============

Requests to WebACLs
-------------------

Request distribution across different WebACLs.

PPL query::

    os> source=waf_logs | stats count() as Count by `webaclId` | sort - Count | head 3;
    fetched rows / total rows = 3/3
    +-------+-------------------------------------------------------------------------------------------+
    | Count | webaclId                                                                                  |
    |-------|-------------------------------------------------------------------------------------------|
    | 1     | arn:aws:wafv2:us-west-2:111111111111:regional/webacl/TestWAF-pdx/12345678-1234-1234-... |
    | 1     | arn:aws:wafv2:us-west-2:222222222222:regional/webacl/TestWAF-pdx/12345678-1234-1234-... |
    | 1     | arn:aws:wafv2:us-west-2:333333333333:regional/webacl/TestWAF-pdx/12345678-1234-1234-... |
    +-------+-------------------------------------------------------------------------------------------+

Sources Analysis
----------------

Analysis of HTTP source identifiers.

PPL query::

    os> source=waf_logs | stats count() as Count by `httpSourceId` | sort - Count | head 5;
    fetched rows / total rows = 3/3
    +-------+---------------------------+
    | Count | httpSourceId              |
    |-------|---------------------------|
    | 1     | 111111111111:yhltew7mtf:dev |
    | 1     | 222222222222:yhltew7mtf:dev |
    | 1     | 333333333333:yhltew7mtf:dev |
    +-------+---------------------------+

Geographic Analysis
===================

Top Client IPs
---------------

Most active client IP addresses.

PPL query::

    os> source=waf_logs | stats count() as Count by `httpRequest.clientIp` | sort - Count | head 10;
    fetched rows / total rows = 3/3
    +-------+----------------------+
    | Count | httpRequest.clientIp |
    |-------|----------------------|
    | 1     | 149.165.180.212      |
    | 1     | 121.236.106.18       |
    | 1     | 108.166.91.31        |
    +-------+----------------------+

Top Countries
-------------

Request distribution by country of origin.

PPL query::

    os> source=waf_logs | stats count() as Count by `httpRequest.country` | sort - Count;
    fetched rows / total rows = 3/3
    +-------+---------------------+
    | Count | httpRequest.country |
    |-------|---------------------|
    | 1     | GY                  |
    | 1     | MX                  |
    | 1     | PN                  |
    +-------+---------------------+

Rule Analysis
=============

Top Terminating Rules
---------------------

Most frequently triggered WAF rules.

PPL query::

    os> source=waf_logs | stats count() as Count by `terminatingRuleId` | sort - Count | head 10;
    fetched rows / total rows = 2/2
    +-------+-------------------+
    | Count | terminatingRuleId |
    |-------|-------------------|
    | 2     | RULE_ID_3         |
    | 1     | RULE_ID_7         |
    +-------+-------------------+

Total Blocked Requests
----------------------

Count of requests blocked by WAF rules.

PPL query::

    os> source=waf_logs | WHERE action = "BLOCK" | STATS count();
    fetched rows / total rows = 1/1
    +----------+
    | count()  |
    |----------|
    | 2        |
    +----------+

URI Analysis
============

Top Request URIs
----------------

Most frequently requested URI paths.

PPL query::

    os> source=waf_logs | stats count() as Count by `httpRequest.uri` | sort - Count | head 10;
    fetched rows / total rows = 1/1
    +-------+------------------+
    | Count | httpRequest.uri  |
    |-------|------------------|
    | 3     | /example-path    |
    +-------+------------------+