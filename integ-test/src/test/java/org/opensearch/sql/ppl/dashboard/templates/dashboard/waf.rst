..
   Copyright OpenSearch Contributors
   SPDX-License-Identifier: Apache-2.0

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
    | 100      |
    +----------+

Request History
---------------

Request patterns over time by action (ALLOW/BLOCK).

PPL query::

    os> source=waf_logs | STATS count() as Count by span(start_time, 30d), action | SORT - Count;
    fetched rows / total rows = 15/15
    +-------+------------------------+--------+
    | Count | span(start_time,30d)   | action |
    |-------|------------------------|--------|
    | 82    | 2025-05-01T00:00:00Z   | ALLOW  |
    | 17    | 2025-05-01T00:00:00Z   | BLOCK  |
    | 1     | 2025-05-01T00:00:00Z   | COUNT  |
    +-------+------------------------+--------+

WebACL Analysis
===============

Requests to WebACLs
-------------------

Request distribution across different WebACLs.

PPL query::

    os> source=waf_logs | stats count() as Count by `webaclId` | sort - Count | head 10;
    fetched rows / total rows = 10/10
    +-------+-------------------------------------------------------------------------------------------+
    | Count | webaclId                                                                                  |
    |-------|-------------------------------------------------------------------------------------------|
    | 1     | arn:aws:wafv2:us-east-1:784781757088:regional/webacl/APIWAF-lkl/01b29038-23ae-14c5-... |
    | 1     | arn:aws:wafv2:eu-central-1:250922725343:regional/webacl/SecurityWAF-ngh/018f30a7-... |
    | 1     | arn:aws:wafv2:us-west-2:712448542372:regional/webacl/DevWAF-nni/04e6fdf3-14a2-1071-... |
    | ...   | ...                                                                                       |
    +-------+-------------------------------------------------------------------------------------------+

Sources Analysis
----------------

Analysis of HTTP source identifiers.

PPL query::

    os> source=waf_logs | stats count() as Count by `httpSourceId` | sort - Count | head 5;
    fetched rows / total rows = 5/5
    +-------+---------------------------+
    | Count | httpSourceId              |
    |-------|---------------------------|
    | 1     | 784781757088:zn99vte24b:staging |
    | 1     | 250922725343:06udlnzsuc:v2 |
    | 1     | 712448542372:h11d127c1c:prod |
    | 1     | 915064614783:oudun2xjou:v1 |
    | 1     | 782258924067:8xbvht9icb:dev |
    +-------+---------------------------+

Geographic Analysis
===================

Top Client IPs
---------------

Most active client IP addresses.

PPL query::

    os> source=waf_logs | stats count() as Count by `httpRequest.clientIp` | sort - Count | head 10;
    fetched rows / total rows = 10/10
    +-------+----------------------+
    | Count | httpRequest.clientIp |
    |-------|----------------------|
    | 1     | 185.114.91.138       |
    | 1     | 155.12.221.78        |
    | 1     | 121.173.165.128      |
    | 1     | 13.234.156.211       |
    | 1     | 142.126.11.6         |
    | ...   | ...                  |
    +-------+----------------------+

Top Countries
-------------

Request distribution by country of origin.

PPL query::

    os> source=waf_logs | stats count() as Count by `httpRequest.country` | sort - Count;
    fetched rows / total rows = 25/25
    +-------+---------------------+
    | Count | httpRequest.country |
    |-------|---------------------|
    | 33    | US                  |
    | 8     | GB                  |
    | 7     | DE                  |
    | 7     | BR                  |
    | 6     | CA                  |
    | 5     | RU                  |
    | 3     | JP                  |
    | 3     | IN                  |
    | 3     | CN                  |
    | 3     | BE                  |
    | 2     | SG                  |
    | 2     | SE                  |
    | 2     | MX                  |
    | 2     | IE                  |
    | 2     | ES                  |
    | 2     | CH                  |
    | 2     | AU                  |
    | 1     | ZA                  |
    | 1     | PT                  |
    | 1     | NL                  |
    | 1     | IT                  |
    | 1     | FR                  |
    | 1     | FI                  |
    | 1     | CL                  |
    | 1     | AT                  |
    +-------+---------------------+

Rule Analysis
=============

Top Terminating Rules
---------------------

Most frequently triggered WAF rules.

PPL query::

    os> source=waf_logs | stats count() as Count by `terminatingRuleId` | sort - Count | head 10;
    fetched rows / total rows = 10/10
    +-------+------------------------------------------+
    | Count | terminatingRuleId                        |
    |-------|------------------------------------------|
    | 13    | AWS-AWSManagedRulesAmazonIpReputationList |
    | 11    | XSSProtectionRule                        |
    | 11    | Default_Action                           |
    | 10    | AWS-AWSManagedRulesKnownBadInputsRuleSet |
    | 8     | CustomRateLimitRule                      |
    | 8     | AWS-AWSManagedRulesCommonRuleSet         |
    | 7     | CustomIPWhitelistRule                    |
    | 7     | AWS-AWSManagedRulesSQLiRuleSet           |
    | 7     | AWS-AWSManagedRulesLinuxRuleSet          |
    | 5     | CSRFProtectionRule                       |
    +-------+------------------------------------------+

Total Blocked Requests
----------------------

Count of requests blocked by WAF rules.

PPL query::

    os> source=waf_logs | WHERE action = "BLOCK" | STATS count();
    fetched rows / total rows = 1/1
    +----------+
    | count()  |
    |----------|
    | 21       |
    +----------+

URI Analysis
============

Top Request URIs
----------------

Most frequently requested URI paths.

PPL query::

    os> source=waf_logs | stats count() as Count by `httpRequest.uri` | sort - Count | head 10;
    fetched rows / total rows = 10/10
    +-------+------------------+
    | Count | httpRequest.uri  |
    |-------|------------------|
    | 5     | /api/v2/search   |
    | 5     | /account         |
    | 4     | /products        |
    | 4     | /css/style.css   |
    | 3     | /test            |
    | 3     | /download        |
    | 3     | /docs            |
    | 3     | /billing         |
    | 3     | /api/v2/users    |
    | 2     | /about           |
    +-------+------------------+