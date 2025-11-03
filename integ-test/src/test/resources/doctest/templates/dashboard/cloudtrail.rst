============================
CloudTrail Dashboard Queries
============================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Description
===========

CloudTrail PPL queries analyze AWS API activity, user behavior, and security events. These queries demonstrate common dashboard patterns for CloudTrail log analysis.

Event Analysis
==============

Total Events Count
------------------

Basic count aggregation for total events.

PPL query::

    os> source=cloudtrail_logs | stats count() as `Event Count`;
    fetched rows / total rows = 1/1
    +-------------+
    | Event Count |
    |-------------|
    | 4           |
    +-------------+

Events Over Time
----------------

Count by timestamp for event history.

PPL query::

    os> source=cloudtrail_logs | stats count() by span(eventTime, 30d);
    fetched rows / total rows = 1/1
    +----------+------------------------+
    | count()  | span(eventTime,30d)    |
    |----------|------------------------|
    | 4        | 2023-12-19 00:00:00    |
    +----------+------------------------+

Events by Account IDs
---------------------

Account-based event aggregation with null filtering.

PPL query::

    os> source=cloudtrail_logs | where isnotnull(userIdentity.accountId) | stats count() as Count by userIdentity.accountId | sort - Count | head 10;
    fetched rows / total rows = 1/1
    +-------+-------------------------+
    | Count | userIdentity.accountId  |
    |-------|-------------------------|
    | 4     | 123456789012            |
    +-------+-------------------------+

Events by Category
------------------

Event category analysis with sorting.

PPL query::

    os> source=cloudtrail_logs | stats count() as Count by eventCategory | sort - Count | head 5;
    fetched rows / total rows = 1/1
    +-------+---------------+
    | Count | eventCategory |
    |-------|---------------|
    | 4     | Management    |
    +-------+---------------+

Service Analysis
================

Top 10 Event APIs
-----------------

Most frequently called API operations.

PPL query::

    os> source=cloudtrail_logs | stats count() as Count by `eventName` | sort - Count | head 10;
    fetched rows / total rows = 4/4
    +-------+-----------------+
    | Count | eventName       |
    |-------|-----------------|
    | 1     | AssumeRole      |
    | 1     | GenerateDataKey |
    | 1     | GetBucketAcl    |
    | 1     | ListLogFiles    |
    +-------+-----------------+

Top 10 Services
---------------

Most active AWS services.

PPL query::

    os> source=cloudtrail_logs | stats count() as Count by `eventSource` | sort - Count | head 10;
    fetched rows / total rows = 4/4
    +-------+--------------------+
    | Count | eventSource        |
    |-------|--------------------| 
    | 1     | sts.amazonaws.com  |
    | 1     | kms.amazonaws.com  |
    | 1     | s3.amazonaws.com   |
    | 1     | logs.amazonaws.com |
    +-------+--------------------+

Security Analysis
=================

Top 10 Source IPs
-----------------

Source IP analysis excluding Amazon internal IPs.

PPL query::

    os> source=cloudtrail_logs | WHERE NOT (sourceIPAddress LIKE '%amazon%.com%') | STATS count() as Count by sourceIPAddress | SORT - Count | HEAD 10;
    fetched rows / total rows = 0/0
    +-------+-----------------+
    | Count | sourceIPAddress |
    |-------|-----------------|
    +-------+-----------------+

Top 10 Users Generating Events
------------------------------

Complex user analysis with multiple fields.

PPL query::

    os> source=cloudtrail_logs | where ISNOTNULL(`userIdentity.accountId`) | STATS count() as Count by `userIdentity.sessionContext.sessionIssuer.userName`, `userIdentity.accountId`, `userIdentity.sessionContext.sessionIssuer.type` | rename `userIdentity.sessionContext.sessionIssuer.userName` as `User Name`, `userIdentity.accountId` as `Account Id`, `userIdentity.sessionContext.sessionIssuer.type` as `Type` | SORT - Count | HEAD 1000;
    fetched rows / total rows = 2/2
    +-------+-----------+--------------+------+
    | Count | User Name | Account Id   | Type |
    |-------|-----------|--------------|------|
    | 1     | TestRole  | 123456789012 | Role |
    | 3     | null      | 123456789012 | null |
    +-------+-----------+--------------+------+

S3 Analysis
===========

S3 Buckets
----------

S3 bucket analysis.

PPL query::

    os> source=cloudtrail_logs | where `eventSource` like 's3%' and isnotnull(`requestParameters.bucketName`) | stats count() as Count by `requestParameters.bucketName` | sort - Count | head 10;
    fetched rows / total rows = 1/1
    +-------+------------------------------------+
    | Count | requestParameters.bucketName       |
    |-------|------------------------------------| 
    | 1     | test-cloudtrail-logs-123456789012  |
    +-------+------------------------------------+