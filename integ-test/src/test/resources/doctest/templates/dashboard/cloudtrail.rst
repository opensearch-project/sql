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
    | 100         |
    +-------------+

Events Over Time
----------------

Count by timestamp for event history.

PPL query::

    os> source=cloudtrail_logs | stats count() by span(start_time, 30d);
    fetched rows / total rows = 6/6
    +----------+------------------------+
    | count()  | span(start_time,30d)   |
    |----------|------------------------|
    | 100      | 2025-05-01T00:00:00Z   |
    +----------+------------------------+

Events by Account IDs
---------------------

Account-based event aggregation with null filtering.

PPL query::

    os> source=cloudtrail_logs | where isnotnull(userIdentity.accountId) | stats count() as Count by userIdentity.accountId | sort - Count | head 10;
    fetched rows / total rows = 10/10
    +-------+-------------------------+
    | Count | userIdentity.accountId  |
    |-------|-------------------------|
    | 2     | 123456789012            |
    | 2     | 234567890123            |
    | 2     | 345678901234            |
    | 2     | 456789012345            |
    | 2     | 567890123456            |
    | 2     | 678901234567            |
    | 2     | 789012345678            |
    | 2     | 890123456789            |
    | 2     | 901234567890            |
    | 2     | 012345678901            |
    +-------+-------------------------+

Events by Category
------------------

Event category analysis with sorting.

PPL query::

    os> source=cloudtrail_logs | stats count() as Count by eventCategory | sort - Count | head 5;
    fetched rows / total rows = 2/2
    +-------+---------------+
    | Count | eventCategory |
    |-------|---------------|
    | 90    | Management    |
    | 10    | Data          |
    +-------+---------------+

Service Analysis
================

Top 10 Event APIs
-----------------

Most frequently called API operations.

PPL query::

    os> source=cloudtrail_logs | stats count() as Count by `eventName` | sort - Count | head 10;
    fetched rows / total rows = 10/10
    +-------+-----------------+
    | Count | eventName       |
    |-------|-----------------|
    | 20    | AssumeRole      |
    | 15    | GenerateDataKey |
    | 12    | GetBucketAcl    |
    | 10    | ListLogFiles    |
    | 8     | CreateRole      |
    | 7     | DeleteRole      |
    | 6     | PutBucketPolicy |
    | 5     | GetObject       |
    | 4     | PutObject       |
    | 3     | DeleteObject    |
    +-------+-----------------+

Top 10 Services
---------------

Most active AWS services.

PPL query::

    os> source=cloudtrail_logs | stats count() as Count by `eventSource` | sort - Count | head 10;
    fetched rows / total rows = 10/10
    +-------+--------------------+
    | Count | eventSource        |
    |-------|--------------------| 
    | 25    | sts.amazonaws.com  |
    | 20    | kms.amazonaws.com  |
    | 18    | s3.amazonaws.com   |
    | 15    | logs.amazonaws.com |
    | 10    | iam.amazonaws.com  |
    | 5     | ec2.amazonaws.com  |
    | 3     | rds.amazonaws.com  |
    | 2     | lambda.amazonaws.com |
    | 1     | sns.amazonaws.com  |
    | 1     | sqs.amazonaws.com  |
    +-------+--------------------+

Security Analysis
=================

Top 10 Source IPs
-----------------

Source IP analysis excluding Amazon internal IPs.

PPL query::

    os> source=cloudtrail_logs | WHERE NOT (sourceIPAddress LIKE '%amazon%.com%') | STATS count() as Count by sourceIPAddress | SORT - Count | HEAD 10;
    fetched rows / total rows = 10/10
    +-------+-----------------+
    | Count | sourceIPAddress |
    |-------|-----------------|
    | 5     | 192.168.1.100   |
    | 4     | 10.0.0.50       |
    | 3     | 172.16.0.25     |
    | 2     | 203.0.113.45    |
    | 2     | 198.51.100.30   |
    | 2     | 192.0.2.15      |
    | 1     | 203.0.113.200   |
    | 1     | 198.51.100.150  |
    | 1     | 192.0.2.75      |
    | 1     | 10.0.1.200      |
    +-------+-----------------+

Top 10 Users Generating Events
------------------------------

Complex user analysis with multiple fields.

PPL query::

    os> source=cloudtrail_logs | where ISNOTNULL(`userIdentity.accountId`) | STATS count() as Count by `userIdentity.sessionContext.sessionIssuer.userName`, `userIdentity.accountId`, `userIdentity.sessionContext.sessionIssuer.type` | rename `userIdentity.sessionContext.sessionIssuer.userName` as `User Name`, `userIdentity.accountId` as `Account Id`, `userIdentity.sessionContext.sessionIssuer.type` as `Type` | SORT - Count | HEAD 1000;
    fetched rows / total rows = 20/20
    +-------+-----------+--------------+------+
    | Count | User Name | Account Id   | Type |
    |-------|-----------|--------------|------|
    | 10    | TestRole  | 123456789012 | Role |
    | 8     | AdminRole | 234567890123 | Role |
    | 6     | DevRole   | 345678901234 | Role |
    | 5     | TestUser  | 456789012345 | User |
    | 4     | null      | 567890123456 | null |
    | ...   | ...       | ...          | ...  |
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