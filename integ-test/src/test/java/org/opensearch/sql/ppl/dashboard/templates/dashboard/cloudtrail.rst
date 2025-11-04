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

.. note::
   Some queries may return results in different orders when multiple records have equal values. This is expected database behavior and does not affect the correctness of the results. For queries with non-deterministic ordering, sample outputs may not be shown, but the query structure and schema remain valid.

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
    fetched rows / total rows = 1/1
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
    | 1     | 598715677952            |
    | 1     | 287645373404            |
    | 1     | 210622981215            |
    | 1     | 343123305904            |
    | 1     | 774538043323            |
    | 1     | 190225759807            |
    | 1     | 999658550876            |
    | 1     | 837288668719            |
    | 1     | 689811372963            |
    | 1     | 585894403030            |
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
    | 100   | Management    |
    +-------+---------------+

Service Analysis
================

Top 10 Event APIs
-----------------

Most frequently called API operations.

PPL query::

    os> source=cloudtrail_logs | stats count() as Count by `eventName` | sort - Count | head 10;
    fetched rows / total rows = 10/10
    +-------+---------------------------+
    | Count | eventName                 |
    |-------|---------------------------|
    | 8     | InvokeFunction            |
    | 6     | GetItem                   |
    | 5     | DescribeImages            |
    | 4     | GetCallerIdentity         |
    | 4     | DescribeSecurityGroups    |
    | 4     | CreateSecurityGroup       |
    | 3     | PutItem                   |
    | 3     | ModifyDBInstance          |
    | 3     | GetObject                 |
    | 3     | DeleteDBInstance          |
    +-------+---------------------------+

Top 10 Services
---------------

Most active AWS services.

PPL query::

    os> source=cloudtrail_logs | stats count() as Count by `eventSource` | sort - Count | head 10;
    fetched rows / total rows = 10/10
    +-------+------------------------------+
    | Count | eventSource                  |
    |-------|------------------------------| 
    | 15    | ec2.amazonaws.com            |
    | 14    | s3.amazonaws.com             |
    | 13    | rds.amazonaws.com            |
    | 10    | dynamodb.amazonaws.com       |
    | 9     | cloudwatch.amazonaws.com     |
    | 8     | sts.amazonaws.com            |
    | 8     | lambda.amazonaws.com         |
    | 8     | iam.amazonaws.com            |
    | 8     | cloudformation.amazonaws.com |
    | 7     | logs.amazonaws.com           |
    +-------+------------------------------+

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
    | 1     | 116.142.58.92   |
    | 1     | 140.38.65.165   |
    | 1     | 58.138.87.219   |
    | 1     | 180.3.121.23    |
    | 1     | 207.28.12.237   |
    | 1     | 210.84.80.238   |
    | 1     | 222.105.156.190 |
    | 1     | 104.201.253.14  |
    | 1     | 207.220.131.48  |
    | 1     | 190.240.94.208  |
    +-------+-----------------+

Top 10 Users Generating Events
------------------------------

Complex user analysis with multiple fields.

PPL query::

    os> source=cloudtrail_logs | where ISNOTNULL(`userIdentity.accountId`) | STATS count() as Count by `userIdentity.sessionContext.sessionIssuer.userName`, `userIdentity.accountId`, `userIdentity.sessionContext.sessionIssuer.type` | rename `userIdentity.sessionContext.sessionIssuer.userName` as `User Name`, `userIdentity.accountId` as `Account Id`, `userIdentity.sessionContext.sessionIssuer.type` as `Type` | SORT - Count | HEAD 1000;
    fetched rows / total rows = 20/20
    +-------+------------------+--------------+-------------+
    | Count | User Name        | Account Id   | Type        |
    |-------|------------------|--------------|-------------|
    | 8     | DataEngineer     | 190225759807 | Role        |
    | 6     | SecurityTeam     | 287645373404 | Role        |
    | 5     | SystemAdmin      | 585894403030 | Role        |
    | 4     | DevOps           | 689811372963 | Role        |
    | 4     | CloudOps         | 343123305904 | Role        |
    | 3     | IAMManager       | 774538043323 | Role        |
    | 3     | AppDeveloper     | 999658550876 | Role        |
    | 3     | NetworkAdmin     | 837288668719 | Role        |
    | 2     | DatabaseAdmin    | 598715677952 | Role        |
    | 2     | ServiceAccount   | 210622981215 | Role        |
    +-------+------------------+--------------+-------------+

Regional Analysis
=================

Events by Region
----------------

Event distribution across AWS regions.

PPL query::

    os> source=cloudtrail_logs | stats count() as Count by `awsRegion` | sort - Count | head 10;
    fetched rows / total rows = 10/10
    +-------+----------------+
    | Count | awsRegion      |
    |-------|----------------|
    | 12    | us-west-1      |
    | 12    | ca-central-1   |
    | 9     | us-west-2      |
    | 8     | ap-southeast-1 |
    | 8     | ap-northeast-1 |
    | 7     | us-east-2      |
    | 7     | sa-east-1      |
    | 7     | eu-north-1     |
    | 7     | ap-south-1     |
    | 6     | ap-southeast-2 |
    +-------+----------------+

EC2 Analysis
============

EC2 Change Event Count
----------------------

EC2 instance lifecycle events (Run, Stop, Terminate).

PPL query::

    os> source=cloudtrail_logs | where eventSource like "ec2%" and (eventName = "RunInstances" or eventName = "TerminateInstances" or eventName = "StopInstances") and not (eventName like "Get%" or eventName like "Describe%" or eventName like "List%" or eventName like "Head%") | stats count() as Count by eventName | sort - Count | head 5;
    fetched rows / total rows = 2/2
    +-------+-------------------+
    | Count | eventName         |
    |-------|-------------------|
    | 1     | TerminateInstances|
    | 1     | RunInstances      |
    +-------+-------------------+

EC2 Users by Session Issuer
---------------------------

Users performing EC2 operations.

PPL query::

    os> source=cloudtrail_logs | where isnotnull(`userIdentity.sessionContext.sessionIssuer.userName`) and `eventSource` like 'ec2%' and not (`eventName` like 'Get%' or `eventName` like 'Describe%' or `eventName` like 'List%' or `eventName` like 'Head%') | stats count() as Count by `userIdentity.sessionContext.sessionIssuer.userName` | sort - Count | head 10;
    fetched rows / total rows = 4/4
    +-------+----------------------------------------------------+
    | Count | userIdentity.sessionContext.sessionIssuer.userName |
    |-------|----------------------------------------------------|
    | 1     | Analyst                                            |
    | 1     | DataEngineer                                       |
    | 1     | ec2-service                                        |
    | 1     |                                                    |
    +-------+----------------------------------------------------+

EC2 Events by Name
------------------

EC2 API operations excluding read-only operations.

PPL query::

    os> source=cloudtrail_logs | where `eventSource` like "ec2%" and not (`eventName` like "Get%" or `eventName` like "Describe%" or `eventName` like "List%" or `eventName` like "Head%") | stats count() as Count by `eventName` | rename `eventName` as `Event Name` | sort - Count | head 10;
    fetched rows / total rows = 3/3
    +-------+---------------------+
    | Count | Event Name          |
    |-------|---------------------|
    | 2     | CreateSecurityGroup |
    | 1     | RunInstances        |
    | 1     | TerminateInstances  |
    +-------+---------------------+

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