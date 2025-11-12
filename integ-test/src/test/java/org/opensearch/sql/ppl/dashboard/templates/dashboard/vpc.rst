..
   Copyright OpenSearch Contributors
   SPDX-License-Identifier: Apache-2.0

===============================
VPC Flow Logs Dashboard Queries
===============================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Description
===========

VPC Flow Logs PPL queries analyze network flow patterns, traffic volume, and AWS service interactions. These queries demonstrate common dashboard patterns for VPC Flow Logs analysis.

Basic Aggregations
==================

Total Requests
--------------

Basic count aggregation for all flow records.

PPL query::

    os> source=vpc_flow_logs | stats count();
    fetched rows / total rows = 1/1
    +----------+
    | count()  |
    |----------|
    | 100      |
    +----------+

Total Flows by Actions
----------------------

Flow distribution by ACCEPT/REJECT actions.

PPL query::

    os> source=vpc_flow_logs | STATS count() as Count by action | SORT - Count | HEAD 5;
    fetched rows / total rows = 2/2
    +-------+--------+
    | Count | action |
    |-------|--------|
    | 92    | ACCEPT |
    | 8     | REJECT |
    +-------+--------+

Time-based Analysis
===================

Flows Over Time
---------------

Flow patterns over time using span functions.

PPL query::

    os> source=vpc_flow_logs | STATS count() by span(`start`, 30d);
    fetched rows / total rows = 7/7
    +----------+----------------------+
    | count()  | span(`start`,30d)    |
    |----------|----------------------|
    | 6        | 2025-04-12 00:00:00  |
    | 24       | 2025-05-12 00:00:00  |
    | 17       | 2025-06-11 00:00:00  |
    | 12       | 2025-07-11 00:00:00  |
    | 17       | 2025-08-10 00:00:00  |
    | 13       | 2025-09-09 00:00:00  |
    | 11       | 2025-10-09 00:00:00  |
    +----------+----------------------+

Bytes Transferred Over Time
---------------------------

Byte transfer trends over time periods.

PPL query::

    os> source=vpc_flow_logs | STATS sum(bytes) by span(`start`, 30d);
    fetched rows / total rows = 7/7
    +------------+----------------------+
    | sum(bytes) | span(`start`,30d)    |
    |------------|----------------------|
    | 385560     | 2025-04-12 00:00:00  |
    | 1470623    | 2025-05-12 00:00:00  |
    | 1326170    | 2025-06-11 00:00:00  |
    | 946422     | 2025-07-11 00:00:00  |
    | 826957     | 2025-08-10 00:00:00  |
    | 719758     | 2025-09-09 00:00:00  |
    | 643042     | 2025-10-09 00:00:00  |
    +------------+----------------------+

Traffic Analysis
================

Top Talkers by Bytes
--------------------

Source IPs generating the most traffic by bytes.

PPL query::

    os> source=vpc_flow_logs | stats sum(bytes) as Bytes by srcaddr | sort - Bytes | head 10;
    fetched rows / total rows = 10/10
    +--------+----------------+
    | Bytes  | srcaddr        |
    |--------|----------------|
    | 267655 | 121.65.198.154 |
    | 259776 | 10.0.91.27     |
    | 214512 | 10.0.165.194   |
    | 210396 | 6.186.106.13   |
    | 192355 | 182.53.30.77   |
    | 187200 | 10.0.163.249   |
    | 183353 | 30.193.135.22  |
    | 182055 | 213.227.231.57 |
    | 176391 | 39.40.182.87   |
    | 175820 | 10.0.14.9      |
    +--------+----------------+

Top Destinations by Bytes
--------------------------

Destination IPs receiving the most bytes.

PPL query::

    os> source=vpc_flow_logs | stats sum(bytes) as Bytes by dstaddr | sort - Bytes | head 10;
    fetched rows / total rows = 10/10
    +--------+----------------+
    | Bytes  | dstaddr        |
    |--------|----------------|
    | 267655 | 10.0.113.54    |
    | 259776 | 11.111.108.48  |
    | 214512 | 223.252.77.226 |
    | 210396 | 10.0.194.75    |
    | 192355 | 10.0.11.144    |
    | 187200 | 120.67.35.74   |
    | 183353 | 10.0.167.74    |
    | 182055 | 10.0.74.110    |
    | 176391 | 10.0.3.220     |
    | 175820 | 10.0.83.167    |
    +--------+----------------+

Packets Transferred Over Time
-----------------------------

Packet transfer trends over time periods.

PPL query::

    os> source=vpc_flow_logs | STATS sum(packets) by span(`start`, 30d);
    fetched rows / total rows = 7/7
    +--------------+----------------------+
    | sum(packets) | span(`start`,30d)    |
    |--------------|----------------------|
    | 360          | 2025-04-12 00:00:00  |
    | 1715         | 2025-05-12 00:00:00  |
    | 1396         | 2025-06-11 00:00:00  |
    | 804          | 2025-07-11 00:00:00  |
    | 941          | 2025-08-10 00:00:00  |
    | 890          | 2025-09-09 00:00:00  |
    | 709          | 2025-10-09 00:00:00  |
    +--------------+----------------------+

Top Talkers by Packets
----------------------

Source IPs generating the most packets.

PPL query::

    os> source=vpc_flow_logs | stats sum(packets) as Packets by srcaddr | sort - Packets | head 10;
    fetched rows / total rows = 10/10
    +---------+----------------+
    | Packets | srcaddr        |
    |---------|----------------|
    | 200     | 10.0.163.249   |
    | 199     | 121.65.198.154 |
    | 198     | 10.0.91.27     |
    | 197     | 6.186.106.13   |
    | 181     | 115.27.64.3    |
    | 181     | 30.193.135.22  |
    | 176     | 10.0.227.35    |
    | 174     | 10.0.99.147    |
    | 171     | 10.0.231.176   |
    | 164     | 10.0.165.194   |
    +---------+----------------+

Top Destinations by Packets
---------------------------

Destination IPs receiving the most packets.

PPL query::

    os> source=vpc_flow_logs | stats sum(packets) as Packets by dstaddr | sort - Packets | head 10;
    fetched rows / total rows = 10/10
    +---------+----------------+
    | Packets | dstaddr        |
    |---------|----------------|
    | 200     | 120.67.35.74   |
    | 199     | 10.0.113.54    |
    | 198     | 11.111.108.48  |
    | 197     | 10.0.194.75    |
    | 181     | 10.0.167.74    |
    | 181     | 10.0.159.18    |
    | 176     | 10.0.62.137    |
    | 174     | 182.58.134.190 |
    | 171     | 34.55.235.91   |
    | 164     | 118.124.149.78 |
    +---------+----------------+

Connection Analysis
===================

Top Talkers by IP Count
-----------------------

Source IPs with most connection attempts.

PPL query::

    os> source=vpc_flow_logs | STATS count() as Count by srcaddr | SORT - Count | HEAD 10;
    fetched rows / total rows = 10/10
    +-------+----------------+
    | Count | srcaddr        |
    |-------|----------------|
    | 1     | 1.24.59.183    |
    | 1     | 10.0.101.123   |
    | 1     | 10.0.107.121   |
    | 1     | 10.0.107.130   |
    | 1     | 10.0.108.29    |
    | 1     | 10.0.115.237   |
    | 1     | 10.0.117.121   |
    | 1     | 10.0.126.80    |
    | 1     | 10.0.13.162    |
    | 1     | 10.0.132.168   |
    +-------+----------------+

Top Destinations by IP Count
----------------------------

Destination IPs with most incoming connections.

PPL query::

    os> source=vpc_flow_logs | stats count() as Requests by dstaddr | sort - Requests | head 10;
    fetched rows / total rows = 10/10
    +----------+----------------+
    | Requests | dstaddr        |
    |----------|----------------|
    | 1        | 10.0.100.62    |
    | 1        | 10.0.107.6     |
    | 1        | 10.0.109.2     |
    | 1        | 10.0.11.144    |
    | 1        | 10.0.113.54    |
    | 1        | 10.0.116.210   |
    | 1        | 10.0.118.54    |
    | 1        | 10.0.127.142   |
    | 1        | 10.0.138.175   |
    | 1        | 10.0.147.33    |
    +----------+----------------+

Heat Map Analysis
=================

Top Talkers Heat Map
--------------------

Source-destination IP pair analysis for traffic patterns.

PPL query::

    os> source=vpc_flow_logs | stats count() as Count by dstaddr, srcaddr | sort - Count | head 100;
    fetched rows / total rows = 100/100
    +-------+----------------+----------------+
    | Count | dstaddr        | srcaddr        |
    |-------|----------------|----------------|
    | 1     | 10.0.100.62    | 1.24.59.183    |
    | 1     | 10.0.107.6     | 10.0.101.123   |
    | ...   | ...            | ...            |
    +-------+----------------+----------------+

