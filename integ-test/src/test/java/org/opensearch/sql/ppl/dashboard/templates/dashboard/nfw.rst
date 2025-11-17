..
   Copyright OpenSearch Contributors
   SPDX-License-Identifier: Apache-2.0

==================================
Network Firewall Dashboard Queries
==================================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Description
===========

Network Firewall PPL queries analyze network traffic patterns, security events, and flow characteristics. These queries demonstrate common dashboard patterns for AWS Network Firewall log analysis.

.. note::
   Some queries may return results in different orders when multiple records have equal values (e.g., count = 1). This is expected database behavior and does not affect the correctness of the results. For queries with non-deterministic ordering, sample outputs may not be shown, but the query structure and schema remain valid.

Traffic Analysis
================

Top Source IP by Packets
-------------------------

Source IPs generating the most network packets over time.

.. note::
   Results may vary in order when packet counts are equal.

PPL query::

    os> source=nfw_logs | stats sum(`event.netflow.pkts`) as packet_count by span(`event.timestamp`, 2d) as timestamp_span, `event.src_ip` | rename `event.src_ip` as `Source IP` | sort - packet_count | head 10;
    fetched rows / total rows = 10/10
    +--------------+---------------------+----------------+
    | packet_count | timestamp_span      | Source IP      |
    |--------------|---------------------|----------------|
    | 53           | 2025-02-23 00:00:00 | 10.170.18.235  |
    | 11           | 2025-02-23 00:00:00 | 8.8.8.8        |
    | 11           | 2025-02-23 00:00:00 | 54.242.115.112 |
    | 1            | 2025-03-27 00:00:00 | 45.82.78.100   |
    | 1            | 2025-03-27 00:00:00 | 20.65.193.116  |
    | 1            | 2025-03-27 00:00:00 | 172.16.0.100   |
    | 1            | 2025-03-27 00:00:00 | 172.16.0.101   |
    | 1            | 2025-03-27 00:00:00 | 172.16.0.102   |
    | 0            | 2025-03-27 00:00:00 | 51.158.113.168 |
    | 0            | 2025-03-27 00:00:00 | 10.2.1.120     |
    +--------------+---------------------+----------------+

Top Application Protocols
--------------------------

Most common application layer protocols.

PPL query::

    os> source=nfw_logs | where isnotnull(`event.app_proto`) | STATS count() as Count by `event.app_proto` | SORT - Count | HEAD 10;
    fetched rows / total rows = 4/4
    +-------+-----------------+
    | Count | event.app_proto |
    |-------|-----------------|
    | 89    | http            |
    | 5     | unknown         |
    | 2     | tls             |
    | 2     | dns             |
    +-------+-----------------+

Protocol Analysis
=================

Top Protocols
-------------

Most common network protocols (TCP, UDP, ICMP).

PPL query::

    os> source=nfw_logs | STATS count() as Count by `event.proto` | SORT - Count | HEAD 10;
    fetched rows / total rows = 3/3
    +-------+-------------+
    | Count | event.proto |
    |-------|-------------|
    | 95    | TCP         |
    | 3     | ICMP        |
    | 2     | UDP         |
    +-------+-------------+

Security Analysis
=================

Top Blocked Source IPs
-----------------------

Source IPs with blocked traffic.

PPL query::

    os> source=nfw_logs | WHERE `event.alert.action` = "blocked" | STATS COUNT() as Count by `event.src_ip` | SORT - Count | HEAD 10;
    fetched rows / total rows = 2/2
    +-------+---------------+
    | Count | event.src_ip  |
    |-------|---------------|
    | 4     | 10.170.18.235 |
    | 1     | 10.2.1.120    |
    +-------+---------------+

Top Blocked Destination IPs
----------------------------

Destinations with blocked traffic.

PPL query::

    os> source=nfw_logs | WHERE `event.alert.action` = "blocked" | STATS COUNT() as Count by `event.dest_ip` | SORT - Count | HEAD 10;
    fetched rows / total rows = 4/4
    +-------+----------------+
    | Count | event.dest_ip  |
    |-------|----------------|
    | 2     | 8.8.8.8        |
    | 1     | 54.146.42.172  |
    | 1     | 54.242.115.112 |
    | 1     | 52.216.211.88  |
    +-------+----------------+

HTTP Analysis
=============

Top HTTP Host Headers
---------------------

HTTP hostname analysis for allowed traffic.

PPL query::

    os> source=nfw_logs | where `event.alert.action` = "allowed" | stats count() as event_count by span(`event.timestamp`, 2d) as time_bucket, `event.http.hostname` | rename `event.http.hostname` as `Hostname` | sort - event_count;
    fetched rows / total rows = 1/1
    +-------------+---------------------+----------+
    | event_count | time_bucket         | Hostname |
    |-------------|---------------------|----------|
    | 1           | 2025-03-27 00:00:00 | null     |
    +-------------+---------------------+----------+

Top Blocked HTTP Host Headers
-----------------------------

HTTP hostname analysis for blocked traffic.

PPL query::

    os> source=nfw_logs | where `event.alert.action` = "blocked" and isnotnull(`event.http.hostname`) | stats count() as event_count by span(`event.timestamp`, 2d) as time_bucket, `event.http.hostname` | rename `event.http.hostname` as `Hostname` | sort - event_count | HEAD 10;
    fetched rows / total rows = 1/1
    +-------------+---------------------+----------------------+
    | event_count | time_bucket         | Hostname             |
    |-------------|---------------------|----------------------|
    | 1           | 2025-02-23 00:00:00 | checkip.amazonaws.com|
    +-------------+---------------------+----------------------+

Top Allowed TLS SNI
-------------------

TLS Server Name Indication analysis for allowed traffic.

PPL query::

    os> source=nfw_logs | where `event.alert.action` = "allowed" | stats count() as event_count by span(`event.timestamp`, 2d) as time_bucket, `event.tls.sni` | rename `event.tls.sni` as `Hostname` | sort - event_count | HEAD 10;
    fetched rows / total rows = 1/1
    +-------------+---------------------+----------+
    | event_count | time_bucket         | Hostname |
    |-------------|---------------------|----------|
    | 1           | 2025-03-27 00:00:00 | null     |
    +-------------+---------------------+----------+

Top Blocked TLS SNI
-------------------

TLS Server Name Indication analysis for blocked traffic.

PPL query::

    os> source=nfw_logs | where `event.alert.action` = "blocked" and isnotnull(`event.tls.sni`) | stats count() as event_count by span(`event.timestamp`, 2d) as time_bucket, `event.tls.sni` | rename `event.tls.sni` as `Hostname` | sort - event_count | HEAD 10;
    fetched rows / total rows = 2/2
    +-------------+---------------------+---------------------------+
    | event_count | time_bucket         | Hostname                  |
    |-------------|---------------------|---------------------------|
    | 1           | 2025-02-23 00:00:00 | checkip.amazonaws.com     |
    | 1           | 2025-03-27 00:00:00 | s3.us-east-1.amazonaws.com|
    +-------------+---------------------+---------------------------+

Top HTTP URI Paths
------------------

Most frequently requested URI paths.

PPL query::

    os> source=nfw_logs | where isnotnull(`event.http.url`) | stats count() as event_count by span(`event.timestamp`, 2d) as timestamp_span, `event.http.url` | rename `event.http.url` as `URL` | sort - event_count | head 10;
    fetched rows / total rows = 1/1
    +-------------+---------------------+-----+
    | event_count | timestamp_span      | URL |
    |-------------|---------------------|-----|
    | 1           | 2025-02-23 00:00:00 | /   |
    +-------------+---------------------+-----+

Top HTTP User Agents
--------------------

Most common HTTP user agents.

PPL query::

    os> source=nfw_logs | where isnotnull(`event.http.http_user_agent`) | stats count() as event_count by span(`event.timestamp`, 2d) as timestamp_span, `event.http.http_user_agent` | rename `event.http.http_user_agent` as `User Agent` | sort - event_count | head 10;
    fetched rows / total rows = 1/1
    +-------------+---------------------+------------+
    | event_count | timestamp_span      | User Agent |
    |-------------|---------------------|------------|
    | 1           | 2025-02-23 00:00:00 | curl/8.5.0 |
    +-------------+---------------------+------------+

Private Link Analysis
=====================

Top Private Link Endpoint Candidates
------------------------------------

Identify potential AWS service endpoints for Private Link.

PPL query::

    os> source=nfw_logs | where (`event.tls.sni` like 's3%') or (`event.http.hostname` like 's3%') or (`event.tls.sni` like 'dynamodb%') or (`event.http.hostname` like 'dynamodb%') or (`event.tls.sni` like 'backup%') or (`event.http.hostname` like 'backup%') | STATS count() as Count by `event.src_ip`, `event.dest_ip`, `event.app_proto`, `event.tls.sni`, `event.http.hostname` | rename `event.tls.sni` as SNI, `event.dest_ip` as Dest_IP, `event.src_ip` as Source_IP, `event.http.hostname` as Hostname, `event.app_proto` as App_Proto | SORT - Count;
    fetched rows / total rows = 1/1
    +-------+---------------------------+---------------+------------+----------+-----------+
    | Count | SNI                       | Dest_IP       | Source_IP  | Hostname | App_Proto |
    |-------|---------------------------|---------------|------------|----------|-----------|  
    | 1     | s3.us-east-1.amazonaws.com| 52.216.211.88 | 10.2.1.120 | null     | tls       |
    +-------+---------------------------+---------------+------------+----------+-----------+

Port Analysis
=============

Top Source Ports
----------------

Most active source ports over time.

PPL query::

    os> source=nfw_logs | stats count() as Count by SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_port` | eval `Source Port` = CAST(`event.src_port` AS STRING) | sort - Count | HEAD 10;
    fetched rows / total rows = 10/10
    +-------+---------------------+---------------+-------------+
    | Count | timestamp_span      | event.src_port| Source Port |
    |-------|---------------------|---------------|-------------|
    | 1     | 2025-02-23 00:00:00 | 52610         | 52610       |
    | 1     | 2025-02-23 00:00:00 | 45550         | 45550       |
    | 1     | 2025-02-23 00:00:00 | 33445         | 33445       |
    | 1     | 2025-02-23 00:00:00 | 22334         | 22334       |
    | 1     | 2025-03-27 00:00:00 | 55123         | 55123       |
    | 1     | 2025-03-27 00:00:00 | 44567         | 44567       |
    | 1     | 2025-03-27 00:00:00 | 33890         | 33890       |
    | 1     | 2025-03-27 00:00:00 | 22445         | 22445       |
    | 1     | 2025-03-27 00:00:00 | 11234         | 11234       |
    | 1     | 2025-03-27 00:00:00 | 9876          | 9876        |
    +-------+---------------------+---------------+-------------+

Top Destination Ports
---------------------

Most active destination ports over time.

PPL query::

    os> source=nfw_logs | stats count() as Count by SPAN(`event.timestamp`, 2d) as timestamp_span, `event.dest_port` | eval `Destination Port` = CAST(`event.dest_port` AS STRING) | sort - Count | HEAD 10;
    fetched rows / total rows = 10/10
    +-------+---------------------+----------------+------------------+
    | Count | timestamp_span      | event.dest_port| Destination Port |
    |-------|---------------------|----------------|------------------|
    | 15    | 2025-02-23 00:00:00 | 80             | 80               |
    | 12    | 2025-02-23 00:00:00 | 443            | 443              |
    | 8     | 2025-02-23 00:00:00 | 53             | 53               |
    | 5     | 2025-03-27 00:00:00 | 8085           | 8085             |
    | 3     | 2025-03-27 00:00:00 | 1433           | 1433             |
    | 2     | 2025-03-27 00:00:00 | 22             | 22               |
    | 2     | 2025-03-27 00:00:00 | 3389           | 3389             |
    | 1     | 2025-03-27 00:00:00 | 5900           | 5900             |
    | 1     | 2025-03-27 00:00:00 | 993            | 993              |
    | 1     | 2025-03-27 00:00:00 | 995            | 995              |
    +-------+---------------------+----------------+------------------+

TCP Flow Analysis
=================

Top TCP Flows
-------------

Most active TCP connections.

PPL query::

    os> source=nfw_logs | WHERE `event.proto` = "TCP" | STATS count() as Count by SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`, `event.dest_ip`, `event.dest_port` | EVAL `Src IP - Dst IP:Port` = CONCAT(`event.src_ip`, " - ", `event.dest_ip`, ": ", CAST(`event.dest_port` AS STRING)) | SORT - Count | HEAD 10;
    fetched rows / total rows = 10/10
    +-------+---------------------+---------------+----------------+----------------+------------------------------------+
    | Count | timestamp_span      | event.src_ip  | event.dest_ip  | event.dest_port| Src IP - Dst IP:Port               |
    |-------|---------------------|---------------|----------------|----------------|------------------------------------|
    | 1     | 2025-02-23 00:00:00 | 10.170.18.235 | 8.8.8.8        | 80             | 10.170.18.235 - 8.8.8.8: 80        |
    | 1     | 2025-02-23 00:00:00 | 10.170.18.235 | 54.242.115.112 | 443            | 10.170.18.235 - 54.242.115.112: 443|
    | 1     | 2025-03-27 00:00:00 | 45.82.78.100  | 10.2.1.120     | 8085           | 45.82.78.100 - 10.2.1.120: 8085    |
    | 1     | 2025-03-27 00:00:00 | 20.65.193.116 | 10.2.1.120     | 1433           | 20.65.193.116 - 10.2.1.120: 1433   |
    | 1     | 2025-03-27 00:00:00 | 172.16.0.100  | 192.168.1.10   | 22             | 172.16.0.100 - 192.168.1.10: 22    |
    | 1     | 2025-03-27 00:00:00 | 172.16.0.101  | 192.168.1.11   | 3389           | 172.16.0.101 - 192.168.1.11: 3389  |
    | 1     | 2025-03-27 00:00:00 | 172.16.0.102  | 192.168.1.12   | 5900           | 172.16.0.102 - 192.168.1.12: 5900  |
    | 1     | 2025-03-27 00:00:00 | 10.0.1.50     | 203.0.113.100  | 993            | 10.0.1.50 - 203.0.113.100: 993     |
    | 1     | 2025-03-27 00:00:00 | 10.0.1.51     | 203.0.113.101  | 995            | 10.0.1.51 - 203.0.113.101: 995     |
    | 1     | 2025-03-27 00:00:00 | 10.0.1.52     | 203.0.113.102  | 25             | 10.0.1.52 - 203.0.113.102: 25      |
    +-------+---------------------+---------------+----------------+----------------+------------------------------------+

Top TCP Flows by Packets
------------------------

TCP connections with highest packet counts.

PPL query::

    os> source=nfw_logs | WHERE `event.proto` = "TCP" | STATS sum(`event.netflow.pkts`) as Packets by SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`, `event.dest_ip`, `event.dest_port` | EVAL `Src IP - Dst IP:Port` = CONCAT(`event.src_ip`, " - ", `event.dest_ip`, ": ", CAST(`event.dest_port` AS STRING)) | SORT - Packets | HEAD 10;
    fetched rows / total rows = 10/10
    +---------+---------------------+---------------+----------------+----------------+------------------------------------+
    | Packets | timestamp_span      | event.src_ip  | event.dest_ip  | event.dest_port| Src IP - Dst IP:Port               |
    |---------|---------------------|---------------|----------------|----------------|------------------------------------|
    | 53      | 2025-02-23 00:00:00 | 10.170.18.235 | 8.8.8.8        | 80             | 10.170.18.235 - 8.8.8.8: 80        |
    | 11      | 2025-02-23 00:00:00 | 10.170.18.235 | 54.242.115.112 | 443            | 10.170.18.235 - 54.242.115.112: 443|
    | 5       | 2025-03-27 00:00:00 | 45.82.78.100  | 10.2.1.120     | 8085           | 45.82.78.100 - 10.2.1.120: 8085    |
    | 3       | 2025-03-27 00:00:00 | 20.65.193.116 | 10.2.1.120     | 1433           | 20.65.193.116 - 10.2.1.120: 1433   |
    | 2       | 2025-03-27 00:00:00 | 172.16.0.100  | 192.168.1.10   | 22             | 172.16.0.100 - 192.168.1.10: 22    |
    | 1       | 2025-03-27 00:00:00 | 172.16.0.101  | 192.168.1.11   | 3389           | 172.16.0.101 - 192.168.1.11: 3389  |
    | 1       | 2025-03-27 00:00:00 | 172.16.0.102  | 192.168.1.12   | 5900           | 172.16.0.102 - 192.168.1.12: 5900  |
    | 1       | 2025-03-27 00:00:00 | 10.0.1.50     | 203.0.113.100  | 993            | 10.0.1.50 - 203.0.113.100: 993     |
    | 1       | 2025-03-27 00:00:00 | 10.0.1.51     | 203.0.113.101  | 995            | 10.0.1.51 - 203.0.113.101: 995     |
    | 1       | 2025-03-27 00:00:00 | 10.0.1.52     | 203.0.113.102  | 25             | 10.0.1.52 - 203.0.113.102: 25      |
    +---------+---------------------+---------------+----------------+----------------+------------------------------------+

Top TCP Flows by Bytes
----------------------

TCP connections with highest byte counts.

PPL query::

    os> source=nfw_logs | WHERE `event.proto` = "TCP" | STATS sum(event.netflow.bytes) as Bytes by SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`, `event.dest_ip`, `event.dest_port` | EVAL `Src IP - Dst IP:Port` = CONCAT(`event.src_ip`, " - ", `event.dest_ip`, ": ", CAST(`event.dest_port` AS STRING)) | SORT - Bytes | HEAD 10;
    fetched rows / total rows = 10/10
    +-------+---------------------+---------------+----------------+----------------+------------------------------------+
    | Bytes | timestamp_span      | event.src_ip  | event.dest_ip  | event.dest_port| Src IP - Dst IP:Port               |
    |-------|---------------------|---------------|----------------|----------------|------------------------------------|
    | 15420 | 2025-02-23 00:00:00 | 10.170.18.235 | 8.8.8.8        | 80             | 10.170.18.235 - 8.8.8.8: 80        |
    | 8950  | 2025-02-23 00:00:00 | 10.170.18.235 | 54.242.115.112 | 443            | 10.170.18.235 - 54.242.115.112: 443|
    | 2048  | 2025-03-27 00:00:00 | 45.82.78.100  | 10.2.1.120     | 8085           | 45.82.78.100 - 10.2.1.120: 8085    |
    | 1536  | 2025-03-27 00:00:00 | 20.65.193.116 | 10.2.1.120     | 1433           | 20.65.193.116 - 10.2.1.120: 1433   |
    | 1024  | 2025-03-27 00:00:00 | 172.16.0.100  | 192.168.1.10   | 22             | 172.16.0.100 - 192.168.1.10: 22    |
    | 512   | 2025-03-27 00:00:00 | 172.16.0.101  | 192.168.1.11   | 3389           | 172.16.0.101 - 192.168.1.11: 3389  |
    | 256   | 2025-03-27 00:00:00 | 172.16.0.102  | 192.168.1.12   | 5900           | 172.16.0.102 - 192.168.1.12: 5900  |
    | 128   | 2025-03-27 00:00:00 | 10.0.1.50     | 203.0.113.100  | 993            | 10.0.1.50 - 203.0.113.100: 993     |
    | 64    | 2025-03-27 00:00:00 | 10.0.1.51     | 203.0.113.101  | 995            | 10.0.1.51 - 203.0.113.101: 995     |
    | 32    | 2025-03-27 00:00:00 | 10.0.1.52     | 203.0.113.102  | 25             | 10.0.1.52 - 203.0.113.102: 25      |
    +-------+---------------------+---------------+----------------+----------------+------------------------------------+

Top TCP Flags
-------------

Most common TCP flag combinations.

PPL query::

    os> source=nfw_logs | STATS count() as Count by `event.tcp.tcp_flags` | SORT - Count | HEAD 10;
    fetched rows / total rows = 10/10
    +-------+---------------------+
    | Count | event.tcp.tcp_flags |
    |-------|---------------------|
    | 8     | null                |
    | 4     | 13                  |
    | 4     | 17                  |
    | 3     | 0                   |
    | 3     | 1                   |
    | 3     | 15                  |
    | 3     | 16                  |
    | 3     | 18                  |
    | 3     | 19                  |
    | 3     | 2                   |
    +-------+---------------------+

UDP Flow Analysis
=================

Top UDP Flows
-------------

Most active UDP connections.

PPL query::

    os> source=nfw_logs | WHERE `event.proto` = "UDP" | STATS count() as Count by SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`, `event.dest_ip`, `event.dest_port` | EVAL `Src IP - Dst IP:Port` = CONCAT(`event.src_ip`, " - ", `event.dest_ip`, ": ", CAST(`event.dest_port` AS STRING)) | SORT - Count | HEAD 10;
    fetched rows / total rows = 2/2
    +-------+---------------------+---------------+----------------+----------------+---------------------------------+
    | Count | timestamp_span      | event.src_ip  | event.dest_ip  | event.dest_port| Src IP - Dst IP:Port            |
    |-------|---------------------|---------------|----------------|----------------|---------------------------------|
    | 1     | 2025-02-23 00:00:00 | 10.0.2.100    | 8.8.8.8        | 53             | 10.0.2.100 - 8.8.8.8: 53        |
    | 1     | 2025-03-27 00:00:00 | 10.0.2.101    | 1.1.1.1        | 53             | 10.0.2.101 - 1.1.1.1: 53        |
    +-------+---------------------+---------------+----------------+----------------+---------------------------------+

Top UDP Flows by Packets
------------------------

UDP connections with highest packet counts.

PPL query::

    os> source=nfw_logs | WHERE `event.proto` = "UDP" | STATS sum(`event.netflow.pkts`) as Packets by SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`, `event.dest_ip`, `event.dest_port` | EVAL `Src IP - Dst IP:Port` = CONCAT(`event.src_ip`, " - ", `event.dest_ip`, ": ", CAST(`event.dest_port` AS STRING)) | SORT - Packets | HEAD 10;
    fetched rows / total rows = 2/2
    +---------+---------------------+---------------+----------------+----------------+---------------------------------+
    | Packets | timestamp_span      | event.src_ip  | event.dest_ip  | event.dest_port| Src IP - Dst IP:Port            |
    |---------|---------------------|---------------|----------------|----------------|---------------------------------|
    | 2       | 2025-02-23 00:00:00 | 10.0.2.100    | 8.8.8.8        | 53             | 10.0.2.100 - 8.8.8.8: 53        |
    | 1       | 2025-03-27 00:00:00 | 10.0.2.101    | 1.1.1.1        | 53             | 10.0.2.101 - 1.1.1.1: 53        |
    +---------+---------------------+---------------+----------------+----------------+---------------------------------+

Top UDP Flows by Bytes
----------------------

UDP connections with highest byte counts.

PPL query::

    os> source=nfw_logs | WHERE `event.proto` = "UDP" | STATS sum(`event.netflow.bytes`) as Bytes by SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`, `event.dest_ip`, `event.dest_port` | EVAL `Src IP - Dst IP:Port` = CONCAT(`event.src_ip`, " - ", `event.dest_ip`, ": ", CAST(`event.dest_port` AS STRING)) | SORT - Bytes | HEAD 10;
    fetched rows / total rows = 2/2
    +-------+---------------------+---------------+----------------+----------------+---------------------------------+
    | Bytes | timestamp_span      | event.src_ip  | event.dest_ip  | event.dest_port| Src IP - Dst IP:Port            |
    |-------|---------------------|---------------|----------------|----------------|---------------------------------|
    | 128   | 2025-02-23 00:00:00 | 10.0.2.100    | 8.8.8.8        | 53             | 10.0.2.100 - 8.8.8.8: 53        |
    | 64    | 2025-03-27 00:00:00 | 10.0.2.101    | 1.1.1.1        | 53             | 10.0.2.101 - 1.1.1.1: 53        |
    +-------+---------------------+---------------+----------------+----------------+---------------------------------+

ICMP Flow Analysis
==================

Top ICMP Flows
--------------

Most active ICMP connections.

PPL query::

    os> source=nfw_logs | WHERE `event.proto` = "ICMP" | STATS count() as Count by SPAN(`event.timestamp`, 1d) as timestamp_span, `event.src_ip`, `event.dest_ip`, `event.dest_port` | EVAL `Src IP - Dst IP:Port` = CONCAT(`event.src_ip`, " - ", `event.dest_ip`, ": ", CAST(`event.dest_port` AS STRING)) | SORT - Count | HEAD 10;
    fetched rows / total rows = 3/3
    +-------+---------------------+---------------+----------------+----------------+---------------------------------+
    | Count | timestamp_span      | event.src_ip  | event.dest_ip  | event.dest_port| Src IP - Dst IP:Port            |
    |-------|---------------------|---------------|----------------|----------------|---------------------------------|
    | 1     | 2025-02-23 00:00:00 | 10.0.3.100    | 8.8.8.8        | 0              | 10.0.3.100 - 8.8.8.8: 0         |
    | 1     | 2025-03-27 00:00:00 | 10.0.3.101    | 1.1.1.1        | 0              | 10.0.3.101 - 1.1.1.1: 0         |
    | 1     | 2025-03-27 00:00:00 | 192.168.2.50  | 203.0.113.200  | 0              | 192.168.2.50 - 203.0.113.200: 0 |
    +-------+---------------------+---------------+----------------+----------------+---------------------------------+

Rule Analysis
=============

Top Drop/Reject Rules
---------------------

Most frequently triggered blocking rules.

PPL query::

    os> source=nfw_logs | WHERE `event.alert.action` = "blocked" | STATS count() as Count by `event.alert.signature_id`, `event.alert.action`, `event.alert.signature`, `event.proto` | RENAME `event.alert.signature_id` as SID, `event.alert.action` as Action, `event.alert.signature` as Message, `event.proto` as Proto | SORT - Count | HEAD 10;
    fetched rows / total rows = 3/3
    +-------+-----+---------+---------------------------+-------+
    | Count | SID | Action  | Message                   | Proto |
    |-------|-----|---------|---------------------------|-------|
    | 3     | 1   | blocked | Suspicious Traffic Block  | TCP   |
    | 1     | 2   | blocked | Port Scan Detection       | TCP   |
    | 1     | 3   | blocked | Malware Communication     | TCP   |
    +-------+-----+---------+---------------------------+-------+

Top Allowed Rules
-----------------

Most frequently triggered allowing rules.

PPL query::

    os> source=nfw_logs | where `event.alert.action` = "allowed" | stats count() as Count by `event.alert.signature_id`, `event.alert.action`, `event.alert.signature`, `event.proto` | rename `event.alert.signature_id` as SID, `event.alert.action` as Action, `event.alert.signature` as Message, `event.proto` as Proto | sort - Count | head 10;
    fetched rows / total rows = 1/1
    +-------+-----+---------+---------------------------+-------+
    | Count | SID | Action  | Message                   | Proto |
    |-------|-----|---------|---------------------------|-------|
    | 1     | 100 | allowed | Standard Web Traffic      | TCP   |
    +-------+-----+---------+---------------------------+-------+

Blocked Traffic Analysis
========================

Top Blocked Destination Ports
-----------------------------

Most frequently blocked destination ports.

PPL query::

    os> source=nfw_logs | WHERE `event.alert.action` = "blocked" | STATS COUNT() as `Count` by `event.dest_port` | EVAL `Destination Port` = CAST(`event.dest_port` as STRING) | SORT - `Count` | HEAD 10;
    fetched rows / total rows = 4/4
    +-------+----------------+------------------+
    | Count | event.dest_port| Destination Port |
    |-------|----------------|------------------|
    | 2     | 53             | 53               |
    | 1     | 80             | 80               |
    | 1     | 443            | 443              |
    | 1     | 22             | 22               |
    +-------+----------------+------------------+

Top Blocked Remote Access Ports
-------------------------------

Blocked connections to remote access ports.

PPL query::

    os> source=nfw_logs | WHERE `event.alert.action` = "blocked" | STATS count() as Count by SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`, `event.dest_ip`, `event.dest_port` | EVAL `Src IP - Dst IP:Port` = CONCAT(`event.src_ip`, " - ", `event.dest_ip`, ": ", CAST(`event.dest_port` AS STRING)) | SORT - Count | HEAD 10;
    fetched rows / total rows = 5/5
    +-------+---------------------+---------------+----------------+----------------+------------------------------------+
    | Count | timestamp_span      | event.src_ip  | event.dest_ip  | event.dest_port| Src IP - Dst IP:Port               |
    |-------|---------------------|---------------|----------------|----------------|------------------------------------|
    | 2     | 2025-02-23 00:00:00 | 10.170.18.235 | 8.8.8.8        | 53             | 10.170.18.235 - 8.8.8.8: 53        |
    | 1     | 2025-02-23 00:00:00 | 10.170.18.235 | 54.146.42.172  | 80             | 10.170.18.235 - 54.146.42.172: 80  |
    | 1     | 2025-02-23 00:00:00 | 10.170.18.235 | 54.242.115.112 | 443            | 10.170.18.235 - 54.242.115.112: 443|
    | 1     | 2025-03-27 00:00:00 | 10.2.1.120    | 52.216.211.88  | 22             | 10.2.1.120 - 52.216.211.88: 22     |
    +-------+---------------------+---------------+----------------+----------------+------------------------------------+

Top Blocked TCP Flows
---------------------

Blocked TCP connections.

PPL query::

    os> source=nfw_logs | WHERE `event.alert.action` = 'blocked' and `event.proto` = 'TCP' | STATS count() as Count by SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`, `event.dest_ip`, `event.dest_port` | EVAL `Src IP - Dst IP:Port` = CONCAT(`event.src_ip`, " - ", `event.dest_ip`, ": ", CAST(`event.dest_port` AS STRING)) | SORT - Count | HEAD 10;
    fetched rows / total rows = 4/4
    +-------+---------------------+---------------+----------------+----------------+------------------------------------+
    | Count | timestamp_span      | event.src_ip  | event.dest_ip  | event.dest_port| Src IP - Dst IP:Port               |
    |-------|---------------------|---------------|----------------|----------------|------------------------------------|
    | 1     | 2025-02-23 00:00:00 | 10.170.18.235 | 54.146.42.172  | 80             | 10.170.18.235 - 54.146.42.172: 80  |
    | 1     | 2025-02-23 00:00:00 | 10.170.18.235 | 54.242.115.112 | 443            | 10.170.18.235 - 54.242.115.112: 443|
    | 1     | 2025-03-27 00:00:00 | 10.2.1.120    | 52.216.211.88  | 22             | 10.2.1.120 - 52.216.211.88: 22     |
    +-------+---------------------+---------------+----------------+----------------+------------------------------------+

Top Blocked UDP Flows
---------------------

Blocked UDP connections.

PPL query::

    os> source=nfw_logs | WHERE `event.alert.action` = 'blocked' and `event.proto` = 'UDP' | STATS count() as Count by SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`, `event.dest_ip`, `event.dest_port` | EVAL `Src IP - Dst IP:Port` = CONCAT(`event.src_ip`, " - ", `event.dest_ip`, ": ", CAST(`event.dest_port` AS STRING)) | SORT - Count | HEAD 10;
    fetched rows / total rows = 1/1
    +-------+---------------------+---------------+----------------+----------------+---------------------------------+
    | Count | timestamp_span      | event.src_ip  | event.dest_ip  | event.dest_port| Src IP - Dst IP:Port            |
    |-------|---------------------|---------------|----------------|----------------|---------------------------------|
    | 2     | 2025-02-23 00:00:00 | 10.170.18.235 | 8.8.8.8        | 53             | 10.170.18.235 - 8.8.8.8: 53     |
    +-------+---------------------+---------------+----------------+----------------+---------------------------------+

Advanced TCP Analysis
=====================

Top TCP Flows SYN with SYN-ACK
------------------------------

TCP connections with SYN and ACK flags.

PPL query::

    os> source=nfw_logs | WHERE `event.proto` = 'TCP' and `event.tcp.syn` = "true" and `event.tcp.ack` = "true" | STATS count() as Count by SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`, `event.src_port`, `event.dest_ip`, `event.dest_port` | EVAL `Src IP:Port - Dst IP:Port` = CONCAT(`event.src_ip`, ": ", CAST(`event.src_port` AS STRING), " - ", `event.dest_ip`, ": ", CAST(`event.dest_port` AS STRING)) | SORT - Count | HEAD 10;
    fetched rows / total rows = 2/2
    +-------+---------------------+---------------+---------------+----------------+-----------------+--------------------------------------+
    | Count | timestamp_span      | event.src_ip  | event.src_port| event.dest_ip  | event.dest_port | Src IP:Port - Dst IP:Port            |
    |-------|---------------------|---------------|---------------|----------------|-----------------|--------------------------------------|
    | 1     | 2025-02-23 00:00:00 | 10.170.18.235 | 52610         | 8.8.8.8        | 80             | 10.170.18.235: 52610 - 8.8.8.8: 80    |
    | 1     | 2025-03-27 00:00:00 | 45.82.78.100  | 45550         | 10.2.1.120     | 8085           | 45.82.78.100: 45550 - 10.2.1.120: 8085|
    +-------+---------------------+---------------+---------------+----------------+-----------------+--------------------------------------+

Top Long-Lived TCP Flows
-------------------------

TCP connections active for extended periods.

PPL query::

    os> source=nfw_logs | WHERE `event.proto` = 'TCP' and `event.netflow.age` > 350 | STATS count() as Count by SPAN(`event.timestamp`, 2d) as timestamp_span, `event.src_ip`, `event.src_port`, `event.dest_ip`, `event.dest_port` | EVAL `Src IP:Port - Dst IP:Port` = CONCAT(`event.src_ip`, ": ", CAST(`event.src_port` AS STRING), " - ", `event.dest_ip`, ": ", CAST(`event.dest_port` AS STRING)) | SORT - Count | HEAD 10;
    fetched rows / total rows = 2/2
    +-------+---------------------+---------------+---------------+----------------+-----------------+------------------------------------------+
    | Count | timestamp_span      | event.src_ip  | event.src_port| event.dest_ip  | event.dest_port | Src IP:Port - Dst IP:Port                |
    |-------|---------------------|---------------|---------------|----------------|-----------------|------------------------------------------|
    | 1     | 2025-03-27 00:00:00 | 45.82.78.100  | 52610         | 10.2.1.120     | 8085            | 45.82.78.100: 52610 - 10.2.1.120: 8085   |
    | 1     | 2025-03-27 00:00:00 | 20.65.193.116 | 45550         | 10.2.1.120     | 1433            | 20.65.193.116: 45550 - 10.2.1.120: 1433  |
    +-------+---------------------+---------------+---------------+----------------+-----------------+------------------------------------------+