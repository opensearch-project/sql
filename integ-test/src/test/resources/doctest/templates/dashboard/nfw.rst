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

Traffic Analysis
================

Top Source IP by Packets
-------------------------

Source IPs generating the most network packets over time.

PPL query::

    os> source=nfw_logs | stats sum(`event.netflow.pkts`) as packet_count by span(`event.timestamp`, 2d) as timestamp_span, `event.src_ip` | rename `event.src_ip` as `Source IP` | sort - packet_count | head 10;
    fetched rows / total rows = 7/7
    +--------------+---------------------+----------------+
    | packet_count | timestamp_span      | Source IP      |
    |--------------|---------------------|----------------|
    | 53           | 2025-02-23 00:00:00 | 10.170.18.235  |
    | 11           | 2025-02-23 00:00:00 | 8.8.8.8        |
    | 11           | 2025-02-23 00:00:00 | 54.242.115.112 |
    | 1            | 2025-03-27 00:00:00 | 45.82.78.100   |
    | 1            | 2025-03-27 00:00:00 | 20.65.193.116  |
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
    | 5     | unknown         |
    | 3     | http            |
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
    | 9     | TCP         |
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

Flow Analysis
=============

Top Long-Lived TCP Flows
-------------------------

TCP connections active for extended periods.

PPL query::

    os> source=nfw_logs | WHERE `event.proto` = 'TCP' and `event.netflow.age` > 350 | STATS count() as Count by SPAN(`event.timestamp`, 2d), `event.src_ip`, `event.src_port`, `event.dest_ip`, `event.dest_port` | EVAL `Src IP:Port - Dst IP:Port` = CONCAT(`event.src_ip`, ": ", CAST(`event.src_port` AS STRING), " - ", `event.dest_ip`, ": ", CAST(`event.dest_port` AS STRING)) | SORT - Count | HEAD 10;
    fetched rows / total rows = 2/2
    +-------+---------------------+---------------+---------------+----------------+-----------------+----------------------------------+
    | Count | SPAN(event.timest.. | event.src_ip  | event.src_port| event.dest_ip  | event.dest_port | Src IP:Port - Dst IP:Port        |
    |-------|---------------------|---------------|---------------|----------------|-----------------|----------------------------------|
    | 1     | 2025-03-27 00:00:00 | 45.82.78.100  | 52610         | 10.2.1.120     | 8085            | 45.82.78.100: 52610 - 10.2.1... |
    | 1     | 2025-03-27 00:00:00 | 20.65.193.116 | 45550         | 10.2.1.120     | 1433            | 20.65.193.116: 45550 - 10.2.... |
    +-------+---------------------+---------------+---------------+----------------+-----------------+----------------------------------+