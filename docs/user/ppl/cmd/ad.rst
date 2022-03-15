=============
ad
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``ad`` command applies Random Cut Forest (RCF) algorithm in ml-commons plugin on the search result returned by a PPL command. Based on the input, two types of RCF algorithms will be utilized: fixed in time RCF for processing time-series data, batch RCF for processing non-time-series data.


Fixed In Time RCF For Time-series Data Command Syntax
=====================================================
ad <shingle_size> <time_decay> <time_field>

* shingle_size: optional. A shingle is a consecutive sequence of the most recent records. The default value is 8.
* time_decay: optional. It specifies how much of the recent past to consider when computing an anomaly score. The default value is 0.001.
* time_field: mandatory. It specifies the time filed for RCF to use as time-series data.


Batch RCF for Non-time-series Data Command Syntax
=================================================
ad <shingle_size> <time_decay>

* shingle_size: optional. A shingle is a consecutive sequence of the most recent records. The default value is 8.
* time_decay: optional. It specifies how much of the recent past to consider when computing an anomaly score. The default value is 0.001.


Example1: Detecting events in New York City from taxi ridership data with time-series data
==========================================================================================

The example trains a RCF model and use the model to detect anomalies in the time-series ridership data.

PPL query::

    os> source=nyc_taxi | fields value, timestamp | AD time_field='timestamp' | where value=10844.0'
    +----------+---------------+-------+---------------+
    | value    | timestamp     | score | anomaly_grade |
    |----------+---------------+-------+---------------|
    | 10844.0  | 1404172800000 | 0.0   |  0.0          |
    +----------+---------------+-------+---------------+


Example2: Detecting events in New York City from taxi ridership data with non-time-series data
==============================================================================================

The example trains a RCF model and use the model to detect anomalies in the non-time-series ridership data.

PPL query::

    os> source=nyc_taxi | fields value | AD | where value=10844.0'
    +----------+--------+-----------+
    | value    | score  | anomalous |
    |----------+--------+-----------|
    | 10844.0  | 0.0    | false     |
    +----------+--------+-----------+
