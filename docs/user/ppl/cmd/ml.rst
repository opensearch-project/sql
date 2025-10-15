==
ml
==

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| Use the ``ml`` command to train/predict/train and predict on any algorithm in the ml-commons plugin on the search result returned by a PPL command.

Syntax
======

AD - Fixed In Time RCF For Time-series Data:
---------------------------------------------

ml action='train' algorithm='rcf' <number_of_trees> <shingle_size> <sample_size> <output_after> <time_decay> <anomaly_rate> <time_field> <date_format> <time_zone>

* number_of_trees: optional integer. Number of trees in the forest. **Default:** 30.
* shingle_size: optional integer. A shingle is a consecutive sequence of the most recent records. **Default:** 8.
* sample_size: optional integer. The sample size used by stream samplers in this forest. **Default:** 256.
* output_after: optional integer. The number of points required by stream samplers before results are returned. **Default:** 32.
* time_decay: optional double. The decay factor used by stream samplers in this forest. **Default:** 0.0001.
* anomaly_rate: optional double. The anomaly rate. **Default:** 0.005.
* time_field: mandatory string. It specifies the time field for RCF to use as time-series data.
* date_format: optional string. It's used for formatting time_field field. **Default:** "yyyy-MM-dd HH:mm:ss".
* time_zone: optional string. It's used for setting time zone for time_field field. **Default:** UTC.
* category_field: optional string. It specifies the category field used to group inputs. Each category will be independently predicted.

AD - Batch RCF for Non-time-series Data:
-----------------------------------------

ml action='train' algorithm='rcf' <number_of_trees> <sample_size> <output_after> <training_data_size> <anomaly_score_threshold>

* number_of_trees: optional integer. Number of trees in the forest. **Default:** 30.
* sample_size: optional integer. Number of random samples given to each tree from the training data set. **Default:** 256.
* output_after: optional integer. The number of points required by stream samplers before results are returned. **Default:** 32.
* training_data_size: optional integer. **Default:** size of your training data set.
* anomaly_score_threshold: optional double. The threshold of anomaly score. **Default:** 1.0.
* category_field: optional string. It specifies the category field used to group inputs. Each category will be independently predicted.

KMEANS:
-------

ml action='train' algorithm='kmeans' <centroids> <iterations> <distance_type>

* centroids: optional integer. The number of clusters you want to group your data points into. **Default:** 2.
* iterations: optional integer. Number of iterations. **Default:** 10.
* distance_type: optional string. The distance type can be COSINE, L1, or EUCLIDEAN. **Default:** EUCLIDEAN.

Example 1: Detecting events in New York City from taxi ridership data with time-series data
===========================================================================================

This example trains an RCF model and uses the model to detect anomalies in the time-series ridership data.

PPL query::

    os> source=nyc_taxi | fields value, timestamp | ml action='train' algorithm='rcf' time_field='timestamp' | where value=10844.0
    fetched rows / total rows = 1/1
    +---------+---------------------+-------+---------------+
    | value   | timestamp           | score | anomaly_grade |
    |---------+---------------------+-------+---------------|
    | 10844.0 | 2014-07-01 00:00:00 | 0.0   | 0.0           |
    +---------+---------------------+-------+---------------+

Example 2: Detecting events in New York City from taxi ridership data with time-series data independently with each category
============================================================================================================================

This example trains an RCF model and uses the model to detect anomalies in the time-series ridership data with multiple category values.

PPL query::

    os> source=nyc_taxi | fields category, value, timestamp | ml action='train' algorithm='rcf' time_field='timestamp' category_field='category' | where value=10844.0 or value=6526.0
    fetched rows / total rows = 2/2
    +----------+---------+---------------------+-------+---------------+
    | category | value   | timestamp           | score | anomaly_grade |
    |----------+---------+---------------------+-------+---------------|
    | night    | 10844.0 | 2014-07-01 00:00:00 | 0.0   | 0.0           |
    | day      | 6526.0  | 2014-07-01 06:00:00 | 0.0   | 0.0           |
    +----------+---------+---------------------+-------+---------------+


Example 3: Detecting events in New York City from taxi ridership data with non-time-series data
===============================================================================================

This example trains an RCF model and uses the model to detect anomalies in the non-time-series ridership data.

PPL query::

    os> source=nyc_taxi | fields value | ml action='train' algorithm='rcf' | where value=10844.0
    fetched rows / total rows = 1/1
    +---------+-------+-----------+
    | value   | score | anomalous |
    |---------+-------+-----------|
    | 10844.0 | 0.0   | False     |
    +---------+-------+-----------+

Example 4: Detecting events in New York City from taxi ridership data with non-time-series data independently with each category
================================================================================================================================

This example trains an RCF model and uses the model to detect anomalies in the non-time-series ridership data with multiple category values.

PPL query::

    os> source=nyc_taxi | fields category, value | ml action='train' algorithm='rcf' category_field='category' | where value=10844.0 or value=6526.0
    fetched rows / total rows = 2/2
    +----------+---------+-------+-----------+
    | category | value   | score | anomalous |
    |----------+---------+-------+-----------|
    | night    | 10844.0 | 0.0   | False     |
    | day      | 6526.0  | 0.0   | False     |
    +----------+---------+-------+-----------+

Example 5: KMEANS - Clustering of Iris Dataset
===============================================

This example shows how to use KMEANS to classify three Iris species (Iris setosa, Iris virginica and Iris versicolor) based on the combination of four features measured from each sample: the length and the width of the sepals and petals.

PPL query::

    os> source=iris_data | fields sepal_length_in_cm, sepal_width_in_cm, petal_length_in_cm, petal_width_in_cm | ml action='train' algorithm='kmeans' centroids=3
    +--------------------+-------------------+--------------------+-------------------+-----------+
    | sepal_length_in_cm | sepal_width_in_cm | petal_length_in_cm | petal_width_in_cm | ClusterID |
    |--------------------+-------------------+--------------------+-------------------+-----------|
    | 5.1                | 3.5               | 1.4                | 0.2               | 1         |
    | 5.6                | 3.0               | 4.1                | 1.3               | 0         |
    | 6.7                | 2.5               | 5.8                | 1.8               | 2         |
    +--------------------+-------------------+--------------------+-------------------+-----------+


Limitations
===========
The ``ml`` command can only work with ``plugins.calcite.enabled=false``.
