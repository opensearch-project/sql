=============
kmeans
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``kmeans`` command applies kmeans algorithm in ml-commons plugin on the search result returned by a PPL command.


Syntax
======
kmeans <cluster-number>

* cluster-number: mandatory. The number of clusters you want to group your data points into.


Example: Clustering of Iris Dataset
===================================

The example shows how to classify three Iris species (Iris setosa, Iris virginica and Iris versicolor) based on the combination of four features measured from each sample: the length and the width of the sepals and petals.

PPL query::

    os> source=iris_data | fields sepal_length_in_cm, sepal_width_in_cm, petal_length_in_cm, petal_width_in_cm | kmeans 3
    +--------------------+-------------------+--------------------+-------------------+-----------+
    | sepal_length_in_cm | sepal_width_in_cm | petal_length_in_cm | petal_width_in_cm | ClusterID |
    |--------------------+-------------------+--------------------+-------------------+-----------|
    | 5.1                | 3.5               | 1.4                | 0.2               | 1         |
    | 5.6                | 3.0               | 4.1                | 1.3               | 0         |
    | 6.7                | 2.5               | 5.8                | 1.8               | 2         |
    +--------------------+-------------------+--------------------+-------------------+-----------+
