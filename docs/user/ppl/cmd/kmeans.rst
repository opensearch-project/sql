=================================
kmeans (deprecated by ml command)
=================================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
===========
| The ``kmeans`` command applies the kmeans algorithm in the ml-commons plugin on the search result returned by a PPL command.


Syntax
======
kmeans <centroids> <iterations> <distance_type>

* centroids: optional. The number of clusters you want to group your data points into. The default value is 2.
* iterations: optional. Number of iterations. The default value is 10.
* distance_type: optional. The distance type can be COSINE, L1, or EUCLIDEAN, The default type is EUCLIDEAN.


Example: Clustering of Iris Dataset
===================================

The example shows how to classify three Iris species (Iris setosa, Iris virginica and Iris versicolor) based on the combination of four features measured from each sample: the length and the width of the sepals and petals.

PPL query::

    > source=iris_data | fields sepal_length_in_cm, sepal_width_in_cm, petal_length_in_cm, petal_width_in_cm | kmeans centroids=3
    +--------------------+-------------------+--------------------+-------------------+-----------+
    | sepal_length_in_cm | sepal_width_in_cm | petal_length_in_cm | petal_width_in_cm | ClusterID |
    |--------------------+-------------------+--------------------+-------------------+-----------|
    | 5.1                | 3.5               | 1.4                | 0.2               | 1         |
    | 5.6                | 3.0               | 4.1                | 1.3               | 0         |
    | 6.7                | 2.5               | 5.8                | 1.8               | 2         |
    +--------------------+-------------------+--------------------+-------------------+-----------+


Limitations
===========
The ``kmeans`` command can only work with ``plugins.calcite.enabled=false``.
It means ``kmeans``  command cannot work together with new PPL commands/functions introduced in 3.0.0 and above.