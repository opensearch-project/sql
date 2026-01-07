
# kmeans (Deprecated)

The `kmeans` command is deprecated in favor of the [`ml` command](ml.md).
{: .warning}

The `kmeans` command applies the k-means algorithm in the ML Commons plugin on the search results returned by a PPL command.

To use the `kmeans` command, `plugins.calcite.enabled` must be set to `false`.
{: .note}

## Syntax

The `kmeans` command has the following syntax:

```syntax
kmeans <centroids> <iterations> <distance_type>
```

## Parameters

The `kmeans` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<centroids>` | Optional | The number of clusters to group data points into. Default is `2`. |
| `<iterations>` | Optional | The number of iterations. Default is `10`. |
| `<distance_type>` | Optional | The distance type. Valid values are `COSINE`, `L1`, and `EUCLIDEAN`. Default is `EUCLIDEAN`. |  
  

## Example: Clustering of the Iris dataset  

The following query classifies three Iris species (Iris setosa, Iris virginica, and Iris versicolor) based on the combination of four features measured from each sample (the lengths and widths of sepals and petals):
  
```ppl
source=iris_data
| fields sepal_length_in_cm, sepal_width_in_cm, petal_length_in_cm, petal_width_in_cm
| kmeans centroids=3
```
  
The query returns the following results:
  
```text
+--------------------+-------------------+--------------------+-------------------+-----------+
| sepal_length_in_cm | sepal_width_in_cm | petal_length_in_cm | petal_width_in_cm | ClusterID |
|--------------------+-------------------+--------------------+-------------------+-----------|
| 5.1                | 3.5               | 1.4                | 0.2               | 1         |
| 5.6                | 3.0               | 4.1                | 1.3               | 0         |
| 6.7                | 2.5               | 5.8                | 1.8               | 2         |
+--------------------+-------------------+--------------------+-------------------+-----------+
```
  

