
# ml

The `ml` command applies machine learning (ML) algorithms from the ML Commons plugin to the search results returned by a PPL command. It supports various ML operations, including anomaly detection and clustering. The command can perform train, predict, or combined train-and-predict operations, depending on the algorithm and specified action.

> **Note**: To use the `ml` command, `plugins.calcite.enabled` must be set to `false`.

The `ml` command supports the following algorithms:

- **Random Cut Forest (RCF)** for anomaly detection, with support for both time-series and non-time-series data

- **K-means** for clustering data points into groups

## Syntax

The `ml` command supports different syntax options, depending on the algorithm.

### Anomaly detection for time-series data

Use this syntax to detect anomalies in time-series data. This method uses the RCF algorithm optimized for sequential data patterns:

```syntax
ml action='train' algorithm='rcf' <number_of_trees> <shingle_size> <sample_size> <output_after> <time_decay> <anomaly_rate> <time_field> <date_format> <time_zone>
```

### Parameters

The fixed-in-time RCF algorithm supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `number_of_trees` | Optional | The number of trees in the forest. Default is `30`. |
| `shingle_size` | Optional | The number of records in a shingle. A shingle is a consecutive sequence of the most recent records. Default is `8`. |
| `sample_size` | Optional | The sample size used by the stream samplers in this forest. Default is `256`. |
| `output_after` | Optional | The number of points required by the stream samplers before results are returned. Default is `32`. |
| `time_decay` | Optional | The decay factor used by the stream samplers in this forest. Default is `0.0001`. |
| `anomaly_rate` | Optional | The anomaly rate. Default is `0.005`. |
| `time_field` | Required | The time field for RCF to use as time-series data. |
| `date_format` | Optional | The format for the `time_field`. Default is `yyyy-MM-dd HH:mm:ss`. |
| `time_zone` | Optional | The time zone for the `time_field`. Default is `UTC`. |
| `category_field` | Optional | The category field used to group input values. The predict operation is applied to each category independently. |

### Anomaly detection for non-time-series data

Use this syntax to detect anomalies in data where the order doesn't matter. This method uses the RCF algorithm optimized for independent data points:

```syntax
ml action='train' algorithm='rcf' <number_of_trees> <sample_size> <output_after> <training_data_size> <anomaly_score_threshold>
```

### Parameters

The batch RCF algorithm supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `number_of_trees` | Optional | The number of trees in the forest. Default is `30`. |
| `sample_size` | Optional | The number of random samples provided to each tree from the training dataset. Default is `256`. |
| `output_after` | Optional | The number of points required by the stream samplers before results are returned. Default is `32`. |
| `training_data_size` | Optional | The size of the training dataset. Default is the full dataset size. |
| `anomaly_score_threshold` | Optional | The anomaly score threshold. Default is `1.0`. |
| `category_field` | Optional | The category field used to group input values. The predict operation is applied to each category independently. |  
  

### K-means clustering

Use this syntax to group data points into clusters based on similarity:

```syntax
ml action='train' algorithm='kmeans' <centroids> <iterations> <distance_type>
```

### Parameters

The k-means clustering algorithm supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `centroids` | Optional | The number of clusters to group data points into. Default is `2`. |
| `iterations` | Optional | The number of iterations. Default is `10`. |
| `distance_type` | Optional | The distance type. Valid values are `COSINE`, `L1`, and `EUCLIDEAN`. Default is `EUCLIDEAN`. |  
  

## Example 1: Time-series anomaly detection

This example trains an RCF model and uses it to detect anomalies in time-series ridership data:
  
```ppl
source=nyc_taxi
| fields value, timestamp
| ml action='train' algorithm='rcf' time_field='timestamp'
| where value=10844.0
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------+---------------------+-------+---------------+
| value   | timestamp           | score | anomaly_grade |
|---------+---------------------+-------+---------------|
| 10844.0 | 2014-07-01 00:00:00 | 0.0   | 0.0           |
+---------+---------------------+-------+---------------+
```
  

## Example 2: Time-series anomaly detection by category

This example trains an RCF model and uses it to detect anomalies in time-series ridership data across multiple category values:
  
```ppl
source=nyc_taxi
| fields category, value, timestamp
| ml action='train' algorithm='rcf' time_field='timestamp' category_field='category'
| where value=10844.0 or value=6526.0
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+----------+---------+---------------------+-------+---------------+
| category | value   | timestamp           | score | anomaly_grade |
|----------+---------+---------------------+-------+---------------|
| night    | 10844.0 | 2014-07-01 00:00:00 | 0.0   | 0.0           |
| day      | 6526.0  | 2014-07-01 06:00:00 | 0.0   | 0.0           |
+----------+---------+---------------------+-------+---------------+
```
  

## Example 3: Non-time-series anomaly detection

This example trains an RCF model and uses it to detect anomalies in non-time-series ridership data:
  
```ppl
source=nyc_taxi
| fields value
| ml action='train' algorithm='rcf'
| where value=10844.0
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 1/1
+---------+-------+-----------+
| value   | score | anomalous |
|---------+-------+-----------|
| 10844.0 | 0.0   | False     |
+---------+-------+-----------+
```
  

## Example 4: Non-time-series anomaly detection by category

This example trains an RCF model and uses it to detect anomalies in non-time-series ridership data across multiple category values:
  
```ppl
source=nyc_taxi
| fields category, value
| ml action='train' algorithm='rcf' category_field='category'
| where value=10844.0 or value=6526.0
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+----------+---------+-------+-----------+
| category | value   | score | anomalous |
|----------+---------+-------+-----------|
| night    | 10844.0 | 0.0   | False     |
| day      | 6526.0  | 0.0   | False     |
+----------+---------+-------+-----------+
```
  

## Example 5: K-means clustering of the Iris dataset  

This example uses k-means clustering to classify three Iris species (Iris setosa, Iris virginica, and Iris versicolor) based on the combination of four features measured from each sample (the lengths and widths of sepals and petals):
  
```ppl
source=iris_data
| fields sepal_length_in_cm, sepal_width_in_cm, petal_length_in_cm, petal_width_in_cm
| ml action='train' algorithm='kmeans' centroids=3
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
  

