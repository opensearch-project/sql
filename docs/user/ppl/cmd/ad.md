
# ad (Deprecated)

> **Warning**: The `ad` command is deprecated in favor of the [`ml` command](./ml.md).

The `ad` command applies the Random Cut Forest (RCF) algorithm in the ML Commons plugin to the search results returned by a PPL command. The command provides two anomaly detection approaches:

- [Anomaly detection for time-series data](#anomaly-detection-for-time-series-data) using the fixed-in-time RCF algorithm
- [Anomaly detection for non-time-series data](#anomaly-detection-for-non-time-series-data) using the batch RCF algorithm

> **Note**: To use the `ad` command, `plugins.calcite.enabled` must be set to `false`.

## Syntax

The `ad` command has two different syntax variants, depending on the algorithm type.

### Anomaly detection for time-series data

Use this syntax to detect anomalies in time-series data. This method uses the fixed-in-time RCF algorithm, which is optimized for sequential data patterns.

The fixed-in-time RCF `ad` command has the following syntax:

```syntax
ad [number_of_trees] [shingle_size] [sample_size] [output_after] [time_decay] [anomaly_rate] <time_field> [date_format] [time_zone] [category_field]
```

### Parameters

The fixed-in-time RCF algorithm supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `time_field` | Required | The time field for RCF to use as time-series data. |
| `number_of_trees` | Optional | The number of trees in the forest. Default is `30`. |
| `shingle_size` | Optional | The number of records in a shingle. A shingle is a consecutive sequence of the most recent records. Default is `8`. |
| `sample_size` | Optional | The sample size used by the stream samplers in this forest. Default is `256`. |
| `output_after` | Optional | The number of points required by the stream samplers before results are returned. Default is `32`. |
| `time_decay` | Optional | The decay factor used by the stream samplers in this forest. Default is `0.0001`. |
| `anomaly_rate` | Optional | The anomaly rate. Default is `0.005`. |
| `date_format` | Optional | The format used for the `time_field` field. Default is `yyyy-MM-dd HH:mm:ss`. |
| `time_zone` | Optional | The time zone for the `time_field` field. Default is `UTC`. |
| `category_field` | Optional | The category field used to group input values. The predict operation is applied to each category independently. |  
  

### Anomaly detection for non-time-series data

Use this syntax to detect anomalies in data where the order doesn't matter. This method uses the batch RCF algorithm, which is optimized for independent data points.

The batch RCF `ad` command has the following syntax:

```syntax
ad [number_of_trees] [sample_size] [output_after] [training_data_size] [anomaly_score_threshold] [category_field]
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
  

## Example 1: Detecting events in New York City taxi ridership time-series data

The following examples use the `nyc_taxi` dataset, which contains New York City taxi ridership data with fields including `value` (number of rides), `timestamp` (time of measurement), and `category` (time period classifications such as 'day' and 'night').

This example trains an RCF model and uses it to detect anomalies in time-series ridership data:
  
```ppl ignore
source=nyc_taxi
| fields value, timestamp
| AD time_field='timestamp'
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
  

## Example 2: Detecting events in New York City taxi ridership time-series data by category

This example trains an RCF model and uses it to detect anomalies in time-series ridership data across multiple category values:
  
```ppl ignore
source=nyc_taxi
| fields category, value, timestamp
| AD time_field='timestamp' category_field='category'
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
  

## Example 3: Detecting events in New York City taxi ridership non-time-series data

This example trains an RCF model and uses it to detect anomalies in non-time-series ridership data:
  
```ppl ignore
source=nyc_taxi
| fields value
| AD
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
  

## Example 4: Detecting events in New York City taxi ridership non-time-series data by category

This example trains an RCF model and uses it to detect anomalies in non-time-series ridership data across multiple category values:
  
```ppl ignore
source=nyc_taxi
| fields category, value
| AD category_field='category'
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
  

