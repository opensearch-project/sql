# ad (deprecated by ml command)


The `ad` command applies Random Cut Forest (RCF) algorithm in the ml-commons plugin on the search results returned by a PPL command. Based on the input, the command uses two types of RCF algorithms: fixed-in-time RCF for processing time-series data, batch RCF for processing non-time-series data.

## Syntax

The following sections describe the syntax for each RCF algorithm type.

## Fixed in time RCF for time-series data

`ad [number_of_trees] [shingle_size] [sample_size] [output_after] [time_decay] [anomaly_rate] <time_field> [date_format] [time_zone] [category_field]`
* `number_of_trees`: optional. Number of trees in the forest. **Default:** 30.  
* `shingle_size`: optional. A shingle is a consecutive sequence of the most recent records. **Default:** 8.  
* `sample_size`: optional. The sample size used by stream samplers in this forest. **Default:** 256.  
* `output_after`: optional. The number of points required by stream samplers before results are returned. **Default:** 32.  
* `time_decay`: optional. The decay factor used by stream samplers in this forest. **Default:** 0.0001.  
* `anomaly_rate`: optional. The anomaly rate. **Default:** 0.005.  
* `time_field`: mandatory. Specifies the time field for RCF to use as time-series data.  
* `date_format`: optional. Used for formatting time_field. **Default:** "yyyy-MM-dd HH:mm:ss".  
* `time_zone`: optional. Used for setting time zone for time_field. **Default:** "UTC".  
* `category_field`: optional. Specifies the category field used to group inputs. Each category will be independently predicted.  
  

## Batch RCF for non-time-series data

`ad [number_of_trees] [sample_size] [output_after] [training_data_size] [anomaly_score_threshold] [category_field]`
* `number_of_trees`: optional. Number of trees in the forest. **Default:** 30.  
* `sample_size`: optional. Number of random samples given to each tree from the training dataset. **Default:** 256.  
* `output_after`: optional. The number of points required by stream samplers before results are returned. **Default:** 32.  
* `training_data_size`: optional. **Default:** size of your training dataset.  
* `anomaly_score_threshold`: optional. The threshold of anomaly score. **Default:** 1.0.  
* `category_field`: optional. Specifies the category field used to group inputs. Each category will be independently predicted.  
  

## Example 1: Detecting events in New York City from taxi ridership data with time-series data  

This example trains an RCF model and uses the model to detect anomalies in the time-series ridership data.
  
```ppl ignore
source=nyc_taxi
| fields value, timestamp
| AD time_field='timestamp'
| where value=10844.0
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+---------+---------------------+-------+---------------+
| value   | timestamp           | score | anomaly_grade |
|---------+---------------------+-------+---------------|
| 10844.0 | 2014-07-01 00:00:00 | 0.0   | 0.0           |
+---------+---------------------+-------+---------------+
```
  

## Example 2: Detecting events in New York City from taxi ridership data with time-series data independently with each category  

This example trains an RCF model and uses the model to detect anomalies in the time-series ridership data with multiple category values.
  
```ppl ignore
source=nyc_taxi
| fields category, value, timestamp
| AD time_field='timestamp' category_field='category'
| where value=10844.0 or value=6526.0
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+----------+---------+---------------------+-------+---------------+
| category | value   | timestamp           | score | anomaly_grade |
|----------+---------+---------------------+-------+---------------|
| night    | 10844.0 | 2014-07-01 00:00:00 | 0.0   | 0.0           |
| day      | 6526.0  | 2014-07-01 06:00:00 | 0.0   | 0.0           |
+----------+---------+---------------------+-------+---------------+
```
  

## Example 3: Detecting events in New York City from taxi ridership data with non-time-series data  

This example trains an RCF model and uses the model to detect anomalies in the non-time-series ridership data.
  
```ppl ignore
source=nyc_taxi
| fields value
| AD
| where value=10844.0
```
  
Expected output:
  
```text
fetched rows / total rows = 1/1
+---------+-------+-----------+
| value   | score | anomalous |
|---------+-------+-----------|
| 10844.0 | 0.0   | False     |
+---------+-------+-----------+
```
  

## Example 4: Detecting events in New York City from taxi ridership data with non-time-series data independently with each category  

This example trains an RCF model and uses the model to detect anomalies in the non-time-series ridership data with multiple category values.
  
```ppl ignore
source=nyc_taxi
| fields category, value
| AD category_field='category'
| where value=10844.0 or value=6526.0
```
  
Expected output:
  
```text
fetched rows / total rows = 2/2
+----------+---------+-------+-----------+
| category | value   | score | anomalous |
|----------+---------+-------+-----------|
| night    | 10844.0 | 0.0   | False     |
| day      | 6526.0  | 0.0   | False     |
+----------+---------+-------+-----------+
```
  

## Limitations  

The `ad` command can only work with `plugins.calcite.enabled=false`.