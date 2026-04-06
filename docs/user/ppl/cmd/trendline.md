
# trendline

The `trendline` command calculates moving averages of fields.

## Syntax

The `trendline` command has the following syntax:

```syntax
trendline [sort [+|-] <sort-field>] (sma | wma)(<number-of-datapoints>, <field>) [as <alias>] [(sma | wma)(<number-of-datapoints>, <field>) [as <alias>]]...
```

## Parameters

The `trendline` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `[+|-]` | Optional | The sort order for the data. `+` specifies ascending order with `NULL`/`MISSING` first, `-` specifies descending order with `NULL`/`MISSING` last. Default is `+`. |
| `<sort-field>` | Required | The field used to sort the data. |
| `(sma | wma)` | Required | The type of moving average to calculate. `sma` calculates the simple moving average with equal weighting for all values, `wma` calculates the weighted moving average with more weight given to recent values. |
| `number-of-datapoints` | Required | The number of data points used to calculate the moving average. Must be greater than zero. |
| `<field>` | Required | The field for which the moving average is calculated. |
| `<alias>` | Optional | The name of the resulting column containing the moving average. Default is the `<field>` name with `_trendline` appended. |

## Example 1: Track whether severity is escalating over time

The following query calculates a 3-point simple moving average of `severityNumber`:
  
```ppl
source=otellogs
| sort `@timestamp`
| trendline sma(3, severityNumber) as sev_trend
| fields severityText, severityNumber, sev_trend
| head 6
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 6/6
+--------------+----------------+--------------------+
| severityText | severityNumber | sev_trend          |
|--------------+----------------+--------------------|
| INFO         | 9              | null               |
| INFO         | 9              | null               |
| WARN         | 13             | 10.333333333333334 |
| ERROR        | 17             | 13.0               |
| DEBUG        | 5              | 11.666666666666666 |
| ERROR        | 17             | 13.0               |
+--------------+----------------+--------------------+
```
  

## Example 2: Use weighted moving average for recent-biased trends

The following query calculates a weighted moving average, which gives more weight to recent values:
  
```ppl
source=otellogs
| sort `@timestamp`
| trendline wma(3, severityNumber) as wma_trend
| fields severityText, severityNumber, wma_trend
| head 6
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 6/6
+--------------+----------------+--------------------+
| severityText | severityNumber | wma_trend          |
|--------------+----------------+--------------------|
| INFO         | 9              | null               |
| INFO         | 9              | null               |
| WARN         | 13             | 11.0               |
| ERROR        | 17             | 14.333333333333334 |
| DEBUG        | 5              | 10.333333333333334 |
| ERROR        | 17             | 13.0               |
+--------------+----------------+--------------------+
```
  
