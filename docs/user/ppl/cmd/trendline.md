
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

## Example 1: Calculate the simple moving average for one field

The following query calculates the simple moving average for one field:
  
```ppl
source=accounts
| trendline sma(2, account_number) as an
| fields an
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+------+
| an   |
|------|
| null |
| 3.5  |
| 9.5  |
| 15.5 |
+------+
```
  

## Example 2: Calculate the simple moving average for multiple fields

The following query calculates the simple moving average for multiple fields:
  
```ppl
source=accounts
| trendline sma(2, account_number) as an sma(2, age) as age_trend
| fields an, age_trend
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+------+-----------+
| an   | age_trend |
|------+-----------|
| null | null      |
| 3.5  | 34.0      |
| 9.5  | 32.0      |
| 15.5 | 30.5      |
+------+-----------+
```
  

## Example 3: Calculate the simple moving average for one field without specifying an alias

The following query calculates the simple moving average for one field without specifying an alias:
  
```ppl
source=accounts
| trendline sma(2, account_number)
| fields account_number_trendline
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+--------------------------+
| account_number_trendline |
|--------------------------|
| null                     |
| 3.5                      |
| 9.5                      |
| 15.5                     |
+--------------------------+
```
  

## Example 4: Calculate the weighted moving average for one field

The following query calculates the weighted moving average for one field:
  
```ppl
source=accounts
| trendline wma(2, account_number)
| fields account_number_trendline
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+--------------------------+
| account_number_trendline |
|--------------------------|
| null                     |
| 4.333333333333333        |
| 10.666666666666666       |
| 16.333333333333332       |
+--------------------------+
```
  

## Limitations

The `trendline` command has the following limitations:

* The `trendline` command requires all values in the specified `<field>` parameter to be non-null. Any rows with `null` values in this field are automatically excluded from the command's output.