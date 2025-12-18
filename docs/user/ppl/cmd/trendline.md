# trendline  

## Description  

The `trendline` command calculates moving averages of fields.
## Syntax  

trendline [sort <[+\|-] sort-field>] \[sma\|wma\](number-of-datapoints, field) [as \<alias\>] [\[sma\|wma\](number-of-datapoints, field) [as \<alias\>]]...
* [+\|-]: optional. The plus [+] stands for ascending order and NULL/MISSING first and a minus [-] stands for descending order and NULL/MISSING last. **Default:** ascending order and NULL/MISSING first.  
* sort-field: mandatory when sorting is used. The field used to sort.  
* sma\|wma: mandatory. Simple Moving Average (sma) applies equal weighting to all values, Weighted Moving Average (wma) applies greater weight to more recent values.  
* number-of-datapoints: mandatory. The number of datapoints to calculate the moving average (must be greater than zero).  
* field: mandatory. The name of the field the moving average should be calculated for.  
* alias: optional. The name of the resulting column containing the moving average. **Default:** field name with "_trendline".  
  
## Example 1: Calculate the simple moving average on one field.  

This example shows how to calculate the simple moving average on one field.
  
```ppl
source=accounts
| trendline sma(2, account_number) as an
| fields an
```
  
Expected output:
  
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
  
## Example 2: Calculate the simple moving average on multiple fields.  

This example shows how to calculate the simple moving average on multiple fields.
  
```ppl
source=accounts
| trendline sma(2, account_number) as an sma(2, age) as age_trend
| fields an, age_trend
```
  
Expected output:
  
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
  
## Example 3: Calculate the simple moving average on one field without specifying an alias.  

This example shows how to calculate the simple moving average on one field.
  
```ppl
source=accounts
| trendline sma(2, account_number)
| fields account_number_trendline
```
  
Expected output:
  
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
  
## Example 4: Calculate the weighted moving average on one field.  

This example shows how to calculate the weighted moving average on one field.
  
```ppl
source=accounts
| trendline wma(2, account_number)
| fields account_number_trendline
```
  
Expected output:
  
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

The `trendline` command requires all values in the specified `field` to be non-null. Any rows with null values present in the calculation field will be automatically excluded from the command's output.