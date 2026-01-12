
# reverse

The `reverse` command reverses the display order of the search results. It returns the same results but in the opposite order.

> **Note**: The `reverse` command processes the entire dataset. If applied directly to millions of records, it consumes significant coordinating node memory resources. Only apply the `reverse` command to smaller datasets, typically after aggregation operations.

## Syntax

The `reverse` command has the following syntax:

```syntax
reverse
```

The `reverse` command takes no parameters.

## Example 1: Basic reverse operation  

The following query reverses the order of all documents in the results:
  
```ppl
source=accounts
| fields account_number, age
| reverse
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+----------------+-----+
| account_number | age |
|----------------+-----|
| 6              | 36  |
| 18             | 33  |
| 1              | 32  |
| 13             | 28  |
+----------------+-----+
```
  

## Example 2: Use the reverse and sort commands

The following query reverses results after sorting documents by age in ascending order, effectively implementing descending order:
  
```ppl
source=accounts
| sort age
| fields account_number, age
| reverse
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+----------------+-----+
| account_number | age |
|----------------+-----|
| 6              | 36  |
| 18             | 33  |
| 1              | 32  |
| 13             | 28  |
+----------------+-----+
```
  

## Example 3: Use the reverse and head commands  

The following query uses the `reverse` command together with the `head` command to retrieve the last two records from the original result order:
  
```ppl
source=accounts
| reverse
| head 2
| fields account_number, age
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 2/2
+----------------+-----+
| account_number | age |
|----------------+-----|
| 6              | 36  |
| 18             | 33  |
+----------------+-----+
```
  

## Example 4: Double reverse  

The following query shows that applying `reverse` twice returns documents in the original order:
  
```ppl
source=accounts
| reverse
| reverse
| fields account_number, age
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+----------------+-----+
| account_number | age |
|----------------+-----|
| 13             | 28  |
| 1              | 32  |
| 18             | 33  |
| 6              | 36  |
+----------------+-----+
```
  

## Example 5: Use the reverse command with a complex pipeline  

The following query uses the `reverse` command with filtering and field selection:
  
```ppl
source=accounts
| where age > 30
| fields account_number, age
| reverse
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 3/3
+----------------+-----+
| account_number | age |
|----------------+-----|
| 6              | 36  |
| 18             | 33  |
| 1              | 32  |
+----------------+-----+
```

