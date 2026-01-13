
# multisearch


The `multisearch` command runs multiple subsearches and merges their results. It allows you to combine data from different queries on the same or different sources. You can optionally apply subsequent processing, such as aggregation or sorting, to the combined results. Each subsearch can have different filtering criteria, data transformations, and field selections. 

Multisearch is particularly useful for comparative analysis, union operations, and creating comprehensive datasets from multiple search criteria. The command supports timestamp-based result interleaving when working with time-series data.

Use multisearch for:

* **Comparative analysis**: Compare metrics across different segments, regions, or time periods.
* **Success rate monitoring**: Calculate success rates by comparing successful to total operations.
* **Multi-source data combination**: Merge data from different indexes or apply different filters to the same source.
* **A/B testing analysis**: Combine results from different test groups for comparison.
* **Time-series data merging**: Interleave events from multiple sources based on timestamps.
 
  

## Syntax

The `multisearch` command has the following syntax:

```syntax
multisearch <subsearch1> <subsearch2> [<subsearch3> ...]
```

The following are examples of the `multisearch` command syntax:

```syntax
| multisearch [search source=table | where condition1] [search source=table | where condition2]
| multisearch [search source=index1 | fields field1, field2] [search source=index2 | fields field1, field2]
| multisearch [search source=table | where status="success"] [search source=table | where status="error"]
```

## Parameters

The `multisearch` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<subsearchN>` | Required | At least two subsearches are required. Each subsearch must be enclosed in square brackets and start with the `search` keyword (`[search source=index | <commands>]`). All PPL commands are supported within subsearches. |
| `<result-processing>` | Optional | Commands applied to the merged results after the multisearch operation (for example, `stats`, `sort`, or `head`). |  

## Example 1: Combining age groups for demographic analysis

This example demonstrates how to merge customers from different age segments into a unified dataset. It combines `young` and `adult` customers into a single result set and adds categorization labels for further analysis:
  
```ppl
| multisearch [search source=accounts
| where age < 30
| eval age_group = "young"
| fields firstname, age, age_group] [search source=accounts
| where age >= 30
| eval age_group = "adult"
| fields firstname, age, age_group]
| sort age
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------+-----+-----------+
| firstname | age | age_group |
|-----------+-----+-----------|
| Nanette   | 28  | young     |
| Amber     | 32  | adult     |
| Dale      | 33  | adult     |
| Hattie    | 36  | adult     |
+-----------+-----+-----------+
```
  

## Example 2: Segmenting accounts by balance tier

This example demonstrates how to create account segments based on balance thresholds for comparative analysis. It separates `high_balance` accounts from `regular` accounts and labels them for easy comparison:
  
```ppl
| multisearch [search source=accounts
| where balance > 20000
| eval query_type = "high_balance"
| fields firstname, balance, query_type] [search source=accounts
| where balance > 0 AND balance <= 20000
| eval query_type = "regular"
| fields firstname, balance, query_type]
| sort balance desc
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------+---------+--------------+
| firstname | balance | query_type   |
|-----------+---------+--------------|
| Amber     | 39225   | high_balance |
| Nanette   | 32838   | high_balance |
| Hattie    | 5686    | regular      |
| Dale      | 4180    | regular      |
+-----------+---------+--------------+
```
  

## Example 3: Merging time-series data from multiple sources

This example demonstrates how to combine time-series data from different sources while maintaining chronological order. The results are automatically sorted by timestamp to create a unified timeline:
  
```ppl
| multisearch [search source=time_data
| where category IN ("A", "B")] [search source=time_data2
| where category IN ("E", "F")]
| fields @timestamp, category, value, timestamp
| head 5
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 5/5
+---------------------+----------+-------+---------------------+
| @timestamp          | category | value | timestamp           |
|---------------------+----------+-------+---------------------|
| 2025-08-01 04:00:00 | E        | 2001  | 2025-08-01 04:00:00 |
| 2025-08-01 03:47:41 | A        | 8762  | 2025-08-01 03:47:41 |
| 2025-08-01 02:30:00 | F        | 2002  | 2025-08-01 02:30:00 |
| 2025-08-01 01:14:11 | B        | 9015  | 2025-08-01 01:14:11 |
| 2025-08-01 01:00:00 | E        | 2003  | 2025-08-01 01:00:00 |
+---------------------+----------+-------+---------------------+
```
  

## Example 4: Handling missing fields across subsearches

This example demonstrates how `multisearch` handles schema differences when subsearches return different fields. When one subsearch includes a field that others don't have, missing values are automatically filled with null values:
  
```ppl
| multisearch [search source=accounts
| where age < 30
| eval young_flag = "yes"
| fields firstname, age, young_flag] [search source=accounts
| where age >= 30
| fields firstname, age]
| sort age
```
  
The query returns the following results:
  
```text
fetched rows / total rows = 4/4
+-----------+-----+------------+
| firstname | age | young_flag |
|-----------+-----+------------|
| Nanette   | 28  | yes        |
| Amber     | 32  | null       |
| Dale      | 33  | null       |
| Hattie    | 36  | null       |
+-----------+-----+------------+
```
  

## Limitations

The `multisearch` command has the following limitations:

* At least two subsearches must be specified.
* When fields with the same name exist across subsearches but have incompatible types, the system automatically resolves conflicts by renaming the conflicting fields. The first occurrence retains the original name, while subsequent conflicting fields are renamed using a numeric suffix (for example, `age` becomes `age0`, `age1`, and so on). This ensures that all data is preserved while maintaining schema consistency.  