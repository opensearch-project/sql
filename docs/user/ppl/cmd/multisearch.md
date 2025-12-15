# multisearch


The `multisearch` command runs multiple search subsearches and merges their results together. The command allows you to combine data from different queries on the same or different sources, and optionally apply subsequent processing to the combined search results.
Key aspects of `multisearch`:
1. Combines results from multiple search operations into a single result set.  
2. Each subsearch can have different filtering criteria, data transformations, and field selections.  
3. Results are merged and can be further processed with aggregations, sorting, and other PPL commands.  
4. Particularly useful for comparative analysis, union operations, and creating comprehensive datasets from multiple search criteria.  
5. Supports timestamp-based result interleaving when working with time-series data.  
  
Use Cases:
* **Comparative Analysis**: Compare metrics across different segments, regions, or time periods  
* **Success Rate Monitoring**: Calculate success rates by comparing successful compared to total operations  
* **Multi-source Data Combination**: Merge data from different indexes or apply different filters to the same source  
* **A/B Testing Analysis**: Combine results from different test groups for comparison  
* **Time-series Data Merging**: Interleave events from multiple sources based on timestamps  
  

## Syntax

Use the following syntax:

`multisearch <subsearch1> <subsearch2> <subsearch3> ...`
* subsearch1, subsearch2, ...: mandatory. At least two subsearches required. Each subsearch must be enclosed in square brackets and start with the `search` keyword. Format: `[search source=index | commands...]`. All PPL commands are supported within subsearches.  
* `result-processing`: optional. Commands applied to the merged results after the multisearch operation, such as `stats`, `sort`, `head`, etc.  
  

## Usage  

Basic multisearch
  
```
| multisearch [search source=table | where condition1] [search source=table | where condition2]
| multisearch [search source=index1 | fields field1, field2] [search source=index2 | fields field1, field2]
| multisearch [search source=table | where status="success"] [search source=table | where status="error"]
```
  

## Example 1: Basic age group analysis  

This example combines young and adult customers into a single result set for further analysis.
  
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
  
Expected output:
  
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
  

## Example 2: Success rate Pattern  

This example combines high-balance and all valid accounts for comparison analysis.
  
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
  
Expected output:
  
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
  

## Example 3: Timestamp interleaving  

This example combines time-series data from multiple sources with automatic timestamp-based ordering.
  
```ppl
| multisearch [search source=time_data
| where category IN ("A", "B")] [search source=time_data2
| where category IN ("E", "F")]
| fields @timestamp, category, value, timestamp
| head 5
```
  
Expected output:
  
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
  

## Example 4: Type compatibility - missing fields  

The following example PPL query demonstrates how missing fields are handled with NULL insertion.
  
```ppl
| multisearch [search source=accounts
| where age < 30
| eval young_flag = "yes"
| fields firstname, age, young_flag] [search source=accounts
| where age >= 30
| fields firstname, age]
| sort age
```
  
Expected output:
  
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

* **Minimum Subsearches**: At least two subsearches must be specified  
* **Schema Compatibility**: When fields with the same name exist across subsearches but have incompatible types, the system automatically resolves conflicts by renaming the conflicting fields. The first occurrence retains the original name, while subsequent conflicting fields are renamed with a numeric suffix (e.g., `age` becomes `age0`, `age1`, etc.). This ensures all data is preserved while maintaining schema consistency.  