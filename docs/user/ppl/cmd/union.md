
# union

The `union` command combines results from multiple datasets using UNION ALL semantics. It merges rows from two or more sources into a single result set, preserving all rows including duplicates. You can optionally apply subsequent processing, such as aggregation or sorting, to the combined results. Each dataset can be a subsearch with different filtering criteria, data transformations, and field selections, or a direct index reference.

Union is particularly useful for combining data from multiple sources, creating comprehensive datasets from different criteria, and consolidating results while handling schema differences through automatic type coercion.

Use union for:

* **Multi-source data combination**: Merge data from different indexes or apply different filters to the same source.
* **Dataset consolidation**: Combine results from different queries while preserving all rows including duplicates.
* **Flexible dataset patterns**: Use subsearches or direct index references with optional maxout control.
* **Schema unification**: Automatically handle different schemas with type coercion for conflicting field types and NULL-fill for missing fields.

## Syntax

The `union` command has the following syntax:

```syntax
union [maxout=<int>] <dataset1> <dataset2> [<dataset3> ...]
```

Each dataset can be:
- **Direct index reference**: `index_name`, `index_pattern*`, `index_alias`
- **Subsearch**: `[search source=index | <commands>]`

The following are examples of the `union` command syntax:

```syntax
| union logs-*, security-logs
| union [search source=accounts | where age > 30], [search source=accounts | where age < 30]
| union maxout=100 [search source=logs | fields user, action], [search source=events | fields user, action]
| union [search source=accounts | where status="active"], [search source=accounts | where status="pending"]
```

## Parameters

The `union` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `maxout` | Optional | Maximum number of results to return from the union operation. Default: unlimited (0). |
| `<datasetN>` | Required | At least two datasets are required. Each dataset can be either a subsearch enclosed in square brackets (`[search source=index | <commands>]`) or a direct index reference (for example, `accounts`, `logs-*`). All PPL commands are supported within subsearches. |
| `<result-processing>` | Optional | Commands applied to the merged results after the union operation (for example, `stats`, `sort`, or `head`). |

## Example 1: Combining age groups for demographic analysis

This example demonstrates how to merge customers from different age segments into a unified dataset. It combines `young` and `adult` customers into a single result set and adds categorization labels for further analysis:

```ppl
| union [search source=accounts
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


## Example 2: Combining filtered subsets from the same index

This example demonstrates how to combine multiple filtered subsets from the same index using union:

```ppl
| union [search source=accounts | where balance > 30000] [search source=accounts | where age < 30]
| fields firstname, age, balance
| sort balance desc
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+-----------+-----+---------+
| firstname | age | balance |
|-----------+-----+---------|
| Amber     | 32  | 39225   |
| Nanette   | 28  | 32838   |
| Nanette   | 28  | 32838   |
+-----------+-----+---------+
```

Note: Nanette appears twice because she meets both conditions (balance > 30000 AND age < 30), demonstrating UNION ALL semantics which preserve all rows including duplicates.


## Example 3: Mid-pipeline union (implicit first dataset)

This example demonstrates using union mid-pipeline where the upstream result is implicitly included as the first dataset:

```ppl
search source=accounts | where age > 30 | union [search source=accounts | where age < 30]
| fields firstname, age
| sort age
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-----------+-----+
| firstname | age |
|-----------+-----|
| Nanette   | 28  |
| Amber     | 32  |
| Dale      | 33  |
| Hattie    | 36  |
+-----------+-----+
```

Note: The upstream result `where age > 30` is automatically the first dataset, then unioned with `where age < 30`.


## Example 4: Using maxout option to limit results

This example demonstrates how to limit the total number of results returned from a union operation using the `maxout` option. Note that UNION ALL semantics preserve duplicate rows:

```ppl
| union maxout=3 [search source=accounts
| where balance > 20000] [search source=accounts
| where age > 30]
| fields firstname, age, balance
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+-----------+-----+---------+
| firstname | age | balance |
|-----------+-----+---------|
| Amber     | 32  | 39225   |
| Nanette   | 28  | 32838   |
| Amber     | 32  | 39225   |
+-----------+-----+---------+
```

Note: Amber appears twice because she meets both conditions (balance > 20000 AND age > 30), demonstrating UNION ALL semantics which preserve all rows including duplicates.


## Example 5: Segmenting accounts by balance tier

This example demonstrates how to create account segments based on balance thresholds for comparative analysis. It separates `high_balance` accounts from `regular` accounts and labels them for easy comparison:

```ppl
| union [search source=accounts
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


## Limitations

The `union` command has the following limitations:

* At least two datasets must be specified.
* When fields with the same name exist across datasets but have different types, the system automatically performs type coercion to find a common supertype:
  * **Compatible numeric types** → wider numeric type (for example, `INTEGER` and `BIGINT` coerce to `BIGINT`; `INTEGER` and `FLOAT` coerce to `FLOAT`)
  * **String types** → `VARCHAR` (for example, `CHAR` and `VARCHAR` coerce to `VARCHAR`)
  * **Temporal types** → wider temporal type (for example, `DATE` and `TIMESTAMP` coerce to `TIMESTAMP`)
  * **Incompatible types** (different type families) → `VARCHAR` fallback (for example, `INTEGER` and `VARCHAR` coerce to `VARCHAR`)
* Missing fields across datasets are automatically filled with `NULL` values to unify schemas.
* Direct index references must be valid index names, patterns, or aliases (for example, `accounts`, `logs-*`, `security-alias`).
