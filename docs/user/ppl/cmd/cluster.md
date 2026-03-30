# cluster

The `cluster` command groups documents into clusters based on text similarity using various clustering algorithms. Documents with similar text content are assigned to the same cluster and receive matching `cluster_id` values.

## Syntax

The `cluster` command has the following syntax:

```syntax
cluster <field> [t=<threshold>] [match=<algorithm>] [labelfield=<field>] [countfield=<field>]
```

## Parameters

The `cluster` command supports the following parameters.

| Parameter | Required/Optional | Description |
| --- | --- | --- |
| `<field>` | Required | The text field to use for clustering analysis. |
| `t` | Optional | Similarity threshold between 0.0 and 1.0. Documents with similarity above this threshold are grouped together. Default is `0.5`. |
| `match` | Optional | Clustering algorithm to use. Valid values are `termlist`, `termset`, `ngramset`. Default is `termlist`. |
| `labelfield` | Optional | Name of the field to store the cluster label. Default is `cluster_id`. |
| `countfield` | Optional | Name of the field to store the cluster size. Default is `cluster_size`. |


## Example 1: Basic text clustering

The following query groups log messages by similarity:

```ppl
source=logs
| cluster message
| fields message, cluster_id, cluster_size
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+------------------------+------------+--------------+
| message                | cluster_id | cluster_size |
|------------------------+------------+--------------|
| login successful       | 0          | 2            |
| login failed           | 1          | 1            |
| logout successful      | 0          | 2            |
| connection timeout     | 2          | 1            |
+------------------------+------------+--------------+
```


## Example 2: Custom similarity threshold

The following query uses a higher similarity threshold to create more distinct clusters:

```ppl
source=logs
| cluster message t=0.8
| fields message, cluster_id, cluster_size
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+------------------------+------------+--------------+
| message                | cluster_id | cluster_size |
|------------------------+------------+--------------|
| login successful       | 0          | 1            |
| login failed           | 1          | 1            |
| logout successful      | 2          | 1            |
| connection timeout     | 3          | 1            |
+------------------------+------------+--------------+
```


## Example 3: Different clustering algorithms

The following query uses the `termset` algorithm for more precise matching:

```ppl
source=logs
| cluster message match=termset
| fields message, cluster_id, cluster_size
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+------------------------+------------+--------------+
| message                | cluster_id | cluster_size |
|------------------------+------------+--------------|
| user authentication    | 0          | 2            |
| user authorization     | 0          | 2            |
| system error           | 1          | 1            |
| network failure        | 2          | 1            |
+------------------------+------------+--------------+
```


## Example 4: Custom field names

The following query uses custom field names for the cluster results:

```ppl
source=logs
| cluster message labelfield=log_group countfield=group_size
| fields message, log_group, group_size
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+------------------------+-----------+------------+
| message                | log_group | group_size |
|------------------------+-----------+------------|
| error processing       | 0         | 3          |
| error handling         | 0         | 3          |
| error occurred         | 0         | 3          |
| success message        | 1         | 1          |
+------------------------+-----------+------------+
```


## Example 5: Clustering with complex analysis

The following query combines clustering with additional analysis operations:

```ppl
source=application_logs
| cluster error_message t=0.7 match=ngramset
| stats count() as occurrence_count by cluster_id, cluster_size
| sort occurrence_count desc
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+------------+--------------+------------------+
| cluster_id | cluster_size | occurrence_count |
|------------+--------------+------------------|
| 0          | 5            | 5                |
| 1          | 3            | 3                |
| 2          | 1            | 1                |
+------------+--------------+------------------+
```

