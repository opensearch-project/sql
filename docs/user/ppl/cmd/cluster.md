# cluster

The `cluster` command groups documents into clusters based on text similarity using various clustering algorithms. Documents with similar text content are assigned to the same cluster and receive matching `cluster_label` values.

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
| `t` | Optional | Similarity threshold between 0.0 and 1.0. Documents with similarity above this threshold are grouped together. Default is `0.8`. |
| `match` | Optional | Clustering algorithm to use. Valid values are `termlist`, `termset`, `ngramset`. Default is `termlist`. |
| `labelfield` | Optional | Name of the field to store the cluster label. Default is `cluster_label`. |
| `countfield` | Optional | Name of the field to store the cluster size. Default is `cluster_count`. |


## Example 1: Basic text clustering

The following query groups log messages by similarity:

```ppl
source=otellogs
| cluster body showcount=true
| fields body, cluster_label, cluster_count
| head 5
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+----------------------------------------------------------------------------------+---------------+---------------+
| body                                                                             | cluster_label | cluster_count |
|----------------------------------------------------------------------------------+---------------+---------------|
| null                                                                             | null          | 30            |
| null                                                                             | 1             | 1             |
| User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart | 2             | 1             |
| null                                                                             | 3             | 1             |
| Payment failed: Insufficient funds for user@example.com                          | 4             | 1             |
+----------------------------------------------------------------------------------+---------------+---------------+
```


## Example 2: Custom similarity threshold

The following query uses a higher similarity threshold to create more distinct clusters:

```ppl
source=otellogs
| cluster body t=0.9 showcount=true
| fields body, cluster_label, cluster_count
| head 8
```

The query returns the following results:

```text
fetched rows / total rows = 8/8
+--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------+
| body                                                                                                                                                   | cluster_label | cluster_count |
|--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------|
| null                                                                                                                                                   | null          | 30            |
| null                                                                                                                                                   | 1             | 1             |
| User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart                                                                       | 2             | 1             |
| null                                                                                                                                                   | 3             | 1             |
| Payment failed: Insufficient funds for user@example.com                                                                                                | 4             | 1             |
| null                                                                                                                                                   | 5             | 1             |
| Query contains Lucene special characters: +field:value -excluded AND (grouped OR terms) NOT "exact phrase" wildcard* fuzzy~2 /regex/ [range TO search] | 6             | 1             |
| null                                                                                                                                                   | 7             | 1             |
+--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------+
```


## Example 3: Different clustering algorithms

The following query uses the `termset` algorithm for more precise matching:

```ppl
source=otellogs
| cluster body match=termset showcount=true
| fields body, cluster_label, cluster_count
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------------------------------------------------------------------------+---------------+---------------+
| body                                                                             | cluster_label | cluster_count |
|----------------------------------------------------------------------------------+---------------+---------------|
| null                                                                             | null          | 30            |
| null                                                                             | 1             | 1             |
| User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart | 2             | 1             |
| null                                                                             | 3             | 1             |
+----------------------------------------------------------------------------------+---------------+---------------+
```


## Example 4: Custom field names

The following query uses custom field names for the cluster results:

```ppl
source=otellogs
| cluster body labelfield=log_group countfield=group_size showcount=true
| fields body, log_group, group_size
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------------------------------------------------------------------------+-----------+------------+
| body                                                                             | log_group | group_size |
|----------------------------------------------------------------------------------+-----------+------------|
| null                                                                             | null      | 30         |
| null                                                                             | 1         | 1          |
| User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart | 2         | 1          |
| null                                                                             | 3         | 1          |
+----------------------------------------------------------------------------------+-----------+------------+
```



