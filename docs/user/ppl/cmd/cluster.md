# cluster

The `cluster` command groups documents into clusters based on text similarity using various clustering algorithms. Documents with similar text content are assigned to the same cluster and receive matching `cluster_label` values. Rows where the source field is null are excluded from the results.

## Syntax

The `cluster` command has the following syntax:

```syntax
cluster <field> [t=<threshold>] [match=<algorithm>] [labelfield=<field>] [countfield=<field>] [showcount=<bool>] [labelonly=<bool>] [delims=<string>]
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
| `showcount` | Optional | Whether to include the cluster count field in the output. Default is `false`. |
| `labelonly` | Optional | When `true`, keeps all rows and only adds the cluster label. When `false` (default), deduplicates by keeping only the first representative row per cluster. Default is `false`. |
| `delims` | Optional | Delimiter characters used for tokenization. Default is `non-alphanumeric` (splits on any non-alphanumeric character). |


## Example 1: Basic text clustering

The following query groups log messages by similarity using default settings:

```ppl
source=otellogs
| cluster body
| fields body, cluster_label
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+
| body                                                                                                                                                   | cluster_label |
|--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------|
| User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart                                                                       | 1             |
| Payment failed: Insufficient funds for user@example.com                                                                                                | 2             |
| Query contains Lucene special characters: +field:value -excluded AND (grouped OR terms) NOT "exact phrase" wildcard* fuzzy~2 /regex/ [range TO search] | 3             |
| 192.168.1.1 - - [15/Jan/2024:10:30:03 +0000] "GET /api/products?search=laptop&category=electronics HTTP/1.1" 200 1234 "-" "Mozilla/5.0"                | 4             |
+--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+
```


## Example 2: Clustering with showcount

The following query uses the `termset` algorithm with a lower threshold to group more messages together, and includes the cluster count:

```ppl
source=otellogs
| cluster body match=termset t=0.3 showcount=true
| fields body, cluster_label, cluster_count
| head 5
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------+
| body                                                                                                                                                   | cluster_label | cluster_count |
|--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------|
| User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart                                                                       | 1             | 3             |
| Payment failed: Insufficient funds for user@example.com                                                                                                | 2             | 3             |
| Query contains Lucene special characters: +field:value -excluded AND (grouped OR terms) NOT "exact phrase" wildcard* fuzzy~2 /regex/ [range TO search] | 3             | 1             |
| Email notification sent to john.doe+newsletter@company.com with subject: 'Welcome! Your order #12345 is confirmed'                                     | 4             | 1             |
| Database connection pool exhausted: postgresql://db.example.com:5432/production                                                                        | 5             | 1             |
+--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------+
```


## Example 3: Custom similarity threshold

The following query uses a higher similarity threshold to create more distinct clusters:

```ppl
source=otellogs
| cluster body t=0.9 showcount=true
| fields body, cluster_label, cluster_count
| head 5
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------+
| body                                                                                                                                                   | cluster_label | cluster_count |
|--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------|
| User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart                                                                       | 1             | 1             |
| Payment failed: Insufficient funds for user@example.com                                                                                                | 2             | 1             |
| Query contains Lucene special characters: +field:value -excluded AND (grouped OR terms) NOT "exact phrase" wildcard* fuzzy~2 /regex/ [range TO search] | 3             | 1             |
| 192.168.1.1 - - [15/Jan/2024:10:30:03 +0000] "GET /api/products?search=laptop&category=electronics HTTP/1.1" 200 1234 "-" "Mozilla/5.0"                | 4             | 1             |
| Email notification sent to john.doe+newsletter@company.com with subject: 'Welcome! Your order #12345 is confirmed'                                     | 5             | 1             |
+--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------+
```


## Example 4: Clustering with termset algorithm

The following query uses the `termset` algorithm which ignores word order when comparing text:

```ppl
source=otellogs
| cluster body match=termset showcount=true
| fields body, cluster_label, cluster_count
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------+
| body                                                                                                                                                   | cluster_label | cluster_count |
|--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------|
| User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart                                                                       | 1             | 1             |
| Payment failed: Insufficient funds for user@example.com                                                                                                | 2             | 1             |
| Query contains Lucene special characters: +field:value -excluded AND (grouped OR terms) NOT "exact phrase" wildcard* fuzzy~2 /regex/ [range TO search] | 3             | 1             |
| 192.168.1.1 - - [15/Jan/2024:10:30:03 +0000] "GET /api/products?search=laptop&category=electronics HTTP/1.1" 200 1234 "-" "Mozilla/5.0"                | 4             | 2             |
+--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------+
```


## Example 5: Clustering with ngramset algorithm

The following query uses the `ngramset` algorithm which compares character trigrams for fuzzy matching:

```ppl
source=otellogs
| cluster body match=ngramset showcount=true
| fields body, cluster_label, cluster_count
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------+
| body                                                                                                                                                   | cluster_label | cluster_count |
|--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------|
| User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart                                                                       | 1             | 1             |
| Payment failed: Insufficient funds for user@example.com                                                                                                | 2             | 1             |
| Query contains Lucene special characters: +field:value -excluded AND (grouped OR terms) NOT "exact phrase" wildcard* fuzzy~2 /regex/ [range TO search] | 3             | 1             |
| 192.168.1.1 - - [15/Jan/2024:10:30:03 +0000] "GET /api/products?search=laptop&category=electronics HTTP/1.1" 200 1234 "-" "Mozilla/5.0"                | 4             | 1             |
+--------------------------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------+
```


## Example 6: Custom field names

The following query uses custom field names for the cluster label and count:

```ppl
source=otellogs
| cluster body match=termset t=0.3 labelfield=log_group countfield=group_size showcount=true
| fields body, log_group, group_size
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+------------+
| body                                                                                                                                                   | log_group | group_size |
|--------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+------------|
| User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart                                                                       | 1         | 3          |
| Payment failed: Insufficient funds for user@example.com                                                                                                | 2         | 3          |
| Query contains Lucene special characters: +field:value -excluded AND (grouped OR terms) NOT "exact phrase" wildcard* fuzzy~2 /regex/ [range TO search] | 3         | 1          |
| Email notification sent to john.doe+newsletter@company.com with subject: 'Welcome! Your order #12345 is confirmed'                                     | 4         | 1          |
+--------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+------------+
```


## Example 7: Label only mode

The following query adds cluster labels to all rows without deduplicating. By default (`labelonly=false`), only the first representative row per cluster is kept:

```ppl
source=otellogs
| cluster body match=termset t=0.3 labelonly=true showcount=true
| fields body, cluster_label, cluster_count
| head 5
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+-----------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------+
| body                                                                                                                                    | cluster_label | cluster_count |
|-----------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------|
| User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart                                                        | 1             | 3             |
| 192.168.1.1 - - [15/Jan/2024:10:30:03 +0000] "GET /api/products?search=laptop&category=electronics HTTP/1.1" 200 1234 "-" "Mozilla/5.0" | 1             | 3             |
| [2024-01-15 10:30:09] production.INFO: User authentication successful for admin@company.org using OAuth2                                | 1             | 3             |
| Payment failed: Insufficient funds for user@example.com                                                                                 | 2             | 3             |
| Elasticsearch query failed: {"query":{"bool":{"must":[{"match":{"email":"*@example.com"}}]}}}                                           | 2             | 3             |
+-----------------------------------------------------------------------------------------------------------------------------------------+---------------+---------------+
```
