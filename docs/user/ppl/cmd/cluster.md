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
| `delims` | Optional | Characters used to split the field into tokens. Accepts either the keyword `non-alphanumeric` (the default, which splits on any character that is not a letter, digit, or underscore) or a string whose individual characters are each treated as a delimiter. For example, `delims=" ,;"` splits on space, comma, and semicolon. |


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
+-----------------------------------------------------------------------------------------------+---------------+
| body                                                                                          | cluster_label |
|-----------------------------------------------------------------------------------------------+---------------|
| [2024-02-01T09:10:00.123Z] "GET /api/products HTTP/1.1" 200 - 1024 45 frontend-6b7b4c9f-x2kl9 | 1             |
| Order #1234 placed successfully by user U100                                                  | 2             |
| Slow query detected: SELECT * FROM products WHERE category = 'electronics' took 3200ms        | 3             |
| Payment failed: connection timeout to payment gateway after 30000ms                           | 4             |
+-----------------------------------------------------------------------------------------------+---------------+
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
+-----------------------------------------------------------------------------------------------+---------------+---------------+
| body                                                                                          | cluster_label | cluster_count |
|-----------------------------------------------------------------------------------------------+---------------+---------------|
| [2024-02-01T09:10:00.123Z] "GET /api/products HTTP/1.1" 200 - 1024 45 frontend-6b7b4c9f-x2kl9 | 1             | 13            |
| Slow query detected: SELECT * FROM products WHERE category = 'electronics' took 3200ms        | 2             | 1             |
| Payment failed: connection timeout to payment gateway after 30000ms                           | 3             | 1             |
| Cache miss for key user:session:U200 in Valkey cluster                                        | 4             | 2             |
| Out of memory: Java heap space - shutting down pod payment-6f8d4b-ht7q3                       | 5             | 1             |
+-----------------------------------------------------------------------------------------------+---------------+---------------+
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
+-----------------------------------------------------------------------------------------------+---------------+---------------+
| body                                                                                          | cluster_label | cluster_count |
|-----------------------------------------------------------------------------------------------+---------------+---------------|
| [2024-02-01T09:10:00.123Z] "GET /api/products HTTP/1.1" 200 - 1024 45 frontend-6b7b4c9f-x2kl9 | 1             | 1             |
| Order #1234 placed successfully by user U100                                                  | 2             | 1             |
| Slow query detected: SELECT * FROM products WHERE category = 'electronics' took 3200ms        | 3             | 1             |
| Payment failed: connection timeout to payment gateway after 30000ms                           | 4             | 1             |
| Cache miss for key user:session:U200 in Valkey cluster                                        | 5             | 1             |
+-----------------------------------------------------------------------------------------------+---------------+---------------+
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
+-----------------------------------------------------------------------------------------------+---------------+---------------+
| body                                                                                          | cluster_label | cluster_count |
|-----------------------------------------------------------------------------------------------+---------------+---------------|
| [2024-02-01T09:10:00.123Z] "GET /api/products HTTP/1.1" 200 - 1024 45 frontend-6b7b4c9f-x2kl9 | 1             | 3             |
| Order #1234 placed successfully by user U100                                                  | 2             | 1             |
| Slow query detected: SELECT * FROM products WHERE category = 'electronics' took 3200ms        | 3             | 1             |
| Payment failed: connection timeout to payment gateway after 30000ms                           | 4             | 1             |
+-----------------------------------------------------------------------------------------------+---------------+---------------+
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
+-----------------------------------------------------------------------------------------------+---------------+---------------+
| body                                                                                          | cluster_label | cluster_count |
|-----------------------------------------------------------------------------------------------+---------------+---------------|
| [2024-02-01T09:10:00.123Z] "GET /api/products HTTP/1.1" 200 - 1024 45 frontend-6b7b4c9f-x2kl9 | 1             | 1             |
| Order #1234 placed successfully by user U100                                                  | 2             | 1             |
| Slow query detected: SELECT * FROM products WHERE category = 'electronics' took 3200ms        | 3             | 1             |
| Payment failed: connection timeout to payment gateway after 30000ms                           | 4             | 1             |
+-----------------------------------------------------------------------------------------------+---------------+---------------+
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
+-----------------------------------------------------------------------------------------------+-----------+------------+
| body                                                                                          | log_group | group_size |
|-----------------------------------------------------------------------------------------------+-----------+------------|
| [2024-02-01T09:10:00.123Z] "GET /api/products HTTP/1.1" 200 - 1024 45 frontend-6b7b4c9f-x2kl9 | 1         | 13         |
| Slow query detected: SELECT * FROM products WHERE category = 'electronics' took 3200ms        | 2         | 1          |
| Payment failed: connection timeout to payment gateway after 30000ms                           | 3         | 1          |
| Cache miss for key user:session:U200 in Valkey cluster                                        | 4         | 2          |
+-----------------------------------------------------------------------------------------------+-----------+------------+
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
+-----------------------------------------------------------------------------------------------+---------------+---------------+
| body                                                                                          | cluster_label | cluster_count |
|-----------------------------------------------------------------------------------------------+---------------+---------------|
| [2024-02-01T09:10:00.123Z] "GET /api/products HTTP/1.1" 200 - 1024 45 frontend-6b7b4c9f-x2kl9 | 1             | 13            |
| Order #1234 placed successfully by user U100                                                  | 1             | 13            |
| NullPointerException in CheckoutService.placeOrder at line 142                                | 1             | 13            |
| User U300 authenticated via OAuth2 from 10.0.0.5                                              | 1             | 13            |
| Connection pool 80% utilized on database replica db-replica-02                                | 1             | 13            |
+-----------------------------------------------------------------------------------------------+---------------+---------------+
```


## Cluster settings

The `cluster` command exposes three cluster-level guardrails. All three are `NodeScope` and dynamic, so a cluster administrator can update them at runtime with the `_cluster/settings` API. They are administrative controls rather than per-query options, and they are shared by every `cluster` query on the node.

| Setting | Default | Minimum | Description |
| --- | --- | --- | --- |
| `plugins.ppl.cluster.buffer.limit` | `50000` | `1` | Number of rows buffered on the coordinating node before an incremental clustering pass runs. This bounds transient memory only and does not change the clustering result. Lower it on memory constrained nodes, raise it to reduce the number of incremental passes. |
| `plugins.ppl.cluster.max.clusters` | `10000` | `1` | Maximum number of distinct clusters the command will create. Once this many clusters exist, every remaining row is folded into its closest existing cluster instead of forming a new one. This value changes the output because it caps how many distinct `cluster_label` values are possible. |
| `plugins.ppl.cluster.max.input.rows` | `1000000` | `1` | Maximum number of input rows the command will process on the coordinating node. Clustering assigns one label per row, so the label array grows in proportion to the input (`O(rows)`). When the input exceeds this ceiling the query fails with a clear error instead of risking coordinator memory exhaustion. Scope the query with a filter or time window, or raise this setting on nodes with more heap. |

## Performance and limitations

Clustering runs on the coordinating node. Every buffered row is compared against the current set of cluster representatives, so the work grows in proportion to the number of rows multiplied by the number of clusters (`O(rows * clusters)`). When the number of clusters is small or moderate, the command costs about the same as a plain scan of the same rows, and its cost is dominated by reading the rows rather than by the clustering itself.

The cost driver is the cluster count, not the row count. On a field with near unique values (for example a field with an embedded identifier, timestamp, or request ID), the command tries to create a very large number of clusters, and the per row comparison cost grows accordingly. `plugins.ppl.cluster.max.clusters` exists to bound this: it caps the comparisons per row so that a high cardinality field degrades gracefully into a fixed number of clusters instead of running unbounded.

Raising `plugins.ppl.cluster.max.clusters` increases the per row comparison cost and can make queries over high cardinality fields very slow. Long running clustering queries are also subject to the standard PPL query timeout (`plugins.ppl.query.timeout`, default 300 seconds). Before clustering a field, prefer a field whose values naturally fall into a bounded number of groups (log message templates, error categories, status lines) rather than a field with near unique values.
