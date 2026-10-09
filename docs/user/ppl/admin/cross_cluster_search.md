# Cross-Cluster Search  

## Introduction  

Cross-cluster search lets any node in a cluster execute search requests against other clusters.
It makes searching easy across all connected clusters, allowing users to use multiple smaller clusters instead of a single large one.
## Configuration  

On the local cluster, add the remote cluster name and the IP address with port 9300 for each seed node.
  
```bash
PUT _cluster/settings
{
  "persistent": {
    "cluster.remote": {
      "<remote-cluster-name>": {
        "seeds": ["<remote-cluster-IP-address>:9300"]
      }
    }
  }
}
```
  
## Using Cross-Cluster Search in PPL  

Perform cross-cluster search by using "\<cluster-name\>:\<index-name\>" as the index identifier.
Example PPL query
  
```ppl
source=my_remote_cluster:accounts
```
  
Expected output:
  
```text
fetched rows / total rows = 4/4
+----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+
| account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname |
|----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------|
| 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     |
| 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     |
| 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    |
| 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    |
+----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+
```
  
To search every connected remote cluster, use `*` as the cluster name, for example `source=*:accounts`.
Local and remote indices can be combined in one query, for example `source=accounts,my_remote_cluster:accounts`.

## Remote Index Fields  

The fields of a remote cluster index can be queried directly. No index needs to exist on the local cluster.
To keep queries fast, the fields of a remote index are remembered for up to 60 seconds after a query reads
them. If a field is added on the remote cluster during that time, a query that uses it can fail with a
"field not found" error until the 60 seconds pass. The same applies to a new index that matches the pattern,
for example after a rollover. Fields are remembered separately for each user and on each node, so the wait
only affects queries on the same index pattern by the same user.

## Unavailable Remote Clusters  

If a remote cluster cannot be reached, the query fails with the same error as a search request, for example
`Unable to open any proxy connections to remote cluster [my_remote_cluster]`. If the remote cluster is
configured with `skip_unavailable: true`, it is skipped and the other clusters in the query are still searched.
If every remote cluster in the query is skipped and no local index is named, the query fails with
`Remote cluster [my_remote_cluster] is unavailable and was skipped (skip_unavailable is true)`.

If the remote cluster is reachable but the index does not exist there, the query fails with `no such index`.

## Limitations  

* Parts of a query can run on the remote cluster as scripts (for example, an aggregation on a field whose type
  differs between indices). Run the same OpenSearch and SQL plugin version on the local and remote clusters: a
  remote cluster on an older version may not support scripts created by a newer local cluster.

## Authentication and Permission  

1. The security plugin authenticates the user on the local cluster.  
2. The security plugin fetches the user’s backend roles on the local cluster.  
3. The call, including the authenticated user, is forwarded to the remote cluster.  
4. The user’s permissions are evaluated on the remote cluster.  
  
Check [Cross-cluster search access control](https://opensearch.org/docs/latest/security/access-control/cross-cluster-search/) for more details.
Example: Create the ppl_role for test_user on local cluster and the ccs_role for test_user on remote cluster. Then test_user could use PPL to query `ppl-security-demo` index on remote cluster.
1. On the local cluster, refer to [Security Settings](security.md) to create role and user for PPL plugin and index access permission.
   If the role lists specific index patterns, also include the cluster-prefixed name (for example `*:ppl-security-demo`),
   because some requests made on the local cluster, such as point-in-time creation, are checked against it.  
2. On the remote cluster, create a new role and grant permission to access index. Create a user with the same name and credentials as the local cluster, and map the user to this role.
   The role needs the same index permissions as PPL on a local cluster (see [Security Settings](security.md)),
   as in the example below. Without them, the query fails with a permission error.  
  
```bash
PUT _plugins/_security/api/roles/ccs_role
{
  "index_permissions":[
    {
      "index_patterns":["ppl-security-demo"],
      "allowed_actions":[
        "indices:admin/shards/search_shards",
        "indices:data/read/search",
        "indices:admin/mappings/get",
        "indices:monitor/settings/get"
      ]
    }
  ]
}
```
  
```bash
PUT _plugins/_security/api/rolesmapping/ccs_role
{
  "backend_roles" : [],
  "hosts" : [],
  "users" : ["test_user"]
}
```
  