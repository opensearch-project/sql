# Security Settings  

## Introduction  

User needs `cluster:admin/opensearch/ppl` permission to use PPL plugin. User also needs indices level permission `indices:admin/mappings/get` to get field mappings, `indices:monitor/settings/get` to get cluster settings, and `indices:data/read/search*` to search index.

### collect command permissions

The `collect` command materializes results into a destination index as an asynchronous background task, so it needs permissions beyond a read-only PPL query:

- Cluster permission `cluster:admin/opensearch/ppl/collect/materialize` (in addition to `cluster:admin/opensearch/ppl`) to authorize the background materialization task.
- On the destination index, write permission such as `indices:data/write/bulk`, plus `indices:admin/mappings/get` to resolve its mapping.
- The usual read permissions on the source index (`indices:data/read/search*`, `indices:admin/mappings/get`, `indices:data/read/point_in_time/create`, `indices:data/read/point_in_time/delete`).

Because the write runs in the background (fire-and-forget), a missing `cluster:admin/opensearch/ppl/collect/materialize` or destination write permission does not fail the request synchronously. The query still returns the preview rows and a `task_id`, and the authorization failure is recorded on the background task, observable via `GET _tasks/<task_id>`.
## Using Rest API  

**--INTRODUCED 2.1--**  

Example: Create the ppl_role for test_user. then test_user could use PPL to query `ppl-security-demo` index.  
1. Create the ppl_role and grant permission to access PPL plugin and access ppl-security-demo index  
  
```bash
PUT _plugins/_security/api/roles/ppl_role
{
  "cluster_permissions": [
    "cluster:admin/opensearch/ppl"
  ],
  "index_permissions": [{
    "index_patterns": [
      "ppl-security-demo"
    ],
    "allowed_actions": [
      "indices:data/read/search*",
      "indices:admin/mappings/get",
      "indices:monitor/settings/get"
    ]
  }]
}

```
  
2. Mapping the test_user to the ppl_role  
  
```bash
PUT _plugins/_security/api/rolesmapping/ppl_role
{
  "backend_roles" : [],
  "hosts" : [],
  "users" : ["test_user"]
}


```
  
## Using Security Dashboard  

**--INTRODUCED 2.1--**  

Example: Create ppl_access permission and add to existing role  
1. Create the ppl_access permission  
  
```bash
PUT _plugins/_security/api/actiongroups/ppl_access
{
  "allowed_actions": [
    "cluster:admin/opensearch/ppl"
  ]
}

```
  
2. Grant the ppl_access permission to ppl_test_role  
  
![Image](https://user-images.githubusercontent.com/2969395/185448976-6c0aed6b-7540-4b99-92c3-362da8ae3763.png)
