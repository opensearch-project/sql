.. highlight:: sh

====================
Cross-Cluster Search
====================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1

Introduction
============
Cross-cluster search lets any node in a cluster execute search requests against other clusters.
It makes searching easy across all connected clusters, allowing users to use multiple smaller clusters instead of a single large one.


Configuration
=============
On the local cluster, add the remote cluster name and the IP address with port 9300 for each seed node. ::

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


Using Cross-Cluster Search in PPL
=================================
Perform cross-cluster search by using "<cluster-name>:<index-name>" as the index identifier.

Example PPL query::

    os> source=my_remote_cluster:accounts;
    fetched rows / total rows = 4/4
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+
    | account_number | firstname | address              | balance | gender | city   | employer | state | age | email                 | lastname |
    |----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------|
    | 1              | Amber     | 880 Holmes Lane      | 39225   | M      | Brogan | Pyrami   | IL    | 32  | amberduke@pyrami.com  | Duke     |
    | 6              | Hattie    | 671 Bristol Street   | 5686    | M      | Dante  | Netagy   | TN    | 36  | hattiebond@netagy.com | Bond     |
    | 13             | Nanette   | 789 Madison Street   | 32838   | F      | Nogal  | Quility  | VA    | 28  | null                  | Bates    |
    | 18             | Dale      | 467 Hutchinson Court | 4180    | M      | Orick  | null     | MD    | 33  | daleadams@boink.com   | Adams    |
    +----------------+-----------+----------------------+---------+--------+--------+----------+-------+-----+-----------------------+----------+


Limitation
==========
Since OpenSearch does not support cross cluster index metadata retrieval, field mapping of a remote cluster index is not available to the local cluster.
(`[Feature] Cross cluster field mappings query #6573 <https://github.com/opensearch-project/OpenSearch/issues/6573>`_)
Therefore, the query engine requires that for any remote cluster index that the users need to search,
the local cluster keep a field mapping system index with the same index name.
This can be done by creating an index on the local cluster with the same name and schema as the remote cluster index.


Authentication and Permission
=============================

1. The security plugin authenticates the user on the local cluster.
2. The security plugin fetches the user’s backend roles on the local cluster.
3. The call, including the authenticated user, is forwarded to the remote cluster.
4. The user’s permissions are evaluated on the remote cluster.

Check `Cross-cluster search access control <https://opensearch.org/docs/latest/security/access-control/cross-cluster-search/>`_ for more details.

Example: Create the ppl_role for test_user on local cluster and the ccs_role for test_user on remote cluster. Then test_user could use PPL to query ``ppl-security-demo`` index on remote cluster.

1. On the local cluster, refer to `Security Settings <security.rst>`_ to create role and user for PPL plugin and index access permission.

2. On the remote cluster, create a new role and grant permission to access index. Create a user with the same name and credentials as the local cluster, and map the user to this role::

    PUT _plugins/_security/api/roles/ccs_role
    {
      "index_permissions":[
        {
          "index_patterns":["ppl-security-demo"],
          "allowed_actions":[
            "indices:admin/shards/search_shards",
            "indices:data/read/search"
          ]
        }
      ]
    }

    PUT _plugins/_security/api/rolesmapping/ccs_role
    {
      "backend_roles" : [],
      "hosts" : [],
      "users" : ["test_user"]
    }
