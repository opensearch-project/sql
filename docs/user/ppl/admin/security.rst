.. highlight:: sh

=================
Security Settings
=================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 1

Introduction
============

User needs ``cluster:admin/opensearch/ppl`` permission to use PPL plugin. User also needs indices level permission ``indices:admin/mappings/get`` to get field mappings, ``indices:monitor/settings/get`` to get cluster settings, and ``indices:data/read/search*`` to search index.

Using Rest API
==============
**--INTRODUCED 2.1--**

Example: Create the ppl_role for test_user. then test_user could use PPL to query ``ppl-security-demo`` index.

1. Create the ppl_role and grand permission to access PPL plugin and access ppl-security-demo index::

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

2. Mapping the test_user to the ppl_role::

    PUT _plugins/_security/api/rolesmapping/ppl_role
    {
      "backend_roles" : [],
      "hosts" : [],
      "users" : ["test_user"]
    }


Using Security Dashboard
========================
**--INTRODUCED 2.1--**

Example: Create ppl_access permission and add to existing role

1. Create the ppl_access permission::

    PUT _plugins/_security/api/actiongroups/ppl_access
    {
      "allowed_actions": [
        "cluster:admin/opensearch/ppl"
      ]
    }

2. Grant the ppl_access permission to ppl_test_role

.. image:: https://user-images.githubusercontent.com/2969395/185448976-6c0aed6b-7540-4b99-92c3-362da8ae3763.png
