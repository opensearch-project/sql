=============
lookup
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``lookup`` command enriches your search data by adding or replacing data from a lookup index (dimension table). You can extend fields of an index with values from a dimension table, append or replace values when lookup condition is matched. As an alternative of join command, lookup command is more suitable for enriching the source data with a static dataset.

Syntax
======
lookup <lookupIndex> (<lookupMappingField> [as <sourceMappingField>])... [(replace | append) (<inputField> [as <outputField>])...]

* lookupIndex: mandatory. The name of lookup index (dimension table).
* lookupMappingField: mandatory. A mapping key in ``lookupIndex``, analogy to a join key from right table. You can specify multiple ``lookupMappingField`` with comma-delimited.
* sourceMappingField: optional. A mapping key from source (left side), analogy to a join key from left side. If not specified, defaults to ``lookupMappingField``.
* inputField: optional. A field in ``lookupIndex`` where matched values are applied to result output. You can specify multiple ``inputField`` with comma-delimited. If not specified, all fields except ``lookupMappingField`` from ``lookupIndex`` are applied to result output.
* outputField: optional. A field of output. You can specify zero or multiple ``outputField``. If ``outputField`` has an existing field name in source query, its values will be replaced or appended by matched values from ``inputField``. If the field specified in ``outputField`` is a new field, in replace strategy, an extended new field will be applied to the results, but fail in append strategy.
* replace | append: optional. The output strategies. If replace, matched values in ``lookupIndex`` field overwrite the values in result. If append, matched values in ``lookupIndex`` field only append to the missing values in result. **Default:** replace.

Usage
=====

Lookup::

    source = table1 | lookup table2 id
    source = table1 | lookup table2 id, name
    source = table1 | lookup table2 id as cid, name
    source = table1 | lookup table2 id as cid, name replace dept as department
    source = table1 | lookup table2 id as cid, name replace dept as department, city as location
    source = table1 | lookup table2 id as cid, name append dept as department
    source = table1 | lookup table2 id as cid, name append dept as department, city as location


Example 1: Replace strategy
===========================

This example shows using the lookup command with the REPLACE strategy to overwrite existing values.

PPL query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_ppl -d '{
	  "query" : """
	  source = worker
	  | LOOKUP work_information uid AS id REPLACE department
	  | fields id, name, occupation, country, salary, department
	  """
	}'

Result set::

    {
      "schema": [
        {
          "name": "id",
          "type": "integer"
        },
        {
          "name": "name",
          "type": "string"
        },
        {
          "name": "occupation",
          "type": "string"
        },
        {
          "name": "country",
          "type": "string"
        },
        {
          "name": "salary",
          "type": "integer"
        },
        {
          "name": "department",
          "type": "string"
        }
      ],
      "datarows": [
        [
          1000,
          "Jake",
          "Engineer",
          "England",
          100000,
          "IT"
        ],
        [
          1001,
          "Hello",
          "Artist",
          "USA",
          70000,
          null
        ],
        [
          1002,
          "John",
          "Doctor",
          "Canada",
          120000,
          "DATA"
        ],
        [
          1003,
          "David",
          "Doctor",
          null,
          120000,
          "HR"
        ],
        [
          1004,
          "David",
          null,
          "Canada",
          0,
          null
        ],
        [
          1005,
          "Jane",
          "Scientist",
          "Canada",
          90000,
          "DATA"
        ]
      ],
      "total": 6,
      "size": 6
    }

Example 2: Append strategy
==========================

This example shows using the lookup command with the APPEND strategy to fill missing values only.

PPL query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_ppl -d '{
	  "query" : """
	  source = worker
	  | LOOKUP work_information uid AS id APPEND department
	  | fields id, name, occupation, country, salary, department
	  """
	}'


Example 3: No inputField specified
====================================

This example shows using the lookup command without specifying inputField, which applies all fields from the lookup index.

PPL query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_ppl -d '{
	  "query" : """
	  source = worker
	  | LOOKUP work_information uid AS id, name
	  | fields id, name, occupation, country, salary, department
	  """
	}'

Result set::

    {
      "schema": [
        {
          "name": "id",
          "type": "integer"
        },
        {
          "name": "name",
          "type": "string"
        },
        {
          "name": "country",
          "type": "string"
        },
        {
          "name": "salary",
          "type": "integer"
        },
        {
          "name": "department",
          "type": "string"
        },
        {
          "name": "occupation",
          "type": "string"
        }
      ],
      "datarows": [
        [
          1000,
          "Jake",
          "England",
          100000,
          "IT",
          "Engineer"
        ],
        [
          1001,
          "Hello",
          "USA",
          70000,
          null,
          null
        ],
        [
          1002,
          "John",
          "Canada",
          120000,
          "DATA",
          "Scientist"
        ],
        [
          1003,
          "David",
          null,
          120000,
          "HR",
          "Doctor"
        ],
        [
          1004,
          "David",
          "Canada",
          0,
          null,
          null
        ],
        [
          1005,
          "Jane",
          "Canada",
          90000,
          "DATA",
          "Engineer"
        ]
      ],
      "total": 6,
      "size": 6
    }

Example 4: OutputField as a new field
======================================

This example shows using the lookup command with outputField as a new field name.

PPL query::

	>> curl -H 'Content-Type: application/json' -X POST localhost:9200/_plugins/_ppl -d '{
	  "query" : """
	  source = worker
	  | LOOKUP work_information name REPLACE occupation AS new_col
	  | fields id, name, occupation, country, salary, new_col
	  """
	}'

Result set::

    {
      "schema": [
        {
          "name": "id",
          "type": "integer"
        },
        {
          "name": "name",
          "type": "string"
        },
        {
          "name": "occupation",
          "type": "string"
        },
        {
          "name": "country",
          "type": "string"
        },
        {
          "name": "salary",
          "type": "integer"
        },
        {
          "name": "new_col",
          "type": "string"
        }
      ],
      "datarows": [
        [
          1003,
          "David",
          "Doctor",
          null,
          120000,
          "Doctor"
        ],
        [
          1004,
          "David",
          null,
          "Canada",
          0,
          "Doctor"
        ],
        [
          1001,
          "Hello",
          "Artist",
          "USA",
          70000,
          null
        ],
        [
          1000,
          "Jake",
          "Engineer",
          "England",
          100000,
          "Engineer"
        ],
        [
          1005,
          "Jane",
          "Scientist",
          "Canada",
          90000,
          "Engineer"
        ],
        [
          1002,
          "John",
          "Doctor",
          "Canada",
          120000,
          "Scientist"
        ]
      ],
      "total": 6,
      "size": 6
    }

