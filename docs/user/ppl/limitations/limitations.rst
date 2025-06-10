===========
Limitations
===========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Inconsistent Field Types across indices
=======================================

* If the same field has different types across indices (e.g., ``field`` is a ``string`` in one index and an ``integer`` in another), PPL selects a field type from one of the indices—this selection is non-deterministic. Fields with other types are ignored during query execution.
* For ``object`` fields, `PPL merges subfields from different indices to tolerate schema variations <https://github.com/opensearch-project/sql/issues/3625>`_.

Unsupported OpenSearch Field Types
==================================

PPL does not support all `OpenSearch data types <https://docs.opensearch.org/docs/latest/field-types/supported-field-types/index/>`_. (e.g., ``flattened``, some complex ``nested`` usages). Unsupported fields are excluded from ``DESCRIBE`` and ``SOURCE`` outputs. At runtime: Queries referencing unsupported fields fail with semantic or resolution errors. Such fields are ignored in projections unless explicitly filtered out or removed at ingestion.

+---------------------------+---------+
| OpenSearch Data Type      | PPL     |
+===========================+=========+
| knn_vector                | Ignored |
+---------------------------+---------+
| Range field types         | Ignored |
+---------------------------+---------+
| Object - flat_object      | Ignored |
+---------------------------+---------+
| Object - join             | Ignored |
+---------------------------+---------+
| String - Match-only text  | Ignored |
+---------------------------+---------+
| String - Wildcard         | Ignored |
+---------------------------+---------+
| String - token_count      | Ignored |
+---------------------------+---------+
| String - constant_keyword | Ignored |
+---------------------------+---------+
| Autocomplete              | Ignored |
+---------------------------+---------+
| Geoshape                  | Ignored |
+---------------------------+---------+
| Cartesian field types     | Ignored |
+---------------------------+---------+
| Rank field types          | Ignored |
+---------------------------+---------+
| Star-tree                 | Ignored |
+---------------------------+---------+
| derived                   | Ignored |
+---------------------------+---------+
| Percolator                | Ignored |
+---------------------------+---------+

Field Parameters
================

For a field to be queryable in PPL, the following index settings must be enabled:

+------------------+--------------------------------------------------+--------------------------------------------------+
| Setting          | Description                                      | Required For                                     |
+==================+==================================================+==================================================+
| _source: true    | Stores the original JSON document                | Required for fetch raw data.                     |
+------------------+--------------------------------------------------+--------------------------------------------------+
| index: true      | Enables field indexing                           | Required for filtering, search, and aggregations |
+------------------+--------------------------------------------------+--------------------------------------------------+
| doc_values: true | Enables columnar access for aggregations/sorting | Required for `stats`, `sort`                     |
+------------------+--------------------------------------------------+--------------------------------------------------+


Nested Field Behavior
=====================

* There are `limitations <https://github.com/opensearch-project/sql/issues/52>`_ regarding the nested levels and query types that needs improvement.

Multi-value Field Behavior
==========================

OpenSearch does not natively support the ARRAY data type but does allow multi-value fields implicitly. The
SQL/PPL plugin adheres strictly to the data type semantics defined in index mappings. When parsing OpenSearch
responses, it expects data to match the declared type and does not account for data in array format. If the
plugins.query.field_type_tolerance setting is enabled, the SQL/PPL plugin will handle array datasets by returning
scalar data types, allowing basic queries (e.g., source = tbl | where condition). However, using multi-value
fields in expressions or functions will result in exceptions. If this setting is disabled or absent, only the
first element of an array is returned, preserving the default behavior.

System Limitation for data-intensive operations
===============================================

Since v3.0.0, PPL introduces commands that may increase data volume. To prevent out-of-memory problem, the system
automatically enforces this system limit in the pushdown context of the physical index scan operator for such commands.
``plugins.query.system_limit``: The size configures the maximum amount of documents to be pull from OpenSearch for
data-intensive operations (e.g. join, lookup). The default value is: 50000.
Note, only apply system limit automatically to data-intensive (data-bloat) operations. For non-data-intensive operations,
the maximum amount of documents to be pull from OpenSearch equals to value of plugins.query.size_limit.
For example, even SORT is a high cost physical operator, the amount of documents to sorted cannot greater than value of
``plugins.query.size_limit``.
