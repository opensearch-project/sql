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
automatically enforces a LogicalSystemLimit operator for such commands.

``plugins.query.system_limit``: The size configures the maximum of rows in the subsearch to data-intensive operations
against (e.g. join, lookup). The default value is: 50000. Value range is from 0 to 2147483647 (Int.MaxValue).
Note, only apply system limit automatically to data-intensive (data-bloat) operations.

PPL commands includes ``join``, ``lookup`` and ``expand`` will be affected by this configuration.
In future, we can add more command argument to control specific command.

For Join, with join type

* SEMI, ANTI: no affect
* RIGHT: add a LogicalSystemLimit operator to left side (main-search)
* Others: add a LogicalSystemLimit operator to right side (sub-search)

For Lookup

* add a LogicalSystemLimit operator to right side (sub-search)

For expand

* add a LogicalSystemLimit operator to right side (sub-search)

The results of impacted search (for example, the lookup table of lookup command, right side of inner join, etc.)
cannot exceed the limitation (50000 rows by default). If the actual number of rows in lookup table or right side
of inner join is greater then the system limit, only the number of rows specified by the configuration will be searched.
You can set the configuration to the maximum integer value (2147483647) if you are certain resources are not a concern.