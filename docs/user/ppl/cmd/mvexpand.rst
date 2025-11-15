=============
mvexpand
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Description
============
| The ``mvexpand`` command expands each value in a multivalue (array) field into a separate row, similar to Splunk's `mvexpand` command.
| For each document, every value in the specified field is returned as a new row. This is especially useful for log analytics and data exploration involving array fields.

| Key features of ``mvexpand``:
- Expands array fields into multiple rows, one per value.
- Supports an optional ``limit`` parameter to restrict the number of expanded values per document.
- Handles empty, null, and non-array fields gracefully.
- Works as a streaming/distributable command for performance and scalability.

Syntax
======
mvexpand <field> [limit=<int>]

* **field**: The multivalue (array) field to expand. (Required)
* **limit**: Maximum number of values per document to expand. (Optional)

Usage
=====
Basic expansion::

    source=logs | mvexpand tags

Expansion with limit::

    source=docs | mvexpand ids limit=3

Limitations
===========
- Only one field can be expanded per mvexpand command.
- For non-array fields, the value is returned as-is.
- For empty or null arrays, no rows are returned.
- Large arrays may be subject to resource/memory limits; exceeding them results in an error or warning.

Output ordering and default limit
--------------------------------
If no `limit` is specified, mvexpand expands all elements in the array (there is no implicit per-document cap). Elements are emitted in the same order they appear in the array (array iteration order). If the underlying field does not provide a defined order, the output order is undefined. Use `limit` to bound the number of expanded rows per document and to avoid resource issues on very large arrays.

Examples and Edge Cases
=======================

Example 1: Basic Expansion
--------------------------
Expand all values from an array field.

Input document::

    { "tags": ["error", "warning", "info"] }

PPL query::

    source=logs | mvexpand tags

Output (example)::

    fetched rows / total rows = 3/3
    +--------+
    | tags   |
    +--------+
    | error  |
    | warning|
    | info   |
    +--------+

Example 2: Expansion with Limit
-------------------------------
Limit the number of expanded values per document.

Input document::

    { "ids": [1, 2, 3, 4, 5] }

PPL query::

    source=docs | mvexpand ids limit=3

Output (example)::

    fetched rows / total rows = 3/3
    +-----+
    | ids |
    +-----+
    |  1  |
    |  2  |
    |  3  |
    +-----+

Example 3: Empty or Null Arrays
------------------------------
Handles documents with empty or null array fields.

Input document::

    { "tags": [] }

PPL query::

    source=logs | mvexpand tags

Output (example)::

    fetched rows / total rows = 0/0
    +------+
    | tags |
    +------+
    +------+

Input document::

    { "tags": null }

PPL query::

    source=logs | mvexpand tags

Output (example)::

    fetched rows / total rows = 0/0
    +------+
    | tags |
    +------+
    +------+

Example 4: Non-array Field
--------------------------
If the field is a single value (not an array), mvexpand returns the value as-is.

Input document::

    { "tags": "error" }

PPL query::

    source=logs | mvexpand tags

Output (example)::

    fetched rows / total rows = 1/1
    +-------+
    | tags  |
    +-------+
    | error |
    +-------+

Example 5: Large Arrays and Memory / resource limits
----------------------------------------------------
If an array is very large it can trigger engine or cluster resource limits and the query can fail with an error. There is no mvexpand-specific configuration. Instead, limits that can cause a query to be terminated are enforced at the node / engine level and by SQL/PPL query controls.

- OpenSearch node protections (for example, heap / query memory limits such as plugins.query.memory_limit) can terminate queries that exceed configured memory budgets.
- SQL/PPL execution limits (timeouts, request/response size limits, and engine memory budgets) also apply to queries that use mvexpand.
- Note: in the current Calcite-based engine, circuit-breaking protections are applied primarily to the index scan operator; protections for other operators (including some operators used internally to implement mvexpand) are under research. Do not assume operator-level circuit breaking will fully protect mvexpand in all cases.

To avoid failures when expanding large arrays:
- Use mvexpand's limit parameter to bound the number of expanded values per document (for example: mvexpand field limit=1000).
- Reduce the input size before expanding (filter with where, project only needed fields).
- Tune cluster and SQL/PPL execution settings (circuit breakers, request/response size, timeouts, memory limits) appropriate for your deployment.

For node and SQL/PPL settings see:
https://docs.opensearch.org/1.0/search-plugins/ppl/settings/

Example 6: Multiple Fields (Limitation)
---------------------------------------
mvexpand only supports expanding one field per command. To expand multiple fields, use multiple mvexpand commands or document the limitation.

PPL query::

    source=docs | mvexpand a | mvexpand b

Example 7: Edge Case - Field Missing
------------------------------------
If the field does not exist in a document, no row is produced for that document.

Input document::

    { "other": [1,2] }

PPL query::

    source=docs | mvexpand tags

Output (example)::

    fetched rows / total rows = 0/0
    +------+
    | tags |
    +------+
    +------+

---