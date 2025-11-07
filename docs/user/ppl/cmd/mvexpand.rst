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
If an array is very large it can trigger engine or cluster resource limits and the query can fail with an error. There is no mvexpand-specific configuration flag that controls resource usage; instead, limits are enforced by the engine and the cluster:

- OpenSearch node-level protections (circuit breakers and JVM/heap safeguards) and request-size protections.
- SQL/PPL execution limits (for example, query timeouts, request-size limits, and engine memory budgets) that apply to the query execution layer.

Behavior of circuit breakers and which operators they protect can vary by release and configuration (some breakers primarily protect memory-heavy operations such as fielddata, aggregations, and certain scan implementations). Because of these distinctions, mvexpand should not be relied on to bypass cluster-level protections — use the command-level ``limit`` to bound per-document expansion and avoid hitting cluster limits.

To avoid failures when expanding large arrays:
- Use the `limit` parameter to restrict the number of expanded values per document (for example: `mvexpand field limit=1000`).
- Filter or narrow the input before expanding (use `where` and `fields` to reduce rows and columns).
- Tune cluster and SQL/PPL execution settings (circuit breakers, request/response size, timeouts, memory limits) appropriate for your deployment. If desired, we can add links to the exact OpenSearch circuit-breaker and SQL/PPL configuration docs for the targeted release.

PPL query::

    source=docs | mvexpand ids

Output (example)::
    Error: Memory/resource limit exceeded while expanding field 'ids'. Please reduce the array size or specify a limit.

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