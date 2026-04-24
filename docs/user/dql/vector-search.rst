
=============
Vector Search
=============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Introduction
============

The ``vectorSearch()`` table function runs a k-NN query against a ``knn_vector``
field and exposes the matching documents as a relation in the ``FROM`` clause.
It relies on the OpenSearch `k-NN plugin
<https://docs.opensearch.org/latest/vector-search/>`_ — the target index must
map the vector field as ``knn_vector`` and the index must be created with
``index.knn: true``.

Relevance is expressed through the OpenSearch ``_score`` metadata field, and
results are returned ordered by ``_score DESC`` by default.

vectorSearch
============

Description
-----------

``vectorSearch(table='<index>', field='<vector-field>', vector='<array>', option='<key=value[,key=value]*>')``

All four arguments are required and must be passed by name. ``table``,
``field``, ``vector``, and ``option`` are string literals.

Arguments
---------

- ``table`` — index to search. The index must have ``index.knn: true`` and
  map the target field as ``knn_vector``.
- ``field`` — name of the ``knn_vector`` field.
- ``vector`` — query vector as a JSON-style array of numbers, passed as a
  string (for example, ``'[0.1, 0.2, 0.3]'``). The vector dimension must match
  the ``knn_vector`` mapping on the target index.
- ``option`` — comma-separated ``key=value`` pairs. Exactly one of ``k``,
  ``max_distance``, or ``min_score`` is required. ``filter_type`` is
  optional.

Supported option keys
---------------------

Option keys are lower-case and case-sensitive. ``K=5`` or ``Filter_Type=post``
will be rejected with an "Unknown option key" error.

- ``k`` — top-k mode. Integer between 1 and 10000. The query returns up to
  ``k`` nearest neighbors.
- ``max_distance`` — radial mode. Returns all documents within the given
  distance of the query vector. ``LIMIT`` is required.
- ``min_score`` — radial mode. Returns all documents with score at or above
  the given threshold. ``LIMIT`` is required.
- ``filter_type`` — ``post`` or ``efficient``. Controls how a ``WHERE``
  clause is applied. See `Filtering`_.

``k``, ``max_distance``, and ``min_score`` are mutually exclusive; specify
exactly one.

Syntax
------

::

    SELECT <projection>
    FROM vectorSearch(
      table='<index>',
      field='<vector-field>',
      vector='<array>',
      option='<key=value[,key=value]*>'
    ) AS <alias>
    [WHERE <predicate on alias non-vector fields>]
    [ORDER BY <alias>._score DESC]
    [LIMIT <n>]

Example 1: Top-k
----------------

Return the five nearest neighbors of a query vector::

    POST /_plugins/_sql
    {
      "query" : """
        SELECT v._id, v._score
        FROM vectorSearch(
          table='my_vectors',
          field='embedding',
          vector='[0.1, 0.2, 0.3]',
          option='k=5'
        ) AS v
      """
    }

In top-k mode, the request size defaults to ``k``; adding ``LIMIT n`` further
reduces the row count, but ``n`` must not exceed ``k``.

Example 2: Radial search (``max_distance``)
-------------------------------------------

Return every document within a maximum distance of the query vector.
``LIMIT`` is required for radial searches — without it the result set is
unbounded::

    POST /_plugins/_sql
    {
      "query" : """
        SELECT v._id, v._score
        FROM vectorSearch(
          table='my_vectors',
          field='embedding',
          vector='[0.1, 0.2, 0.3]',
          option='max_distance=0.5'
        ) AS v
        LIMIT 100
      """
    }

Example 3: Radial search (``min_score``)
----------------------------------------

Return every document whose score is at least the given threshold::

    POST /_plugins/_sql
    {
      "query" : """
        SELECT v._id, v._score
        FROM vectorSearch(
          table='my_vectors',
          field='embedding',
          vector='[0.1, 0.2, 0.3]',
          option='min_score=0.8'
        ) AS v
        LIMIT 100
      """
    }

Filtering
=========

A ``WHERE`` clause on non-vector fields of the ``vectorSearch()`` alias is
pushed down to OpenSearch when it can be translated to an OpenSearch filter.
Two placement strategies are available via the ``filter_type`` option:

- ``post`` — the ``WHERE`` predicate is applied as a non-scoring
  ``bool.filter`` alongside the k-NN query. The k-NN query runs first and
  its results are then filtered.
- ``efficient`` — the ``WHERE`` predicate is embedded directly inside the
  k-NN query (``knn.filter``), enabling pre-filtering during the ANN search.
  See the `k-NN filtering guide <https://docs.opensearch.org/latest/vector-search/filter-search-knn/efficient-knn-filtering/>`_
  for engine and method requirements.

Behavior depends on whether ``filter_type`` is specified:

- **Omitted** — pushdown is attempted using the ``post`` placement.
  Predicates that translate to native OpenSearch queries are pushed down as a
  ``bool.filter`` alongside the k-NN query. Predicates that do not have a
  native equivalent (for example, arithmetic or function calls on indexed
  fields) are pushed down as an OpenSearch script query and evaluated
  server-side. Only when predicate translation itself fails does the engine
  fall back to evaluating the ``WHERE`` clause in memory after the k-NN
  results are returned. A query with no ``WHERE`` clause is valid.
- **Explicit (``post`` or ``efficient``)** — a ``WHERE`` clause is required,
  and it must be translatable to an OpenSearch filter query. If the
  ``WHERE`` clause is missing or cannot be translated, the query fails with
  a descriptive error. This applies to both ``post`` and ``efficient``.
  Specifying ``filter_type=post`` explicitly is useful when you want the
  query to fail fast rather than silently fall back to in-memory filtering.

Example 4: Implicit pushdown (no ``filter_type``)
-------------------------------------------------

::

    POST /_plugins/_sql
    {
      "query" : """
        SELECT v._id, v._score, v.category
        FROM vectorSearch(
          table='my_vectors',
          field='embedding',
          vector='[0.1, 0.2, 0.3]',
          option='k=10'
        ) AS v
        WHERE v.category = 'books'
      """
    }

Example 5: Efficient (pre-)filtering
------------------------------------

::

    POST /_plugins/_sql
    {
      "query" : """
        SELECT v._id, v._score, v.category
        FROM vectorSearch(
          table='my_vectors',
          field='embedding',
          vector='[0.1, 0.2, 0.3]',
          option='k=10,filter_type=efficient'
        ) AS v
        WHERE v.category = 'books'
      """
    }

Scoring, sorting, and limits
============================

- ``vectorSearch()`` exposes the OpenSearch ``_score`` metadata field on the
  alias. Select it as ``<alias>._score``.
- Results are returned in ``_score DESC`` order by default. The only
  supported ``ORDER BY`` expression is ``<alias>._score DESC``.
- In top-k mode (``k=N``), ``LIMIT n`` is optional; when present, ``n`` must
  be ``≤ k``.
- In radial mode (``max_distance`` or ``min_score``), ``LIMIT`` is required.

Limitations
===========

The following shapes are outside the ``vectorSearch()`` preview contract:

- ``GROUP BY`` and aggregations over a ``vectorSearch()`` relation are
  rejected with an error.
- An outer ``WHERE`` clause applied to a ``vectorSearch()`` subquery is
  rejected with an error, because the predicate would be evaluated only
  after the top-k rows have been selected by vector distance and can
  silently yield zero rows. Place the predicate inside the subquery,
  directly on the ``vectorSearch()`` alias, so it can participate in
  ``WHERE`` pushdown.
- ``JOIN`` between a ``vectorSearch()`` relation and another relation is
  not validated.
- ``UNION`` / ``INTERSECT`` / ``EXCEPT`` combining a ``vectorSearch()``
  relation with another relation are not validated.
- Multiple ``vectorSearch()`` calls in the same query are not validated.
- The query vector must be supplied as a literal. Parameterized vectors
  (for example, values bound from another column) are not supported.
