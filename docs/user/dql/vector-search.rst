
==============================
Vector Search [Experimental]
==============================

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2

Introduction
============

``vectorSearch()`` is an experimental feature. Syntax, options, and
pushdown behavior may change in future releases based on feedback.

The ``vectorSearch()`` table function runs a k-NN query against a ``knn_vector``
field and exposes the matching documents as a relation in the ``FROM`` clause.
It relies on the OpenSearch `k-NN plugin
<https://docs.opensearch.org/latest/vector-search/>`_. The target index must
map the vector field as ``knn_vector`` and the index must be created with
``index.knn: true``.

The SQL layer translates ``vectorSearch()`` into an OpenSearch search
request whose body is native k-NN query DSL; the query vector is parsed
into a numeric array before that DSL is emitted.

Relevance is expressed through the OpenSearch ``_score`` metadata field, and
results are returned ordered by ``_score DESC`` by default.

vectorSearch
============

Description
-----------

``vectorSearch(table='<index>', field='<vector-field>', vector='<array>', option='<key=value[,key=value]*>')``

All four arguments are required and must be passed by name as string
literals. Positional arguments, or a mix of positional and named
arguments, are not supported. For example, the following is invalid::

    FROM vectorSearch('my_vectors', field='embedding',
                      vector='[0.1,0.2]', option='k=5') AS v

A table alias is required. Projected fields are referenced through the
alias (``v._id``, ``v._score``, ``v.category``).

If the ``opensearch-knn`` plugin is not installed on the target cluster,
query execution fails with a ``vectorSearch() requires the k-NN plugin``
error. ``_explain`` continues to work without the plugin.

Arguments
---------

- ``table``: single concrete index or alias to search. Wildcards
  (``*``), comma-separated multi-index targets, ``_all``, ``.``, and
  ``..`` are not supported. The target index must have
  ``index.knn: true`` and map the target field as ``knn_vector``. A
  normal alias name is accepted. If the alias resolves to multiple
  backing indices, the SQL layer does not prevalidate that every
  backing index has a compatible ``knn_vector`` mapping, dimension, or
  engine; OpenSearch execution remains the source of truth for those
  checks.
- ``field``: name of the ``knn_vector`` field.
- ``vector``: query vector as a JSON-style array of numbers, passed as a
  string (for example, ``'[0.1, 0.2, 0.3]'``). Components must be
  comma-separated finite numbers. Semicolon, colon, and pipe separators
  are not supported, and empty components (for example, ``'[1.0,,2.0]'``
  or ``'[1.0,]'``) return an error. The vector dimension must match the
  ``knn_vector`` mapping on the target index.
- ``option``: comma-separated ``key=value`` pairs. Exactly one of ``k``,
  ``max_distance``, or ``min_score`` is required. ``filter_type`` is
  optional.

Supported option keys
---------------------

Option keys are lower-case and case-sensitive. ``K=5`` or
``Filter_Type=post`` returns an "Unknown option key" error.

- ``k``: top-k mode. Integer between 1 and 10000. The query returns up to
  ``k`` nearest neighbors.
- ``max_distance``: radial mode. Non-negative number. Matches documents
  within the given distance of the query vector. ``LIMIT`` is required and
  caps the returned rows.
- ``min_score``: radial mode. Non-negative number. Matches documents with
  score at or above the given threshold. ``LIMIT`` is required and caps
  the returned rows.
- ``filter_type``: ``post`` or ``efficient``. Controls how a ``WHERE``
  clause is applied. See `Filtering`_.

``k``, ``max_distance``, and ``min_score`` are mutually exclusive; specify
exactly one.

Native k-NN tuning options (for example, ``method_parameters.ef_search``,
``method_parameters.nprobes``, ``rescore.oversample_factor``) are not
supported through ``vectorSearch()`` and return an "Unknown option
key" error.

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

Return up to the specified ``LIMIT`` documents within a maximum distance
of the query vector. ``LIMIT`` is required for radial searches; without
it the result set would be unbounded::

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

Return up to the specified ``LIMIT`` documents whose score is at or
above the given threshold. ``LIMIT`` is required for radial searches;
without it the result set would be unbounded::

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

- ``efficient`` (default): the ``WHERE`` predicate is embedded directly
  inside the k-NN query (``knn.filter``), enabling native efficient
  k-NN filtering during vector search. Efficient filtering depends on
  native k-NN engine and method support; if the target index does not
  support ``knn.filter`` for the configured engine and method, set
  ``filter_type=post``. See the `k-NN filtering guide
  <https://docs.opensearch.org/latest/vector-search/filter-search-knn/efficient-knn-filtering/>`_
  for engine and method requirements.
- ``post``: the k-NN query is placed in a scoring (``bool.must``)
  context and the ``WHERE`` predicate is placed as a non-scoring
  ``bool.filter`` outside the k-NN clause. This is Boolean filter
  placement, not the REST ``post_filter`` parameter, and may return
  fewer than ``k`` rows when the filter is selective.

Full-text predicates (``match``, ``match_phrase``, ``multi_match``, and
the rest of the full-text family) under a ``WHERE`` clause are used as
filters, not as hybrid keyword-vector score fusion. Their placement
follows ``filter_type``: the default (``efficient``) embeds supported
full-text predicates under ``knn.filter``, while ``post`` places them
in ``bool.filter`` outside the k-NN clause. In both cases they restrict
which candidates are retained but their text relevance score does not
combine with the vector ``_score``. ``vectorSearch()`` is not a hybrid
vector + text relevance scorer.

Behavior depends on whether ``filter_type`` is specified:

- **Omitted (default, ``efficient``)**: the ``WHERE`` predicate is
  embedded under ``knn.filter`` so the k-NN engine applies native
  efficient filtering during vector search. A query with no ``WHERE``
  clause is valid. ``efficient`` supports simple native filters:
  ``term``, ``range``, ``wildcard``, ``exists``, full-text family
  (``match``, ``match_phrase``, ``match_phrase_prefix``,
  ``match_bool_prefix``, ``multi_match``, ``query_string``,
  ``simple_query_string``), and boolean combinations of those filters.
  Predicates that compile to script queries (arithmetic, function calls
  on indexed fields, ``CASE``, date math), nested predicates, and other
  query shapes are not supported under ``knn.filter`` and return an
  error. Set ``filter_type=post`` to apply such predicates after the
  k-NN search. If the predicate cannot be translated to an OpenSearch
  filter query at all (a distinct translation failure from the
  unsupported-shape cases above), the default path falls back to
  evaluating the ``WHERE`` clause in memory after the k-NN results are
  returned.
- **Explicit ``efficient``**: same contract as the default. Specifying
  it is useful when a query should be explicit about the placement
  strategy and should fail if the predicate cannot be safely embedded
  under ``knn.filter``.
- **Explicit ``post``**: a ``WHERE`` clause is required and must be
  translatable to an OpenSearch filter query. Predicates that translate
  to native OpenSearch queries are pushed down as a ``bool.filter``
  alongside the k-NN query. Predicates that do not have a native
  equivalent (for example, arithmetic or function calls on indexed
  fields) are pushed down as an OpenSearch script query and evaluated
  server-side. If predicate translation itself fails, the query returns
  an error; there is no silent in-memory fallback under explicit
  ``post``. Use ``filter_type=post`` when the predicate shape is not
  supported by efficient filtering.

Example 4: Default efficient filtering (no ``filter_type``)
-----------------------------------------------------------

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

The predicate is embedded under ``knn.filter`` so the k-NN engine
applies native efficient filtering during vector search.

Example 5: Post-filtering for predicates not supported by efficient mode
------------------------------------------------------------------------

Use ``filter_type=post`` for predicates that do not fit the ``efficient``
allow-list, such as arithmetic or function calls on indexed fields::

    POST /_plugins/_sql
    {
      "query" : """
        SELECT v._id, v._score, v.category
        FROM vectorSearch(
          table='my_vectors',
          field='embedding',
          vector='[0.1, 0.2, 0.3]',
          option='k=10,filter_type=post'
        ) AS v
        WHERE v.price * 1.1 < 100
      """
    }

Scoring, sorting, and limits
============================

- ``vectorSearch()`` exposes the OpenSearch ``_score`` metadata field on the
  alias. For an alias ``v``, select it as ``v._score``.
- ``_score`` can be selected and referenced in ``ORDER BY``, but it cannot
  appear in ``WHERE``. Use ``option='min_score=...'`` for score-threshold
  vector search.
- Results are returned in ``_score DESC`` order by default. The only
  supported ``ORDER BY`` expression is ``<alias>._score DESC`` (for
  example, ``v._score DESC``).
- In top-k mode (``k=N``), ``LIMIT n`` is optional; when present, ``n`` must
  be ``≤ k``.
- In radial mode (``max_distance`` or ``min_score``), ``LIMIT`` is required.
- ``OFFSET`` is not supported on ``vectorSearch()``. Use ``LIMIT`` only.

Limitations
===========

The following are not supported on ``vectorSearch()``:

- ``GROUP BY`` and aggregations directly over a ``vectorSearch()``
  relation are not supported and return an error.
- Operators wrapped around a ``vectorSearch()`` subquery are rejected
  when they would run after ``vectorSearch()`` has already produced a
  finite result set, because they can silently yield zero, skipped, or
  incorrectly ordered rows. Specifically, an outer ``WHERE``,
  ``ORDER BY``, ``OFFSET`` (non-zero), ``GROUP BY``, aggregation, or
  ``DISTINCT`` applied to a ``vectorSearch()`` subquery returns an
  error. Place ``WHERE`` predicates inside the subquery, directly on
  the ``vectorSearch()`` alias, so that they participate in ``WHERE``
  pushdown. A plain outer ``LIMIT`` (without ``OFFSET``) wrapping a
  ``vectorSearch()`` subquery is allowed and caps the returned rows.
- ``JOIN`` between a ``vectorSearch()`` relation and another relation is
  not supported.
- ``UNION`` / ``INTERSECT`` / ``EXCEPT`` combining a ``vectorSearch()``
  relation with another relation is not supported.
- Multiple ``vectorSearch()`` calls in the same query are not supported.
- The query vector must be supplied as a literal. Parameterized vectors
  (for example, values bound from another column) are not supported.
- Indexes that define a user field named ``_score`` cannot be queried
  with ``vectorSearch()`` because ``_score`` is reserved for the
  synthetic vector score exposed on the alias. Rename the field or query
  the index with a plain ``SELECT``.
