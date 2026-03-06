.. highlight:: sh

===============
SQL Dialect API
===============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Overview
========

The SQL dialect endpoint enables ClickHouse SQL compatibility on the existing ``/_plugins/_sql`` endpoint. By adding a ``?dialect=clickhouse`` query parameter, you can submit ClickHouse SQL queries directly to OpenSearch. The plugin translates ClickHouse-specific functions and syntax into OpenSearch-compatible equivalents via the Calcite query engine.

This is designed for migration scenarios where users move from ClickHouse to OpenSearch for real-time analytics without rewriting queries — particularly useful for Grafana dashboards.

.. note::

   The dialect endpoint requires the Calcite engine to be enabled. Set ``plugins.sql.calcite.engine.enabled`` to ``true`` in your cluster settings.


Usage
=====

Send a POST request to ``/_plugins/_sql`` with the ``dialect`` query parameter::

    >> curl -H 'Content-Type: application/json' \
         -X POST 'localhost:9200/_plugins/_sql?dialect=clickhouse' \
         -d '{
      "query": "SELECT toStartOfHour(timestamp) AS hour, count() FROM logs GROUP BY hour ORDER BY hour"
    }'

The response uses the same JDBC JSON format as standard SQL queries::

    {
      "schema": [
        {"name": "hour", "type": "timestamp"},
        {"name": "count()", "type": "long"}
      ],
      "datarows": [
        ["2024-01-01T00:00:00Z", 42],
        ["2024-01-01T01:00:00Z", 37]
      ],
      "total": 2,
      "size": 2,
      "status": 200
    }


Supported ClickHouse Functions
==============================

Time-Bucketing Functions
------------------------

These functions are translated to ``DATE_TRUNC`` expressions.


+---------------------------+-------------------------------+
| ClickHouse Function       | OpenSearch Equivalent         |
+===========================+===============================+
| ``toStartOfHour(col)``    | ``DATE_TRUNC('HOUR', col)``   |
+---------------------------+-------------------------------+
| ``toStartOfDay(col)``     | ``DATE_TRUNC('DAY', col)``    |
+---------------------------+-------------------------------+
| ``toStartOfMinute(col)``  | ``DATE_TRUNC('MINUTE', col)`` |
+---------------------------+-------------------------------+
| ``toStartOfWeek(col)``    | ``DATE_TRUNC('WEEK', col)``   |
+---------------------------+-------------------------------+
| ``toStartOfMonth(col)``   | ``DATE_TRUNC('MONTH', col)``  |
+---------------------------+-------------------------------+
| ``toStartOfInterval(col,  | ``DATE_TRUNC(unit, col)``     |
| INTERVAL N unit)``        |                               |
+---------------------------+-------------------------------+

Type-Conversion Functions
-------------------------

These functions are translated to ``CAST`` expressions.

+---------------------+----------------------------+
| ClickHouse Function | OpenSearch Equivalent      |
+=====================+============================+
| ``toDateTime(x)``   | ``CAST(x AS TIMESTAMP)``   |
+---------------------+----------------------------+
| ``toDate(x)``       | ``CAST(x AS DATE)``        |
+---------------------+----------------------------+
| ``toString(x)``     | ``CAST(x AS VARCHAR)``     |
+---------------------+----------------------------+
| ``toUInt32(x)``     | ``CAST(x AS INTEGER)``     |
+---------------------+----------------------------+
| ``toInt32(x)``      | ``CAST(x AS INTEGER)``     |
+---------------------+----------------------------+
| ``toInt64(x)``      | ``CAST(x AS BIGINT)``      |
+---------------------+----------------------------+
| ``toFloat64(x)``    | ``CAST(x AS DOUBLE)``      |
+---------------------+----------------------------+
| ``toFloat32(x)``    | ``CAST(x AS FLOAT)``       |
+---------------------+----------------------------+

Aggregate Functions
-------------------

+---------------------+----------------------------+
| ClickHouse Function | OpenSearch Equivalent      |
+=====================+============================+
| ``uniq(expr)``      | ``COUNT(DISTINCT expr)``   |
+---------------------+----------------------------+
| ``uniqExact(expr)`` | ``COUNT(DISTINCT expr)``   |
+---------------------+----------------------------+
| ``groupArray(expr)``| ``ARRAY_AGG(expr)``        |
+---------------------+----------------------------+
| ``count()``         | ``COUNT(*)``               |
+---------------------+----------------------------+

Conditional Functions
---------------------

+--------------------------------------+----------------------------------------------+
| ClickHouse Function                  | OpenSearch Equivalent                        |
+======================================+==============================================+
| ``if(cond, then_val, else_val)``     | ``CASE WHEN cond THEN then_val               |
|                                      | ELSE else_val END``                          |
+--------------------------------------+----------------------------------------------+
| ``multiIf(c1, v1, c2, v2, default)``| ``CASE WHEN c1 THEN v1 WHEN c2 THEN v2      |
|                                      | ELSE default END``                           |
+--------------------------------------+----------------------------------------------+

Special Functions
-----------------

+--------------------------------------+----------------------------------------------+
| ClickHouse Function                  | OpenSearch Equivalent                        |
+======================================+==============================================+
| ``quantile(level)(expr)``            | ``PERCENTILE_CONT(level) WITHIN GROUP        |
|                                      | (ORDER BY expr)``                            |
+--------------------------------------+----------------------------------------------+
| ``formatDateTime(dt, fmt)``          | ``DATE_FORMAT(dt, fmt)``                     |
+--------------------------------------+----------------------------------------------+
| ``now()``                            | ``CURRENT_TIMESTAMP``                        |
+--------------------------------------+----------------------------------------------+
| ``today()``                          | ``CURRENT_DATE``                             |
+--------------------------------------+----------------------------------------------+


Known Behavioral Differences
============================

Some translated functions have semantic differences from their native ClickHouse counterparts:

**uniq() — Approximation vs Exact Count**
  ClickHouse ``uniq()`` uses HyperLogLog approximation (~2% error rate for large cardinalities). The translated ``COUNT(DISTINCT)`` is exact. Use ``uniqExact()`` in ClickHouse if you need exact counts — the translation is equivalent.

**toDateTime() — NULL Handling**
  ClickHouse ``toDateTime()`` returns ``NULL`` for unparseable strings (e.g., ``toDateTime('not-a-date')`` → NULL). The translated ``CAST(x AS TIMESTAMP)`` may throw a runtime error for invalid input. Pre-validate your data or handle NULLs explicitly.

**Timezone Differences**
  ClickHouse time-bucketing functions (``toStartOfHour``, ``toStartOfInterval``, etc.) use the *server timezone* by default. The translated ``DATE_TRUNC`` uses the *session timezone*. Time-bucket boundaries may shift if server and session timezones differ.

**Unsigned Integer Types**
  ClickHouse distinguishes ``toUInt32`` (unsigned) from ``toInt32`` (signed). OpenSearch has no unsigned integer types, so ``toUInt32`` maps to ``CAST(x AS INTEGER)`` (signed). Values exceeding ``Integer.MAX_VALUE`` in the unsigned range may overflow.

**groupArray() — Ordering**
  ClickHouse ``groupArray()`` preserves insertion order. The translated ``ARRAY_AGG()`` order is implementation-defined unless an explicit ``ORDER BY`` is specified within the aggregate.

**quantile() — Interpolation**
  ClickHouse ``quantile()`` uses a sampling-based approximation (t-digest). The translated ``PERCENTILE_CONT`` uses linear interpolation on the exact sorted dataset. Results may diverge for small datasets or extreme quantile levels.

**now() — Precision**
  ClickHouse ``now()`` returns second-precision DateTime. ``CURRENT_TIMESTAMP`` may return higher precision (milliseconds or microseconds) depending on the engine.

**formatDateTime() — Format Patterns**
  ClickHouse format specifiers (e.g., ``%Y-%m-%d %H:%M:%S``) are passed through as-is. No automatic pattern conversion is performed.


Clause Stripping
================

ClickHouse-specific clauses that OpenSearch does not support are automatically stripped before query parsing. The preprocessor is token-aware — it only strips top-level clause occurrences and preserves keywords inside string literals, comments, and function arguments.

Stripped Clauses
----------------

**FORMAT** — Removed along with its argument::

    -- Input
    SELECT * FROM logs FORMAT JSONEachRow

    -- After preprocessing
    SELECT * FROM logs

**SETTINGS** — Removed along with all key=value pairs::

    -- Input
    SELECT * FROM logs SETTINGS max_threads=4, max_memory_usage=1000000

    -- After preprocessing
    SELECT * FROM logs

**FINAL** — Removed (used for ReplacingMergeTree deduplication)::

    -- Input
    SELECT * FROM logs FINAL

    -- After preprocessing
    SELECT * FROM logs

**Multiple clauses** are stripped regardless of order::

    -- Input
    SELECT * FROM logs FINAL SETTINGS max_threads=4 FORMAT JSON

    -- After preprocessing
    SELECT * FROM logs

Preserved Contexts
------------------

Keywords inside string literals, comments, and function arguments are not stripped::

    -- String literal: preserved
    SELECT 'FORMAT' AS label FROM logs

    -- Block comment: preserved
    SELECT /* FORMAT JSON */ * FROM logs

    -- Line comment: preserved
    SELECT * FROM logs -- FINAL

    -- Function argument: preserved
    SELECT format(col, 'JSON') FROM logs


Grafana Migration Tips
======================

If you are migrating Grafana dashboards from a ClickHouse datasource to OpenSearch:

1. **Install the OpenSearch datasource plugin** in Grafana if not already installed.

2. **Configure the datasource** to point to your OpenSearch cluster's SQL endpoint. In the datasource settings, set the URL to your OpenSearch endpoint (e.g., ``https://your-cluster:9200``).

3. **Append the dialect parameter** to the SQL endpoint path. In the OpenSearch datasource configuration, set the path to ``/_plugins/_sql?dialect=clickhouse`` so all queries from this datasource use ClickHouse SQL syntax.

4. **Enable the Calcite engine** on your OpenSearch cluster::

    PUT _cluster/settings
    {
      "persistent": {
        "plugins.sql.calcite.engine.enabled": true
      }
    }

5. **Test your dashboards**. Most ClickHouse time-series queries using ``toStartOfHour``, ``toStartOfDay``, ``count()``, ``uniq()``, and similar functions should work without modification.

6. **Review behavioral differences** (see above). Pay attention to:

   - ``uniq()`` returns exact counts instead of approximate — results may differ slightly for high-cardinality columns
   - Timezone handling may differ if your ClickHouse server timezone differs from the OpenSearch session timezone
   - ``FORMAT``, ``SETTINGS``, and ``FINAL`` clauses are silently stripped

7. **Remove unsupported clauses** if you prefer explicit control. While the preprocessor strips ``FORMAT``, ``SETTINGS``, and ``FINAL`` automatically, you may want to remove them from your queries for clarity.


Error Responses
===============

The dialect endpoint returns structured error responses with appropriate HTTP status codes.

+----------------------------+--------+---------------------------------------------------+
| Error Condition            | Status | Description                                       |
+============================+========+===================================================+
| Unknown dialect            | 400    | Dialect not registered. Response includes list of  |
|                            |        | supported dialects.                               |
+----------------------------+--------+---------------------------------------------------+
| Empty dialect parameter    | 400    | The ``dialect`` parameter must be non-empty.       |
+----------------------------+--------+---------------------------------------------------+
| Calcite engine disabled    | 400    | Dialect support requires the Calcite engine.       |
+----------------------------+--------+---------------------------------------------------+
| SQL parse error            | 400    | Malformed query. Includes line/column position     |
|                            |        | where available.                                  |
+----------------------------+--------+---------------------------------------------------+
| Unsupported function       | 422    | Function not recognized. Includes function name    |
|                            |        | and available alternatives.                       |
+----------------------------+--------+---------------------------------------------------+
| Missing index              | 404    | Query references a non-existent index.             |
+----------------------------+--------+---------------------------------------------------+
| Internal error             | 500    | Sanitized message with ``internal_id`` for log     |
|                            |        | correlation. No stack traces exposed.             |
+----------------------------+--------+---------------------------------------------------+

Example — unknown dialect::

    >> curl -X POST 'localhost:9200/_plugins/_sql?dialect=clickhous' \
         -H 'Content-Type: application/json' \
         -d '{"query": "SELECT 1"}'

    {
      "error_type": "UNKNOWN_DIALECT",
      "message": "Unknown SQL dialect 'clickhous'. Supported dialects: [clickhouse]",
      "dialect_requested": "clickhous"
    }

Example — parse error::

    {
      "error": {
        "reason": "Invalid Query",
        "details": "...",
        "type": "DialectQueryException",
        "position": {"line": 1, "column": 8}
      },
      "status": 400
    }


Extending with Custom Dialects
==============================

The dialect framework is designed to be extensible. Third-party developers can add support for additional SQL dialects (e.g., Presto, Trino, MySQL) by implementing the ``DialectPlugin`` interface and registering it via the Java ``ServiceLoader`` SPI mechanism.

Implementing the DialectPlugin Interface
-----------------------------------------

Create a class that implements ``org.opensearch.sql.api.dialect.DialectPlugin``. You must provide five components:

- **dialectName()** — A unique identifier used in the ``?dialect=`` query parameter.
- **preprocessor()** — A ``QueryPreprocessor`` that strips or transforms dialect-specific clauses before Calcite parsing.
- **parserConfig()** — A Calcite ``SqlParser.Config`` controlling quoting style and case sensitivity for your dialect.
- **operatorTable()** — A Calcite ``SqlOperatorTable`` that maps dialect-specific functions to Calcite equivalents.
- **sqlDialect()** — A Calcite ``SqlDialect`` subclass for unparsing RelNode plans back to your dialect's SQL.

All returned components must be thread-safe or stateless, as they are called concurrently from multiple request threads.

Minimal Example
---------------

.. code-block:: java

    package com.example.dialect;

    import org.apache.calcite.sql.SqlDialect;
    import org.apache.calcite.sql.SqlOperatorTable;
    import org.apache.calcite.sql.fun.SqlStdOperatorTable;
    import org.apache.calcite.sql.parser.SqlParser;
    import org.apache.calcite.sql.validate.SqlConformanceEnum;
    import org.opensearch.sql.api.dialect.DialectPlugin;
    import org.opensearch.sql.api.dialect.QueryPreprocessor;

    public class MyCustomDialectPlugin implements DialectPlugin {

        public static final MyCustomDialectPlugin INSTANCE = new MyCustomDialectPlugin();

        @Override
        public String dialectName() {
            return "mycustomdialect";
        }

        @Override
        public QueryPreprocessor preprocessor() {
            // No-op preprocessor if no dialect-specific clauses need stripping
            return query -> query;
        }

        @Override
        public SqlParser.Config parserConfig() {
            return SqlParser.config()
                .withCaseSensitive(false);
        }

        @Override
        public SqlOperatorTable operatorTable() {
            // Return a custom operator table with dialect function mappings,
            // or use the standard table if no custom functions are needed
            return SqlStdOperatorTable.instance();
        }

        @Override
        public SqlDialect sqlDialect() {
            return SqlDialect.DatabaseProduct.UNKNOWN.getDialect();
        }
    }

Packaging as a JAR
------------------

1. Build your ``DialectPlugin`` implementation into a JAR file.

2. Create a ServiceLoader descriptor file in your JAR at::

       META-INF/services/org.opensearch.sql.api.dialect.DialectPlugin

3. The file should contain the fully qualified class name of your implementation, one per line::

       com.example.dialect.MyCustomDialectPlugin

4. Place the JAR on the OpenSearch SQL plugin's classpath.

Registering via ServiceLoader
-----------------------------

At startup, the OpenSearch SQL plugin can discover and register third-party dialect plugins using Java's ``ServiceLoader`` mechanism. The framework looks for implementations of ``org.opensearch.sql.api.dialect.DialectPlugin`` declared in ``META-INF/services`` descriptor files on the classpath.

The registration flow is:

1. The plugin initialization code calls ``ServiceLoader.load(DialectPlugin.class)``.
2. Each discovered ``DialectPlugin`` is registered with the ``DialectRegistry`` via ``register(plugin)``.
3. After all plugins are registered, the registry is frozen with ``freeze()`` — no further registrations are accepted.
4. The dialect is now available via the ``?dialect=<name>`` query parameter.

.. note::

   Built-in dialects (e.g., ClickHouse) are registered programmatically during plugin initialization and do not use the ServiceLoader mechanism. ServiceLoader is reserved for third-party extensions packaged as separate JARs.

.. warning::

   Third-party dialect JARs must be compatible with the version of the OpenSearch SQL plugin they are loaded into. The ``DialectPlugin`` interface may evolve across major versions.
