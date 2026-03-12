/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.dialect;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * A self-contained dialect implementation providing all components needed to parse, translate, and
 * unparse queries in a specific SQL dialect.
 *
 * <p>Each dialect plugin supplies a {@link QueryPreprocessor} for stripping dialect-specific
 * clauses, a {@link SqlParser.Config} for dialect-aware parsing, a {@link SqlOperatorTable} for
 * dialect function resolution, and a {@link SqlDialect} subclass for unparsing RelNode plans back
 * to dialect-compatible SQL.
 *
 * <h3>Thread-safety</h3>
 *
 * Implementations MUST be thread-safe. All methods may be called concurrently from multiple
 * request-handling threads. Returned components (preprocessor, operator table, etc.) MUST also be
 * thread-safe or stateless.
 *
 * <h3>Lifecycle</h3>
 *
 * <ol>
 *   <li><b>Construction</b>: Plugin is instantiated during system startup.
 *   <li><b>Registration</b>: Plugin is registered with {@link DialectRegistry}.
 *   <li><b>Serving</b>: Plugin methods are called concurrently for each dialect query.
 *   <li><b>Shutdown</b>: No explicit close — plugins should not hold external resources.
 * </ol>
 *
 * <h3>Extension</h3>
 *
 * Third-party dialects can implement this interface and register via {@link
 * DialectRegistry#register} during plugin initialization, or via ServiceLoader SPI in a future
 * release.
 */
public interface DialectPlugin {

  /**
   * Returns the unique dialect name used in the {@code ?dialect=} query parameter (e.g.,
   * "clickhouse"). This name is used for registration in the {@link DialectRegistry} and for
   * matching against the dialect parameter in incoming REST requests.
   *
   * <p>The returned value must be non-null, non-empty, and stable across invocations.
   *
   * @return the dialect name, never {@code null}
   */
  String dialectName();

  /**
   * Returns the preprocessor that strips or transforms dialect-specific clauses from the raw query
   * string before it reaches the Calcite SQL parser.
   *
   * <p>The returned preprocessor must be thread-safe or stateless, as it may be invoked
   * concurrently from multiple request-handling threads.
   *
   * @return the query preprocessor for this dialect, never {@code null}
   */
  QueryPreprocessor preprocessor();

  /**
   * Returns the Calcite {@link SqlParser.Config} for this dialect, controlling quoting style, case
   * sensitivity, and other parser behavior.
   *
   * <p>The returned config is typically an immutable value object and is safe for concurrent use.
   *
   * @return the parser configuration for this dialect, never {@code null}
   */
  SqlParser.Config parserConfig();

  /**
   * Returns the {@link SqlOperatorTable} containing dialect-specific function definitions. This
   * table is chained with Calcite's default operator table during query validation so that
   * dialect-specific functions are resolved alongside standard SQL functions.
   *
   * <p>The returned operator table must be thread-safe, as it may be queried concurrently from
   * multiple request-handling threads.
   *
   * @return the operator table for this dialect, never {@code null}
   */
  SqlOperatorTable operatorTable();

  /**
   * Returns the Calcite {@link SqlDialect} subclass used for unparsing RelNode logical plans back
   * into SQL compatible with this dialect.
   *
   * <p>The returned dialect instance must be thread-safe or stateless.
   *
   * @return the SQL dialect for unparsing, never {@code null}
   */
  SqlDialect sqlDialect();
}
