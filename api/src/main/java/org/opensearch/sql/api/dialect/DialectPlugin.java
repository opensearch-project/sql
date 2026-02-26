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
 */
public interface DialectPlugin {

  /** Returns the unique dialect name used in the {@code ?dialect=} parameter (e.g., "clickhouse"). */
  String dialectName();

  /** Returns the preprocessor that strips/transforms dialect-specific clauses before parsing. */
  QueryPreprocessor preprocessor();

  /** Returns the Calcite {@link SqlParser.Config} for this dialect (quoting, case sensitivity, etc.). */
  SqlParser.Config parserConfig();

  /** Returns the {@link SqlOperatorTable} containing dialect-specific function definitions. */
  SqlOperatorTable operatorTable();

  /** Returns the Calcite {@link SqlDialect} subclass for unparsing RelNode plans back to this dialect's SQL. */
  SqlDialect sqlDialect();
}
