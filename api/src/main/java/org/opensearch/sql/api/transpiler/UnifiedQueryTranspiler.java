/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.transpiler;

import lombok.Builder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

/**
 * Transpiles Calcite logical plans ({@link RelNode}) into SQL strings for various target databases.
 * Uses Calcite's {@link RelToSqlConverter} to perform the conversion, respecting the specified SQL
 * dialect and formatting options.
 */
@Builder
public class UnifiedQueryTranspiler {

  private final SqlDialect dialect;

  /**
   * Converts a Calcite logical plan to a SQL string using the configured target dialect.
   *
   * @param plan the logical plan to convert (must not be null)
   * @return the generated SQL string
   */
  public String toSql(RelNode plan) {
    try {
      RelToSqlConverter converter = new RelToSqlConverter(dialect);
      SqlNode sqlNode = converter.visitRoot(plan).asStatement();
      return sqlNode.toSqlString(dialect).getSql();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to transpile logical plan to SQL", e);
    }
  }
}
