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
import org.opensearch.sql.calcite.knn.KnnErrors;
import org.opensearch.sql.calcite.knn.KnnPlanBuilder;

/**
 * Transpiles Calcite logical plans ({@link RelNode}) into SQL strings for various target databases.
 * Uses Calcite's {@link RelToSqlConverter} to perform the conversion, respecting the specified SQL
 * dialect.
 */
@Builder
public class UnifiedQueryTranspiler {

  /** Target SQL dialect */
  private final SqlDialect dialect;

  /**
   * Converts a Calcite logical plan to a SQL string using the configured target dialect.
   *
   * @param plan the logical plan to convert (must not be null)
   * @return the generated SQL string
   * @throws org.opensearch.sql.exception.SemanticCheckException if the plan has a k-NN step: no SQL
   *     dialect has the k-NN plugin's approximate nearest-neighbour search
   */
  public String toSql(RelNode plan) {
    if (KnnPlanBuilder.containsKnnScore(plan)) {
      throw KnnErrors.notTranspilable();
    }
    try {
      RelToSqlConverter converter = new RelToSqlConverter(dialect);
      SqlNode sqlNode = converter.visitRoot(plan).asStatement();
      return sqlNode.toSqlString(dialect).getSql();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to transpile logical plan to SQL", e);
    }
  }
}
