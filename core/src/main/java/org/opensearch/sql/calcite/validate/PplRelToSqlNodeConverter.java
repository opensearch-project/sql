/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;

/**
 * An extension of {@link RelToSqlConverter} to convert a relation algebra tree, translated from a
 * PPL query, into a SQL statement.
 *
 * <p>This converter is used during the validation phase to convert RelNode back to SqlNode for
 * validation and type checking using Calcite's SqlValidator.
 */
public class PplRelToSqlNodeConverter extends RelToSqlConverter {
  /**
   * Creates a RelToSqlConverter for PPL.
   *
   * @param dialect the SQL dialect to use for conversion
   */
  public PplRelToSqlNodeConverter(SqlDialect dialect) {
    super(dialect);
  }

  /** Override Correlate visitor to pass on join type */
  @Override
  public Result visit(Correlate e) {
    Result result = super.visit(e);
    SqlNode from = result.asSelect().getFrom();
    if (e.getJoinType() != JoinRelType.INNER && from instanceof SqlJoin join) {
      JoinType joinType;
      try {
        joinType = JoinType.valueOf(e.getJoinType().name());
      } catch (IllegalArgumentException ignored) {
        return result;
      }
      join.setOperand(2, joinType.symbol(POS));
      // INNER, LEFT, RIGHT, FULL, or ASOF join requires a condition
      // Use ON TRUE to satisfy SQL syntax because the actual correlation condition logic is inside
      // the subquery's WHERE clause
      join.setOperand(4, JoinConditionType.ON.symbol(POS));
      join.setOperand(5, SqlLiteral.createBoolean(true, POS));
    }
    return result;
  }
}
