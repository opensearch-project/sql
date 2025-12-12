/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Join;
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

  /**
   * Override to convert ANTI and SEMI joins to Spark SQL's native LEFT ANTI JOIN and LEFT SEMI JOIN
   * syntax, instead of using NOT EXISTS / EXISTS subqueries.
   *
   * <p>The default implementation in {@link RelToSqlConverter#visitAntiOrSemiJoin} converts
   * ANTI/SEMI joins to standard SQL using NOT EXISTS / EXISTS subqueries. However, a subtle bug in
   * calcite (as of Calcite 1.41) leads to incorrect results after the conversion: correlation
   * variables in the subquery are generated as unqualified identifiers.
   *
   * <p>For example:
   *
   * <pre>{@code
   * -- Base implementation generates:
   * SELECT ... FROM table1 AS t0
   * WHERE ... AND NOT EXISTS (
   *   SELECT 1 FROM table2 AS t2
   *   WHERE name = t2.name    -- 'name' is unqualified!
   * )
   * }</pre>
   *
   * <p>The unqualified {@code name} is resolved to the inner scope (t2.name) instead of the outer
   * scope (t0.name), resulting in incorrect results.
   *
   * <p>The override implementation uses ANTI / SEMI join syntax:
   *
   * <pre>{@code
   * SELECT ... FROM table1 AS t0
   * LEFT ANTI JOIN table2 AS t2 ON t0.name = t2.name
   * }</pre>
   */
  @Override
  protected Result visitAntiOrSemiJoin(Join e) {
    final Result leftResult = visitInput(e, 0).resetAlias();
    final Result rightResult = visitInput(e, 1).resetAlias();
    final Context leftContext = leftResult.qualifiedContext();
    final Context rightContext = rightResult.qualifiedContext();

    JoinType joinType =
        e.getJoinType() == JoinRelType.ANTI ? JoinType.LEFT_ANTI_JOIN : JoinType.LEFT_SEMI_JOIN;
    SqlNode sqlCondition = convertConditionToSqlNode(e.getCondition(), leftContext, rightContext);
    SqlNode join =
        new SqlJoin(
            POS,
            leftResult.asFrom(),
            SqlLiteral.createBoolean(false, POS),
            joinType.symbol(POS), // LEFT ANTI JOIN or LEFT SEMI JOIN
            rightResult.asFrom(),
            JoinConditionType.ON.symbol(POS),
            sqlCondition);

    return result(join, leftResult, rightResult);
  }
}
