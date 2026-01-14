/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate.converters;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * An extension of {@link RelToSqlConverter} to convert a relation algebra tree, translated from a
 * PPL query, into a SQL statement.
 *
 * <p>This converter is used during the validation phase to convert RelNode back to SqlNode for
 * validation and type checking using Calcite's SqlValidator.
 */
public class OpenSearchRelToSqlConverter extends RelToSqlConverter {
  /**
   * Creates a RelToSqlConverter for PPL.
   *
   * @param dialect the SQL dialect to use for conversion
   */
  public OpenSearchRelToSqlConverter(SqlDialect dialect) {
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

  @Override
  public Result visit(Aggregate e) {
    Result r = super.visit(e);
    if (!e.getHints().isEmpty()) {
      List<SqlNode> hints =
          e.getHints().stream()
              .map(relHint -> (SqlNode) toSqlHint(relHint, POS))
              .collect(Collectors.toCollection(ArrayList::new));
      r.asSelect().setHints(SqlNodeList.of(POS, hints));
    }
    return r;
  }

  /**
   * Converts a RelHint to a SqlHint.
   *
   * <p>Copied from {@link RelToSqlConverter#toSqlHint(RelHint, SqlParserPos)} (as Calcite 1.41) as
   * it is private there
   */
  private static SqlHint toSqlHint(RelHint hint, SqlParserPos pos) {
    if (hint.kvOptions != null) {
      return new SqlHint(
          pos,
          new SqlIdentifier(hint.hintName, pos),
          SqlNodeList.of(
              pos,
              hint.kvOptions.entrySet().stream()
                  .flatMap(
                      e ->
                          Stream.of(
                              new SqlIdentifier(e.getKey(), pos),
                              SqlLiteral.createCharString(e.getValue(), pos)))
                  .collect(Collectors.toList())),
          SqlHint.HintOptionFormat.KV_LIST);
    } else if (hint.listOptions != null) {
      return new SqlHint(
          pos,
          new SqlIdentifier(hint.hintName, pos),
          SqlNodeList.of(
              pos,
              hint.listOptions.stream()
                  .map(e -> SqlLiteral.createCharString(e, pos))
                  .collect(Collectors.toList())),
          SqlHint.HintOptionFormat.LITERAL_LIST);
    }
    return new SqlHint(
        pos,
        new SqlIdentifier(hint.hintName, pos),
        SqlNodeList.EMPTY,
        SqlHint.HintOptionFormat.EMPTY);
  }
}
