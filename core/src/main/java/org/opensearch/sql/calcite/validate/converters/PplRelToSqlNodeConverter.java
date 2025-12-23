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
public class PplRelToSqlNodeConverter extends RelToSqlConverter {
  /**
   * Initializes a RelToSqlConverter customized for PPL using the specified SQL dialect.
   *
   * @param dialect the SQL dialect to apply for SQL generation and dialect-specific behavior
   */
  public PplRelToSqlNodeConverter(SqlDialect dialect) {
    super(dialect);
  }

  /**
   * Propagates a Correlate node's join type into the generated SQL when the FROM clause is a SqlJoin.
   *
   * <p>If the correlate's join type is not INNER and the result's FROM is a SqlJoin, attempts to map
   * the relational join type to a SQL join type and apply it to the SqlJoin. When applied, the join
   * condition is set to ON TRUE so SQL syntax remains valid (the actual correlation predicate is kept
   * inside the subquery's WHERE clause). If the join type cannot be mapped, the result is returned
   * unchanged.
   *
   * @return the original or modified Result reflecting the correlate's join type in the SQL
   */
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
   * Translate SEMI and ANTI joins into Spark SQL native LEFT_SEMI_JOIN or LEFT_ANTI_JOIN
   * semantics by producing a SqlJoin with an ON condition instead of EXISTS/NOT EXISTS subqueries.
   *
   * <p>This avoids a Calcite issue where correlation references inside generated subqueries can
   * become unqualified and resolve to the wrong scope.
   *
   * @return a Result whose FROM node is a SqlJoin using LEFT_SEMI_JOIN or LEFT_ANTI_JOIN with the
   *         converted join condition, and which composes the left and right input results
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

  /**
   * Converts an Aggregate relational node to SQL and attaches any relational hints to the resulting SELECT node.
   *
   * If the aggregate contains RelHint entries, they are converted to SqlHint instances and set on the Result's
   * SELECT node as a hint list.
   *
   * @param e the Aggregate relational node to convert
   * @return the conversion Result whose SELECT node includes converted hints when present
   */
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
   * Convert a Calcite RelHint into an equivalent SqlHint.
   *
   * Converts key-value options to a KV_LIST, literal options to a LITERAL_LIST,
   * and produces an EMPTY hint when no options are present.
   *
   * @param hint the relational hint to convert
   * @param pos  parser position to attach to created SQL nodes
   * @return a SqlHint representing the same hint and options as {@code hint}
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