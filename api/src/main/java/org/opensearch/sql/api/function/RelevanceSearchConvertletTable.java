/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.ReflectiveConvertletTable;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/**
 * Convertlet table that intercepts relevance search function calls during SQL-to-RelNode conversion
 * and rewrites them as {@link org.apache.calcite.rex.RexCall} nodes using the PPL operators from
 * {@link PPLBuiltinOperators}.
 *
 * <p>This allows the SQL path to use lightweight {@link SqlExtensionFunctions} operators for
 * parsing and validation, while producing RexCall nodes with PPL operators that {@link
 * org.opensearch.sql.opensearch.request.PredicateAnalyzer} recognizes for pushdown — without
 * touching the PPL type checker in core/.
 */
public class RelevanceSearchConvertletTable extends ReflectiveConvertletTable {

  private final StandardConvertletTable standard = StandardConvertletTable.INSTANCE;

  public RelevanceSearchConvertletTable() {
    registerOp(SqlExtensionFunctions.MATCH, swapOperator(PPLBuiltinOperators.MATCH));
    registerOp(SqlExtensionFunctions.MATCH_PHRASE, swapOperator(PPLBuiltinOperators.MATCH_PHRASE));
    registerOp(
        SqlExtensionFunctions.MATCH_BOOL_PREFIX,
        swapOperator(PPLBuiltinOperators.MATCH_BOOL_PREFIX));
    registerOp(
        SqlExtensionFunctions.MATCH_PHRASE_PREFIX,
        swapOperator(PPLBuiltinOperators.MATCH_PHRASE_PREFIX));
    registerOp(SqlExtensionFunctions.MULTI_MATCH, swapOperator(PPLBuiltinOperators.MULTI_MATCH));
    registerOp(
        SqlExtensionFunctions.SIMPLE_QUERY_STRING,
        swapOperator(PPLBuiltinOperators.SIMPLE_QUERY_STRING));
    registerOp(SqlExtensionFunctions.QUERY_STRING, swapOperator(PPLBuiltinOperators.QUERY_STRING));
  }

  @Override
  public SqlRexConvertlet get(SqlCall call) {
    SqlRexConvertlet convertlet = super.get(call);
    return convertlet != null ? convertlet : standard.get(call);
  }

  /**
   * Creates a convertlet that converts operands using standard conversion, then wraps them in a
   * {@link org.apache.calcite.rex.RexCall} with the given PPL operator. This swaps the lightweight
   * SQL operator (used for validation) with the PPL operator (used for pushdown), bypassing the PPL
   * type checker entirely since {@link RexBuilder#makeCall} with an explicit return type skips
   * operand type checking.
   */
  private static SqlRexConvertlet swapOperator(SqlOperator pplOperator) {
    return (cx, call) -> {
      RexBuilder rexBuilder = cx.getRexBuilder();
      List<RexNode> operands = new ArrayList<>();
      for (int i = 0; i < call.operandCount(); i++) {
        operands.add(cx.convertExpression(call.operand(i)));
      }
      return rexBuilder.makeCall(
          rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN), pplOperator, operands);
    };
  }
}
