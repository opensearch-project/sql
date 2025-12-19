/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.ReflectiveConvertletTable;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.checkerframework.checker.initialization.qual.UnderInitialization;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.utils.OpenSearchTypeUtil;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

public class PplConvertletTable extends ReflectiveConvertletTable {
  public static PplConvertletTable INSTANCE = new PplConvertletTable();
  private final Map<SqlOperator, SqlRexConvertlet> map = new HashMap<>();

  private PplConvertletTable() {
    super();
    registerOperator(SqlStdOperatorTable.EQUALS, ipConvertlet(PPLBuiltinOperators.EQUALS_IP));
    registerOperator(
        SqlStdOperatorTable.NOT_EQUALS, ipConvertlet(PPLBuiltinOperators.NOT_EQUALS_IP));
    registerOperator(
        SqlStdOperatorTable.GREATER_THAN, ipConvertlet(PPLBuiltinOperators.GREATER_IP));
    registerOperator(
        SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, ipConvertlet(PPLBuiltinOperators.GTE_IP));
    registerOperator(SqlStdOperatorTable.LESS_THAN, ipConvertlet(PPLBuiltinOperators.LESS_IP));
    registerOperator(
        SqlStdOperatorTable.LESS_THAN_OR_EQUAL, ipConvertlet(PPLBuiltinOperators.LTE_IP));
    // There is no implementation for PPLBuiltinOperators.ATAN. It needs to be replaced to
    // SqlStdOperatorTable.ATAN when converted to RelNode
    registerOperator(
        PPLBuiltinOperators.ATAN,
        (cx, call) -> {
          ((SqlBasicCall) call).setOperator(SqlStdOperatorTable.ATAN);
          return StandardConvertletTable.INSTANCE.convertCall(cx, call);
        });
  }

  @Override
  public @Nullable SqlRexConvertlet get(SqlCall call) {
    SqlRexConvertlet custom = map.get(call.getOperator());
    if (custom != null) return custom;
    return StandardConvertletTable.INSTANCE.get(call);
  }

  /** Registers a convertlet for a given operator instance. */
  private void registerOperator(
      @UnderInitialization PplConvertletTable this, SqlOperator op, SqlRexConvertlet convertlet) {
    map.put(op, convertlet);
  }

  private SqlRexConvertlet ipConvertlet(SqlFunction substitute) {
    return (cx, call) -> {
      final RexCall e = (RexCall) StandardConvertletTable.INSTANCE.convertCall(cx, call);
      RelDataType type1 = e.getOperands().get(0).getType();
      RelDataType type2 = e.getOperands().get(1).getType();
      if (OpenSearchTypeUtil.isIp(type1) || OpenSearchTypeUtil.isIp(type2)) {
        return StandardConvertletTable.INSTANCE.convertFunction(cx, substitute, call);
      }
      return e;
    };
  }
}
