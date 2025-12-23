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

  /**
   * Initializes the convertlet table and registers PPL-specific operator convertlets.
   *
   * <p>Registers IP-aware convertlets for standard comparison operators so expressions
   * involving OpenSearch IP types use the corresponding PPL IP operators. Also registers
   * a special convertlet for the PPL ATAN operator that remaps it to the standard SQL
   * ATAN operator before conversion.
   */
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

  /**
   * Selects the appropriate convertlet for the given SQL call, preferring registered custom convertlets.
   *
   * @param call the SQL call whose operator determines which convertlet to use
   * @return the custom convertlet registered for the call's operator, or the convertlet from StandardConvertletTable if none was registered; may be `null` if no convertlet is available
   */
  @Override
  public @Nullable SqlRexConvertlet get(SqlCall call) {
    SqlRexConvertlet custom = map.get(call.getOperator());
    if (custom != null) return custom;
    return StandardConvertletTable.INSTANCE.get(call);
  }

  /**
   * Registers a convertlet for the specified SQL operator by storing it in the internal map.
   *
   * @param op the SQL operator to associate the convertlet with
   * @param convertlet the convertlet to register for the operator
   */
  private void registerOperator(
      @UnderInitialization PplConvertletTable this, SqlOperator op, SqlRexConvertlet convertlet) {
    map.put(op, convertlet);
  }

  /**
   * Create a convertlet that substitutes a binary operator with the given PPL IP-specific function when either operand is an IP type.
   *
   * @param substitute the PPL `SqlFunction` to use as a replacement when an operand is an IP type
   * @return a `SqlRexConvertlet` that converts the call to `substitute` if either operand's type is IP, otherwise returns the standard converted `RexCall`
   */
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