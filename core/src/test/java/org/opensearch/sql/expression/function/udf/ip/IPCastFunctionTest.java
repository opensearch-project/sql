/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.ip;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

public class IPCastFunctionTest {

  private final RexBuilder rexBuilder = new RexBuilder(OpenSearchTypeFactory.TYPE_FACTORY);
  private final SqlOperator castIpOperator = PPLBuiltinOperators.CAST_IP;

  @Test
  void testCastValidIPv4String() {
    RexNode ipString = rexBuilder.makeLiteral("192.168.1.1");
    RexNode castExpr = rexBuilder.makeCall(castIpOperator, ipString);

    // Verify the return type is IP
    RelDataType returnType = castExpr.getType();
    assertEquals(
        ExprUDT.EXPR_IP.name(),
        ((org.opensearch.sql.calcite.type.ExprSqlType) returnType).getUdt().name());
  }

  @Test
  void testCastValidIPv6String() {
    RexNode ipString = rexBuilder.makeLiteral("2001:db8:85a3:0:0:8a2e:370:7334");
    RexNode castExpr = rexBuilder.makeCall(castIpOperator, ipString);

    // Verify the return type is IP
    RelDataType returnType = castExpr.getType();
    assertEquals(
        ExprUDT.EXPR_IP.name(),
        ((org.opensearch.sql.calcite.type.ExprSqlType) returnType).getUdt().name());
  }

  @Test
  void testCastInvalidIPString() {
    RexNode ipString = rexBuilder.makeLiteral("invalid_ip");
    RexNode castExpr = rexBuilder.makeCall(castIpOperator, ipString);

    // Verify the return type is IP
    RelDataType returnType = castExpr.getType();
    assertEquals(
        ExprUDT.EXPR_IP.name(),
        ((org.opensearch.sql.calcite.type.ExprSqlType) returnType).getUdt().name());
  }

  @Test
  void testCastNullValue() {
    RexNode nullNode =
        rexBuilder.makeNullLiteral(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR));
    RexNode castExpr = rexBuilder.makeCall(castIpOperator, nullNode);

    // Verify the return type is IP
    RelDataType returnType = castExpr.getType();
    assertEquals(
        ExprUDT.EXPR_IP.name(),
        ((org.opensearch.sql.calcite.type.ExprSqlType) returnType).getUdt().name());
  }
}
