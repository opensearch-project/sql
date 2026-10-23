/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.*;
import static org.opensearch.sql.data.type.ExprCoreType.BINARY;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.type.ExprCoreType;

class CoercionUtilsTest {

  private static final RexBuilder REX_BUILDER = new RexBuilder(OpenSearchTypeFactory.TYPE_FACTORY);

  private static RexNode nullLiteral(ExprCoreType type) {
    return REX_BUILDER.makeNullLiteral(OpenSearchTypeFactory.convertExprTypeToRelDataType(type));
  }

  private static Stream<Arguments> commonWidestTypeArguments() {
    return Stream.of(
        Arguments.of(STRING, INTEGER, DOUBLE),
        Arguments.of(INTEGER, STRING, DOUBLE),
        Arguments.of(STRING, DOUBLE, DOUBLE),
        Arguments.of(INTEGER, BOOLEAN, null),
        Arguments.of(BINARY, STRING, BINARY),
        Arguments.of(STRING, BINARY, BINARY));
  }

  @ParameterizedTest
  @MethodSource("commonWidestTypeArguments")
  public void findCommonWidestType(
      ExprCoreType left, ExprCoreType right, ExprCoreType expectedCommonType) {
    assertEquals(
        expectedCommonType, CoercionUtils.resolveCommonType(left, right).orElseGet(() -> null));
  }

  @Test
  void widenArgumentsUnifiesPlainTimestampWithDateUdtBounds() {
    // Reproduces the BETWEEN/IN operand set seen on a non-UDT path (e.g. the analytics engine's
    // parquet scan): a standard Calcite TIMESTAMP field against EXPR_DATE UDT bounds.
    // leastRestrictive
    // returns no common type for this mix, but the temporal widening used by comparison operators
    // must resolve all three to a single timestamp type so the predicate can run.
    RexNode plainTimestampField =
        REX_BUILDER.makeInputRef(
            OpenSearchTypeFactory.TYPE_FACTORY.createTypeWithNullability(
                OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP), true),
            0);
    RexNode dateLower =
        REX_BUILDER.makeNullLiteral(
            OpenSearchTypeFactory.TYPE_FACTORY.createUDT(
                OpenSearchTypeFactory.ExprUDT.EXPR_DATE, true));
    RexNode dateUpper =
        REX_BUILDER.makeNullLiteral(
            OpenSearchTypeFactory.TYPE_FACTORY.createUDT(
                OpenSearchTypeFactory.ExprUDT.EXPR_DATE, true));

    List<RexNode> widened =
        CoercionUtils.widenArguments(
            REX_BUILDER, List.of(plainTimestampField, dateLower, dateUpper));

    assertNotNull(widened);
    assertEquals(3, widened.size());
    for (RexNode node : widened) {
      assertEquals(
          ExprCoreType.TIMESTAMP,
          OpenSearchTypeFactory.convertRelDataTypeToExprType(node.getType()));
    }
  }

  @Test
  void castArgumentsReturnsExactMatchWhenAvailable() {
    PPLTypeChecker typeChecker =
        new StubTypeChecker(List.of(List.of(sqlType(INTEGER)), List.of(sqlType(DOUBLE))));
    List<RexNode> arguments = List.of(nullLiteral(INTEGER));

    List<RexNode> result = CoercionUtils.castArguments(REX_BUILDER, typeChecker, arguments);

    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals(
        INTEGER, OpenSearchTypeFactory.convertRelDataTypeToExprType(result.getFirst().getType()));
  }

  @Test
  void castArgumentsFallsBackToWidestCandidate() {
    PPLTypeChecker typeChecker =
        new StubTypeChecker(List.of(List.of(sqlType(ExprCoreType.LONG)), List.of(sqlType(DOUBLE))));
    List<RexNode> arguments = List.of(nullLiteral(STRING));

    List<RexNode> result = CoercionUtils.castArguments(REX_BUILDER, typeChecker, arguments);

    assertNotNull(result);
    assertEquals(
        DOUBLE, OpenSearchTypeFactory.convertRelDataTypeToExprType(result.getFirst().getType()));
  }

  @Test
  void castArgumentsReturnsNullWhenNoCompatibleSignatureExists() {
    PPLTypeChecker typeChecker =
        new StubTypeChecker(
            List.of(
                List.of(OpenSearchTypeFactory.TYPE_FACTORY.createSqlType(SqlTypeName.GEOMETRY))));
    List<RexNode> arguments = List.of(nullLiteral(INTEGER));

    assertNull(CoercionUtils.castArguments(REX_BUILDER, typeChecker, arguments));
  }

  private static RelDataType sqlType(ExprCoreType type) {
    return OpenSearchTypeFactory.convertExprTypeToRelDataType(type);
  }

  private static class StubTypeChecker implements PPLTypeChecker {
    private final List<List<RelDataType>> signatures;

    private StubTypeChecker(List<List<RelDataType>> signatures) {
      this.signatures = signatures;
    }

    @Override
    public boolean checkOperandTypes(List<RelDataType> types) {
      return false;
    }

    @Override
    public String getAllowedSignatures() {
      return "";
    }

    @Override
    public List<List<RelDataType>> getParameterTypes() {
      return signatures;
    }
  }
}
