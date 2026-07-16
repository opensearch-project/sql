/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.*;

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
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.data.type.ExprCoreType;

class CoercionUtilsTest {

  private static final OpenSearchTypeFactory TF = OpenSearchTypeFactory.TYPE_FACTORY;
  private static final RexBuilder REX_BUILDER = new RexBuilder(TF);

  private static RelDataType type(SqlTypeName name) {
    return TF.createSqlType(name);
  }

  private static RexNode nullLiteral(RelDataType type) {
    return REX_BUILDER.makeNullLiteral(type);
  }

  private static Stream<Arguments> commonWidestTypeArguments() {
    RelDataType string = type(SqlTypeName.VARCHAR);
    RelDataType integer = type(SqlTypeName.INTEGER);
    RelDataType doubleT = type(SqlTypeName.DOUBLE);
    RelDataType bool = type(SqlTypeName.BOOLEAN);
    RelDataType binary = TF.createUDT(ExprUDT.EXPR_BINARY);
    RelDataType varbinary = type(SqlTypeName.VARBINARY);
    return Stream.of(
        Arguments.of(string, integer, doubleT),
        Arguments.of(integer, string, doubleT),
        Arguments.of(string, doubleT, doubleT),
        Arguments.of(integer, bool, null),
        Arguments.of(binary, string, varbinary),
        Arguments.of(string, binary, varbinary));
  }

  @ParameterizedTest
  @MethodSource("commonWidestTypeArguments")
  public void findCommonWidestType(RelDataType left, RelDataType right, RelDataType expected) {
    if (expected == null) {
      assertTrue(CoercionUtils.resolveCommonType(left, right).isEmpty());
    } else {
      assertEquals(
          expected.getSqlTypeName(),
          CoercionUtils.resolveCommonType(left, right)
              .map(RelDataType::getSqlTypeName)
              .orElse(null));
    }
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
        new StubTypeChecker(
            List.of(List.of(type(SqlTypeName.INTEGER)), List.of(type(SqlTypeName.DOUBLE))));
    List<RexNode> arguments = List.of(nullLiteral(type(SqlTypeName.INTEGER)));

    List<RexNode> result = CoercionUtils.castArguments(REX_BUILDER, typeChecker, arguments);

    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals(SqlTypeName.INTEGER, result.getFirst().getType().getSqlTypeName());
  }

  @Test
  void castArgumentsFallsBackToWidestCandidate() {
    PPLTypeChecker typeChecker =
        new StubTypeChecker(
            List.of(List.of(type(SqlTypeName.BIGINT)), List.of(type(SqlTypeName.DOUBLE))));
    List<RexNode> arguments = List.of(nullLiteral(type(SqlTypeName.VARCHAR)));

    List<RexNode> result = CoercionUtils.castArguments(REX_BUILDER, typeChecker, arguments);

    assertNotNull(result);
    assertEquals(SqlTypeName.DOUBLE, result.getFirst().getType().getSqlTypeName());
  }

  @Test
  void castArgumentsReturnsNullWhenNoCompatibleSignatureExists() {
    PPLTypeChecker typeChecker = new StubTypeChecker(List.of(List.of(type(SqlTypeName.GEOMETRY))));
    List<RexNode> arguments = List.of(nullLiteral(type(SqlTypeName.INTEGER)));

    assertNull(CoercionUtils.castArguments(REX_BUILDER, typeChecker, arguments));
  }

  @Test
  void hasStringDetectsCharacterArguments() {
    assertTrue(
        CoercionUtils.hasString(
            List.of(
                nullLiteral(type(SqlTypeName.VARCHAR)), nullLiteral(type(SqlTypeName.INTEGER)))));
    assertFalse(
        CoercionUtils.hasString(
            List.of(nullLiteral(type(SqlTypeName.INTEGER)), nullLiteral(PPLOperandTypes.IP_UDT))));
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
