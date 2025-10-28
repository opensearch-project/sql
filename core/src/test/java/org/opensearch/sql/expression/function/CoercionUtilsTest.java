/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.*;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

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
        Arguments.of(INTEGER, BOOLEAN, null));
  }

  @ParameterizedTest
  @MethodSource("commonWidestTypeArguments")
  public void findCommonWidestType(
      ExprCoreType left, ExprCoreType right, ExprCoreType expectedCommonType) {
    assertEquals(
        expectedCommonType, CoercionUtils.resolveCommonType(left, right).orElseGet(() -> null));
  }

  @Test
  void castArgumentsReturnsExactMatchWhenAvailable() {
    PPLTypeChecker typeChecker = new StubTypeChecker(List.of(List.of(INTEGER), List.of(DOUBLE)));
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
        new StubTypeChecker(List.of(List.of(ExprCoreType.LONG), List.of(DOUBLE)));
    List<RexNode> arguments = List.of(nullLiteral(STRING));

    List<RexNode> result = CoercionUtils.castArguments(REX_BUILDER, typeChecker, arguments);

    assertNotNull(result);
    assertEquals(
        DOUBLE, OpenSearchTypeFactory.convertRelDataTypeToExprType(result.getFirst().getType()));
  }

  @Test
  void castArgumentsReturnsNullWhenNoCompatibleSignatureExists() {
    PPLTypeChecker typeChecker = new StubTypeChecker(List.of(List.of(ExprCoreType.GEO_POINT)));
    List<RexNode> arguments = List.of(nullLiteral(INTEGER));

    assertNull(CoercionUtils.castArguments(REX_BUILDER, typeChecker, arguments));
  }

  private static class StubTypeChecker implements PPLTypeChecker {
    private final List<List<ExprType>> signatures;

    private StubTypeChecker(List<List<ExprType>> signatures) {
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
    public List<List<ExprType>> getParameterTypes() {
      return signatures;
    }
  }
}
