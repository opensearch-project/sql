/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLambdaRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RexConverterTest {

  private RexBuilder rexBuilder;
  private RelDataType intType;

  @BeforeEach
  void setUp() {
    rexBuilder = new RexBuilder(OpenSearchTypeFactory.TYPE_FACTORY);
    intType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
  }

  @Test
  void testVisitInputRef_returnsUnchanged() {
    RexConverter converter = new RexConverter();
    RexInputRef inputRef = rexBuilder.makeInputRef(intType, 0);

    RexNode result = inputRef.accept(converter);

    assertSame(inputRef, result);
  }

  @Test
  void testVisitLiteral_returnsUnchanged() {
    RexConverter converter = new RexConverter();
    RexLiteral literal = rexBuilder.makeExactLiteral(BigDecimal.valueOf(42));

    RexNode result = literal.accept(converter);

    assertSame(literal, result);
  }

  @Test
  void testVisitCall_withNoTransformation_returnsOriginal() {
    RexConverter converter = new RexConverter();
    RexNode left = rexBuilder.makeInputRef(intType, 0);
    RexNode right = rexBuilder.makeInputRef(intType, 1);
    RexCall call = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.PLUS, left, right);

    RexNode result = call.accept(converter);

    assertSame(call, result);
  }

  @Test
  void testVisitCall_withTransformation_returnsNewNode() {
    RexConverter converter =
        new RexConverter() {
          @Override
          public RexNode visitInputRef(RexInputRef inputRef) {
            return rexBuilder.makeInputRef(inputRef.getType(), inputRef.getIndex() + 10);
          }
        };

    RexNode left = rexBuilder.makeInputRef(intType, 0);
    RexNode right = rexBuilder.makeInputRef(intType, 1);
    RexCall call = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.PLUS, left, right);

    RexNode result = call.accept(converter);

    assertNotSame(call, result);
    RexCall resultCall = (RexCall) result;
    assertEquals(2, resultCall.getOperands().size());
    assertEquals(10, ((RexInputRef) resultCall.getOperands().get(0)).getIndex());
    assertEquals(11, ((RexInputRef) resultCall.getOperands().get(1)).getIndex());
  }

  @Test
  void testVisitCall_nestedCalls_transformsRecursively() {
    RexConverter converter =
        new RexConverter() {
          @Override
          public RexNode visitInputRef(RexInputRef inputRef) {
            return rexBuilder.makeInputRef(inputRef.getType(), inputRef.getIndex() + 1);
          }
        };

    RexNode a = rexBuilder.makeInputRef(intType, 0);
    RexNode b = rexBuilder.makeInputRef(intType, 1);
    RexNode c = rexBuilder.makeInputRef(intType, 2);
    RexCall innerCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.PLUS, a, b);
    RexCall outerCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, innerCall, c);

    RexNode result = outerCall.accept(converter);

    assertNotSame(outerCall, result);
    RexCall resultCall = (RexCall) result;
    RexCall innerResult = (RexCall) resultCall.getOperands().get(0);
    assertEquals(1, ((RexInputRef) innerResult.getOperands().get(0)).getIndex());
    assertEquals(2, ((RexInputRef) innerResult.getOperands().get(1)).getIndex());
    assertEquals(3, ((RexInputRef) resultCall.getOperands().get(1)).getIndex());
  }

  @Test
  void testVisitLambda_withNoTransformation_returnsOriginal() {
    RexConverter converter = new RexConverter();
    RexLambdaRef lambdaRef = new RexLambdaRef(0, "x", intType);
    RexLambda lambda =
        (RexLambda) rexBuilder.makeLambdaCall(lambdaRef, java.util.List.of(lambdaRef));

    RexNode result = lambda.accept(converter);

    assertSame(lambda, result);
  }

  @Test
  void testVisitLambda_withTransformation_throwsException() {
    RexConverter converter =
        new RexConverter() {
          @Override
          public RexNode visitLambdaRef(RexLambdaRef lambdaRef) {
            return new RexLambdaRef(
                lambdaRef.getIndex() + 1, lambdaRef.getName(), lambdaRef.getType());
          }
        };

    RexLambdaRef lambdaRef = new RexLambdaRef(0, "x", intType);
    RexLambda lambda =
        (RexLambda) rexBuilder.makeLambdaCall(lambdaRef, java.util.List.of(lambdaRef));

    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> lambda.accept(converter));
    assertEquals(
        "RexLambda transformation not supported. Override visitLambda() to handle this case.",
        exception.getMessage());
  }

  @Test
  void testCustomConverter_selectiveTransformation() {
    RexConverter converter =
        new RexConverter() {
          @Override
          public RexNode visitInputRef(RexInputRef inputRef) {
            if (inputRef.getIndex() == 0) {
              return rexBuilder.makeInputRef(inputRef.getType(), 99);
            }
            return inputRef;
          }
        };

    RexNode input0 = rexBuilder.makeInputRef(intType, 0);
    RexNode input1 = rexBuilder.makeInputRef(intType, 1);
    RexCall call = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.PLUS, input0, input1);

    RexNode result = call.accept(converter);

    assertNotSame(call, result);
    RexCall resultCall = (RexCall) result;
    assertEquals(99, ((RexInputRef) resultCall.getOperands().get(0)).getIndex());
    assertEquals(1, ((RexInputRef) resultCall.getOperands().get(1)).getIndex());
  }
}
