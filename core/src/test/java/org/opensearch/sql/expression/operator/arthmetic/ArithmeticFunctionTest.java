/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.operator.arthmetic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.config.TestConfig.INT_TYPE_MISSING_VALUE_FIELD;
import static org.opensearch.sql.config.TestConfig.INT_TYPE_NULL_VALUE_FIELD;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.ref;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.data.type.WideningTypeRule;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ArithmeticFunctionTest extends ExpressionTestBase {

  private static Stream<Arguments> arithmeticFunctionArguments() {
    List<ExprValue> numberOp1 = Arrays.asList(new ExprByteValue(3), new ExprShortValue(3),
        new ExprIntegerValue(3), new ExprLongValue(3L), new ExprFloatValue(3f),
        new ExprDoubleValue(3D));
    List<ExprValue> numberOp2 =
        Arrays.asList(new ExprByteValue(2), new ExprShortValue(2), new ExprIntegerValue(2),
            new ExprLongValue(3L),
            new ExprFloatValue(2f), new ExprDoubleValue(2D));
    return Lists.cartesianProduct(numberOp1, numberOp2).stream()
        .map(list -> Arguments.of(list.get(0), list.get(1)));
  }

  private static Stream<Arguments> arithmeticOperatorArguments() {
    return Stream
        .of(BuiltinFunctionName.ADD, BuiltinFunctionName.SUBTRACT, BuiltinFunctionName.MULTIPLY,
            BuiltinFunctionName.DIVIDE, BuiltinFunctionName.DIVIDE).map(Arguments::of);
  }

  @ParameterizedTest(name = "add({1}, {2})")
  @MethodSource("arithmeticFunctionArguments")
  public void add(ExprValue op1, ExprValue op2) {
    FunctionExpression expression = DSL.add(literal(op1), literal(op2));
    ExprType expectedType = WideningTypeRule.max(op1.type(), op2.type());
    assertEquals(expectedType, expression.type());
    assertValueEqual(BuiltinFunctionName.ADD, expectedType, op1, op2, expression.valueOf());
    assertEquals(String.format("+(%s, %s)", op1.toString(), op2.toString()), expression.toString());
  }

  @ParameterizedTest(name = "{0}(int,null)")
  @MethodSource("arithmeticOperatorArguments")
  public void arithmetic_int_null(BuiltinFunctionName builtinFunctionName) {
    Function<
        List<Expression>, FunctionExpression> function = functionMapping(builtinFunctionName);

    FunctionExpression functionExpression =
        function.apply(Arrays.asList(literal(integerValue(1)),
            ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER)));
    assertEquals(INTEGER, functionExpression.type());
    assertEquals(LITERAL_NULL, functionExpression.valueOf(valueEnv()));

    functionExpression = function.apply(
        Arrays.asList(ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER), literal(integerValue(1))));
    assertEquals(INTEGER, functionExpression.type());
    assertEquals(LITERAL_NULL, functionExpression.valueOf(valueEnv()));
  }

  @ParameterizedTest(name = "{0}(int,missing)")
  @MethodSource("arithmeticOperatorArguments")
  public void arithmetic_int_missing(BuiltinFunctionName builtinFunctionName) {
    Function<
        List<Expression>, FunctionExpression> function = functionMapping(builtinFunctionName);
    FunctionExpression functionExpression =
        function.apply(Arrays.asList(literal(integerValue(1)),
            ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER)));
    assertEquals(INTEGER, functionExpression.type());
    assertEquals(LITERAL_MISSING, functionExpression.valueOf(valueEnv()));

    functionExpression = function.apply(Arrays.asList(ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER),
        literal(integerValue(1))));
    assertEquals(INTEGER, functionExpression.type());
    assertEquals(LITERAL_MISSING, functionExpression.valueOf(valueEnv()));
  }

  @ParameterizedTest(name = "{0}(null,missing)")
  @MethodSource("arithmeticOperatorArguments")
  public void arithmetic_null_missing(BuiltinFunctionName builtinFunctionName) {
    Function<
        List<Expression>, FunctionExpression> function = functionMapping(builtinFunctionName);
    FunctionExpression functionExpression = function.apply(
        Arrays.asList(ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER),
            ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER)));
    assertEquals(INTEGER, functionExpression.type());
    assertEquals(LITERAL_NULL, functionExpression.valueOf(valueEnv()));

    functionExpression = function.apply(
        Arrays.asList(ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER),
            ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER)));
    assertEquals(INTEGER, functionExpression.type());
    assertEquals(LITERAL_MISSING, functionExpression.valueOf(valueEnv()));

    functionExpression = function.apply(
        Arrays.asList(ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER),
            ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER)));
    assertEquals(INTEGER, functionExpression.type());
    assertEquals(LITERAL_MISSING, functionExpression.valueOf(valueEnv()));

    functionExpression = function.apply(
        Arrays.asList(ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER),
            ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER)));
    assertEquals(INTEGER, functionExpression.type());
    assertEquals(LITERAL_MISSING, functionExpression.valueOf(valueEnv()));
  }

  @ParameterizedTest(name = "subtract({1}, {2})")
  @MethodSource("arithmeticFunctionArguments")
  public void subtract(ExprValue op1, ExprValue op2) {
    FunctionExpression expression = DSL.subtract(literal(op1), literal(op2));
    ExprType expectedType = WideningTypeRule.max(op1.type(), op2.type());
    assertEquals(expectedType, expression.type());
    assertValueEqual(BuiltinFunctionName.SUBTRACT, expectedType, op1, op2,
        expression.valueOf());
    assertEquals(String.format("-(%s, %s)", op1.toString(), op2.toString()),
        expression.toString());
  }

  @ParameterizedTest(name = "multiply({1}, {2})")
  @MethodSource("arithmeticFunctionArguments")
  public void multiply(ExprValue op1, ExprValue op2) {
    FunctionExpression expression = DSL.multiply(literal(op1), literal(op2));
    ExprType expectedType = WideningTypeRule.max(op1.type(), op2.type());
    assertEquals(expectedType, expression.type());
    assertValueEqual(BuiltinFunctionName.MULTIPLY, expectedType, op1, op2,
        expression.valueOf());
    assertEquals(String.format("*(%s, %s)", op1.toString(), op2.toString()),
        expression.toString());
  }

  @ParameterizedTest(name = "divide({1}, {2})")
  @MethodSource("arithmeticFunctionArguments")
  public void divide(ExprValue op1, ExprValue op2) {
    FunctionExpression expression = DSL.divide(literal(op1), literal(op2));
    ExprType expectedType = WideningTypeRule.max(op1.type(), op2.type());
    assertEquals(expectedType, expression.type());
    assertValueEqual(BuiltinFunctionName.DIVIDE, expectedType, op1, op2, expression.valueOf());
    assertEquals(String.format("/(%s, %s)", op1.toString(), op2.toString()),
        expression.toString());

    expression = DSL.divide(literal(op1), literal(new ExprShortValue(0)));
    expectedType = WideningTypeRule.max(op1.type(), SHORT);
    assertEquals(expectedType, expression.type());
    assertTrue(expression.valueOf(valueEnv()).isNull());
    assertEquals(String.format("/(%s, 0)", op1.toString()), expression.toString());
  }

  @ParameterizedTest(name = "module({1}, {2})")
  @MethodSource("arithmeticFunctionArguments")
  public void module(ExprValue op1, ExprValue op2) {
    FunctionExpression expression = DSL.module(literal(op1), literal(op2));
    ExprType expectedType = WideningTypeRule.max(op1.type(), op2.type());
    assertEquals(expectedType, expression.type());
    assertValueEqual(BuiltinFunctionName.MODULES, expectedType, op1, op2, expression.valueOf());
    assertEquals(String.format("%%(%s, %s)", op1.toString(), op2.toString()),
        expression.toString());

    expression = DSL.module(literal(op1), literal(new ExprShortValue(0)));
    expectedType = WideningTypeRule.max(op1.type(), SHORT);
    assertEquals(expectedType, expression.type());
    assertTrue(expression.valueOf(valueEnv()).isNull());
    assertEquals(String.format("%%(%s, 0)", op1.toString()), expression.toString());
  }

  protected void assertValueEqual(BuiltinFunctionName builtinFunctionName, ExprType type,
                                  ExprValue op1,
                                  ExprValue op2,
                                  ExprValue actual) {
    switch ((ExprCoreType) type) {
      case BYTE:
        Byte vb1 = op1.byteValue();
        Byte vb2 = op2.byteValue();
        Integer vbActual = actual.integerValue();
        switch (builtinFunctionName) {
          case ADD:
            assertEquals(vb1 + vb2, vbActual);
            return;
          case SUBTRACT:
            assertEquals(vb1 - vb2, vbActual);
            return;
          case DIVIDE:
            assertEquals(vb1 / vb2, vbActual);
            return;
          case MULTIPLY:
            assertEquals(vb1 * vb2, vbActual);
            return;
          case MODULES:
            assertEquals(vb1 % vb2, vbActual);
            return;
          default:
            throw new IllegalStateException("illegal function name: " + builtinFunctionName);
        }
      case SHORT:
        Short vs1 = op1.shortValue();
        Short vs2 = op2.shortValue();
        Integer vsActual = actual.integerValue();
        switch (builtinFunctionName) {
          case ADD:
            assertEquals(vs1 + vs2, vsActual);
            return;
          case SUBTRACT:
            assertEquals(vs1 - vs2, vsActual);
            return;
          case DIVIDE:
            assertEquals(vs1 / vs2, vsActual);
            return;
          case MULTIPLY:
            assertEquals(vs1 * vs2, vsActual);
            return;
          case MODULES:
            assertEquals(vs1 % vs2, vsActual);
            return;
          default:
            throw new IllegalStateException("illegal function name " + builtinFunctionName);
        }
      case INTEGER:
        Integer vi1 = ExprValueUtils.getIntegerValue(op1);
        Integer vi2 = ExprValueUtils.getIntegerValue(op2);
        Integer viActual = ExprValueUtils.getIntegerValue(actual);
        switch (builtinFunctionName) {
          case ADD:
            assertEquals(vi1 + vi2, viActual);
            return;
          case SUBTRACT:
            assertEquals(vi1 - vi2, viActual);
            return;
          case DIVIDE:
            assertEquals(vi1 / vi2, viActual);
            return;
          case MULTIPLY:
            assertEquals(vi1 * vi2, viActual);
            return;
          case MODULES:
            assertEquals(vi1 % vi2, viActual);
            return;
          default:
            throw new IllegalStateException("illegal function name " + builtinFunctionName);
        }
      case LONG:
        Long vl1 = ExprValueUtils.getLongValue(op1);
        Long vl2 = ExprValueUtils.getLongValue(op2);
        Long vlActual = ExprValueUtils.getLongValue(actual);
        switch (builtinFunctionName) {
          case ADD:
            assertEquals(vl1 + vl2, vlActual);
            return;
          case SUBTRACT:
            assertEquals(vl1 - vl2, vlActual);
            return;
          case DIVIDE:
            assertEquals(vl1 / vl2, vlActual);
            return;
          case MULTIPLY:
            assertEquals(vl1 * vl2, vlActual);
            return;
          case MODULES:
            assertEquals(vl1 % vl2, vlActual);
            return;
          default:
            throw new IllegalStateException("illegal function name " + builtinFunctionName);
        }
      case FLOAT:
        Float vf1 = ExprValueUtils.getFloatValue(op1);
        Float vf2 = ExprValueUtils.getFloatValue(op2);
        Float vfActual = ExprValueUtils.getFloatValue(actual);
        switch (builtinFunctionName) {
          case ADD:
            assertEquals(vf1 + vf2, vfActual);
            return;
          case SUBTRACT:
            assertEquals(vf1 - vf2, vfActual);
            return;
          case DIVIDE:
            assertEquals(vf1 / vf2, vfActual);
            return;
          case MULTIPLY:
            assertEquals(vf1 * vf2, vfActual);
            return;
          case MODULES:
            assertEquals(vf1 % vf2, vfActual);
            return;
          default:
            throw new IllegalStateException("illegal function name " + builtinFunctionName);
        }
      case DOUBLE:
        Double vd1 = ExprValueUtils.getDoubleValue(op1);
        Double vd2 = ExprValueUtils.getDoubleValue(op2);
        Double vdActual = ExprValueUtils.getDoubleValue(actual);
        switch (builtinFunctionName) {
          case ADD:
            assertEquals(vd1 + vd2, vdActual);
            return;
          case SUBTRACT:
            assertEquals(vd1 - vd2, vdActual);
            return;
          case DIVIDE:
            assertEquals(vd1 / vd2, vdActual);
            return;
          case MULTIPLY:
            assertEquals(vd1 * vd2, vdActual);
            return;
          case MODULES:
            assertEquals(vd1 % vd2, vdActual);
            return;
          default:
            throw new IllegalStateException("illegal function name " + builtinFunctionName);
        }
      default:
        throw new IllegalStateException("illegal function name " + builtinFunctionName);
    }
  }
}
