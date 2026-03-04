/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.operator.arthmetic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.expression.DSL.literal;

import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
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
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ArithmeticFunctionTest extends ExpressionTestBase {
  private static Stream<Arguments> arithmeticFunctionArguments() {
    List<ExprValue> numberOp1 =
        Arrays.asList(
            new ExprByteValue(3),
            new ExprShortValue(3),
            new ExprIntegerValue(3),
            new ExprLongValue(3L),
            new ExprFloatValue(3f),
            new ExprDoubleValue(3D));
    List<ExprValue> numberOp2 =
        Arrays.asList(
            new ExprByteValue(2),
            new ExprShortValue(2),
            new ExprIntegerValue(2),
            new ExprLongValue(3L),
            new ExprFloatValue(2f),
            new ExprDoubleValue(2D));
    return Lists.cartesianProduct(numberOp1, numberOp2).stream()
        .map(list -> Arguments.of(list.get(0), list.get(1)));
  }

  private static Stream<Arguments> arithmeticOperatorArguments() {
    return Stream.of(
            BuiltinFunctionName.ADD,
            BuiltinFunctionName.SUBTRACT,
            BuiltinFunctionName.MULTIPLY,
            BuiltinFunctionName.DIVIDE,
            BuiltinFunctionName.DIVIDE)
        .map(Arguments::of);
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

  @ParameterizedTest(name = "addFunction({1}, {2})")
  @MethodSource("arithmeticFunctionArguments")
  public void addFunction(ExprValue op1, ExprValue op2) {
    FunctionExpression expression = DSL.addFunction(literal(op1), literal(op2));
    ExprType expectedType = WideningTypeRule.max(op1.type(), op2.type());
    assertEquals(expectedType, expression.type());
    assertValueEqual(BuiltinFunctionName.ADDFUNCTION, expectedType, op1, op2, expression.valueOf());
    assertEquals(
        String.format("add(%s, %s)", op1.toString(), op2.toString()), expression.toString());
  }

  @ParameterizedTest(name = "subtract({1}, {2})")
  @MethodSource("arithmeticFunctionArguments")
  public void subtract(ExprValue op1, ExprValue op2) {
    FunctionExpression expression = DSL.subtract(literal(op1), literal(op2));
    ExprType expectedType = WideningTypeRule.max(op1.type(), op2.type());
    assertEquals(expectedType, expression.type());
    assertValueEqual(BuiltinFunctionName.SUBTRACT, expectedType, op1, op2, expression.valueOf());
    assertEquals(String.format("-(%s, %s)", op1.toString(), op2.toString()), expression.toString());
  }

  @ParameterizedTest(name = "subtractFunction({1}, {2})")
  @MethodSource("arithmeticFunctionArguments")
  public void subtractFunction(ExprValue op1, ExprValue op2) {
    FunctionExpression expression = DSL.subtractFunction(literal(op1), literal(op2));
    ExprType expectedType = WideningTypeRule.max(op1.type(), op2.type());
    assertEquals(expectedType, expression.type());
    assertValueEqual(
        BuiltinFunctionName.SUBTRACTFUNCTION, expectedType, op1, op2, expression.valueOf());
    assertEquals(
        String.format("subtract(%s, %s)", op1.toString(), op2.toString()), expression.toString());
  }

  @ParameterizedTest(name = "mod({1}, {2})")
  @MethodSource("arithmeticFunctionArguments")
  public void mod(ExprValue op1, ExprValue op2) {
    FunctionExpression expression = DSL.mod(literal(op1), literal(op2));
    ExprType expectedType = WideningTypeRule.max(op1.type(), op2.type());
    assertEquals(expectedType, expression.type());
    assertValueEqual(BuiltinFunctionName.MOD, expectedType, op1, op2, expression.valueOf());
    assertEquals(
        String.format("mod(%s, %s)", op1.toString(), op2.toString()), expression.toString());

    expression = DSL.mod(literal(op1), literal(new ExprByteValue(0)));
    assertTrue(expression.valueOf(valueEnv()).isNull());
    assertEquals(String.format("mod(%s, 0)", op1.toString()), expression.toString());
  }

  @ParameterizedTest(name = "modulus({1}, {2})")
  @MethodSource("arithmeticFunctionArguments")
  public void modulus(ExprValue op1, ExprValue op2) {
    FunctionExpression expression = DSL.modulus(literal(op1), literal(op2));
    ExprType expectedType = WideningTypeRule.max(op1.type(), op2.type());
    assertEquals(expectedType, expression.type());
    assertValueEqual(BuiltinFunctionName.MODULUS, expectedType, op1, op2, expression.valueOf());
    assertEquals(
        String.format("%%(%s, %s)", op1.toString(), op2.toString()), expression.toString());

    expression = DSL.modulus(literal(op1), literal(new ExprByteValue(0)));
    assertTrue(expression.valueOf(valueEnv()).isNull());
    assertEquals(String.format("%%(%s, 0)", op1.toString()), expression.toString());
  }

  @ParameterizedTest(name = "modulusFunction({1}, {2})")
  @MethodSource("arithmeticFunctionArguments")
  public void modulusFunction(ExprValue op1, ExprValue op2) {
    FunctionExpression expression = DSL.modulusFunction(literal(op1), literal(op2));
    ExprType expectedType = WideningTypeRule.max(op1.type(), op2.type());
    assertEquals(expectedType, expression.type());
    assertValueEqual(
        BuiltinFunctionName.MODULUSFUNCTION, expectedType, op1, op2, expression.valueOf());
    assertEquals(
        String.format("modulus(%s, %s)", op1.toString(), op2.toString()), expression.toString());

    expression = DSL.modulusFunction(literal(op1), literal(new ExprByteValue(0)));
    assertTrue(expression.valueOf(valueEnv()).isNull());
    assertEquals(String.format("modulus(%s, 0)", op1.toString()), expression.toString());
  }

  @ParameterizedTest(name = "multiply({1}, {2})")
  @MethodSource("arithmeticFunctionArguments")
  public void multiply(ExprValue op1, ExprValue op2) {
    FunctionExpression expression = DSL.multiply(literal(op1), literal(op2));
    ExprType expectedType = WideningTypeRule.max(op1.type(), op2.type());
    assertEquals(expectedType, expression.type());
    assertValueEqual(BuiltinFunctionName.MULTIPLY, expectedType, op1, op2, expression.valueOf());
    assertEquals(String.format("*(%s, %s)", op1.toString(), op2.toString()), expression.toString());
  }

  @ParameterizedTest(name = "multiplyFunction({1}, {2})")
  @MethodSource("arithmeticFunctionArguments")
  public void multiplyFunction(ExprValue op1, ExprValue op2) {
    FunctionExpression expression = DSL.multiplyFunction(literal(op1), literal(op2));
    ExprType expectedType = WideningTypeRule.max(op1.type(), op2.type());
    assertEquals(expectedType, expression.type());
    assertValueEqual(
        BuiltinFunctionName.MULTIPLYFUNCTION, expectedType, op1, op2, expression.valueOf());
    assertEquals(
        String.format("multiply(%s, %s)", op1.toString(), op2.toString()), expression.toString());
  }

  @ParameterizedTest(name = "divide({1}, {2})")
  @MethodSource("arithmeticFunctionArguments")
  public void divide(ExprValue op1, ExprValue op2) {
    FunctionExpression expression = DSL.divide(literal(op1), literal(op2));
    ExprType expectedType = WideningTypeRule.max(op1.type(), op2.type());
    assertEquals(expectedType, expression.type());
    assertValueEqual(BuiltinFunctionName.DIVIDE, expectedType, op1, op2, expression.valueOf());
    assertEquals(String.format("/(%s, %s)", op1.toString(), op2.toString()), expression.toString());

    expression = DSL.divide(literal(op1), literal(new ExprByteValue(0)));
    assertTrue(expression.valueOf(valueEnv()).isNull());
    assertEquals(String.format("/(%s, 0)", op1.toString()), expression.toString());
  }

  @ParameterizedTest(name = "divideFunction({1}, {2})")
  @MethodSource("arithmeticFunctionArguments")
  public void divideFunction(ExprValue op1, ExprValue op2) {
    FunctionExpression expression = DSL.divideFunction(literal(op1), literal(op2));
    ExprType expectedType = WideningTypeRule.max(op1.type(), op2.type());
    assertEquals(expectedType, expression.type());
    assertValueEqual(
        BuiltinFunctionName.DIVIDEFUNCTION, expectedType, op1, op2, expression.valueOf());
    assertEquals(
        String.format("divide(%s, %s)", op1.toString(), op2.toString()), expression.toString());

    expression = DSL.divideFunction(literal(op1), literal(new ExprByteValue(0)));
    assertTrue(expression.valueOf(valueEnv()).isNull());
    assertEquals(String.format("divide(%s, 0)", op1.toString()), expression.toString());
  }

  @ParameterizedTest(name = "multipleParameters({1},{2})")
  @MethodSource("arithmeticFunctionArguments")
  public void multipleParameters(ExprValue op1) {
    assertThrows(
        ExpressionEvaluationException.class,
        () -> DSL.add(literal(op1), literal(op1), literal(op1)));
    assertThrows(
        ExpressionEvaluationException.class,
        () -> DSL.addFunction(literal(op1), literal(op1), literal(op1)));

    assertThrows(
        ExpressionEvaluationException.class,
        () -> DSL.subtract(literal(op1), literal(op1), literal(op1)));
    assertThrows(
        ExpressionEvaluationException.class,
        () -> DSL.subtractFunction(literal(op1), literal(op1), literal(op1)));

    assertThrows(
        ExpressionEvaluationException.class,
        () -> DSL.multiply(literal(op1), literal(op1), literal(op1)));
    assertThrows(
        ExpressionEvaluationException.class,
        () -> DSL.multiplyFunction(literal(op1), literal(op1), literal(op1)));

    assertThrows(
        ExpressionEvaluationException.class,
        () -> DSL.divide(literal(op1), literal(op1), literal(op1)));
    assertThrows(
        ExpressionEvaluationException.class,
        () -> DSL.divideFunction(literal(op1), literal(op1), literal(op1)));

    assertThrows(
        ExpressionEvaluationException.class,
        () -> DSL.mod(literal(op1), literal(op1), literal(op1)));
    assertThrows(
        ExpressionEvaluationException.class,
        () -> DSL.modulus(literal(op1), literal(op1), literal(op1)));
    assertThrows(
        ExpressionEvaluationException.class,
        () -> DSL.modulusFunction(literal(op1), literal(op1), literal(op1)));
  }

  protected void assertValueEqual(
      BuiltinFunctionName builtinFunctionName,
      ExprType type,
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
          case ADDFUNCTION:
            assertEquals(vb1 + vb2, vbActual);
            return;
          case DIVIDE:
          case DIVIDEFUNCTION:
            assertEquals(vb1 / vb2, vbActual);
            return;
          case MOD:
          case MODULUS:
          case MODULUSFUNCTION:
            assertEquals(vb1 % vb2, vbActual);
            return;
          case MULTIPLY:
          case MULTIPLYFUNCTION:
            assertEquals(vb1 * vb2, vbActual);
            return;
          case SUBTRACT:
          case SUBTRACTFUNCTION:
            assertEquals(vb1 - vb2, vbActual);
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
          case ADDFUNCTION:
            assertEquals(vs1 + vs2, vsActual);
            return;
          case DIVIDE:
          case DIVIDEFUNCTION:
            assertEquals(vs1 / vs2, vsActual);
            return;
          case MOD:
          case MODULUS:
          case MODULUSFUNCTION:
            assertEquals(vs1 % vs2, vsActual);
            return;
          case MULTIPLY:
          case MULTIPLYFUNCTION:
            assertEquals(vs1 * vs2, vsActual);
            return;
          case SUBTRACT:
          case SUBTRACTFUNCTION:
            assertEquals(vs1 - vs2, vsActual);
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
          case ADDFUNCTION:
            assertEquals(vi1 + vi2, viActual);
            return;
          case DIVIDE:
          case DIVIDEFUNCTION:
            assertEquals(vi1 / vi2, viActual);
            return;
          case MOD:
          case MODULUS:
          case MODULUSFUNCTION:
            assertEquals(vi1 % vi2, viActual);
            return;
          case MULTIPLY:
          case MULTIPLYFUNCTION:
            assertEquals(vi1 * vi2, viActual);
            return;
          case SUBTRACT:
          case SUBTRACTFUNCTION:
            assertEquals(vi1 - vi2, viActual);
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
          case ADDFUNCTION:
            assertEquals(vl1 + vl2, vlActual);
            return;
          case DIVIDE:
          case DIVIDEFUNCTION:
            assertEquals(vl1 / vl2, vlActual);
            return;
          case MOD:
          case MODULUS:
          case MODULUSFUNCTION:
            assertEquals(vl1 % vl2, vlActual);
            return;
          case MULTIPLY:
          case MULTIPLYFUNCTION:
            assertEquals(vl1 * vl2, vlActual);
            return;
          case SUBTRACT:
          case SUBTRACTFUNCTION:
            assertEquals(vl1 - vl2, vlActual);
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
          case ADDFUNCTION:
            assertEquals(vf1 + vf2, vfActual);
            return;
          case DIVIDE:
          case DIVIDEFUNCTION:
            assertEquals(vf1 / vf2, vfActual);
            return;
          case MODULUS:
          case MODULUSFUNCTION:
          case MOD:
            assertEquals(vf1 % vf2, vfActual);
            return;
          case MULTIPLY:
          case MULTIPLYFUNCTION:
            assertEquals(vf1 * vf2, vfActual);
            return;
          case SUBTRACT:
          case SUBTRACTFUNCTION:
            assertEquals(vf1 - vf2, vfActual);
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
          case ADDFUNCTION:
            assertEquals(vd1 + vd2, vdActual);
            return;
          case DIVIDE:
          case DIVIDEFUNCTION:
            assertEquals(vd1 / vd2, vdActual);
            return;
          case MOD:
          case MODULUS:
          case MODULUSFUNCTION:
            assertEquals(vd1 % vd2, vdActual);
            return;
          case MULTIPLY:
          case MULTIPLYFUNCTION:
            assertEquals(vd1 * vd2, vdActual);
            return;
          case SUBTRACT:
          case SUBTRACTFUNCTION:
            assertEquals(vd1 - vd2, vdActual);
            return;
          default:
            throw new IllegalStateException("illegal function name " + builtinFunctionName);
        }
      default:
        throw new IllegalStateException("illegal function name " + builtinFunctionName);
    }
  }
}
