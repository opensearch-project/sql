/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.operator.arthmetic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.model.ExprValueUtils.getDoubleValue;
import static org.opensearch.sql.data.model.ExprValueUtils.getFloatValue;
import static org.opensearch.sql.data.type.ExprCoreType.BYTE;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.utils.MatcherUtils.hasType;
import static org.opensearch.sql.utils.MatcherUtils.hasValue;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class MathematicalFunctionTest extends ExpressionTestBase {

  private static Stream<Arguments> testLogByteArguments() {
    Stream.Builder<Arguments> builder = Stream.builder();
    return builder.add(Arguments.of((byte) 2, (byte) 2)).build();
  }

  private static Stream<Arguments> testLogShortArguments() {
    Stream.Builder<Arguments> builder = Stream.builder();
    return builder.add(Arguments.of((short) 2, (short) 2)).build();
  }

  private static Stream<Arguments> testLogIntegerArguments() {
    Stream.Builder<Arguments> builder = Stream.builder();
    return builder.add(Arguments.of(2, 2)).build();
  }

  private static Stream<Arguments> testLogLongArguments() {
    Stream.Builder<Arguments> builder = Stream.builder();
    return builder.add(Arguments.of(2L, 2L)).build();
  }

  private static Stream<Arguments> testLogFloatArguments() {
    Stream.Builder<Arguments> builder = Stream.builder();
    return builder.add(Arguments.of(2F, 2F)).build();
  }

  private static Stream<Arguments> testLogDoubleArguments() {
    Stream.Builder<Arguments> builder = Stream.builder();
    return builder.add(Arguments.of(2D, 2D)).build();
  }

  private static Stream<Arguments> testLogInvalidDoubleArguments() {
    return Stream.of(Arguments.of(0D, -2D), Arguments.of(0D, 2D), Arguments.of(2D, 0D));
  }

  private static Stream<Arguments> trigonometricArguments() {
    Stream.Builder<Arguments> builder = Stream.builder();
    return builder
        .add(Arguments.of(1))
        .add(Arguments.of(1L))
        .add(Arguments.of(1F))
        .add(Arguments.of(1D))
        .build();
  }

  private static Stream<Arguments> trigonometricDoubleArguments() {
    Stream.Builder<Arguments> builder = Stream.builder();
    return builder
        .add(Arguments.of(1, 2))
        .add(Arguments.of(1L, 2L))
        .add(Arguments.of(1F, 2F))
        .add(Arguments.of(1D, 2D))
        .build();
  }

  /** Test abs with byte value. */
  @ParameterizedTest(name = "abs({0})")
  @ValueSource(bytes = {-2, 2})
  public void abs_byte_value(Byte value) {
    FunctionExpression abs = DSL.abs(DSL.literal(value));
    assertThat(abs.valueOf(valueEnv()), allOf(hasType(BYTE), hasValue(((byte) Math.abs(value)))));
    assertEquals(String.format("abs(%s)", value.toString()), abs.toString());
  }

  /** Test abs with integer value. */
  @ParameterizedTest(name = "abs({0})")
  @ValueSource(ints = {-2, 2})
  public void abs_int_value(Integer value) {
    FunctionExpression abs = DSL.abs(DSL.literal(value));
    assertThat(abs.valueOf(valueEnv()), allOf(hasType(INTEGER), hasValue(Math.abs(value))));
    assertEquals(String.format("abs(%s)", value.toString()), abs.toString());
  }

  /** Test abs with long value. */
  @ParameterizedTest(name = "abs({0})")
  @ValueSource(longs = {-2L, 2L})
  public void abs_long_value(Long value) {
    FunctionExpression abs = DSL.abs(DSL.literal(value));
    assertThat(abs.valueOf(valueEnv()), allOf(hasType(LONG), hasValue(Math.abs(value))));
    assertEquals(String.format("abs(%s)", value.toString()), abs.toString());
  }

  /** Test abs with float value. */
  @ParameterizedTest(name = "abs({0})")
  @ValueSource(floats = {-2f, 2f})
  public void abs_float_value(Float value) {
    FunctionExpression abs = DSL.abs(DSL.literal(value));
    assertThat(abs.valueOf(valueEnv()), allOf(hasType(FLOAT), hasValue(Math.abs(value))));
    assertEquals(String.format("abs(%s)", value.toString()), abs.toString());
  }

  /** Test abs with double value. */
  @ParameterizedTest(name = "abs({0})")
  @ValueSource(doubles = {-2L, 2L})
  public void abs_double_value(Double value) {
    FunctionExpression abs = DSL.abs(DSL.literal(value));
    assertThat(abs.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.abs(value))));
    assertEquals(String.format("abs(%s)", value.toString()), abs.toString());
  }

  /** Test abs with short value. */
  @ParameterizedTest(name = "abs({0})")
  @ValueSource(shorts = {-2, 2})
  public void abs_short_value(Short value) {
    FunctionExpression abs = DSL.abs(DSL.literal(new ExprShortValue(value)));
    assertThat(
        abs.valueOf(valueEnv()),
        allOf(hasType(SHORT), hasValue(Integer.valueOf(Math.abs(value)).shortValue())));
    assertEquals(String.format("abs(%s)", value.toString()), abs.toString());
  }

  /** Test ceil/ceiling with integer value. */
  @ParameterizedTest(name = "ceil({0})")
  @ValueSource(ints = {2, -2})
  public void ceil_int_value(Integer value) {
    FunctionExpression ceil = DSL.ceil(DSL.literal(value));
    assertThat(ceil.valueOf(valueEnv()), allOf(hasType(LONG), hasValue((long) Math.ceil(value))));
    assertEquals(String.format("ceil(%s)", value.toString()), ceil.toString());

    FunctionExpression ceiling = DSL.ceiling(DSL.literal(value));
    assertThat(
        ceiling.valueOf(valueEnv()), allOf(hasType(LONG), hasValue((long) Math.ceil(value))));
    assertEquals(String.format("ceiling(%s)", value.toString()), ceiling.toString());
  }

  /** Test ceil/ceiling with long value. */
  @ParameterizedTest(name = "ceil({0})")
  @ValueSource(longs = {2L, -2L})
  public void ceil_long_value(Long value) {
    FunctionExpression ceil = DSL.ceil(DSL.literal(value));
    assertThat(ceil.valueOf(valueEnv()), allOf(hasType(LONG), hasValue((long) Math.ceil(value))));
    assertEquals(String.format("ceil(%s)", value.toString()), ceil.toString());

    FunctionExpression ceiling = DSL.ceiling(DSL.literal(value));
    assertThat(
        ceiling.valueOf(valueEnv()), allOf(hasType(LONG), hasValue((long) Math.ceil(value))));
    assertEquals(String.format("ceiling(%s)", value.toString()), ceiling.toString());
  }

  /** Test ceil/ceiling with long value. */
  @ParameterizedTest(name = "ceil({0})")
  @ValueSource(longs = {9223372036854775805L, -9223372036854775805L})
  public void ceil_long_value_long(Long value) {
    FunctionExpression ceil = DSL.ceil(DSL.literal(value));
    assertThat(ceil.valueOf(valueEnv()), allOf(hasType(LONG), hasValue((long) Math.ceil(value))));
    assertEquals(String.format("ceil(%s)", value.toString()), ceil.toString());

    FunctionExpression ceiling = DSL.ceiling(DSL.literal(value));
    assertThat(
        ceiling.valueOf(valueEnv()), allOf(hasType(LONG), hasValue((long) Math.ceil(value))));
    assertEquals(String.format("ceiling(%s)", value.toString()), ceiling.toString());
  }

  /** Test ceil/ceiling with float value. */
  @ParameterizedTest(name = "ceil({0})")
  @ValueSource(floats = {2F, -2F})
  public void ceil_float_value(Float value) {
    FunctionExpression ceil = DSL.ceil(DSL.literal(value));
    assertThat(ceil.valueOf(valueEnv()), allOf(hasType(LONG), hasValue((long) Math.ceil(value))));
    assertEquals(String.format("ceil(%s)", value.toString()), ceil.toString());

    FunctionExpression ceiling = DSL.ceiling(DSL.literal(value));
    assertThat(
        ceiling.valueOf(valueEnv()), allOf(hasType(LONG), hasValue((long) Math.ceil(value))));
    assertEquals(String.format("ceiling(%s)", value.toString()), ceiling.toString());
  }

  /** Test ceil/ceiling with double value. */
  @ParameterizedTest(name = "ceil({0})")
  @ValueSource(doubles = {-2L, 2L})
  public void ceil_double_value(Double value) {
    FunctionExpression ceil = DSL.ceil(DSL.literal(value));
    assertThat(ceil.valueOf(valueEnv()), allOf(hasType(LONG), hasValue((long) Math.ceil(value))));
    assertEquals(String.format("ceil(%s)", value.toString()), ceil.toString());

    FunctionExpression ceiling = DSL.ceiling(DSL.literal(value));
    assertThat(
        ceiling.valueOf(valueEnv()), allOf(hasType(LONG), hasValue((long) Math.ceil(value))));
    assertEquals(String.format("ceiling(%s)", value.toString()), ceiling.toString());
  }

  /** Test conv from decimal base with string as a number. */
  @ParameterizedTest(name = "conv({0})")
  @ValueSource(strings = {"1", "0", "-1"})
  public void conv_from_decimal(String value) {
    FunctionExpression conv = DSL.conv(DSL.literal(value), DSL.literal(10), DSL.literal(2));
    assertThat(
        conv.valueOf(valueEnv()),
        allOf(hasType(STRING), hasValue(Integer.toString(Integer.parseInt(value), 2))));
    assertEquals(String.format("conv(\"%s\", 10, 2)", value), conv.toString());

    conv = DSL.conv(DSL.literal(value), DSL.literal(10), DSL.literal(8));
    assertThat(
        conv.valueOf(valueEnv()),
        allOf(hasType(STRING), hasValue(Integer.toString(Integer.parseInt(value), 8))));
    assertEquals(String.format("conv(\"%s\", 10, 8)", value), conv.toString());

    conv = DSL.conv(DSL.literal(value), DSL.literal(10), DSL.literal(16));
    assertThat(
        conv.valueOf(valueEnv()),
        allOf(hasType(STRING), hasValue(Integer.toString(Integer.parseInt(value), 16))));
    assertEquals(String.format("conv(\"%s\", 10, 16)", value), conv.toString());
  }

  /** Test conv from decimal base with integer as a number. */
  @ParameterizedTest(name = "conv({0})")
  @ValueSource(ints = {1, 0, -1})
  public void conv_from_decimal(Integer value) {
    FunctionExpression conv = DSL.conv(DSL.literal(value), DSL.literal(10), DSL.literal(2));
    assertThat(
        conv.valueOf(valueEnv()), allOf(hasType(STRING), hasValue(Integer.toString(value, 2))));
    assertEquals(String.format("conv(%s, 10, 2)", value), conv.toString());

    conv = DSL.conv(DSL.literal(value), DSL.literal(10), DSL.literal(8));
    assertThat(
        conv.valueOf(valueEnv()), allOf(hasType(STRING), hasValue(Integer.toString(value, 8))));
    assertEquals(String.format("conv(%s, 10, 8)", value), conv.toString());

    conv = DSL.conv(DSL.literal(value), DSL.literal(10), DSL.literal(16));
    assertThat(
        conv.valueOf(valueEnv()), allOf(hasType(STRING), hasValue(Integer.toString(value, 16))));
    assertEquals(String.format("conv(%s, 10, 16)", value), conv.toString());
  }

  /** Test conv to decimal base with string as a number. */
  @ParameterizedTest(name = "conv({0})")
  @ValueSource(strings = {"11", "0", "11111"})
  public void conv_to_decimal(String value) {
    FunctionExpression conv = DSL.conv(DSL.literal(value), DSL.literal(2), DSL.literal(10));
    assertThat(
        conv.valueOf(valueEnv()),
        allOf(hasType(STRING), hasValue(Integer.toString(Integer.parseInt(value, 2)))));
    assertEquals(String.format("conv(\"%s\", 2, 10)", value), conv.toString());

    conv = DSL.conv(DSL.literal(value), DSL.literal(8), DSL.literal(10));
    assertThat(
        conv.valueOf(valueEnv()),
        allOf(hasType(STRING), hasValue(Integer.toString(Integer.parseInt(value, 8)))));
    assertEquals(String.format("conv(\"%s\", 8, 10)", value), conv.toString());

    conv = DSL.conv(DSL.literal(value), DSL.literal(16), DSL.literal(10));
    assertThat(
        conv.valueOf(valueEnv()),
        allOf(hasType(STRING), hasValue(Integer.toString(Integer.parseInt(value, 16)))));
    assertEquals(String.format("conv(\"%s\", 16, 10)", value), conv.toString());
  }

  /** Test conv to decimal base with integer as a number. */
  @ParameterizedTest(name = "conv({0})")
  @ValueSource(ints = {11, 0, 11111})
  public void conv_to_decimal(Integer value) {
    FunctionExpression conv = DSL.conv(DSL.literal(value), DSL.literal(2), DSL.literal(10));
    assertThat(
        conv.valueOf(valueEnv()),
        allOf(hasType(STRING), hasValue(Integer.toString(Integer.parseInt(value.toString(), 2)))));
    assertEquals(String.format("conv(%s, 2, 10)", value), conv.toString());

    conv = DSL.conv(DSL.literal(value), DSL.literal(8), DSL.literal(10));
    assertThat(
        conv.valueOf(valueEnv()),
        allOf(hasType(STRING), hasValue(Integer.toString(Integer.parseInt(value.toString(), 8)))));
    assertEquals(String.format("conv(%s, 8, 10)", value), conv.toString());

    conv = DSL.conv(DSL.literal(value), DSL.literal(16), DSL.literal(10));
    assertThat(
        conv.valueOf(valueEnv()),
        allOf(hasType(STRING), hasValue(Integer.toString(Integer.parseInt(value.toString(), 16)))));
    assertEquals(String.format("conv(%s, 16, 10)", value), conv.toString());
  }

  /** Test crc32 with string value. */
  @ParameterizedTest(name = "crc({0})")
  @ValueSource(strings = {"odfe", "sql"})
  public void crc32_string_value(String value) {
    FunctionExpression crc = DSL.crc32(DSL.literal(value));
    CRC32 crc32 = new CRC32();
    crc32.update(value.getBytes());
    assertThat(crc.valueOf(valueEnv()), allOf(hasType(LONG), hasValue(crc32.getValue())));
    assertEquals(String.format("crc32(\"%s\")", value), crc.toString());
  }

  /** Test constant e. */
  @Test
  public void test_e() {
    FunctionExpression e = DSL.euler();
    assertThat(e.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.E)));
  }

  /** Test exp with integer value. */
  @ParameterizedTest(name = "exp({0})")
  @ValueSource(ints = {-2, 2})
  public void exp_int_value(Integer value) {
    FunctionExpression exp = DSL.exp(DSL.literal(value));
    assertThat(exp.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.exp(value))));
    assertEquals(String.format("exp(%s)", value.toString()), exp.toString());
  }

  /** Test exp with long value. */
  @ParameterizedTest(name = "exp({0})")
  @ValueSource(longs = {-2L, 2L})
  public void exp_long_value(Long value) {
    FunctionExpression exp = DSL.exp(DSL.literal(value));
    assertThat(exp.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.exp(value))));
    assertEquals(String.format("exp(%s)", value.toString()), exp.toString());
  }

  /** Test exp with float value. */
  @ParameterizedTest(name = "exp({0})")
  @ValueSource(floats = {-2F, 2F})
  public void exp_float_value(Float value) {
    FunctionExpression exp = DSL.exp(DSL.literal(value));
    assertThat(exp.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.exp(value))));
    assertEquals(String.format("exp(%s)", value.toString()), exp.toString());
  }

  /** Test exp with double value. */
  @ParameterizedTest(name = "exp({0})")
  @ValueSource(doubles = {-2D, 2D})
  public void exp_double_value(Double value) {
    FunctionExpression exp = DSL.exp(DSL.literal(value));
    assertThat(exp.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.exp(value))));
    assertEquals(String.format("exp(%s)", value.toString()), exp.toString());
  }

  /** Test expm1 with integer value. */
  @ParameterizedTest(name = "expm1({0})")
  @ValueSource(ints = {-1, 0, 1, Integer.MAX_VALUE, Integer.MIN_VALUE})
  public void expm1_int_value(Integer value) {
    FunctionExpression expm1 = DSL.expm1(DSL.literal(value));
    assertThat(expm1.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.expm1(value))));
    assertEquals(String.format("expm1(%s)", value), expm1.toString());
  }

  /** Test expm1 with long value. */
  @ParameterizedTest(name = "expm1({0})")
  @ValueSource(longs = {-1L, 0L, 1L, Long.MAX_VALUE, Long.MIN_VALUE})
  public void expm1_long_value(Long value) {
    FunctionExpression expm1 = DSL.expm1(DSL.literal(value));
    assertThat(expm1.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.expm1(value))));
    assertEquals(String.format("expm1(%s)", value), expm1.toString());
  }

  /** Test expm1 with float value. */
  @ParameterizedTest(name = "expm1({0})")
  @ValueSource(floats = {-1.5F, -1F, 0F, 1F, 1.5F, Float.MAX_VALUE, Float.MIN_VALUE})
  public void expm1_float_value(Float value) {
    FunctionExpression expm1 = DSL.expm1(DSL.literal(value));
    assertThat(expm1.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.expm1(value))));
    assertEquals(String.format("expm1(%s)", value), expm1.toString());
  }

  /** Test expm1 with double value. */
  @ParameterizedTest(name = "expm1({0})")
  @ValueSource(doubles = {-1.5D, -1D, 0D, 1D, 1.5D, Double.MAX_VALUE, Double.MIN_VALUE})
  public void expm1_double_value(Double value) {
    FunctionExpression expm1 = DSL.expm1(DSL.literal(value));
    assertThat(expm1.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.expm1(value))));
    assertEquals(String.format("expm1(%s)", value), expm1.toString());
  }

  /** Test expm1 with short value. */
  @ParameterizedTest(name = "expm1({0})")
  @ValueSource(shorts = {-1, 0, 1, Short.MAX_VALUE, Short.MIN_VALUE})
  public void expm1_short_value(Short value) {
    FunctionExpression expm1 = DSL.expm1(DSL.literal(value));
    assertThat(expm1.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.expm1(value))));
    assertEquals(String.format("expm1(%s)", value), expm1.toString());
  }

  /** Test expm1 with byte value. */
  @ParameterizedTest(name = "expm1({0})")
  @ValueSource(bytes = {-1, 0, 1, Byte.MAX_VALUE, Byte.MIN_VALUE})
  public void expm1_byte_value(Byte value) {
    FunctionExpression expm1 = DSL.expm1(DSL.literal(value));
    assertThat(expm1.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.expm1(value))));
    assertEquals(String.format("expm1(%s)", value), expm1.toString());
  }

  /** Test floor with integer value. */
  @ParameterizedTest(name = "floor({0})")
  @ValueSource(ints = {-2, 2})
  public void floor_int_value(Integer value) {
    FunctionExpression floor = DSL.floor(DSL.literal(value));
    assertThat(floor.valueOf(valueEnv()), allOf(hasType(LONG), hasValue((long) Math.floor(value))));
    assertEquals(String.format("floor(%s)", value), floor.toString());
  }

  /** Test floor with long value. */
  @ParameterizedTest(name = "floor({0})")
  @ValueSource(longs = {-2L, 2L})
  public void floor_long_value(Long value) {
    FunctionExpression floor = DSL.floor(DSL.literal(value));
    assertThat(floor.valueOf(valueEnv()), allOf(hasType(LONG), hasValue((long) Math.floor(value))));
    assertEquals(String.format("floor(%s)", value.toString()), floor.toString());
  }

  /** Test floor with float value. */
  @ParameterizedTest(name = "floor({0})")
  @ValueSource(floats = {-2F, 2F})
  public void floor_float_value(Float value) {
    FunctionExpression floor = DSL.floor(DSL.literal(value));
    assertThat(floor.valueOf(valueEnv()), allOf(hasType(LONG), hasValue((long) Math.floor(value))));
    assertEquals(String.format("floor(%s)", value.toString()), floor.toString());
  }

  /** Test floor with double value. */
  @ParameterizedTest(name = "floor({0})")
  @ValueSource(doubles = {-2D, 2D})
  public void floor_double_value(Double value) {
    FunctionExpression floor = DSL.floor(DSL.literal(value));
    assertThat(floor.valueOf(valueEnv()), allOf(hasType(LONG), hasValue((long) Math.floor(value))));
    assertEquals(String.format("floor(%s)", value.toString()), floor.toString());
  }

  /** Test ln with integer value. */
  @ParameterizedTest(name = "ln({0})")
  @ValueSource(ints = {2, 3})
  public void ln_int_value(Integer value) {
    FunctionExpression ln = DSL.ln(DSL.literal(value));
    assertThat(ln.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.log(value))));
    assertEquals(String.format("ln(%s)", value.toString()), ln.toString());
  }

  /** Test ln with long value. */
  @ParameterizedTest(name = "ln({0})")
  @ValueSource(longs = {2L, 3L})
  public void ln_long_value(Long value) {
    FunctionExpression ln = DSL.ln(DSL.literal(value));
    assertThat(ln.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.log(value))));
    assertEquals(String.format("ln(%s)", value.toString()), ln.toString());
  }

  /** Test ln with float value. */
  @ParameterizedTest(name = "ln({0})")
  @ValueSource(floats = {2F, 3F})
  public void ln_float_value(Float value) {
    FunctionExpression ln = DSL.ln(DSL.literal(value));
    assertThat(ln.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.log(value))));
    assertEquals(String.format("ln(%s)", value.toString()), ln.toString());
  }

  /** Test ln with double value. */
  @ParameterizedTest(name = "ln({0})")
  @ValueSource(doubles = {2D, 3D})
  public void ln_double_value(Double value) {
    FunctionExpression ln = DSL.ln(DSL.literal(value));
    assertThat(ln.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.log(value))));
    assertEquals(String.format("ln(%s)", value.toString()), ln.toString());
  }

  /** Test ln with invalid value. */
  @ParameterizedTest(name = "ln({0})")
  @ValueSource(doubles = {0D, -3D})
  public void ln_invalid_value(Double value) {
    FunctionExpression ln = DSL.ln(DSL.literal(value));
    assertEquals(DOUBLE, ln.type());
    assertTrue(ln.valueOf(valueEnv()).isNull());
  }

  /** Test log with 1 int argument. */
  @ParameterizedTest(name = "log({0})")
  @ValueSource(ints = {2, 3})
  public void log_int_value(Integer v) {
    FunctionExpression log = DSL.log(DSL.literal(v));
    assertEquals(log.type(), DOUBLE);
    assertThat(getDoubleValue(log.valueOf(valueEnv())), closeTo(Math.log(v), 0.0001));
    assertEquals(String.format("log(%s)", v.toString()), log.toString());
  }

  /** Test log with 1 long argument. */
  @ParameterizedTest(name = "log({0})")
  @ValueSource(longs = {2L, 3L})
  public void log_int_value(Long v) {
    FunctionExpression log = DSL.log(DSL.literal(v));
    assertEquals(log.type(), DOUBLE);
    assertThat(getDoubleValue(log.valueOf(valueEnv())), closeTo(Math.log(v), 0.0001));
    assertEquals(String.format("log(%s)", v.toString()), log.toString());
  }

  /** Test log with 1 float argument. */
  @ParameterizedTest(name = "log({0})")
  @ValueSource(floats = {2F, 3F})
  public void log_float_value(Float v) {
    FunctionExpression log = DSL.log(DSL.literal(v));
    assertEquals(log.type(), DOUBLE);
    assertThat(getDoubleValue(log.valueOf(valueEnv())), closeTo(Math.log(v), 0.0001));
    assertEquals(String.format("log(%s)", v.toString()), log.toString());
  }

  /** Test log with 1 double argument. */
  @ParameterizedTest(name = "log({0})")
  @ValueSource(doubles = {2D, 3D})
  public void log_double_value(Double v) {
    FunctionExpression log = DSL.log(DSL.literal(v));
    assertEquals(log.type(), DOUBLE);
    assertThat(getDoubleValue(log.valueOf(valueEnv())), closeTo(Math.log(v), 0.0001));
    assertEquals(String.format("log(%s)", v.toString()), log.toString());
  }

  /** Test log with 1 invalid value. */
  @ParameterizedTest(name = "log({0})")
  @ValueSource(doubles = {0D, -3D})
  public void log_invalid_value(Double value) {
    FunctionExpression log = DSL.log(DSL.literal(value));
    assertEquals(DOUBLE, log.type());
    assertTrue(log.valueOf(valueEnv()).isNull());
  }

  /** Test log with 2 int arguments. */
  @ParameterizedTest(name = "log({0}, {1})")
  @MethodSource("testLogIntegerArguments")
  public void log_two_int_value(Integer v1, Integer v2) {
    FunctionExpression log = DSL.log(DSL.literal(v1), DSL.literal(v2));
    assertEquals(log.type(), DOUBLE);
    assertThat(
        getDoubleValue(log.valueOf(valueEnv())), closeTo(Math.log(v2) / Math.log(v1), 0.0001));
    assertEquals(String.format("log(%s, %s)", v1.toString(), v2.toString()), log.toString());
  }

  /** Test log with 2 long arguments. */
  @ParameterizedTest(name = "log({0}, {1})")
  @MethodSource("testLogLongArguments")
  public void log_two_long_value(Long v1, Long v2) {
    FunctionExpression log = DSL.log(DSL.literal(v1), DSL.literal(v2));
    assertEquals(log.type(), DOUBLE);
    assertThat(
        getDoubleValue(log.valueOf(valueEnv())), closeTo(Math.log(v2) / Math.log(v1), 0.0001));
    assertEquals(String.format("log(%s, %s)", v1.toString(), v2.toString()), log.toString());
  }

  /** Test log with 2 float arguments. */
  @ParameterizedTest(name = "log({0}, {1})")
  @MethodSource("testLogFloatArguments")
  public void log_two_double_value(Float v1, Float v2) {
    FunctionExpression log = DSL.log(DSL.literal(v1), DSL.literal(v2));
    assertEquals(log.type(), DOUBLE);
    assertThat(
        getDoubleValue(log.valueOf(valueEnv())), closeTo(Math.log(v2) / Math.log(v1), 0.0001));
    assertEquals(String.format("log(%s, %s)", v1.toString(), v2.toString()), log.toString());
  }

  /** Test log with 2 double arguments. */
  @ParameterizedTest(name = "log({0}, {1})")
  @MethodSource("testLogDoubleArguments")
  public void log_two_double_value(Double v1, Double v2) {
    FunctionExpression log = DSL.log(DSL.literal(v1), DSL.literal(v2));
    assertEquals(log.type(), DOUBLE);
    assertThat(
        getDoubleValue(log.valueOf(valueEnv())), closeTo(Math.log(v2) / Math.log(v1), 0.0001));
    assertEquals(String.format("log(%s, %s)", v1.toString(), v2.toString()), log.toString());
  }

  /** Test log with 2 invalid double arguments. */
  @ParameterizedTest(name = "log({0}, {2})")
  @MethodSource("testLogInvalidDoubleArguments")
  public void log_two_invalid_double_value(Double v1, Double v2) {
    FunctionExpression log = DSL.log(DSL.literal(v1), DSL.literal(v2));
    assertEquals(log.type(), DOUBLE);
    assertTrue(log.valueOf(valueEnv()).isNull());
  }

  /** Test log10 with int value. */
  @ParameterizedTest(name = "log10({0})")
  @ValueSource(ints = {2, 3})
  public void log10_int_value(Integer v) {
    FunctionExpression log = DSL.log10(DSL.literal(v));
    assertEquals(log.type(), DOUBLE);
    assertThat(getDoubleValue(log.valueOf(valueEnv())), closeTo(Math.log10(v), 0.0001));
    assertEquals(String.format("log10(%s)", v.toString()), log.toString());
  }

  /** Test log10 with long value. */
  @ParameterizedTest(name = "log10({0})")
  @ValueSource(longs = {2L, 3L})
  public void log10_long_value(Long v) {
    FunctionExpression log = DSL.log10(DSL.literal(v));
    assertEquals(log.type(), DOUBLE);
    assertThat(getDoubleValue(log.valueOf(valueEnv())), closeTo(Math.log10(v), 0.0001));
    assertEquals(String.format("log10(%s)", v.toString()), log.toString());
  }

  /** Test log10 with float value. */
  @ParameterizedTest(name = "log10({0})")
  @ValueSource(floats = {2F, 3F})
  public void log10_float_value(Float v) {
    FunctionExpression log = DSL.log10(DSL.literal(v));
    assertEquals(log.type(), DOUBLE);
    assertThat(getDoubleValue(log.valueOf(valueEnv())), closeTo(Math.log10(v), 0.0001));
    assertEquals(String.format("log10(%s)", v.toString()), log.toString());
  }

  /** Test log10 with int value. */
  @ParameterizedTest(name = "log10({0})")
  @ValueSource(doubles = {2D, 3D})
  public void log10_double_value(Double v) {
    FunctionExpression log = DSL.log10(DSL.literal(v));
    assertEquals(log.type(), DOUBLE);
    assertThat(getDoubleValue(log.valueOf(valueEnv())), closeTo(Math.log10(v), 0.0001));
    assertEquals(String.format("log10(%s)", v.toString()), log.toString());
  }

  /** Test log10 with 1 invalid double argument. */
  @ParameterizedTest(name = "log10({0})")
  @ValueSource(doubles = {0D, -3D})
  public void log10_two_invalid_value(Double v) {
    FunctionExpression log = DSL.log10(DSL.literal(v));
    assertEquals(log.type(), DOUBLE);
    assertTrue(log.valueOf(valueEnv()).isNull());
  }

  /** Test log2 with int value. */
  @ParameterizedTest(name = "log10({0})")
  @ValueSource(ints = {2, 3})
  public void log2_int_value(Integer v) {
    FunctionExpression log = DSL.log2(DSL.literal(v));
    assertEquals(log.type(), DOUBLE);
    assertThat(getDoubleValue(log.valueOf(valueEnv())), closeTo(Math.log(v) / Math.log(2), 0.0001));
    assertEquals(String.format("log2(%s)", v.toString()), log.toString());
  }

  /** Test log2 with long value. */
  @ParameterizedTest(name = "log10({0})")
  @ValueSource(longs = {2L, 3L})
  public void log2_long_value(Long v) {
    FunctionExpression log = DSL.log2(DSL.literal(v));
    assertEquals(log.type(), DOUBLE);
    assertThat(getDoubleValue(log.valueOf(valueEnv())), closeTo(Math.log(v) / Math.log(2), 0.0001));
    assertEquals(String.format("log2(%s)", v.toString()), log.toString());
  }

  /** Test log2 with float value. */
  @ParameterizedTest(name = "log10({0})")
  @ValueSource(floats = {2F, 3F})
  public void log2_float_value(Float v) {
    FunctionExpression log = DSL.log2(DSL.literal(v));
    assertEquals(log.type(), DOUBLE);
    assertThat(getDoubleValue(log.valueOf(valueEnv())), closeTo(Math.log(v) / Math.log(2), 0.0001));
    assertEquals(String.format("log2(%s)", v.toString()), log.toString());
  }

  /** Test log2 with double value. */
  @ParameterizedTest(name = "log10({0})")
  @ValueSource(doubles = {2D, 3D})
  public void log2_double_value(Double v) {
    FunctionExpression log = DSL.log2(DSL.literal(v));
    assertEquals(log.type(), DOUBLE);
    assertThat(getDoubleValue(log.valueOf(valueEnv())), closeTo(Math.log(v) / Math.log(2), 0.0001));
    assertEquals(String.format("log2(%s)", v.toString()), log.toString());
  }

  /** Test log2 with an invalid double value. */
  @ParameterizedTest(name = "log2({0})")
  @ValueSource(doubles = {0D, -2D})
  public void log2_invalid_double_value(Double v) {
    FunctionExpression log = DSL.log2(DSL.literal(v));
    assertEquals(log.type(), DOUBLE);
    assertTrue(log.valueOf(valueEnv()).isNull());
  }

  /** Test mod with byte value. */
  @ParameterizedTest(name = "mod({0}, {1})")
  @MethodSource("testLogByteArguments")
  public void mod_byte_value(Byte v1, Byte v2) {
    FunctionExpression mod = DSL.mod(DSL.literal(v1), DSL.literal(v2));

    assertThat(
        mod.valueOf(valueEnv()),
        allOf(hasType(BYTE), hasValue(Integer.valueOf(v1 % v2).byteValue())));
    assertEquals(String.format("mod(%s, %s)", v1, v2), mod.toString());

    mod = DSL.mod(DSL.literal(v1), DSL.literal(new ExprByteValue(0)));
    assertEquals(BYTE, mod.type());
    assertTrue(mod.valueOf(valueEnv()).isNull());
  }

  /** Test mod with short value. */
  @ParameterizedTest(name = "mod({0}, {1})")
  @MethodSource("testLogShortArguments")
  public void mod_short_value(Short v1, Short v2) {
    FunctionExpression mod = DSL.mod(DSL.literal(v1), DSL.literal(v2));

    assertThat(
        mod.valueOf(valueEnv()),
        allOf(hasType(SHORT), hasValue(Integer.valueOf(v1 % v2).shortValue())));
    assertEquals(String.format("mod(%s, %s)", v1, v2), mod.toString());

    mod = DSL.mod(DSL.literal(v1), DSL.literal(new ExprShortValue(0)));
    assertEquals(SHORT, mod.type());
    assertTrue(mod.valueOf(valueEnv()).isNull());
  }

  /** Test mod with integer value. */
  @ParameterizedTest(name = "mod({0}, {1})")
  @MethodSource("testLogIntegerArguments")
  public void mod_int_value(Integer v1, Integer v2) {
    FunctionExpression mod = DSL.mod(DSL.literal(v1), DSL.literal(v2));
    assertThat(mod.valueOf(valueEnv()), allOf(hasType(INTEGER), hasValue(v1 % v2)));
    assertEquals(String.format("mod(%s, %s)", v1, v2), mod.toString());

    mod = DSL.mod(DSL.literal(v1), DSL.literal(0));
    assertEquals(INTEGER, mod.type());
    assertTrue(mod.valueOf(valueEnv()).isNull());
  }

  /** Test mod with long value. */
  @ParameterizedTest(name = "mod({0}, {1})")
  @MethodSource("testLogLongArguments")
  public void mod_long_value(Long v1, Long v2) {
    FunctionExpression mod = DSL.mod(DSL.literal(v1), DSL.literal(v2));
    assertThat(mod.valueOf(valueEnv()), allOf(hasType(LONG), hasValue(v1 % v2)));
    assertEquals(String.format("mod(%s, %s)", v1, v2), mod.toString());

    mod = DSL.mod(DSL.literal(v1), DSL.literal(0));
    assertEquals(LONG, mod.type());
    assertTrue(mod.valueOf(valueEnv()).isNull());
  }

  /** Test mod with long value. */
  @ParameterizedTest(name = "mod({0}, {1})")
  @MethodSource("testLogFloatArguments")
  public void mod_float_value(Float v1, Float v2) {
    FunctionExpression mod = DSL.mod(DSL.literal(v1), DSL.literal(v2));
    assertThat(mod.valueOf(valueEnv()), allOf(hasType(FLOAT), hasValue(v1 % v2)));
    assertEquals(String.format("mod(%s, %s)", v1, v2), mod.toString());

    mod = DSL.mod(DSL.literal(v1), DSL.literal(0));
    assertEquals(FLOAT, mod.type());
    assertTrue(mod.valueOf(valueEnv()).isNull());
  }

  /** Test mod with double value. */
  @ParameterizedTest(name = "mod({0}, {1})")
  @MethodSource("testLogDoubleArguments")
  public void mod_double_value(Double v1, Double v2) {
    FunctionExpression mod = DSL.mod(DSL.literal(v1), DSL.literal(v2));
    assertThat(mod.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(v1 % v2)));
    assertEquals(String.format("mod(%s, %s)", v1, v2), mod.toString());

    mod = DSL.mod(DSL.literal(v1), DSL.literal(0));
    assertEquals(DOUBLE, mod.type());
    assertTrue(mod.valueOf(valueEnv()).isNull());
  }

  /** Test pow/power with short value. */
  @ParameterizedTest(name = "pow({0}, {1}")
  @MethodSource("testLogShortArguments")
  public void pow_short_value(Short v1, Short v2) {
    FunctionExpression pow = DSL.pow(DSL.literal(v1), DSL.literal(v2));
    assertThat(pow.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.pow(v1, v2))));
    assertEquals(String.format("pow(%s, %s)", v1, v2), pow.toString());

    FunctionExpression power = DSL.power(DSL.literal(v1), DSL.literal(v2));
    assertThat(power.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.pow(v1, v2))));
    assertEquals(String.format("pow(%s, %s)", v1, v2), pow.toString());
  }

  /** Test pow/power with integer value. */
  @ParameterizedTest(name = "pow({0}, {1}")
  @MethodSource("testLogIntegerArguments")
  public void pow_int_value(Integer v1, Integer v2) {
    FunctionExpression pow = DSL.pow(DSL.literal(v1), DSL.literal(v2));
    assertThat(pow.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.pow(v1, v2))));
    assertEquals(String.format("pow(%s, %s)", v1, v2), pow.toString());

    FunctionExpression power = DSL.power(DSL.literal(v1), DSL.literal(v2));
    assertThat(power.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.pow(v1, v2))));
    assertEquals(String.format("pow(%s, %s)", v1, v2), pow.toString());
  }

  /** Test pow/power with long value. */
  @ParameterizedTest(name = "pow({0}, {1}")
  @MethodSource("testLogLongArguments")
  public void pow_long_value(Long v1, Long v2) {
    FunctionExpression pow = DSL.pow(DSL.literal(v1), DSL.literal(v2));
    assertThat(pow.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.pow(v1, v2))));
    assertEquals(String.format("pow(%s, %s)", v1, v2), pow.toString());

    FunctionExpression power = DSL.power(DSL.literal(v1), DSL.literal(v2));
    assertThat(power.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.pow(v1, v2))));
    assertEquals(String.format("pow(%s, %s)", v1, v2), pow.toString());
  }

  /** Test pow/power with float value. */
  @ParameterizedTest(name = "pow({0}, {1}")
  @MethodSource("testLogFloatArguments")
  public void pow_float_value(Float v1, Float v2) {
    FunctionExpression pow = DSL.pow(DSL.literal(v1), DSL.literal(v2));
    assertThat(pow.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.pow(v1, v2))));
    assertEquals(String.format("pow(%s, %s)", v1, v2), pow.toString());

    FunctionExpression power = DSL.power(DSL.literal(v1), DSL.literal(v2));
    assertThat(power.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.pow(v1, v2))));
    assertEquals(String.format("pow(%s, %s)", v1, v2), pow.toString());
  }

  /** Test pow/power with double value. */
  @ParameterizedTest(name = "pow({0}, {1}")
  @MethodSource("testLogDoubleArguments")
  public void pow_double_value(Double v1, Double v2) {
    FunctionExpression pow = DSL.pow(DSL.literal(v1), DSL.literal(v2));
    assertThat(pow.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.pow(v1, v2))));
    assertEquals(String.format("pow(%s, %s)", v1, v2), pow.toString());

    FunctionExpression power = DSL.power(DSL.literal(v1), DSL.literal(v2));
    assertThat(power.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.pow(v1, v2))));
    assertEquals(String.format("pow(%s, %s)", v1, v2), pow.toString());
  }

  /** Test pow/power with null output. */
  @Test
  public void pow_null_output() {
    FunctionExpression pow = DSL.pow(DSL.literal((double) -2), DSL.literal(1.5));
    assertEquals(pow.type(), DOUBLE);
    assertEquals(String.format("pow(%s, %s)", (double) -2, 1.5), pow.toString());
    assertTrue(pow.valueOf(valueEnv()).isNull());

    pow = DSL.pow(DSL.literal((float) -2), DSL.literal((float) 1.5));
    assertEquals(pow.type(), DOUBLE);
    assertEquals(String.format("pow(%s, %s)", (float) -2, (float) 1.5), pow.toString());
    assertTrue(pow.valueOf(valueEnv()).isNull());
  }

  /** Test pow/power with edge cases. */
  @Test
  public void pow_edge_cases() {
    FunctionExpression pow = DSL.pow(DSL.literal((double) -2), DSL.literal((double) 2));
    assertEquals(pow.type(), DOUBLE);
    assertEquals(String.format("pow(%s, %s)", (double) -2, (double) 2), pow.toString());
    assertThat(pow.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.pow(-2, 2))));

    pow = DSL.pow(DSL.literal((double) 2), DSL.literal((double) 1.5));
    assertEquals(pow.type(), DOUBLE);
    assertEquals(String.format("pow(%s, %s)", (double) 2, (double) 1.5), pow.toString());
    assertThat(pow.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.pow(2, 1.5))));

    pow = DSL.pow(DSL.literal((float) -2), DSL.literal((float) 2));
    assertEquals(pow.type(), DOUBLE);
    assertEquals(String.format("pow(%s, %s)", (float) -2, (float) 2), pow.toString());
    assertThat(
        pow.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.pow((float) -2, (float) 2))));

    pow = DSL.pow(DSL.literal((float) 2), DSL.literal((float) 1.5));
    assertEquals(pow.type(), DOUBLE);
    assertEquals(String.format("pow(%s, %s)", (float) 2, (float) 1.5), pow.toString());
    assertThat(
        pow.valueOf(valueEnv()),
        allOf(hasType(DOUBLE), hasValue(Math.pow((float) 2, (float) 1.5))));
  }

  /** Test rint with byte value. */
  @ParameterizedTest(name = "rint({0})")
  @ValueSource(bytes = {-1, 0, 1, Byte.MAX_VALUE, Byte.MIN_VALUE})
  public void rint_byte_value(Byte value) {
    FunctionExpression rint = DSL.rint(DSL.literal(value));
    assertThat(rint.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.rint(value))));
    assertEquals(String.format("rint(%s)", value), rint.toString());
  }

  /** Test rint with short value. */
  @ParameterizedTest(name = "rint({0})")
  @ValueSource(shorts = {-1, 0, 1, Short.MAX_VALUE, Short.MIN_VALUE})
  public void rint_short_value(Short value) {
    FunctionExpression rint = DSL.rint(DSL.literal(value));
    assertThat(rint.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.rint(value))));
    assertEquals(String.format("rint(%s)", value), rint.toString());
  }

  /** Test rint with integer value. */
  @ParameterizedTest(name = "rint({0})")
  @ValueSource(ints = {-1, 0, 1, Integer.MAX_VALUE, Integer.MIN_VALUE})
  public void rint_int_value(Integer value) {
    FunctionExpression rint = DSL.rint(DSL.literal(value));
    assertThat(rint.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.rint(value))));
    assertEquals(String.format("rint(%s)", value), rint.toString());
  }

  /** Test rint with long value. */
  @ParameterizedTest(name = "rint({0})")
  @ValueSource(longs = {-1L, 0L, 1L, Long.MAX_VALUE, Long.MIN_VALUE})
  public void rint_long_value(Long value) {
    FunctionExpression rint = DSL.rint(DSL.literal(value));
    assertThat(rint.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.rint(value))));
    assertEquals(String.format("rint(%s)", value), rint.toString());
  }

  /** Test rint with float value. */
  @ParameterizedTest(name = "rint({0})")
  @ValueSource(
      floats = {
        -1F,
        -0.75F,
        -0.5F,
        0F,
        0.5F,
        0.500000001F,
        0.75F,
        1F,
        1.9999F,
        42.42F,
        Float.MAX_VALUE,
        Float.MIN_VALUE
      })
  public void rint_float_value(Float value) {
    FunctionExpression rint = DSL.rint(DSL.literal(value));
    assertThat(rint.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.rint(value))));
    assertEquals(String.format("rint(%s)", value), rint.toString());
  }

  /** Test rint with double value. */
  @ParameterizedTest(name = "rint({0})")
  @ValueSource(
      doubles = {
        -1F,
        -0.75F,
        -0.5F,
        0F,
        0.5F,
        0.500000001F,
        0.75F,
        1F,
        1.9999F,
        42.42F,
        Double.MAX_VALUE,
        Double.MIN_VALUE
      })
  public void rint_double_value(Double value) {
    FunctionExpression rint = DSL.rint(DSL.literal(value));
    assertThat(rint.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.rint(value))));
    assertEquals(String.format("rint(%s)", value), rint.toString());
  }

  /** Test round with integer value. */
  @ParameterizedTest(name = "round({0}")
  @ValueSource(ints = {21, -21})
  public void round_int_value(Integer value) {
    FunctionExpression round = DSL.round(DSL.literal(value));
    assertThat(round.valueOf(valueEnv()), allOf(hasType(LONG), hasValue((long) Math.round(value))));
    assertEquals(String.format("round(%s)", value), round.toString());

    round = DSL.round(DSL.literal(value), DSL.literal(1));
    assertThat(
        round.valueOf(valueEnv()),
        allOf(
            hasType(LONG),
            hasValue(new BigDecimal(value).setScale(1, RoundingMode.HALF_UP).longValue())));
    assertEquals(String.format("round(%s, 1)", value), round.toString());

    round = DSL.round(DSL.literal(value), DSL.literal(-1));
    assertThat(
        round.valueOf(valueEnv()),
        allOf(
            hasType(LONG),
            hasValue(new BigDecimal(value).setScale(-1, RoundingMode.HALF_UP).longValue())));
    assertEquals(String.format("round(%s, -1)", value), round.toString());
  }

  /** Test round with long value. */
  @ParameterizedTest(name = "round({0}")
  @ValueSource(longs = {21L, -21L})
  public void round_long_value(Long value) {
    FunctionExpression round = DSL.round(DSL.literal(value));
    assertThat(round.valueOf(valueEnv()), allOf(hasType(LONG), hasValue((long) Math.round(value))));
    assertEquals(String.format("round(%s)", value), round.toString());

    round = DSL.round(DSL.literal(value), DSL.literal(1));
    assertThat(
        round.valueOf(valueEnv()),
        allOf(
            hasType(LONG),
            hasValue(new BigDecimal(value).setScale(1, RoundingMode.HALF_UP).longValue())));
    assertEquals(String.format("round(%s, 1)", value), round.toString());

    round = DSL.round(DSL.literal(value), DSL.literal(-1));
    assertThat(
        round.valueOf(valueEnv()),
        allOf(
            hasType(LONG),
            hasValue(new BigDecimal(value).setScale(-1, RoundingMode.HALF_UP).longValue())));
    assertEquals(String.format("round(%s, -1)", value), round.toString());
  }

  /** Test round with float value. */
  @ParameterizedTest(name = "round({0}")
  @ValueSource(floats = {21F, -21F})
  public void round_float_value(Float value) {
    FunctionExpression round = DSL.round(DSL.literal(value));
    assertThat(
        round.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue((double) Math.round(value))));
    assertEquals(String.format("round(%s)", value), round.toString());

    round = DSL.round(DSL.literal(value), DSL.literal(1));
    assertThat(
        round.valueOf(valueEnv()),
        allOf(
            hasType(DOUBLE),
            hasValue(new BigDecimal(value).setScale(1, RoundingMode.HALF_UP).doubleValue())));
    assertEquals(String.format("round(%s, 1)", value), round.toString());

    round = DSL.round(DSL.literal(value), DSL.literal(-1));
    assertThat(
        round.valueOf(valueEnv()),
        allOf(
            hasType(DOUBLE),
            hasValue(new BigDecimal(value).setScale(-1, RoundingMode.HALF_UP).doubleValue())));
    assertEquals(String.format("round(%s, -1)", value), round.toString());
  }

  /** Test round with double value. */
  @ParameterizedTest(name = "round({0}")
  @ValueSource(doubles = {21D, -21D})
  public void round_double_value(Double value) {
    FunctionExpression round = DSL.round(DSL.literal(value));
    assertThat(
        round.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue((double) Math.round(value))));
    assertEquals(String.format("round(%s)", value), round.toString());

    round = DSL.round(DSL.literal(value), DSL.literal(1));
    assertThat(
        round.valueOf(valueEnv()),
        allOf(
            hasType(DOUBLE),
            hasValue(new BigDecimal(value).setScale(1, RoundingMode.HALF_UP).doubleValue())));
    assertEquals(String.format("round(%s, 1)", value), round.toString());

    round = DSL.round(DSL.literal(value), DSL.literal(-1));
    assertThat(
        round.valueOf(valueEnv()),
        allOf(
            hasType(DOUBLE),
            hasValue(new BigDecimal(value).setScale(-1, RoundingMode.HALF_UP).doubleValue())));
    assertEquals(String.format("round(%s, -1)", value), round.toString());
  }

  /** Test sign with integer value. */
  @ParameterizedTest(name = "sign({0})")
  @ValueSource(ints = {2, -2})
  public void sign_int_value(Integer value) {
    FunctionExpression sign = DSL.sign(DSL.literal(value));
    assertThat(
        sign.valueOf(valueEnv()), allOf(hasType(INTEGER), hasValue((int) Math.signum(value))));
    assertEquals(String.format("sign(%s)", value), sign.toString());
  }

  /** Test sign with long value. */
  @ParameterizedTest(name = "sign({0})")
  @ValueSource(longs = {2L, -2L})
  public void sign_long_value(Long value) {
    FunctionExpression sign = DSL.sign(DSL.literal(value));
    assertThat(
        sign.valueOf(valueEnv()), allOf(hasType(INTEGER), hasValue((int) Math.signum(value))));
    assertEquals(String.format("sign(%s)", value), sign.toString());
  }

  /** Test sign with float value. */
  @ParameterizedTest(name = "sign({0})")
  @ValueSource(floats = {2F, -2F})
  public void sign_float_value(Float value) {
    FunctionExpression sign = DSL.sign(DSL.literal(value));
    assertThat(
        sign.valueOf(valueEnv()), allOf(hasType(INTEGER), hasValue((int) Math.signum(value))));
    assertEquals(String.format("sign(%s)", value), sign.toString());
  }

  /** Test sign with double value. */
  @ParameterizedTest(name = "sign({0})")
  @ValueSource(doubles = {2, -2})
  public void sign_double_value(Double value) {
    FunctionExpression sign = DSL.sign(DSL.literal(value));
    assertThat(
        sign.valueOf(valueEnv()), allOf(hasType(INTEGER), hasValue((int) Math.signum(value))));
    assertEquals(String.format("sign(%s)", value), sign.toString());
  }

  /** Test signum with byte value. */
  @ParameterizedTest(name = "signum({0})")
  @ValueSource(bytes = {2, 0, -2})
  public void signum_bytes_value(Byte value) {
    FunctionExpression signum = DSL.signum(DSL.literal(value));
    assertThat(
        signum.valueOf(valueEnv()), allOf(hasType(INTEGER), hasValue((int) Math.signum(value))));
    assertEquals(String.format("signum(%s)", value), signum.toString());
  }

  /** Test signum with short value. */
  @ParameterizedTest(name = "signum({0})")
  @ValueSource(shorts = {2, 0, -2})
  public void signum_short_value(Short value) {
    FunctionExpression signum = DSL.signum(DSL.literal(value));
    assertThat(
        signum.valueOf(valueEnv()), allOf(hasType(INTEGER), hasValue((int) Math.signum(value))));
    assertEquals(String.format("signum(%s)", value), signum.toString());
  }

  /** Test signum with integer value. */
  @ParameterizedTest(name = "signum({0})")
  @ValueSource(ints = {2, 0, -2})
  public void signum_int_value(Integer value) {
    FunctionExpression signum = DSL.signum(DSL.literal(value));
    assertThat(
        signum.valueOf(valueEnv()), allOf(hasType(INTEGER), hasValue((int) Math.signum(value))));
    assertEquals(String.format("signum(%s)", value), signum.toString());
  }

  /** Test signum with long value. */
  @ParameterizedTest(name = "signum({0})")
  @ValueSource(longs = {2L, 0L, -2L})
  public void signum_long_value(Long value) {
    FunctionExpression signum = DSL.signum(DSL.literal(value));
    assertThat(
        signum.valueOf(valueEnv()), allOf(hasType(INTEGER), hasValue((int) Math.signum(value))));
    assertEquals(String.format("signum(%s)", value), signum.toString());
  }

  /** Test signum with float value. */
  @ParameterizedTest(name = "signum({0})")
  @ValueSource(floats = {2F, 0F, -2F})
  public void signum_float_value(Float value) {
    FunctionExpression signum = DSL.signum(DSL.literal(value));
    assertThat(
        signum.valueOf(valueEnv()), allOf(hasType(INTEGER), hasValue((int) Math.signum(value))));
    assertEquals(String.format("signum(%s)", value), signum.toString());
  }

  /** Test signum with double value. */
  @ParameterizedTest(name = "signum({0})")
  @ValueSource(doubles = {2, 0, -2})
  public void signum_double_value(Double value) {
    FunctionExpression signum = DSL.signum(DSL.literal(value));
    assertThat(
        signum.valueOf(valueEnv()), allOf(hasType(INTEGER), hasValue((int) Math.signum(value))));
    assertEquals(String.format("signum(%s)", value), signum.toString());
  }  

  /** Test sinh with byte value. */
  @ParameterizedTest(name = "sinh({0})")
  @ValueSource(bytes = {-1, 1, 2, Byte.MAX_VALUE, Byte.MIN_VALUE})
  public void sinh_byte_value(Byte value) {
    FunctionExpression sinh = DSL.sinh(DSL.literal(value));
    assertThat(sinh.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.sinh(value))));
    assertEquals(String.format("sinh(%s)", value), sinh.toString());
  }

  /** Test sinh with short value. */
  @ParameterizedTest(name = "sinh({0})")
  @ValueSource(shorts = {-1, 1, 2, Short.MAX_VALUE, Short.MIN_VALUE})
  public void sinh_short_value(Short value) {
    FunctionExpression sinh = DSL.sinh(DSL.literal(value));
    assertThat(sinh.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.sinh(value))));
    assertEquals(String.format("sinh(%s)", value), sinh.toString());
  }

  /** Test sinh with integer value. */
  @ParameterizedTest(name = "sinh({0})")
  @ValueSource(ints = {-1, 1, 2, Integer.MAX_VALUE, Integer.MIN_VALUE})
  public void sinh_int_value(Integer value) {
    FunctionExpression sinh = DSL.sinh(DSL.literal(value));
    assertThat(sinh.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.sinh(value))));
    assertEquals(String.format("sinh(%s)", value), sinh.toString());
  }

  /** Test sinh with long value. */
  @ParameterizedTest(name = "sinh({0})")
  @ValueSource(longs = {-1L, 1L, 2L, Long.MAX_VALUE, Long.MIN_VALUE})
  public void sinh_long_value(Long value) {
    FunctionExpression sinh = DSL.sinh(DSL.literal(value));
    assertThat(sinh.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.sinh(value))));
    assertEquals(String.format("sinh(%s)", value), sinh.toString());
  }

  /** Test sinh with float value. */
  @ParameterizedTest(name = "sinh({0})")
  @ValueSource(floats = {-1.5F, -1F, 1F, 1.5F, 2F, 2.7F, Float.MAX_VALUE, Float.MIN_VALUE})
  public void sinh_float_value(Float value) {
    FunctionExpression sinh = DSL.sinh(DSL.literal(value));
    assertThat(sinh.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.sinh(value))));
    assertEquals(String.format("sinh(%s)", value), sinh.toString());
  }

  /** Test sinh with double value. */
  @ParameterizedTest(name = "sinh({0})")
  @ValueSource(doubles = {-1.5, -1D, 1D, 1.5D, 2D, 2.7D, Double.MAX_VALUE, Double.MIN_VALUE})
  public void sinh_double_value(Double value) {
    FunctionExpression sinh = DSL.sinh(DSL.literal(value));
    assertThat(sinh.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.sinh(value))));
    assertEquals(String.format("sinh(%s)", value), sinh.toString());
  }

  /** Test sqrt with int value. */
  @ParameterizedTest(name = "sqrt({0})")
  @ValueSource(ints = {1, 2})
  public void sqrt_int_value(Integer value) {
    FunctionExpression sqrt = DSL.sqrt(DSL.literal(value));
    assertThat(sqrt.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.sqrt(value))));
    assertEquals(String.format("sqrt(%s)", value), sqrt.toString());
  }

  /** Test sqrt with long value. */
  @ParameterizedTest(name = "sqrt({0})")
  @ValueSource(longs = {1L, 2L})
  public void sqrt_long_value(Long value) {
    FunctionExpression sqrt = DSL.sqrt(DSL.literal(value));
    assertThat(sqrt.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.sqrt(value))));
    assertEquals(String.format("sqrt(%s)", value), sqrt.toString());
  }

  /** Test sqrt with float value. */
  @ParameterizedTest(name = "sqrt({0})")
  @ValueSource(floats = {1F, 2F})
  public void sqrt_float_value(Float value) {
    FunctionExpression sqrt = DSL.sqrt(DSL.literal(value));
    assertThat(sqrt.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.sqrt(value))));
    assertEquals(String.format("sqrt(%s)", value), sqrt.toString());
  }

  /** Test sqrt with double value. */
  @ParameterizedTest(name = "sqrt({0})")
  @ValueSource(doubles = {1D, 2D})
  public void sqrt_double_value(Double value) {
    FunctionExpression sqrt = DSL.sqrt(DSL.literal(value));
    assertThat(sqrt.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.sqrt(value))));
    assertEquals(String.format("sqrt(%s)", value), sqrt.toString());
  }

  /** Test sqrt with negative value. */
  @ParameterizedTest(name = "sqrt({0})")
  @ValueSource(doubles = {-1D, -2D})
  public void sqrt_negative_value(Double value) {
    FunctionExpression sqrt = DSL.sqrt(DSL.literal(value));
    assertEquals(DOUBLE, sqrt.type());
    assertTrue(sqrt.valueOf(valueEnv()).isNull());
  }

  /** Test truncate with integer value. */
  @ParameterizedTest(name = "truncate({0}, {1})")
  @ValueSource(ints = {2, -2, Integer.MAX_VALUE, Integer.MIN_VALUE})
  public void truncate_int_value(Integer value) {
    FunctionExpression truncate = DSL.truncate(DSL.literal(value), DSL.literal(1));
    assertThat(
        truncate.valueOf(valueEnv()),
        allOf(
            hasType(LONG),
            hasValue(BigDecimal.valueOf(value).setScale(1, RoundingMode.DOWN).longValue())));
    assertEquals(String.format("truncate(%s, 1)", value), truncate.toString());
  }

  /** Test truncate with long value. */
  @ParameterizedTest(name = "truncate({0}, {1})")
  @ValueSource(longs = {2L, -2L, Long.MAX_VALUE, Long.MIN_VALUE})
  public void truncate_long_value(Long value) {
    FunctionExpression truncate = DSL.truncate(DSL.literal(value), DSL.literal(1));
    assertThat(
        truncate.valueOf(valueEnv()),
        allOf(
            hasType(LONG),
            hasValue(BigDecimal.valueOf(value).setScale(1, RoundingMode.DOWN).longValue())));
    assertEquals(String.format("truncate(%s, 1)", value), truncate.toString());
  }

  /** Test truncate with float value. */
  @ParameterizedTest(name = "truncate({0}, {1})")
  @ValueSource(floats = {2F, -2F, Float.MAX_VALUE, Float.MIN_VALUE})
  public void truncate_float_value(Float value) {
    FunctionExpression truncate = DSL.truncate(DSL.literal(value), DSL.literal(1));
    assertThat(
        truncate.valueOf(valueEnv()),
        allOf(
            hasType(DOUBLE),
            hasValue(BigDecimal.valueOf(value).setScale(1, RoundingMode.DOWN).doubleValue())));
    assertEquals(String.format("truncate(%s, 1)", value), truncate.toString());
  }

  /** Test truncate with double value. */
  @ParameterizedTest(name = "truncate({0}, {1})")
  @ValueSource(
      doubles = {
        2D,
        -9.223372036854776e+18D,
        -2147483649.0D,
        -2147483648.0D,
        -32769.0D,
        -32768.0D,
        -34.84D,
        -2.0D,
        -1.2D,
        -1.0D,
        0.0D,
        1.0D,
        1.3D,
        2.0D,
        1004.3D,
        32767.0D,
        32768.0D,
        2147483647.0D,
        2147483648.0D,
        9.223372036854776e+18D,
        Double.MAX_VALUE,
        Double.MIN_VALUE
      })
  public void truncate_double_value(Double value) {
    FunctionExpression truncate = DSL.truncate(DSL.literal(value), DSL.literal(1));
    assertThat(
        truncate.valueOf(valueEnv()),
        allOf(
            hasType(DOUBLE),
            hasValue(BigDecimal.valueOf(value).setScale(1, RoundingMode.DOWN).doubleValue())));
    assertEquals(String.format("truncate(%s, 1)", value), truncate.toString());
  }

  /** Test constant pi. */
  @Test
  public void test_pi() {
    FunctionExpression pi = DSL.pi();
    assertThat(pi.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.PI)));
  }

  /** Test rand with no argument. */
  @Test
  public void rand_no_arg() {
    FunctionExpression rand = DSL.rand();
    assertEquals(FLOAT, rand.type());
    assertTrue(
        getFloatValue(rand.valueOf(valueEnv())) >= 0
            && getFloatValue(rand.valueOf(valueEnv())) < 1);
    assertEquals("rand()", rand.toString());
  }

  /** Test rand with integer value. */
  @ParameterizedTest(name = "rand({0})")
  @ValueSource(ints = {2, 3})
  public void rand_int_value(Integer n) {
    FunctionExpression rand = DSL.rand(DSL.literal(n));
    assertEquals(FLOAT, rand.type());
    assertTrue(
        getFloatValue(rand.valueOf(valueEnv())) >= 0
            && getFloatValue(rand.valueOf(valueEnv())) < 1);
    assertEquals(getFloatValue(rand.valueOf(valueEnv())), new Random(n).nextFloat());
    assertEquals(String.format("rand(%s)", n), rand.toString());
  }

  /** Test acos with integer, long, float, double values. */
  @ParameterizedTest(name = "acos({0})")
  @MethodSource("trigonometricArguments")
  public void test_acos(Number value) {
    FunctionExpression acos = DSL.acos(DSL.literal(value));
    assertThat(
        acos.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.acos(value.doubleValue()))));
    assertEquals(String.format("acos(%s)", value), acos.toString());
  }

  /** Test acos with illegal values. */
  @ParameterizedTest(name = "acos({0})")
  @ValueSource(doubles = {2D, -2D})
  public void acos_with_illegal_value(Number value) {
    FunctionExpression acos = DSL.acos(DSL.literal(value));
    assertEquals(DOUBLE, acos.type());
    assertTrue(acos.valueOf(valueEnv()).isNull());
  }

  /** Test asin with integer, long, float, double values. */
  @ParameterizedTest(name = "asin({0})")
  @MethodSource("trigonometricArguments")
  public void test_asin(Number value) {
    FunctionExpression asin = DSL.asin(DSL.literal(value));
    assertThat(
        asin.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.asin(value.doubleValue()))));
    assertEquals(String.format("asin(%s)", value), asin.toString());
  }

  /** Test asin with illegal value. */
  @ParameterizedTest(name = "asin({0})")
  @ValueSource(doubles = {2D, -2D})
  public void asin_with_illegal_value(Number value) {
    FunctionExpression asin = DSL.asin(DSL.literal(value));
    assertEquals(DOUBLE, asin.type());
    assertTrue(asin.valueOf(valueEnv()).isNull());
  }

  /** Test atan with one argument integer, long, float, double values. */
  @ParameterizedTest(name = "atan({0})")
  @MethodSource("trigonometricArguments")
  public void atan_one_arg(Number value) {
    FunctionExpression atan = DSL.atan(DSL.literal(value));
    assertThat(
        atan.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.atan(value.doubleValue()))));
    assertEquals(String.format("atan(%s)", value), atan.toString());
  }

  /** Test atan with two arguments of integer, long, float, double values. */
  @ParameterizedTest(name = "atan({0}, {1})")
  @MethodSource("trigonometricDoubleArguments")
  public void atan_two_args(Number v1, Number v2) {
    FunctionExpression atan = DSL.atan(DSL.literal(v1), DSL.literal(v2));
    assertThat(
        atan.valueOf(valueEnv()),
        allOf(hasType(DOUBLE), hasValue(Math.atan2(v1.doubleValue(), v2.doubleValue()))));
    assertEquals(String.format("atan(%s, %s)", v1, v2), atan.toString());
  }

  /** Test atan2 with integer, long, float, double values. */
  @ParameterizedTest(name = "atan2({0}, {1})")
  @MethodSource("trigonometricDoubleArguments")
  public void test_atan2(Number v1, Number v2) {
    FunctionExpression atan2 = DSL.atan2(DSL.literal(v1), DSL.literal(v2));
    assertThat(
        atan2.valueOf(valueEnv()),
        allOf(hasType(DOUBLE), hasValue(Math.atan2(v1.doubleValue(), v2.doubleValue()))));
    assertEquals(String.format("atan2(%s, %s)", v1, v2), atan2.toString());
  }

  /** Test cos with integer, long, float, double values. */
  @ParameterizedTest(name = "cos({0})")
  @MethodSource("trigonometricArguments")
  public void test_cos(Number value) {
    FunctionExpression cos = DSL.cos(DSL.literal(value));
    assertThat(
        cos.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.cos(value.doubleValue()))));
    assertEquals(String.format("cos(%s)", value), cos.toString());
  }

  /** Test cosh with byte value. */
  @ParameterizedTest(name = "cosh({0})")
  @ValueSource(bytes = {-1, 1, 2})
  public void cosh_byte_value(Byte value) {
    FunctionExpression cosh = DSL.cosh(DSL.literal(value));
    assertThat(cosh.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.cosh(value))));
    assertEquals(String.format("cosh(%s)", value), cosh.toString());
  }

  /** Test cosh with short value. */
  @ParameterizedTest(name = "cosh({0})")
  @ValueSource(shorts = {-1, 1, 2})
  public void cosh_short_value(Short value) {
    FunctionExpression cosh = DSL.cosh(DSL.literal(value));
    assertThat(cosh.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.cosh(value))));
    assertEquals(String.format("cosh(%s)", value), cosh.toString());
  }

  /** Test cosh with integer value. */
  @ParameterizedTest(name = "cosh({0})")
  @ValueSource(ints = {-1, 1, 2})
  public void cosh_int_value(Integer value) {
    FunctionExpression cosh = DSL.cosh(DSL.literal(value));
    assertThat(cosh.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.cosh(value))));
    assertEquals(String.format("cosh(%s)", value), cosh.toString());
  }

  /** Test cosh with long value. */
  @ParameterizedTest(name = "cosh({0})")
  @ValueSource(longs = {-1L, 1L, 2L})
  public void cosh_long_value(Long value) {
    FunctionExpression cosh = DSL.cosh(DSL.literal(value));
    assertThat(cosh.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.cosh(value))));
    assertEquals(String.format("cosh(%s)", value), cosh.toString());
  }

  /** Test cosh with float value. */
  @ParameterizedTest(name = "cosh({0})")
  @ValueSource(floats = {-1F, 1F, 2F})
  public void cosh_float_value(Float value) {
    FunctionExpression cosh = DSL.cosh(DSL.literal(value));
    assertThat(cosh.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.cosh(value))));
    assertEquals(String.format("cosh(%s)", value), cosh.toString());
  }

  /** Test cosh with double value. */
  @ParameterizedTest(name = "cosh({0})")
  @ValueSource(doubles = {-1D, 1D, 2D})
  public void cosh_double_value(Double value) {
    FunctionExpression cosh = DSL.cosh(DSL.literal(value));
    assertThat(cosh.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.cosh(value))));
    assertEquals(String.format("cosh(%s)", value), cosh.toString());
  }

  /** Test cot with integer, long, float, double values. */
  @ParameterizedTest(name = "cot({0})")
  @MethodSource("trigonometricArguments")
  public void test_cot(Number value) {
    FunctionExpression cot = DSL.cot(DSL.literal(value));
    assertThat(
        cot.valueOf(valueEnv()),
        allOf(hasType(DOUBLE), hasValue(1 / Math.tan(value.doubleValue()))));
    assertEquals(String.format("cot(%s)", value), cot.toString());
  }

  /** Test cot with out-of-range value 0. */
  @ParameterizedTest(name = "cot({0})")
  @ValueSource(doubles = {0})
  public void cot_with_zero(Number value) {
    FunctionExpression cot = DSL.cot(DSL.literal(value));
    assertThrows(
        ArithmeticException.class,
        () -> cot.valueOf(valueEnv()),
        String.format("Out of range value for cot(%s)", value));
  }

  /** Test degrees with integer, long, float, double values. */
  @ParameterizedTest(name = "degrees({0})")
  @MethodSource("trigonometricArguments")
  public void test_degrees(Number value) {
    FunctionExpression degrees = DSL.degrees(DSL.literal(value));
    assertThat(
        degrees.valueOf(valueEnv()),
        allOf(hasType(DOUBLE), hasValue(Math.toDegrees(value.doubleValue()))));
    assertEquals(String.format("degrees(%s)", value), degrees.toString());
  }

  /** Test radians with integer, long, float, double values. */
  @ParameterizedTest(name = "radians({0})")
  @MethodSource("trigonometricArguments")
  public void test_radians(Number value) {
    FunctionExpression radians = DSL.radians(DSL.literal(value));
    assertThat(
        radians.valueOf(valueEnv()),
        allOf(hasType(DOUBLE), hasValue(Math.toRadians(value.doubleValue()))));
    assertEquals(String.format("radians(%s)", value), radians.toString());
  }

  /** Test sin with integer, long, float, double values. */
  @ParameterizedTest(name = "sin({0})")
  @MethodSource("trigonometricArguments")
  public void test_sin(Number value) {
    FunctionExpression sin = DSL.sin(DSL.literal(value));
    assertThat(
        sin.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.sin(value.doubleValue()))));
    assertEquals(String.format("sin(%s)", value), sin.toString());
  }

  /** Test tan with integer, long, float, double values. */
  @ParameterizedTest(name = "tan({0})")
  @MethodSource("trigonometricArguments")
  public void test_tan(Number value) {
    FunctionExpression tan = DSL.tan(DSL.literal(value));
    assertThat(
        tan.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.tan(value.doubleValue()))));
    assertEquals(String.format("tan(%s)", value), tan.toString());
  }

  /** Test cbrt with int value. */
  @ParameterizedTest(name = "cbrt({0})")
  @ValueSource(ints = {1, 2})
  public void cbrt_int_value(Integer value) {
    FunctionExpression cbrt = DSL.cbrt(DSL.literal(value));
    assertThat(cbrt.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.cbrt(value))));
    assertEquals(String.format("cbrt(%s)", value), cbrt.toString());
  }

  /** Test cbrt with long value. */
  @ParameterizedTest(name = "cbrt({0})")
  @ValueSource(longs = {1L, 2L})
  public void cbrt_long_value(Long value) {
    FunctionExpression cbrt = DSL.cbrt(DSL.literal(value));
    assertThat(cbrt.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.cbrt(value))));
    assertEquals(String.format("cbrt(%s)", value), cbrt.toString());
  }

  /** Test cbrt with float value. */
  @ParameterizedTest(name = "cbrt({0})")
  @ValueSource(floats = {1F, 2F})
  public void cbrt_float_value(Float value) {
    FunctionExpression cbrt = DSL.cbrt(DSL.literal(value));
    assertThat(cbrt.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.cbrt(value))));
    assertEquals(String.format("cbrt(%s)", value), cbrt.toString());
  }

  /** Test cbrt with double value. */
  @ParameterizedTest(name = "cbrt({0})")
  @ValueSource(doubles = {1D, 2D, Double.MAX_VALUE, Double.MIN_VALUE})
  public void cbrt_double_value(Double value) {
    FunctionExpression cbrt = DSL.cbrt(DSL.literal(value));
    assertThat(cbrt.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.cbrt(value))));
    assertEquals(String.format("cbrt(%s)", value), cbrt.toString());
  }

  /** Test cbrt with negative value. */
  @ParameterizedTest(name = "cbrt({0})")
  @ValueSource(doubles = {-1D, -2D})
  public void cbrt_negative_value(Double value) {
    FunctionExpression cbrt = DSL.cbrt(DSL.literal(value));
    assertThat(cbrt.valueOf(valueEnv()), allOf(hasType(DOUBLE), hasValue(Math.cbrt(value))));
    assertEquals(String.format("cbrt(%s)", value), cbrt.toString());
  }
}
