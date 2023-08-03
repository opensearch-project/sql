/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.operator.arthmetic;

import static org.opensearch.sql.data.type.ExprCoreType.BYTE;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.function.FunctionDSL.define;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.expression.function.SerializableFunction;

@UtilityClass
public class MathematicalFunction {
  /**
   * Register Mathematical Functions.
   *
   * @param repository {@link BuiltinFunctionRepository}.
   */
  public static void register(BuiltinFunctionRepository repository) {
    repository.register(abs());
    repository.register(acos());
    repository.register(asin());
    repository.register(atan());
    repository.register(atan2());
    repository.register(cbrt());
    repository.register(ceil());
    repository.register(ceiling());
    repository.register(conv());
    repository.register(cos());
    repository.register(cosh());
    repository.register(cot());
    repository.register(crc32());
    repository.register(degrees());
    repository.register(euler());
    repository.register(exp());
    repository.register(expm1());
    repository.register(floor());
    repository.register(ln());
    repository.register(log());
    repository.register(log10());
    repository.register(log2());
    repository.register(mod());
    repository.register(pi());
    repository.register(pow());
    repository.register(power());
    repository.register(radians());
    repository.register(rand());
    repository.register(rint());
    repository.register(round());
    repository.register(sign());
    repository.register(signum());
    repository.register(sin());
    repository.register(sinh());
    repository.register(sqrt());
    repository.register(tan());
    repository.register(truncate());
  }

  /**
   * Base function for math functions with similar formats that return DOUBLE.
   *
   * @param functionName BuiltinFunctionName of math function.
   * @param formula lambda function of math formula.
   * @param returnType data type return type of the calling function
   * @return DefaultFunctionResolver for math functions.
   */
  private static DefaultFunctionResolver baseMathFunction(
      FunctionName functionName,
      SerializableFunction<ExprValue, ExprValue> formula,
      ExprCoreType returnType) {
    return define(
        functionName,
        ExprCoreType.numberTypes().stream()
            .map(type -> impl(nullMissingHandling(formula), returnType, type))
            .collect(Collectors.toList()));
  }

  /**
   * <b>Definition of abs() function.<\b><br>
   * The supported signature of abs() function are INT -> INT LONG -> LONG FLOAT -> FLOAT DOUBLE ->
   * DOUBLE
   */
  private static DefaultFunctionResolver abs() {
    return define(
        BuiltinFunctionName.ABS.getName(),
        impl(nullMissingHandling(v -> new ExprByteValue(Math.abs(v.byteValue()))), BYTE, BYTE),
        impl(nullMissingHandling(v -> new ExprShortValue(Math.abs(v.shortValue()))), SHORT, SHORT),
        impl(
            nullMissingHandling(v -> new ExprIntegerValue(Math.abs(v.integerValue()))),
            INTEGER,
            INTEGER),
        impl(nullMissingHandling(v -> new ExprLongValue(Math.abs(v.longValue()))), LONG, LONG),
        impl(nullMissingHandling(v -> new ExprFloatValue(Math.abs(v.floatValue()))), FLOAT, FLOAT),
        impl(
            nullMissingHandling(v -> new ExprDoubleValue(Math.abs(v.doubleValue()))),
            DOUBLE,
            DOUBLE));
  }

  /**
   * <b>Definition of ceil(x)/ceiling(x) function.<\b><br>
   * Calculate the next highest integer that x rounds up to The supported signature of ceil/ceiling
   * function is DOUBLE -> INTEGER
   */
  private static DefaultFunctionResolver ceil() {
    return define(
        BuiltinFunctionName.CEIL.getName(),
        impl(
            nullMissingHandling(v -> new ExprLongValue(Math.ceil(v.doubleValue()))), LONG, DOUBLE));
  }

  private static DefaultFunctionResolver ceiling() {
    return define(
        BuiltinFunctionName.CEILING.getName(),
        impl(
            nullMissingHandling(v -> new ExprLongValue(Math.ceil(v.doubleValue()))), LONG, DOUBLE));
  }

  /**
   * <b>Definition of conv(x, a, b) function.<\b><br>
   * Convert number x from base a to base b<br>
   * The supported signature of floor function is<br>
   * (STRING, INTEGER, INTEGER) -> STRING<br>
   * (INTEGER, INTEGER, INTEGER) -> STRING
   */
  private static DefaultFunctionResolver conv() {
    return define(
        BuiltinFunctionName.CONV.getName(),
        impl(
            nullMissingHandling(
                (x, a, b) ->
                    new ExprStringValue(
                        Integer.toString(
                            Integer.parseInt(x.stringValue(), a.integerValue()),
                            b.integerValue()))),
            STRING,
            STRING,
            INTEGER,
            INTEGER),
        impl(
            nullMissingHandling(
                (x, a, b) ->
                    new ExprStringValue(
                        Integer.toString(
                            Integer.parseInt(x.integerValue().toString(), a.integerValue()),
                            b.integerValue()))),
            STRING,
            INTEGER,
            INTEGER,
            INTEGER));
  }

  /**
   * <b>Definition of crc32(x) function.<\b><br>
   * Calculate a cyclic redundancy check value and returns a 32-bit unsigned value<br>
   * The supported signature of crc32 function is<br>
   * STRING -> LONG
   */
  private static DefaultFunctionResolver crc32() {
    return define(
        BuiltinFunctionName.CRC32.getName(),
        impl(
            nullMissingHandling(
                v -> {
                  CRC32 crc = new CRC32();
                  crc.update(v.stringValue().getBytes());
                  return new ExprLongValue(crc.getValue());
                }),
            LONG,
            STRING));
  }

  /**
   * <b>Definition of e() function.</b><br>
   * Get the Euler's number. () -> DOUBLE
   */
  private static DefaultFunctionResolver euler() {
    return define(BuiltinFunctionName.E.getName(), impl(() -> new ExprDoubleValue(Math.E), DOUBLE));
  }

  /**
   * <b>Definition of exp(x) function.<b><br>
   * Calculate exponent function e to the x The supported signature of exp function is
   * INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE
   */
  private static DefaultFunctionResolver exp() {
    return baseMathFunction(
        BuiltinFunctionName.EXP.getName(),
        v -> new ExprDoubleValue(Math.exp(v.doubleValue())),
        DOUBLE);
  }

  /**
   * <b>Definition of expm1(x) function.</b><br>
   * Calculate exponent function e to the x, minus 1 The supported signature of exp function is
   * INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE
   */
  private static DefaultFunctionResolver expm1() {
    return baseMathFunction(
        BuiltinFunctionName.EXPM1.getName(),
        v -> new ExprDoubleValue(Math.expm1(v.doubleValue())),
        DOUBLE);
  }

  /**
   * <b>Definition of floor(x) function.</b><br>
   * Calculate the next nearest whole integer that x rounds down to The supported signature of floor
   * function is DOUBLE -> INTEGER
   */
  private static DefaultFunctionResolver floor() {
    return define(
        BuiltinFunctionName.FLOOR.getName(),
        impl(
            nullMissingHandling(v -> new ExprLongValue(Math.floor(v.doubleValue()))),
            LONG,
            DOUBLE));
  }

  /**
   * Definition of ln(x) function. Calculate the natural logarithm of x The supported signature of
   * ln function is INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE
   */
  private static DefaultFunctionResolver ln() {
    return baseMathFunction(
        BuiltinFunctionName.LN.getName(),
        v ->
            v.doubleValue() <= 0
                ? ExprNullValue.of()
                : new ExprDoubleValue(Math.log(v.doubleValue())),
        DOUBLE);
  }

  /**
   * <b>Definition of log(b, x) function.</b><br>
   * Calculate the logarithm of x using b as the base The supported signature of log function is (b:
   * INTEGER/LONG/FLOAT/DOUBLE, x: INTEGER/LONG/FLOAT/DOUBLE]) -> DOUBLE
   */
  private static DefaultFunctionResolver log() {
    ImmutableList.Builder<
            SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>>
        builder = new ImmutableList.Builder<>();

    // build unary log(x), SHORT/INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE
    for (ExprType type : ExprCoreType.numberTypes()) {
      builder.add(
          impl(
              nullMissingHandling(
                  v ->
                      v.doubleValue() <= 0
                          ? ExprNullValue.of()
                          : new ExprDoubleValue(Math.log(v.doubleValue()))),
              DOUBLE,
              type));
    }

    // build binary function log(b, x)
    for (ExprType baseType : ExprCoreType.numberTypes()) {
      for (ExprType numberType : ExprCoreType.numberTypes()) {
        builder.add(
            impl(
                nullMissingHandling(
                    (b, x) ->
                        b.doubleValue() <= 0 || x.doubleValue() <= 0
                            ? ExprNullValue.of()
                            : new ExprDoubleValue(
                                Math.log(x.doubleValue()) / Math.log(b.doubleValue()))),
                DOUBLE,
                baseType,
                numberType));
      }
    }
    return define(BuiltinFunctionName.LOG.getName(), builder.build());
  }

  /**
   * <b>Definition of log10(x) function.</b><br>
   * Calculate base-10 logarithm of x The supported signature of<br>
   * log function is SHORT/INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE
   */
  private static DefaultFunctionResolver log10() {
    return baseMathFunction(
        BuiltinFunctionName.LOG10.getName(),
        v ->
            v.doubleValue() <= 0
                ? ExprNullValue.of()
                : new ExprDoubleValue(Math.log10(v.doubleValue())),
        DOUBLE);
  }

  /**
   * <b>Definition of log2(x) function.</b><br>
   * Calculate base-2 logarithm of x The supported signature of log<br>
   * function is SHORT/INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE
   */
  private static DefaultFunctionResolver log2() {
    return baseMathFunction(
        BuiltinFunctionName.LOG2.getName(),
        v ->
            v.doubleValue() <= 0
                ? ExprNullValue.of()
                : new ExprDoubleValue(Math.log(v.doubleValue()) / Math.log(2)),
        DOUBLE);
  }

  /**
   * <b>Definition of mod(x, y) function.</b><br>
   * Calculate the remainder of x divided by y<br>
   * The supported signature of mod function is<br>
   * (x: INTEGER/LONG/FLOAT/DOUBLE, y: INTEGER/LONG/FLOAT/DOUBLE)<br>
   * -> wider type between types of x and y
   */
  private static DefaultFunctionResolver mod() {
    return define(
        BuiltinFunctionName.MOD.getName(),
        impl(
            nullMissingHandling(
                (v1, v2) ->
                    v2.byteValue() == 0
                        ? ExprNullValue.of()
                        : new ExprByteValue(v1.byteValue() % v2.byteValue())),
            BYTE,
            BYTE,
            BYTE),
        impl(
            nullMissingHandling(
                (v1, v2) ->
                    v2.shortValue() == 0
                        ? ExprNullValue.of()
                        : new ExprShortValue(v1.shortValue() % v2.shortValue())),
            SHORT,
            SHORT,
            SHORT),
        impl(
            nullMissingHandling(
                (v1, v2) ->
                    v2.shortValue() == 0
                        ? ExprNullValue.of()
                        : new ExprIntegerValue(
                            Math.floorMod(v1.integerValue(), v2.integerValue()))),
            INTEGER,
            INTEGER,
            INTEGER),
        impl(
            nullMissingHandling(
                (v1, v2) ->
                    v2.shortValue() == 0
                        ? ExprNullValue.of()
                        : new ExprLongValue(Math.floorMod(v1.longValue(), v2.longValue()))),
            LONG,
            LONG,
            LONG),
        impl(
            nullMissingHandling(
                (v1, v2) ->
                    v2.shortValue() == 0
                        ? ExprNullValue.of()
                        : new ExprFloatValue(v1.floatValue() % v2.floatValue())),
            FLOAT,
            FLOAT,
            FLOAT),
        impl(
            nullMissingHandling(
                (v1, v2) ->
                    v2.shortValue() == 0
                        ? ExprNullValue.of()
                        : new ExprDoubleValue(v1.doubleValue() % v2.doubleValue())),
            DOUBLE,
            DOUBLE,
            DOUBLE));
  }

  /**
   * <b>Definition of pi() function.</b><br>
   * Get the value of pi.<br>
   * () -> DOUBLE
   */
  private static DefaultFunctionResolver pi() {
    return define(
        BuiltinFunctionName.PI.getName(), impl(() -> new ExprDoubleValue(Math.PI), DOUBLE));
  }

  /**
   * <b>Definition of pow(x, y)/power(x, y) function.</b><br>
   * Calculate the value of x raised to the power of y<br>
   * The supported signature of pow/power function is<br>
   * (INTEGER, INTEGER) -> DOUBLE<br>
   * (LONG, LONG) -> DOUBLE<br>
   * (FLOAT, FLOAT) -> DOUBLE<br>
   * (DOUBLE, DOUBLE) -> DOUBLE
   */
  private static DefaultFunctionResolver pow() {
    return define(BuiltinFunctionName.POW.getName(), powerFunctionImpl());
  }

  private static DefaultFunctionResolver power() {
    return define(BuiltinFunctionName.POWER.getName(), powerFunctionImpl());
  }

  private List<SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>>
      powerFunctionImpl() {
    return Arrays.asList(
        impl(
            nullMissingHandling(
                (v1, v2) -> new ExprDoubleValue(Math.pow(v1.shortValue(), v2.shortValue()))),
            DOUBLE,
            SHORT,
            SHORT),
        impl(
            nullMissingHandling(
                (v1, v2) -> new ExprDoubleValue(Math.pow(v1.integerValue(), v2.integerValue()))),
            DOUBLE,
            INTEGER,
            INTEGER),
        impl(
            nullMissingHandling(
                (v1, v2) -> new ExprDoubleValue(Math.pow(v1.longValue(), v2.longValue()))),
            DOUBLE,
            LONG,
            LONG),
        impl(
            nullMissingHandling(
                (v1, v2) ->
                    v1.floatValue() <= 0 && v2.floatValue() != Math.floor(v2.floatValue())
                        ? ExprNullValue.of()
                        : new ExprDoubleValue(Math.pow(v1.floatValue(), v2.floatValue()))),
            DOUBLE,
            FLOAT,
            FLOAT),
        impl(
            nullMissingHandling(
                (v1, v2) ->
                    v1.doubleValue() <= 0 && v2.doubleValue() != Math.floor(v2.doubleValue())
                        ? ExprNullValue.of()
                        : new ExprDoubleValue(Math.pow(v1.doubleValue(), v2.doubleValue()))),
            DOUBLE,
            DOUBLE,
            DOUBLE));
  }

  /**
   * <b>Definition of rand() and rand(N) function.</b><br>
   * rand() returns a random floating-point value in the range 0 <= value < 1.0<br>
   * If integer N is specified, the seed is initialized prior to execution.<br>
   * One implication of this behavior is with identical argument N,rand(N) returns the same value
   * <br>
   * each time, and thus produces a repeatable sequence of column values. The supported signature of
   * <br>
   * rand function is ([INTEGER]) -> FLOAT
   */
  private static DefaultFunctionResolver rand() {
    return define(
        BuiltinFunctionName.RAND.getName(),
        impl(() -> new ExprFloatValue(new Random().nextFloat()), FLOAT),
        impl(
            nullMissingHandling(v -> new ExprFloatValue(new Random(v.integerValue()).nextFloat())),
            FLOAT,
            INTEGER));
  }

  /**
   * <b>Definition of rint(x) function.</b><br>
   * Returns the closest whole integer value to x<br>
   * The supported signature is<br>
   * BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE
   */
  private static DefaultFunctionResolver rint() {
    return baseMathFunction(
        BuiltinFunctionName.RINT.getName(),
        v -> new ExprDoubleValue(Math.rint(v.doubleValue())),
        DOUBLE);
  }

  /**
   * <b>Definition of round(x)/round(x, d) function.</b><br>
   * Rounds the argument x to d decimal places, d defaults to 0 if not specified.<br>
   * The supported signature of round function is<br>
   * (x: INTEGER [, y: INTEGER]) -> INTEGER<br>
   * (x: LONG [, y: INTEGER]) -> LONG<br>
   * (x: FLOAT [, y: INTEGER]) -> FLOAT<br>
   * (x: DOUBLE [, y: INTEGER]) -> DOUBLE
   */
  private static DefaultFunctionResolver round() {
    return define(
        BuiltinFunctionName.ROUND.getName(),
        // rand(x)
        impl(
            nullMissingHandling(v -> new ExprLongValue((long) Math.round(v.integerValue()))),
            LONG,
            INTEGER),
        impl(
            nullMissingHandling(v -> new ExprLongValue((long) Math.round(v.longValue()))),
            LONG,
            LONG),
        impl(
            nullMissingHandling(v -> new ExprDoubleValue((double) Math.round(v.floatValue()))),
            DOUBLE,
            FLOAT),
        impl(
            nullMissingHandling(
                v ->
                    new ExprDoubleValue(
                        new BigDecimal(v.doubleValue())
                            .setScale(0, RoundingMode.HALF_UP)
                            .doubleValue())),
            DOUBLE,
            DOUBLE),

        // rand(x, d)
        impl(
            nullMissingHandling(
                (x, d) ->
                    new ExprLongValue(
                        new BigDecimal(x.integerValue())
                            .setScale(d.integerValue(), RoundingMode.HALF_UP)
                            .longValue())),
            LONG,
            INTEGER,
            INTEGER),
        impl(
            nullMissingHandling(
                (x, d) ->
                    new ExprLongValue(
                        new BigDecimal(x.longValue())
                            .setScale(d.integerValue(), RoundingMode.HALF_UP)
                            .longValue())),
            LONG,
            LONG,
            INTEGER),
        impl(
            nullMissingHandling(
                (x, d) ->
                    new ExprDoubleValue(
                        new BigDecimal(x.floatValue())
                            .setScale(d.integerValue(), RoundingMode.HALF_UP)
                            .doubleValue())),
            DOUBLE,
            FLOAT,
            INTEGER),
        impl(
            nullMissingHandling(
                (x, d) ->
                    new ExprDoubleValue(
                        new BigDecimal(x.doubleValue())
                            .setScale(d.integerValue(), RoundingMode.HALF_UP)
                            .doubleValue())),
            DOUBLE,
            DOUBLE,
            INTEGER));
  }

  /**
   * <b>Definition of sign(x) function.</b><br>
   * Returns the sign of the argument as -1, 0, or 1<br>
   * depending on whether x is negative, zero, or positive<br>
   * The supported signature is<br>
   * SHORT/INTEGER/LONG/FLOAT/DOUBLE -> INTEGER
   */
  private static DefaultFunctionResolver sign() {
    return baseMathFunction(
        BuiltinFunctionName.SIGN.getName(),
        v -> new ExprIntegerValue(Math.signum(v.doubleValue())),
        INTEGER);
  }

  /**
   * <b>Definition of signum(x) function.</b><br>
   * Returns the sign of the argument as -1.0, 0, or 1.0<br>
   * depending on whether x is negative, zero, or positive<br>
   * The supported signature is<br>
   * BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE -> INTEGER
   */
  private static DefaultFunctionResolver signum() {
    return baseMathFunction(
        BuiltinFunctionName.SIGNUM.getName(),
        v -> new ExprIntegerValue(Math.signum(v.doubleValue())),
        INTEGER);
  }

  /**
   * <b>Definition of sinh(x) function.</b><br>
   * Returns the hyperbolix sine of x, defined as (((e^x) - (e^(-x))) / 2)<br>
   * The supported signature is<br>
   * BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE
   */
  private static DefaultFunctionResolver sinh() {
    return baseMathFunction(
        BuiltinFunctionName.SINH.getName(),
        v -> new ExprDoubleValue(Math.sinh(v.doubleValue())),
        DOUBLE);
  }

  /**
   * <b>Definition of sqrt(x) function.</b><br>
   * Calculate the square root of a non-negative number x<br>
   * The supported signature is<br>
   * INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE
   */
  private static DefaultFunctionResolver sqrt() {
    return baseMathFunction(
        BuiltinFunctionName.SQRT.getName(),
        v ->
            v.doubleValue() < 0
                ? ExprNullValue.of()
                : new ExprDoubleValue(Math.sqrt(v.doubleValue())),
        DOUBLE);
  }

  /**
   * <b>Definition of cbrt(x) function.</b><br>
   * Calculate the cube root of a number x<br>
   * The supported signature is<br>
   * INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE
   */
  private static DefaultFunctionResolver cbrt() {
    return baseMathFunction(
        BuiltinFunctionName.CBRT.getName(),
        v -> new ExprDoubleValue(Math.cbrt(v.doubleValue())),
        DOUBLE);
  }

  /**
   * <b>Definition of truncate(x, d) function.</b><br>
   * Returns the number x, truncated to d decimal places<br>
   * The supported signature of round function is<br>
   * (x: INTEGER, y: INTEGER) -> LONG<br>
   * (x: LONG, y: INTEGER) -> LONG<br>
   * (x: FLOAT, y: INTEGER) -> DOUBLE<br>
   * (x: DOUBLE, y: INTEGER) -> DOUBLE
   */
  private static DefaultFunctionResolver truncate() {
    return define(
        BuiltinFunctionName.TRUNCATE.getName(),
        impl(
            nullMissingHandling(
                (x, y) ->
                    new ExprLongValue(
                        BigDecimal.valueOf(x.integerValue())
                            .setScale(y.integerValue(), RoundingMode.DOWN)
                            .longValue())),
            LONG,
            INTEGER,
            INTEGER),
        impl(
            nullMissingHandling(
                (x, y) ->
                    new ExprLongValue(
                        BigDecimal.valueOf(x.longValue())
                            .setScale(y.integerValue(), RoundingMode.DOWN)
                            .longValue())),
            LONG,
            LONG,
            INTEGER),
        impl(
            nullMissingHandling(
                (x, y) ->
                    new ExprDoubleValue(
                        BigDecimal.valueOf(x.floatValue())
                            .setScale(y.integerValue(), RoundingMode.DOWN)
                            .doubleValue())),
            DOUBLE,
            FLOAT,
            INTEGER),
        impl(
            nullMissingHandling(
                (x, y) ->
                    new ExprDoubleValue(
                        BigDecimal.valueOf(x.doubleValue())
                            .setScale(y.integerValue(), RoundingMode.DOWN)
                            .doubleValue())),
            DOUBLE,
            DOUBLE,
            INTEGER));
  }

  /**
   * <b>Definition of acos(x) function.</b><br>
   * Calculates the arc cosine of x, that is, the value whose cosine is x.<br>
   * Returns NULL if x is not in the range -1 to 1.<br>
   * The supported signature of acos function is<br>
   * INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE
   */
  private static DefaultFunctionResolver acos() {
    return define(
        BuiltinFunctionName.ACOS.getName(),
        ExprCoreType.numberTypes().stream()
            .map(
                type ->
                    impl(
                        nullMissingHandling(
                            v ->
                                v.doubleValue() < -1 || v.doubleValue() > 1
                                    ? ExprNullValue.of()
                                    : new ExprDoubleValue(Math.acos(v.doubleValue()))),
                        DOUBLE,
                        type))
            .collect(Collectors.toList()));
  }

  /**
   * <b>Definition of asin(x) function.</b><br>
   * Calculates the arc sine of x, that is, the value whose sine is x.<br>
   * Returns NULL if x is not in the range -1 to 1.<br>
   * The supported signature of asin function is<br>
   * INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE<br>
   */
  private static DefaultFunctionResolver asin() {
    return define(
        BuiltinFunctionName.ASIN.getName(),
        ExprCoreType.numberTypes().stream()
            .map(
                type ->
                    impl(
                        nullMissingHandling(
                            v ->
                                v.doubleValue() < -1 || v.doubleValue() > 1
                                    ? ExprNullValue.of()
                                    : new ExprDoubleValue(Math.asin(v.doubleValue()))),
                        DOUBLE,
                        type))
            .collect(Collectors.toList()));
  }

  /**
   * <b>Definition of atan(x) and atan(y, x) function.</b><br>
   * atan(x) calculates the arc tangent of x, that is, the value whose tangent is x.<br>
   * atan(y, x) calculates the arc tangent of y / x, except that the signs of both arguments<br>
   * are used to determine the quadrant of the result.<br>
   * The supported signature of atan function is<br>
   * (x: INTEGER/LONG/FLOAT/DOUBLE, y: INTEGER/LONG/FLOAT/DOUBLE) -> DOUBLE
   */
  private static DefaultFunctionResolver atan() {
    ImmutableList.Builder<
            SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>>
        builder = new ImmutableList.Builder<>();

    for (ExprType type : ExprCoreType.numberTypes()) {
      builder.add(
          impl(
              nullMissingHandling(x -> new ExprDoubleValue(Math.atan(x.doubleValue()))),
              type,
              DOUBLE));
      builder.add(
          impl(
              nullMissingHandling(
                  (y, x) -> new ExprDoubleValue(Math.atan2(y.doubleValue(), x.doubleValue()))),
              DOUBLE,
              type,
              type));
    }

    return define(BuiltinFunctionName.ATAN.getName(), builder.build());
  }

  /**
   * <b>Definition of atan2(y, x) function.</b><br>
   * Calculates the arc tangent of y / x, except that the signs of both arguments are used to
   * determine the quadrant of the result.<br>
   * The supported signature of atan2 function is<br>
   * (x: INTEGER/LONG/FLOAT/DOUBLE, y: INTEGER/LONG/FLOAT/DOUBLE) -> DOUBLE
   */
  private static DefaultFunctionResolver atan2() {
    ImmutableList.Builder<
            SerializableFunction<FunctionName, Pair<FunctionSignature, FunctionBuilder>>>
        builder = new ImmutableList.Builder<>();

    for (ExprType type : ExprCoreType.numberTypes()) {
      builder.add(
          impl(
              nullMissingHandling(
                  (y, x) -> new ExprDoubleValue(Math.atan2(y.doubleValue(), x.doubleValue()))),
              DOUBLE,
              type,
              type));
    }

    return define(BuiltinFunctionName.ATAN2.getName(), builder.build());
  }

  /**
   * <b>Definition of cos(x) function.</b><br>
   * Calculates the cosine of X, where X is given in radians<br>
   * The supported signature of cos function is<br>
   * INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE
   */
  private static DefaultFunctionResolver cos() {
    return baseMathFunction(
        BuiltinFunctionName.COS.getName(),
        v -> new ExprDoubleValue(Math.cos(v.doubleValue())),
        DOUBLE);
  }

  /**
   * <b>Definition of cosh(x) function.</b><br>
   * Returns the hyperbolic cosine of x, defined as (((e^x) + (e^(-x))) / 2)<br>
   * The supported signature is<br>
   * BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE
   */
  private static DefaultFunctionResolver cosh() {
    return baseMathFunction(
        BuiltinFunctionName.COSH.getName(),
        v -> new ExprDoubleValue(Math.cosh(v.doubleValue())),
        DOUBLE);
  }

  /**
   * <b>Definition of cot(x) function.<\b><br>
   * Calculates the cotangent of x The supported signature of cot function is
   * INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE
   */
  private static DefaultFunctionResolver cot() {
    return define(
        BuiltinFunctionName.COT.getName(),
        ExprCoreType.numberTypes().stream()
            .map(
                type ->
                    impl(
                        nullMissingHandling(
                            v -> {
                              Double value = v.doubleValue();
                              if (value == 0) {
                                throw new ArithmeticException(
                                    String.format("Out of range value for cot(%s)", value));
                              }
                              return new ExprDoubleValue(1 / Math.tan(value));
                            }),
                        DOUBLE,
                        type))
            .collect(Collectors.toList()));
  }

  /**
   * <b>Definition of degrees(x) function.</b><br>
   * Converts x from radians to degrees The supported signature of degrees function is
   * INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE
   */
  private static DefaultFunctionResolver degrees() {
    return baseMathFunction(
        BuiltinFunctionName.DEGREES.getName(),
        v -> new ExprDoubleValue(Math.toDegrees(v.doubleValue())),
        DOUBLE);
  }

  /**
   * <b>Definition of radians(x) function.</b><br>
   * Converts x from degrees to radians The supported signature of radians function is
   * INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE
   */
  private static DefaultFunctionResolver radians() {
    return baseMathFunction(
        BuiltinFunctionName.RADIANS.getName(),
        v -> new ExprDoubleValue(Math.toRadians(v.doubleValue())),
        DOUBLE);
  }

  /**
   * <b>Definition of sin(x) function.</b><br>
   * Calculates the sine of x, where x is given in radians The supported signature of sin function
   * is INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE
   */
  private static DefaultFunctionResolver sin() {
    return baseMathFunction(
        BuiltinFunctionName.SIN.getName(),
        v -> new ExprDoubleValue(Math.sin(v.doubleValue())),
        DOUBLE);
  }

  /**
   * <b>Definition of tan(x) function.</b><br>
   * Calculates the tangent of x, where x is given in radians The supported signature of tan
   * function is INTEGER/LONG/FLOAT/DOUBLE -> DOUBLE
   */
  private static DefaultFunctionResolver tan() {
    return baseMathFunction(
        BuiltinFunctionName.TAN.getName(),
        v -> new ExprDoubleValue(Math.tan(v.doubleValue())),
        DOUBLE);
  }
}
