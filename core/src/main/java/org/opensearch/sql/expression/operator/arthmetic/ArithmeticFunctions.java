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
import static org.opensearch.sql.expression.function.FunctionDSL.define;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;

import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionName;

/**
 * The definition of arithmetic function<br>
 * add, Accepts two numbers and produces a number.<br>
 * subtract, Accepts two numbers and produces a number.<br>
 * multiply, Accepts two numbers and produces a number.<br>
 * divide, Accepts two numbers and produces a number.<br>
 * module, Accepts two numbers and produces a number.
 */
@UtilityClass
public class ArithmeticFunctions {
  /**
   * Register Arithmetic Function.
   *
   * @param repository {@link BuiltinFunctionRepository}.
   */
  public static void register(BuiltinFunctionRepository repository) {
    repository.register(add());
    repository.register(addFunction());
    repository.register(divide());
    repository.register(divideFunction());
    repository.register(mod());
    repository.register(modulus());
    repository.register(modulusFunction());
    repository.register(multiply());
    repository.register(multiplyFunction());
    repository.register(subtract());
    repository.register(subtractFunction());
  }

  /**
   * Definition of add(x, y) function.<br>
   * Returns the number x plus number y<br>
   * The supported signature of add function is<br>
   * (x: BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE, y: BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE)<br>
   * -> wider type between types of x and y
   */
  private static DefaultFunctionResolver addBase(FunctionName functionName) {
    return define(
        functionName,
        impl(
            nullMissingHandling((v1, v2) -> new ExprByteValue(v1.byteValue() + v2.byteValue())),
            BYTE,
            BYTE,
            BYTE),
        impl(
            nullMissingHandling((v1, v2) -> new ExprShortValue(v1.shortValue() + v2.shortValue())),
            SHORT,
            SHORT,
            SHORT),
        impl(
            nullMissingHandling(
                (v1, v2) ->
                    new ExprIntegerValue(Math.addExact(v1.integerValue(), v2.integerValue()))),
            INTEGER,
            INTEGER,
            INTEGER),
        impl(
            nullMissingHandling(
                (v1, v2) -> new ExprLongValue(Math.addExact(v1.longValue(), v2.longValue()))),
            LONG,
            LONG,
            LONG),
        impl(
            nullMissingHandling((v1, v2) -> new ExprFloatValue(v1.floatValue() + v2.floatValue())),
            FLOAT,
            FLOAT,
            FLOAT),
        impl(
            nullMissingHandling(
                (v1, v2) -> new ExprDoubleValue(v1.doubleValue() + v2.doubleValue())),
            DOUBLE,
            DOUBLE,
            DOUBLE));
  }

  private static DefaultFunctionResolver add() {
    return addBase(BuiltinFunctionName.ADD.getName());
  }

  private static DefaultFunctionResolver addFunction() {
    return addBase(BuiltinFunctionName.ADDFUNCTION.getName());
  }

  /**
   * Definition of divide(x, y) function.<br>
   * Returns the number x divided by number y<br>
   * The supported signature of divide function is<br>
   * (x: BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE, y: BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE)<br>
   * -> wider type between types of x and y
   */
  private static DefaultFunctionResolver divideBase(FunctionName functionName) {
    return define(
        functionName,
        impl(
            nullMissingHandling(
                (v1, v2) ->
                    v2.byteValue() == 0
                        ? ExprNullValue.of()
                        : new ExprByteValue(v1.byteValue() / v2.byteValue())),
            BYTE,
            BYTE,
            BYTE),
        impl(
            nullMissingHandling(
                (v1, v2) ->
                    v2.shortValue() == 0
                        ? ExprNullValue.of()
                        : new ExprShortValue(v1.shortValue() / v2.shortValue())),
            SHORT,
            SHORT,
            SHORT),
        impl(
            nullMissingHandling(
                (v1, v2) ->
                    v2.integerValue() == 0
                        ? ExprNullValue.of()
                        : new ExprIntegerValue(v1.integerValue() / v2.integerValue())),
            INTEGER,
            INTEGER,
            INTEGER),
        impl(
            nullMissingHandling(
                (v1, v2) ->
                    v2.longValue() == 0
                        ? ExprNullValue.of()
                        : new ExprLongValue(v1.longValue() / v2.longValue())),
            LONG,
            LONG,
            LONG),
        impl(
            nullMissingHandling(
                (v1, v2) ->
                    v2.floatValue() == 0
                        ? ExprNullValue.of()
                        : new ExprFloatValue(v1.floatValue() / v2.floatValue())),
            FLOAT,
            FLOAT,
            FLOAT),
        impl(
            nullMissingHandling(
                (v1, v2) ->
                    v2.doubleValue() == 0
                        ? ExprNullValue.of()
                        : new ExprDoubleValue(v1.doubleValue() / v2.doubleValue())),
            DOUBLE,
            DOUBLE,
            DOUBLE));
  }

  private static DefaultFunctionResolver divide() {
    return divideBase(BuiltinFunctionName.DIVIDE.getName());
  }

  private static DefaultFunctionResolver divideFunction() {
    return divideBase(BuiltinFunctionName.DIVIDEFUNCTION.getName());
  }

  /**
   * Definition of modulus(x, y) function.<br>
   * Returns the number x modulo by number y<br>
   * The supported signature of modulo function is<br>
   * (x: BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE, y: BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE)<br>
   * -> wider type between types of x and y
   */
  private static DefaultFunctionResolver modulusBase(FunctionName functionName) {
    return define(
        functionName,
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
                    v2.integerValue() == 0
                        ? ExprNullValue.of()
                        : new ExprIntegerValue(v1.integerValue() % v2.integerValue())),
            INTEGER,
            INTEGER,
            INTEGER),
        impl(
            nullMissingHandling(
                (v1, v2) ->
                    v2.longValue() == 0
                        ? ExprNullValue.of()
                        : new ExprLongValue(v1.longValue() % v2.longValue())),
            LONG,
            LONG,
            LONG),
        impl(
            nullMissingHandling(
                (v1, v2) ->
                    v2.floatValue() == 0
                        ? ExprNullValue.of()
                        : new ExprFloatValue(v1.floatValue() % v2.floatValue())),
            FLOAT,
            FLOAT,
            FLOAT),
        impl(
            nullMissingHandling(
                (v1, v2) ->
                    v2.doubleValue() == 0
                        ? ExprNullValue.of()
                        : new ExprDoubleValue(v1.doubleValue() % v2.doubleValue())),
            DOUBLE,
            DOUBLE,
            DOUBLE));
  }

  private static DefaultFunctionResolver mod() {
    return modulusBase(BuiltinFunctionName.MOD.getName());
  }

  private static DefaultFunctionResolver modulus() {
    return modulusBase(BuiltinFunctionName.MODULUS.getName());
  }

  private static DefaultFunctionResolver modulusFunction() {
    return modulusBase(BuiltinFunctionName.MODULUSFUNCTION.getName());
  }

  /**
   * Definition of multiply(x, y) function.<br>
   * Returns the number x multiplied by number y<br>
   * The supported signature of multiply function is<br>
   * (x: BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE, y: BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE)<br>
   * </> -> wider type between types of x and y
   */
  private static DefaultFunctionResolver multiplyBase(FunctionName functionName) {
    return define(
        functionName,
        impl(
            nullMissingHandling((v1, v2) -> new ExprByteValue(v1.byteValue() * v2.byteValue())),
            BYTE,
            BYTE,
            BYTE),
        impl(
            nullMissingHandling((v1, v2) -> new ExprShortValue(v1.shortValue() * v2.shortValue())),
            SHORT,
            SHORT,
            SHORT),
        impl(
            nullMissingHandling(
                (v1, v2) ->
                    new ExprIntegerValue(Math.multiplyExact(v1.integerValue(), v2.integerValue()))),
            INTEGER,
            INTEGER,
            INTEGER),
        impl(
            nullMissingHandling(
                (v1, v2) -> new ExprLongValue(Math.multiplyExact(v1.longValue(), v2.longValue()))),
            LONG,
            LONG,
            LONG),
        impl(
            nullMissingHandling((v1, v2) -> new ExprFloatValue(v1.floatValue() * v2.floatValue())),
            FLOAT,
            FLOAT,
            FLOAT),
        impl(
            nullMissingHandling(
                (v1, v2) -> new ExprDoubleValue(v1.doubleValue() * v2.doubleValue())),
            DOUBLE,
            DOUBLE,
            DOUBLE));
  }

  private static DefaultFunctionResolver multiply() {
    return multiplyBase(BuiltinFunctionName.MULTIPLY.getName());
  }

  private static DefaultFunctionResolver multiplyFunction() {
    return multiplyBase(BuiltinFunctionName.MULTIPLYFUNCTION.getName());
  }

  /**
   * Definition of subtract(x, y) function.<br>
   * Returns the number x minus number y<br>
   * The supported signature of subtract function is<br>
   * (x: BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE, y: BYTE/SHORT/INTEGER/LONG/FLOAT/DOUBLE)<br>
   * -> wider type between types of x and y
   */
  private static DefaultFunctionResolver subtractBase(FunctionName functionName) {
    return define(
        functionName,
        impl(
            nullMissingHandling((v1, v2) -> new ExprByteValue(v1.byteValue() - v2.byteValue())),
            BYTE,
            BYTE,
            BYTE),
        impl(
            nullMissingHandling((v1, v2) -> new ExprShortValue(v1.shortValue() - v2.shortValue())),
            SHORT,
            SHORT,
            SHORT),
        impl(
            nullMissingHandling(
                (v1, v2) ->
                    new ExprIntegerValue(Math.subtractExact(v1.integerValue(), v2.integerValue()))),
            INTEGER,
            INTEGER,
            INTEGER),
        impl(
            nullMissingHandling(
                (v1, v2) -> new ExprLongValue(Math.subtractExact(v1.longValue(), v2.longValue()))),
            LONG,
            LONG,
            LONG),
        impl(
            nullMissingHandling((v1, v2) -> new ExprFloatValue(v1.floatValue() - v2.floatValue())),
            FLOAT,
            FLOAT,
            FLOAT),
        impl(
            nullMissingHandling(
                (v1, v2) -> new ExprDoubleValue(v1.doubleValue() - v2.doubleValue())),
            DOUBLE,
            DOUBLE,
            DOUBLE));
  }

  private static DefaultFunctionResolver subtract() {
    return subtractBase(BuiltinFunctionName.SUBTRACT.getName());
  }

  private static DefaultFunctionResolver subtractFunction() {
    return subtractBase(BuiltinFunctionName.SUBTRACTFUNCTION.getName());
  }
}
