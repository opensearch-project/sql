/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.text;

import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.function.FunctionDSL.define;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.expression.function.SerializableBiFunction;
import org.opensearch.sql.expression.function.SerializableTriFunction;

/**
 * The definition of text functions. 1) have the clear interface for function define. 2) the
 * implementation should rely on ExprValue.
 */
@UtilityClass
public class TextFunction {
  private static String EMPTY_STRING = "";

  /**
   * Register String Functions.
   *
   * @param repository {@link BuiltinFunctionRepository}.
   */
  public void register(BuiltinFunctionRepository repository) {
    repository.register(ascii());
    repository.register(concat());
    repository.register(concat_ws());
    repository.register(left());
    repository.register(length());
    repository.register(locate());
    repository.register(lower());
    repository.register(ltrim());
    repository.register(position());
    repository.register(replace());
    repository.register(reverse());
    repository.register(right());
    repository.register(rtrim());
    repository.register(strcmp());
    repository.register(substr());
    repository.register(substring());
    repository.register(trim());
    repository.register(upper());
  }

  /**
   * Gets substring starting at given point, for optional given length. Form of this function using
   * keywords instead of comma delimited variables is not supported. Supports following signatures:
   * (STRING, INTEGER)/(STRING, INTEGER, INTEGER) -> STRING
   */
  private DefaultFunctionResolver substringSubstr(FunctionName functionName) {
    return define(
        functionName,
        impl(nullMissingHandling(TextFunction::exprSubstrStart), STRING, STRING, INTEGER),
        impl(
            nullMissingHandling(TextFunction::exprSubstrStartLength),
            STRING,
            STRING,
            INTEGER,
            INTEGER));
  }

  private DefaultFunctionResolver substring() {
    return substringSubstr(BuiltinFunctionName.SUBSTRING.getName());
  }

  private DefaultFunctionResolver substr() {
    return substringSubstr(BuiltinFunctionName.SUBSTR.getName());
  }

  /** Removes leading whitespace from string. Supports following signatures: STRING -> STRING */
  private DefaultFunctionResolver ltrim() {
    return define(
        BuiltinFunctionName.LTRIM.getName(),
        impl(
            nullMissingHandling((v) -> new ExprStringValue(v.stringValue().stripLeading())),
            STRING,
            STRING));
  }

  /** Removes trailing whitespace from string. Supports following signatures: STRING -> STRING */
  private DefaultFunctionResolver rtrim() {
    return define(
        BuiltinFunctionName.RTRIM.getName(),
        impl(
            nullMissingHandling((v) -> new ExprStringValue(v.stringValue().stripTrailing())),
            STRING,
            STRING));
  }

  /**
   * Removes leading and trailing whitespace from string. Has option to specify a String to trim
   * instead of whitespace but this is not yet supported. Supporting String specification requires
   * finding keywords inside TRIM command. Supports following signatures: STRING -> STRING
   */
  private DefaultFunctionResolver trim() {
    return define(
        BuiltinFunctionName.TRIM.getName(),
        impl(
            nullMissingHandling((v) -> new ExprStringValue(v.stringValue().trim())),
            STRING,
            STRING));
  }

  /** Converts String to lowercase. Supports following signatures: STRING -> STRING */
  private DefaultFunctionResolver lower() {
    return define(
        BuiltinFunctionName.LOWER.getName(),
        impl(
            nullMissingHandling((v) -> new ExprStringValue((v.stringValue().toLowerCase()))),
            STRING,
            STRING));
  }

  /** Converts String to uppercase. Supports following signatures: STRING -> STRING */
  private DefaultFunctionResolver upper() {
    return define(
        BuiltinFunctionName.UPPER.getName(),
        impl(
            nullMissingHandling((v) -> new ExprStringValue((v.stringValue().toUpperCase()))),
            STRING,
            STRING));
  }

  /**
   * Concatenates a list of Strings. Supports following signatures: (STRING, STRING, ...., STRING)
   * -> STRING
   */
  private DefaultFunctionResolver concat() {
    FunctionName concatFuncName = BuiltinFunctionName.CONCAT.getName();
    return define(
        concatFuncName,
        funcName ->
            Pair.of(
                new FunctionSignature(concatFuncName, Collections.singletonList(ARRAY)),
                (funcProp, args) ->
                    new FunctionExpression(funcName, args) {
                      @Override
                      public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
                        List<ExprValue> exprValues =
                            args.stream()
                                .map(arg -> arg.valueOf(valueEnv))
                                .collect(Collectors.toList());
                        if (exprValues.stream().anyMatch(ExprValue::isMissing)) {
                          return ExprValueUtils.missingValue();
                        }
                        if (exprValues.stream().anyMatch(ExprValue::isNull)) {
                          return ExprValueUtils.nullValue();
                        }
                        return new ExprStringValue(
                            exprValues.stream()
                                .map(ExprValue::stringValue)
                                .collect(Collectors.joining()));
                      }

                      @Override
                      public ExprType type() {
                        return STRING;
                      }
                    }));
  }

  /**
   * TODO: https://github.com/opendistro-for-elasticsearch/sql/issues/710 Extend to accept variable
   * argument amounts. Concatenates a list of Strings with a separator string. Supports following
   * signatures: (STRING, STRING, STRING) -> STRING
   */
  private DefaultFunctionResolver concat_ws() {
    return define(
        BuiltinFunctionName.CONCAT_WS.getName(),
        impl(
            nullMissingHandling(
                (sep, str1, str2) ->
                    new ExprStringValue(
                        str1.stringValue() + sep.stringValue() + str2.stringValue())),
            STRING,
            STRING,
            STRING,
            STRING));
  }

  /** Calculates length of String in bytes. Supports following signatures: STRING -> INTEGER */
  private DefaultFunctionResolver length() {
    return define(
        BuiltinFunctionName.LENGTH.getName(),
        impl(
            nullMissingHandling((str) -> new ExprIntegerValue(str.stringValue().getBytes().length)),
            INTEGER,
            STRING));
  }

  /**
   * Does String comparison of two Strings and returns Integer value. Supports following signatures:
   * (STRING, STRING) -> INTEGER
   */
  private DefaultFunctionResolver strcmp() {
    return define(
        BuiltinFunctionName.STRCMP.getName(),
        impl(
            nullMissingHandling(
                (str1, str2) ->
                    new ExprIntegerValue(
                        Integer.compare(str1.stringValue().compareTo(str2.stringValue()), 0))),
            INTEGER,
            STRING,
            STRING));
  }

  /**
   * Returns the rightmost len characters from the string str, or NULL if any argument is NULL.
   * Supports following signatures: (STRING, INTEGER) -> STRING
   */
  private DefaultFunctionResolver right() {
    return define(
        BuiltinFunctionName.RIGHT.getName(),
        impl(nullMissingHandling(TextFunction::exprRight), STRING, STRING, INTEGER));
  }

  /**
   * Returns the leftmost len characters from the string str, or NULL if any argument is NULL.
   * Supports following signature: (STRING, INTEGER) -> STRING
   */
  private DefaultFunctionResolver left() {
    return define(
        BuiltinFunctionName.LEFT.getName(),
        impl(nullMissingHandling(TextFunction::exprLeft), STRING, STRING, INTEGER));
  }

  /**
   * Returns the numeric value of the leftmost character of the string str. Returns 0 if str is the
   * empty string. Returns NULL if str is NULL. ASCII() works for 8-bit characters. Supports
   * following signature: STRING -> INTEGER
   */
  private DefaultFunctionResolver ascii() {
    return define(
        BuiltinFunctionName.ASCII.getName(),
        impl(nullMissingHandling(TextFunction::exprAscii), INTEGER, STRING));
  }

  /**
   * LOCATE(substr, str) returns the position of the first occurrence of substring substr in string
   * str. LOCATE(substr, str, pos) returns the position of the first occurrence of substring substr
   * in string str, starting at position pos. Returns 0 if substr is not in str. Returns NULL if any
   * argument is NULL. Supports following signature: (STRING, STRING) -> INTEGER (STRING, STRING,
   * INTEGER) -> INTEGER
   */
  private DefaultFunctionResolver locate() {
    return define(
        BuiltinFunctionName.LOCATE.getName(),
        impl(
            nullMissingHandling(
                (SerializableBiFunction<ExprValue, ExprValue, ExprValue>) TextFunction::exprLocate),
            INTEGER,
            STRING,
            STRING),
        impl(
            nullMissingHandling(
                (SerializableTriFunction<ExprValue, ExprValue, ExprValue, ExprValue>)
                    TextFunction::exprLocate),
            INTEGER,
            STRING,
            STRING,
            INTEGER));
  }

  /**
   * Returns the position of the first occurrence of a substring in a string starting from 1.
   * Returns 0 if substring is not in string. Returns NULL if any argument is NULL. Supports
   * following signature: (STRING IN STRING) -> INTEGER
   */
  private DefaultFunctionResolver position() {
    return define(
        BuiltinFunctionName.POSITION.getName(),
        impl(
            nullMissingHandling(
                (SerializableBiFunction<ExprValue, ExprValue, ExprValue>) TextFunction::exprLocate),
            INTEGER,
            STRING,
            STRING));
  }

  /**
   * REPLACE(str, from_str, to_str) returns the string str with all occurrences of the string
   * from_str replaced by the string to_str. REPLACE() performs a case-sensitive match when
   * searching for from_str. Supports following signature: (STRING, STRING, STRING) -> STRING
   */
  private DefaultFunctionResolver replace() {
    return define(
        BuiltinFunctionName.REPLACE.getName(),
        impl(nullMissingHandling(TextFunction::exprReplace), STRING, STRING, STRING, STRING));
  }

  /**
   * REVERSE(str) returns reversed string of the string supplied as an argument Returns NULL if the
   * argument is NULL. Supports the following signature: (STRING) -> STRING
   */
  private DefaultFunctionResolver reverse() {
    return define(
        BuiltinFunctionName.REVERSE.getName(),
        impl(nullMissingHandling(TextFunction::exprReverse), STRING, STRING));
  }

  private static ExprValue exprSubstrStart(ExprValue exprValue, ExprValue start) {
    int startIdx = start.integerValue();
    if (startIdx == 0) {
      return new ExprStringValue(EMPTY_STRING);
    }
    String str = exprValue.stringValue();
    return exprSubStr(str, startIdx, str.length());
  }

  private static ExprValue exprSubstrStartLength(
      ExprValue exprValue, ExprValue start, ExprValue length) {
    int startIdx = start.integerValue();
    int len = length.integerValue();
    if ((startIdx == 0) || (len == 0)) {
      return new ExprStringValue(EMPTY_STRING);
    }
    String str = exprValue.stringValue();
    return exprSubStr(str, startIdx, len);
  }

  private static ExprValue exprSubStr(String str, int start, int len) {
    // Correct negative start
    start = (start > 0) ? (start - 1) : (str.length() + start);

    if (start > str.length()) {
      return new ExprStringValue(EMPTY_STRING);
    } else if ((start + len) > str.length()) {
      return new ExprStringValue(str.substring(start));
    }
    return new ExprStringValue(str.substring(start, start + len));
  }

  private static ExprValue exprRight(ExprValue str, ExprValue len) {
    if (len.integerValue() <= 0) {
      return new ExprStringValue("");
    }
    String stringValue = str.stringValue();
    int left = Math.max(stringValue.length() - len.integerValue(), 0);
    return new ExprStringValue(str.stringValue().substring(left));
  }

  private static ExprValue exprLeft(ExprValue expr, ExprValue length) {
    String stringValue = expr.stringValue();
    int right = length.integerValue();
    return new ExprStringValue(stringValue.substring(0, Math.min(right, stringValue.length())));
  }

  private static ExprValue exprAscii(ExprValue expr) {
    return new ExprIntegerValue((int) expr.stringValue().charAt(0));
  }

  private static ExprValue exprLocate(ExprValue subStr, ExprValue str) {
    return new ExprIntegerValue(str.stringValue().indexOf(subStr.stringValue()) + 1);
  }

  private static ExprValue exprLocate(ExprValue subStr, ExprValue str, ExprValue pos) {
    return new ExprIntegerValue(
        str.stringValue().indexOf(subStr.stringValue(), pos.integerValue() - 1) + 1);
  }

  private static ExprValue exprReplace(ExprValue str, ExprValue from, ExprValue to) {
    return new ExprStringValue(str.stringValue().replaceAll(from.stringValue(), to.stringValue()));
  }

  private static ExprValue exprReverse(ExprValue str) {
    return new ExprStringValue(new StringBuilder(str.stringValue()).reverse().toString());
  }
}
