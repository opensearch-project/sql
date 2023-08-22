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
   * <b>Gets substring starting at given point, for optional given length.</b><br>
   * Form of this function using keywords instead of comma delimited variables is not supported.<br>
   * Supports following signatures:<br>
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

  /**
   * <b>Removes leading whitespace from string.</b><br>
   * Supports following signatures:<br>
   * STRING -> STRING
   */
  private DefaultFunctionResolver ltrim() {
    return define(
        BuiltinFunctionName.LTRIM.getName(),
        impl(
            nullMissingHandling((v) -> new ExprStringValue(v.stringValue().stripLeading())),
            STRING,
            STRING));
  }

  /**
   * <b>Removes trailing whitespace from string.</b><br>
   * Supports following signatures:<br>
   * STRING -> STRING
   */
  private DefaultFunctionResolver rtrim() {
    return define(
        BuiltinFunctionName.RTRIM.getName(),
        impl(
            nullMissingHandling((v) -> new ExprStringValue(v.stringValue().stripTrailing())),
            STRING,
            STRING));
  }

  /**
   * <b>Removes leading and trailing whitespace from string.</b><br>
   * Has option to specify a String to trim instead of whitespace but this is not yet supported.<br>
   * Supporting String specification requires finding keywords inside TRIM command.<br>
   * Supports following signatures:<br>
   * STRING -> STRING
   */
  private DefaultFunctionResolver trim() {
    return define(
        BuiltinFunctionName.TRIM.getName(),
        impl(
            nullMissingHandling((v) -> new ExprStringValue(v.stringValue().trim())),
            STRING,
            STRING));
  }

  /**
   * <b>Converts String to lowercase.</b><br>
   * Supports following signatures:<br>
   * STRING -> STRING
   */
  private DefaultFunctionResolver lower() {
    return define(
        BuiltinFunctionName.LOWER.getName(),
        impl(
            nullMissingHandling((v) -> new ExprStringValue((v.stringValue().toLowerCase()))),
            STRING,
            STRING));
  }

  /**
   * <b>Converts String to uppercase.</b><br>
   * Supports following signatures:<br>
   * STRING -> STRING
   */
  private DefaultFunctionResolver upper() {
    return define(
        BuiltinFunctionName.UPPER.getName(),
        impl(
            nullMissingHandling((v) -> new ExprStringValue((v.stringValue().toUpperCase()))),
            STRING,
            STRING));
  }

  /**
   * <b>Concatenates a list of Strings.</b><br>
   * Supports following signatures:<br>
   * (STRING, STRING, ...., STRING) -> STRING
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
   * TODO: https://github.com/opendistro-for-elasticsearch/sql/issues/710<br>
   * Extend to accept variable argument amounts.<br>
   * <br>
   * Concatenates a list of Strings with a separator string. Supports following<br>
   * signatures:<br>
   * (STRING, STRING, STRING) -> STRING
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

  /**
   * <b>Calculates length of String in bytes.</b><br>
   * Supports following signatures:<br>
   * STRING -> INTEGER
   */
  private DefaultFunctionResolver length() {
    return define(
        BuiltinFunctionName.LENGTH.getName(),
        impl(
            nullMissingHandling((str) -> new ExprIntegerValue(str.stringValue().getBytes().length)),
            INTEGER,
            STRING));
  }

  /**
   * <b>Does String comparison of two Strings and returns Integer value.</b><br>
   * Supports following signatures:<br>
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
   * <b>Returns the rightmost len characters from the string str, or NULL if any argument is
   * NULL.</b><br>
   * Supports following signatures:<br>
   * (STRING, INTEGER) -> STRING
   */
  private DefaultFunctionResolver right() {
    return define(
        BuiltinFunctionName.RIGHT.getName(),
        impl(nullMissingHandling(TextFunction::exprRight), STRING, STRING, INTEGER));
  }

  /**
   * <b>Returns the leftmost len characters from the string str, or NULL if any argument is
   * NULL.</b><br>
   * Supports following signature:<br>
   * (STRING, INTEGER) -> STRING
   */
  private DefaultFunctionResolver left() {
    return define(
        BuiltinFunctionName.LEFT.getName(),
        impl(nullMissingHandling(TextFunction::exprLeft), STRING, STRING, INTEGER));
  }

  /**
   * <b>Returns the numeric value of the leftmost character of the string str.</b><br>
   * Returns 0 if str is the empty string. Returns NULL if str is NULL.<br>
   * ASCII() works for 8-bit characters.<br>
   * Supports following signature:<br>
   * STRING -> INTEGER
   */
  private DefaultFunctionResolver ascii() {
    return define(
        BuiltinFunctionName.ASCII.getName(),
        impl(nullMissingHandling(TextFunction::exprAscii), INTEGER, STRING));
  }

  /**
   * <b>LOCATE(substr, str) returns the position of the first occurrence of substring substr</b><br>
   * in string str. LOCATE(substr, str, pos) returns the position of the first occurrence<br>
   * of substring substr in string str, starting at position pos.<br>
   * Returns 0 if substr is not in str.<br>
   * Returns NULL if any argument is NULL.<br>
   * Supports following signature:<br>
   * (STRING, STRING) -> INTEGER<br>
   * (STRING, STRING, INTEGER) -> INTEGER
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
   * <b>Returns the position of the first occurrence of a substring in a string starting from 1.</b>
   * <br>
   * Returns 0 if substring is not in string.<br>
   * Returns NULL if any argument is NULL.<br>
   * Supports following signature:<br>
   * (STRING IN STRING) -> INTEGER
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
   * <b>REPLACE(str, from_str, to_str) returns the string str with all occurrences of<br>
   * the string from_str replaced by the string to_str.</b><br>
   * REPLACE() performs a case-sensitive match when searching for from_str.<br>
   * Supports following signature:<br>
   * (STRING, STRING, STRING) -> STRING
   */
  private DefaultFunctionResolver replace() {
    return define(
        BuiltinFunctionName.REPLACE.getName(),
        impl(nullMissingHandling(TextFunction::exprReplace), STRING, STRING, STRING, STRING));
  }

  /**
   * <b>REVERSE(str) returns reversed string of the string supplied as an argument</b></b><br>
   * Returns NULL if the argument is NULL.<br>
   * Supports the following signature:<br>
   * (STRING) -> STRING
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
