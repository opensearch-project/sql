/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.text;

import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.function.FunctionDSL.define;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;

import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Match;
import java.util.Map;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.expression.function.SerializableBiFunction;
import org.opensearch.sql.expression.function.SerializableTriFunction;


/**
 * The definition of text functions.
 * 1) have the clear interface for function define.
 * 2) the implementation should rely on ExprValue.
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
    repository.register(substr());
    repository.register(substring());
    repository.register(ltrim());
    repository.register(rtrim());
    repository.register(trim());
    repository.register(lower());
    repository.register(upper());
    repository.register(concat());
    repository.register(concat_ws());
    repository.register(length());
    repository.register(strcmp());
    repository.register(right());
    repository.register(left());
    repository.register(ascii());
    repository.register(locate());
    repository.register(regex());
    repository.register(replace());
  }

  /**
   * Gets substring starting at given point, for optional given length.
   * Form of this function using keywords instead of comma delimited variables is not supported.
   * Supports following signatures:
   * (STRING, INTEGER)/(STRING, INTEGER, INTEGER) -> STRING
   */
  private FunctionResolver substringSubstr(FunctionName functionName) {
    return define(functionName,
            impl(nullMissingHandling(TextFunction::exprSubstrStart),
                    STRING, STRING, INTEGER),
            impl(nullMissingHandling(TextFunction::exprSubstrStartLength),
                    STRING, STRING, INTEGER, INTEGER));
  }

  private FunctionResolver substring() {
    return substringSubstr(BuiltinFunctionName.SUBSTRING.getName());
  }

  private FunctionResolver substr() {
    return substringSubstr(BuiltinFunctionName.SUBSTR.getName());
  }

  /**
   * Removes leading whitespace from string.
   * Supports following signatures:
   * STRING -> STRING
   */
  private FunctionResolver ltrim() {
    return define(BuiltinFunctionName.LTRIM.getName(),
        impl(nullMissingHandling((v) -> new ExprStringValue(v.stringValue().stripLeading())),
            STRING, STRING));
  }

  /**
   * Removes trailing whitespace from string.
   * Supports following signatures:
   * STRING -> STRING
   */
  private FunctionResolver rtrim() {
    return define(BuiltinFunctionName.RTRIM.getName(),
        impl(nullMissingHandling((v) -> new ExprStringValue(v.stringValue().stripTrailing())),
                STRING, STRING));
  }

  /**
   * Removes leading and trailing whitespace from string.
   * Has option to specify a String to trim instead of whitespace but this is not yet supported.
   * Supporting String specification requires finding keywords inside TRIM command.
   * Supports following signatures:
   * STRING -> STRING
   */
  private FunctionResolver trim() {
    return define(BuiltinFunctionName.TRIM.getName(),
        impl(nullMissingHandling((v) -> new ExprStringValue(v.stringValue().trim())),
            STRING, STRING));
  }

  /**
   * Converts String to lowercase.
   * Supports following signatures:
   * STRING -> STRING
   */
  private FunctionResolver lower() {
    return define(BuiltinFunctionName.LOWER.getName(),
        impl(nullMissingHandling((v) -> new ExprStringValue((v.stringValue().toLowerCase()))),
            STRING, STRING)
    );
  }

  /**
   * Converts String to uppercase.
   * Supports following signatures:
   * STRING -> STRING
   */
  private FunctionResolver upper() {
    return define(BuiltinFunctionName.UPPER.getName(),
        impl(nullMissingHandling((v) -> new ExprStringValue((v.stringValue().toUpperCase()))),
            STRING, STRING)
    );
  }

  /**
   * TODO: https://github.com/opendistro-for-elasticsearch/sql/issues/710
   *  Extend to accept variable argument amounts.
   * Concatenates a list of Strings.
   * Supports following signatures:
   * (STRING, STRING) -> STRING
   */
  private FunctionResolver concat() {
    return define(BuiltinFunctionName.CONCAT.getName(),
        impl(nullMissingHandling((str1, str2) ->
            new ExprStringValue(str1.stringValue() + str2.stringValue())), STRING, STRING, STRING));
  }

  /**
   * TODO: https://github.com/opendistro-for-elasticsearch/sql/issues/710
   *  Extend to accept variable argument amounts.
   * Concatenates a list of Strings with a separator string.
   * Supports following signatures:
   * (STRING, STRING, STRING) -> STRING
   */
  private FunctionResolver concat_ws() {
    return define(BuiltinFunctionName.CONCAT_WS.getName(),
        impl(nullMissingHandling((sep, str1, str2) ->
            new ExprStringValue(str1.stringValue() + sep.stringValue() + str2.stringValue())),
                STRING, STRING, STRING, STRING));
  }

  /**
   * Calculates length of String in bytes.
   * Supports following signatures:
   * STRING -> INTEGER
   */
  private FunctionResolver length() {
    return define(BuiltinFunctionName.LENGTH.getName(),
        impl(nullMissingHandling((str) ->
            new ExprIntegerValue(str.stringValue().getBytes().length)), INTEGER, STRING));
  }

  /**
   * Does String comparison of two Strings and returns Integer value.
   * Supports following signatures:
   * (STRING, STRING) -> INTEGER
   */
  private FunctionResolver strcmp() {
    return define(BuiltinFunctionName.STRCMP.getName(),
        impl(nullMissingHandling((str1, str2) ->
            new ExprIntegerValue(Integer.compare(
                str1.stringValue().compareTo(str2.stringValue()), 0))),
            INTEGER, STRING, STRING));
  }

  /**
   * Returns the rightmost len characters from the string str, or NULL if any argument is NULL.
   * Supports following signatures:
   * (STRING, INTEGER) -> STRING
   */
  private FunctionResolver right() {
    return define(BuiltinFunctionName.RIGHT.getName(),
            impl(nullMissingHandling(TextFunction::exprRight), STRING, STRING, INTEGER));
  }

  /**
   * Returns the leftmost len characters from the string str, or NULL if any argument is NULL.
   * Supports following signature:
   * (STRING, INTEGER) -> STRING
   */
  private FunctionResolver left() {
    return define(BuiltinFunctionName.LEFT.getName(),
        impl(nullMissingHandling(TextFunction::exprLeft), STRING, STRING, INTEGER));
  }

  /**
   * Returns the numeric value of the leftmost character of the string str.
   * Returns 0 if str is the empty string. Returns NULL if str is NULL.
   * ASCII() works for 8-bit characters.
   * Supports following signature:
   * STRING -> INTEGER
   */
  private FunctionResolver ascii() {
    return define(BuiltinFunctionName.ASCII.getName(),
        impl(nullMissingHandling(TextFunction::exprAscii), INTEGER, STRING));
  }

  /**
   * LOCATE(substr, str) returns the position of the first occurrence of substring substr
   * in string str. LOCATE(substr, str, pos) returns the position of the first occurrence
   * of substring substr in string str, starting at position pos.
   * Returns 0 if substr is not in str.
   * Returns NULL if any argument is NULL.
   * Supports following signature:
   * (STRING, STRING) -> INTEGER
   * (STRING, STRING, INTEGER) -> INTEGER
   */
  private FunctionResolver locate() {
    return define(BuiltinFunctionName.LOCATE.getName(),
        impl(nullMissingHandling(
            (SerializableBiFunction<ExprValue, ExprValue, ExprValue>)
                TextFunction::exprLocate), INTEGER, STRING, STRING),
        impl(nullMissingHandling(
            (SerializableTriFunction<ExprValue, ExprValue, ExprValue, ExprValue>)
                TextFunction::exprLocate), INTEGER, STRING, STRING, INTEGER));
  }

  /**
   * REPLACE(str, from_str, to_str) returns the string str with all occurrences of
   * the string from_str replaced by the string to_str.
   * REPLACE() performs a case-sensitive match when searching for from_str.
   * Supports following signature:
   * (STRING, STRING, STRING) -> STRING
   */
  private FunctionResolver replace() {
    return define(BuiltinFunctionName.REPLACE.getName(),
        impl(nullMissingHandling(TextFunction::exprReplace), STRING, STRING, STRING, STRING));
  }

  private FunctionResolver regex() {
    return define(BuiltinFunctionName.REGEX.getName(),
        impl(nullMissingHandling(TextFunction::exprRegex), STRING, STRING, STRING, STRING));
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

  static int n = 1;

  private static ExprValue exprRegex(ExprValue str, ExprValue pattern, ExprValue group) {
    System.out.println(" ❗n: " + n++);
    String rawText = str.stringValue();
    String rawPattern = pattern.stringValue();
    String targetGroup = group.stringValue();

//    Map<String, String> groups = new HashMap<>();
//    Matcher m = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*?)>").matcher(rawPattern);
//    while (m.find()) {
//      groups.put(m.group(1), "");
//    }
//
//    Matcher matcher = Pattern.compile(rawPattern).matcher(str.stringValue());
//    Map<String, ExprValue> exprValueMap = new LinkedHashMap<>();
//    if (matcher.matches()) {
//      groups.forEach((field, rawType) -> {
//        String rawMatch = matcher.group(field);
//        exprValueMap.put(field, new ExprStringValue(rawMatch));
//      });
//    }
//    return exprValueMap.get(targetGroup);

    GrokCompiler grokCompiler = GrokCompiler.newInstance();
    grokCompiler.registerDefaultPatterns();

    /* Grok pattern to compile, here httpd logs */
    final Grok grok = grokCompiler.compile(rawPattern);
    Match gm = grok.match(rawText);

    /* Get the map with matches */
    final Map<String, Object> capture = gm.capture();
    System.out.println(" ❗capture: " + capture);
    return new ExprStringValue(capture.get(targetGroup).toString());
  }

  private static ExprValue exprReplace(ExprValue str, ExprValue from, ExprValue to) {
    return new ExprStringValue(str.stringValue().replaceAll(from.stringValue(), to.stringValue()));
  }
}

