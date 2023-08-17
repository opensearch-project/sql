/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.text;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.missingValue;
import static org.opensearch.sql.data.model.ExprValueUtils.nullValue;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;

public class TextFunctionTest extends ExpressionTestBase {

  private static Stream<SubstringInfo> getStringsForSubstr() {
    return Stream.of(
        new SubstringInfo("", 1, 1, ""),
        new SubstringInfo("Quadratically", 5, null, "ratically"),
        new SubstringInfo("foobarbar", 4, null, "barbar"),
        new SubstringInfo("Quadratically", 5, 6, "ratica"),
        new SubstringInfo("Quadratically", 5, 600, "ratically"),
        new SubstringInfo("Quadratically", 500, 1, ""),
        new SubstringInfo("Quadratically", 500, null, ""),
        new SubstringInfo("Sakila", -3, null, "ila"),
        new SubstringInfo("Sakila", -5, 3, "aki"),
        new SubstringInfo("Sakila", -4, 2, "ki"),
        new SubstringInfo("Quadratically", 0, null, ""),
        new SubstringInfo("Sakila", 0, 2, ""),
        new SubstringInfo("Sakila", 2, 0, ""),
        new SubstringInfo("Sakila", 0, 0, ""));
  }

  private static Stream<String> getStringsForUpperAndLower() {
    return Stream.of(
        "test", " test", "test ", " test ", "TesT", "TEST", " TEST", "TEST ", " TEST ", " ", "");
  }

  private static Stream<StringPatternPair> getStringsForComparison() {
    return Stream.of(
        new StringPatternPair("Michael!", "Michael!"),
        new StringPatternPair("hello", "world"),
        new StringPatternPair("world", "hello"));
  }

  private static Stream<String> getStringsForTrim() {
    return Stream.of(" test", "     test", "test     ", "test", "     test    ", "", " ");
  }

  private static Stream<List<String>> getStringsForConcat() {
    return Stream.of(ImmutableList.of("hello", "world"), ImmutableList.of("123", "5325"));
  }

  private static Stream<List<String>> getMultipleStringsForConcat() {
    return Stream.of(
        ImmutableList.of("he", "llo", "wo", "rld", "!"),
        ImmutableList.of("0", "123", "53", "25", "7"));
  }

  interface SubstrSubstring {
    FunctionExpression getFunction(SubstringInfo strInfo);
  }

  class Substr implements SubstrSubstring {
    public FunctionExpression getFunction(SubstringInfo strInfo) {
      FunctionExpression expr;
      if (strInfo.getLen() == null) {
        expr = DSL.substr(DSL.literal(strInfo.getExpr()), DSL.literal(strInfo.getStart()));
      } else {
        expr =
            DSL.substr(
                DSL.literal(strInfo.getExpr()),
                DSL.literal(strInfo.getStart()),
                DSL.literal(strInfo.getLen()));
      }
      return expr;
    }
  }

  class Substring implements SubstrSubstring {
    public FunctionExpression getFunction(SubstringInfo strInfo) {
      FunctionExpression expr;
      if (strInfo.getLen() == null) {
        expr = DSL.substring(DSL.literal(strInfo.getExpr()), DSL.literal(strInfo.getStart()));
      } else {
        expr =
            DSL.substring(
                DSL.literal(strInfo.getExpr()),
                DSL.literal(strInfo.getStart()),
                DSL.literal(strInfo.getLen()));
      }
      return expr;
    }
  }

  @AllArgsConstructor
  @Getter
  static class StringPatternPair {
    private final String str;
    private final String patt;

    int strCmpTest() {
      return Integer.compare(str.compareTo(patt), 0);
    }
  }

  @AllArgsConstructor
  @Getter
  static class SubstringInfo {
    String expr;
    Integer start;
    Integer len;
    String res;
  }

  @ParameterizedTest
  @MethodSource("getStringsForSubstr")
  void substrSubstring(SubstringInfo s) {
    substrSubstringTest(s, new Substr());
    substrSubstringTest(s, new Substring());
  }

  void substrSubstringTest(SubstringInfo strInfo, SubstrSubstring substrSubstring) {
    FunctionExpression expr = substrSubstring.getFunction(strInfo);
    assertEquals(STRING, expr.type());
    assertEquals(strInfo.getRes(), eval(expr).stringValue());
  }

  @ParameterizedTest
  @MethodSource("getStringsForTrim")
  void ltrim(String str) {
    FunctionExpression expression = DSL.ltrim(DSL.literal(str));
    assertEquals(STRING, expression.type());
    assertEquals(str.stripLeading(), eval(expression).stringValue());
  }

  @ParameterizedTest
  @MethodSource("getStringsForTrim")
  void rtrim(String str) {
    FunctionExpression expression = DSL.rtrim(DSL.literal(str));
    assertEquals(STRING, expression.type());
    assertEquals(str.stripTrailing(), eval(expression).stringValue());
  }

  @ParameterizedTest
  @MethodSource("getStringsForTrim")
  void trim(String str) {
    FunctionExpression expression = DSL.trim(DSL.literal(str));
    assertEquals(STRING, expression.type());
    assertEquals(str.trim(), eval(expression).stringValue());
  }

  @ParameterizedTest
  @MethodSource("getStringsForConcat")
  void concat(List<String> strings) {
    testConcatString(strings);

    // Since `concat` isn't wrapped with `nullMissingHandling` (which has its own tests),
    // we have to test there case with NULL and MISSING values
    Expression nullRef = mock(Expression.class);
    Expression missingRef = mock(Expression.class);
    when(nullRef.valueOf(any())).thenReturn(nullValue());
    when(missingRef.valueOf(any())).thenReturn(missingValue());
    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(DSL.concat(missingRef, DSL.literal("1"))));
    // If any of the expressions is a NULL value, it returns NULL.
    assertEquals(nullValue(), eval(DSL.concat(nullRef, DSL.literal("1"))));
    assertEquals(missingValue(), eval(DSL.concat(DSL.literal("1"), missingRef)));
    assertEquals(nullValue(), eval(DSL.concat(DSL.literal("1"), nullRef)));
  }

  @ParameterizedTest
  @MethodSource("getStringsForConcat")
  void concat_ws(List<String> strings) {
    testConcatString(strings, ",");
  }

  @Test
  void right() {
    FunctionExpression expression =
        DSL.right(
            DSL.literal(new ExprStringValue("foobarbar")), DSL.literal(new ExprIntegerValue(4)));
    assertEquals(STRING, expression.type());
    assertEquals("rbar", eval(expression).stringValue());

    expression = DSL.right(DSL.literal("foo"), DSL.literal(10));
    assertEquals(STRING, expression.type());
    assertEquals("foo", eval(expression).value());

    expression = DSL.right(DSL.literal("foo"), DSL.literal(0));
    assertEquals(STRING, expression.type());
    assertEquals("", eval(expression).value());

    expression = DSL.right(DSL.literal(""), DSL.literal(10));
    assertEquals(STRING, expression.type());
    assertEquals("", eval(expression).value());
  }

  @Test
  void left() {
    FunctionExpression expression =
        DSL.left(
            DSL.literal(new ExprStringValue("helloworld")), DSL.literal(new ExprIntegerValue(5)));
    assertEquals(STRING, expression.type());
    assertEquals("hello", eval(expression).stringValue());

    expression = DSL.left(DSL.literal("hello"), DSL.literal(10));
    assertEquals(STRING, expression.type());
    assertEquals("hello", eval(expression).value());

    expression = DSL.left(DSL.literal("hello"), DSL.literal(0));
    assertEquals(STRING, expression.type());
    assertEquals("", eval(expression).value());

    expression = DSL.left(DSL.literal(""), DSL.literal(10));
    assertEquals(STRING, expression.type());
    assertEquals("", eval(expression).value());
  }

  @Test
  void ascii() {
    FunctionExpression expression = DSL.ascii(DSL.literal(new ExprStringValue("hello")));
    assertEquals(INTEGER, expression.type());
    assertEquals(104, eval(expression).integerValue());
    assertEquals(0, DSL.ascii(DSL.literal("")).valueOf().integerValue());
  }

  @Test
  void locate() {
    FunctionExpression expression = DSL.locate(DSL.literal("world"), DSL.literal("helloworld"));
    assertEquals(INTEGER, expression.type());
    assertEquals(6, eval(expression).integerValue());

    expression = DSL.locate(DSL.literal("world"), DSL.literal("helloworldworld"), DSL.literal(7));
    assertEquals(INTEGER, expression.type());
    assertEquals(11, eval(expression).integerValue());
  }

  @Test
  void position() {
    FunctionExpression expression =
        DSL.position(DSL.literal("world"), DSL.literal("helloworldworld"));
    assertEquals(INTEGER, expression.type());
    assertEquals(6, eval(expression).integerValue());

    expression = DSL.position(DSL.literal("abc"), DSL.literal("hello world"));
    assertEquals(INTEGER, expression.type());
    assertEquals(0, eval(expression).integerValue());
  }

  @Test
  void replace() {
    FunctionExpression expression =
        DSL.replace(DSL.literal("helloworld"), DSL.literal("world"), DSL.literal("opensearch"));
    assertEquals(STRING, expression.type());
    assertEquals("helloopensearch", eval(expression).stringValue());
  }

  @Test
  void reverse() {
    FunctionExpression expression = DSL.reverse(DSL.literal("abcde"));
    assertEquals(STRING, expression.type());
    assertEquals("edcba", eval(expression).stringValue());
  }

  void testConcatString(List<String> strings) {
    String expected = null;
    if (strings.stream().noneMatch(Objects::isNull)) {
      expected = String.join("", strings);
    }

    FunctionExpression expression =
        DSL.concat(DSL.literal(strings.get(0)), DSL.literal(strings.get(1)));
    assertEquals(STRING, expression.type());
    assertEquals(expected, eval(expression).stringValue());
  }

  void testConcatString(List<String> strings, String delim) {
    String expected = strings.stream().filter(Objects::nonNull).collect(Collectors.joining(","));

    FunctionExpression expression =
        DSL.concat_ws(DSL.literal(delim), DSL.literal(strings.get(0)), DSL.literal(strings.get(1)));
    assertEquals(STRING, expression.type());
    assertEquals(expected, eval(expression).stringValue());
  }

  @ParameterizedTest
  @MethodSource("getMultipleStringsForConcat")
  void testConcatMultipleString(List<String> strings) {
    String expected = null;
    if (strings.stream().noneMatch(Objects::isNull)) {
      expected = String.join("", strings);
    }

    FunctionExpression expression =
        DSL.concat(
            DSL.literal(strings.get(0)),
            DSL.literal(strings.get(1)),
            DSL.literal(strings.get(2)),
            DSL.literal(strings.get(3)),
            DSL.literal(strings.get(4)));
    assertEquals(STRING, expression.type());
    assertEquals(expected, eval(expression).stringValue());
  }

  @ParameterizedTest
  @MethodSource("getStringsForUpperAndLower")
  void length(String str) {
    FunctionExpression expression = DSL.length(DSL.literal(new ExprStringValue(str)));
    assertEquals(INTEGER, expression.type());
    assertEquals(str.getBytes().length, eval(expression).integerValue());
  }

  @ParameterizedTest
  @MethodSource("getStringsForComparison")
  void strcmp(StringPatternPair stringPatternPair) {
    FunctionExpression expression =
        DSL.strcmp(
            DSL.literal(new ExprStringValue(stringPatternPair.getStr())),
            DSL.literal(new ExprStringValue(stringPatternPair.getPatt())));
    assertEquals(INTEGER, expression.type());
    assertEquals(stringPatternPair.strCmpTest(), eval(expression).integerValue());
  }

  @ParameterizedTest
  @MethodSource("getStringsForUpperAndLower")
  void lower(String str) {
    FunctionExpression expression = DSL.lower(DSL.literal(new ExprStringValue(str)));
    assertEquals(STRING, expression.type());
    assertEquals(stringValue(str.toLowerCase()), eval(expression));
  }

  @ParameterizedTest
  @MethodSource("getStringsForUpperAndLower")
  void upper(String str) {
    FunctionExpression expression = DSL.upper(DSL.literal(new ExprStringValue(str)));
    assertEquals(STRING, expression.type());
    assertEquals(stringValue(str.toUpperCase()), eval(expression));
  }

  private ExprValue eval(Expression expression) {
    return expression.valueOf();
  }
}
