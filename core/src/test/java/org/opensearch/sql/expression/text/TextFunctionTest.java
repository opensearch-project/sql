/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.text;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;

@ExtendWith(MockitoExtension.class)
public class TextFunctionTest extends ExpressionTestBase {
  @Mock Environment<Expression, ExprValue> env;

  @Mock Expression nullRef;

  @Mock Expression missingRef;

  private static List<SubstringInfo> SUBSTRING_STRINGS =
      ImmutableList.of(
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
  private static List<String> UPPER_LOWER_STRINGS =
      ImmutableList.of(
          "test", " test", "test ", " test ", "TesT", "TEST", " TEST", "TEST ", " TEST ", " ", "");
  private static List<StringPatternPair> STRING_PATTERN_PAIRS =
      ImmutableList.of(
          new StringPatternPair("Michael!", "Michael!"),
          new StringPatternPair("hello", "world"),
          new StringPatternPair("world", "hello"));
  private static List<String> TRIM_STRINGS =
      ImmutableList.of(" test", "     test", "test     ", "test", "     test    ", "", " ");
  private static List<List<String>> CONCAT_STRING_LISTS =
      ImmutableList.of(ImmutableList.of("hello", "world"), ImmutableList.of("123", "5325"));
  private static List<List<String>> CONCAT_STRING_LISTS_WITH_MANY_STRINGS =
      ImmutableList.of(
          ImmutableList.of("he", "llo", "wo", "rld", "!"),
          ImmutableList.of("0", "123", "53", "25", "7"));

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

  @BeforeEach
  public void setup() {
    when(nullRef.valueOf(env)).thenReturn(nullValue());
    when(missingRef.valueOf(env)).thenReturn(missingValue());
  }

  @Test
  public void substrSubstring() {
    SUBSTRING_STRINGS.forEach(s -> substrSubstringTest(s, new Substr()));
    SUBSTRING_STRINGS.forEach(s -> substrSubstringTest(s, new Substring()));

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(DSL.substr(missingRef, DSL.literal(1))));
    assertEquals(nullValue(), eval(DSL.substr(nullRef, DSL.literal(1))));
    assertEquals(missingValue(), eval(DSL.substring(missingRef, DSL.literal(1))));
    assertEquals(nullValue(), eval(DSL.substring(nullRef, DSL.literal(1))));

    when(nullRef.type()).thenReturn(INTEGER);
    when(missingRef.type()).thenReturn(INTEGER);
    assertEquals(missingValue(), eval(DSL.substr(DSL.literal("hello"), missingRef)));
    assertEquals(nullValue(), eval(DSL.substr(DSL.literal("hello"), nullRef)));
    assertEquals(missingValue(), eval(DSL.substring(DSL.literal("hello"), missingRef)));
    assertEquals(nullValue(), eval(DSL.substring(DSL.literal("hello"), nullRef)));
  }

  void substrSubstringTest(SubstringInfo strInfo, SubstrSubstring substrSubstring) {
    FunctionExpression expr = substrSubstring.getFunction(strInfo);
    assertEquals(STRING, expr.type());
    assertEquals(strInfo.getRes(), eval(expr).stringValue());
  }

  @Test
  public void ltrim() {
    TRIM_STRINGS.forEach(this::ltrimString);

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(DSL.ltrim(missingRef)));
    assertEquals(nullValue(), eval(DSL.ltrim(nullRef)));
  }

  @Test
  public void rtrim() {
    TRIM_STRINGS.forEach(this::rtrimString);

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(DSL.ltrim(missingRef)));
    assertEquals(nullValue(), eval(DSL.ltrim(nullRef)));
  }

  @Test
  public void trim() {
    TRIM_STRINGS.forEach(this::trimString);

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(DSL.ltrim(missingRef)));
    assertEquals(nullValue(), eval(DSL.ltrim(nullRef)));
  }

  void ltrimString(String str) {
    FunctionExpression expression = DSL.ltrim(DSL.literal(str));
    assertEquals(STRING, expression.type());
    assertEquals(str.stripLeading(), eval(expression).stringValue());
  }

  void rtrimString(String str) {
    FunctionExpression expression = DSL.rtrim(DSL.literal(str));
    assertEquals(STRING, expression.type());
    assertEquals(str.stripTrailing(), eval(expression).stringValue());
  }

  void trimString(String str) {
    FunctionExpression expression = DSL.trim(DSL.literal(str));
    assertEquals(STRING, expression.type());
    assertEquals(str.trim(), eval(expression).stringValue());
  }

  @Test
  public void lower() {
    UPPER_LOWER_STRINGS.forEach(this::testLowerString);

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(DSL.lower(missingRef)));
    assertEquals(nullValue(), eval(DSL.lower(nullRef)));
  }

  @Test
  public void upper() {
    UPPER_LOWER_STRINGS.forEach(this::testUpperString);

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(DSL.upper(missingRef)));
    assertEquals(nullValue(), eval(DSL.upper(nullRef)));
  }

  @Test
  void concat() {
    CONCAT_STRING_LISTS.forEach(this::testConcatString);
    CONCAT_STRING_LISTS_WITH_MANY_STRINGS.forEach(this::testConcatMultipleString);

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(DSL.concat(missingRef, DSL.literal("1"))));
    // If any of the expressions is a NULL value, it returns NULL.
    assertEquals(nullValue(), eval(DSL.concat(nullRef, DSL.literal("1"))));
    assertEquals(missingValue(), eval(DSL.concat(DSL.literal("1"), missingRef)));
    assertEquals(nullValue(), eval(DSL.concat(DSL.literal("1"), nullRef)));
  }

  @Test
  void concat_ws() {
    CONCAT_STRING_LISTS.forEach(s -> testConcatString(s, ","));

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(
        missingValue(), eval(DSL.concat_ws(missingRef, DSL.literal("1"), DSL.literal("1"))));
    assertEquals(nullValue(), eval(DSL.concat_ws(nullRef, DSL.literal("1"), DSL.literal("1"))));
    assertEquals(
        missingValue(), eval(DSL.concat_ws(DSL.literal("1"), missingRef, DSL.literal("1"))));
    assertEquals(nullValue(), eval(DSL.concat_ws(DSL.literal("1"), nullRef, DSL.literal("1"))));
    assertEquals(
        missingValue(), eval(DSL.concat_ws(DSL.literal("1"), DSL.literal("1"), missingRef)));
    assertEquals(nullValue(), eval(DSL.concat_ws(DSL.literal("1"), DSL.literal("1"), nullRef)));
  }

  @Test
  void length() {
    UPPER_LOWER_STRINGS.forEach(this::testLengthString);

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(DSL.length(missingRef)));
    assertEquals(nullValue(), eval(DSL.length(nullRef)));
  }

  @Test
  void strcmp() {
    STRING_PATTERN_PAIRS.forEach(this::testStcmpString);

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(DSL.strcmp(missingRef, missingRef)));
    assertEquals(nullValue(), eval(DSL.strcmp(nullRef, nullRef)));
    assertEquals(missingValue(), eval(DSL.strcmp(nullRef, missingRef)));
    assertEquals(missingValue(), eval(DSL.strcmp(missingRef, nullRef)));
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

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(INTEGER);
    assertEquals(missingValue(), eval(DSL.right(nullRef, missingRef)));
    assertEquals(nullValue(), eval(DSL.right(nullRef, DSL.literal(new ExprIntegerValue(1)))));

    when(nullRef.type()).thenReturn(INTEGER);
    assertEquals(nullValue(), eval(DSL.right(DSL.literal(new ExprStringValue("value")), nullRef)));
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

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(INTEGER);
    assertEquals(missingValue(), eval(DSL.left(nullRef, missingRef)));
    assertEquals(nullValue(), eval(DSL.left(nullRef, DSL.literal(new ExprIntegerValue(1)))));

    when(nullRef.type()).thenReturn(INTEGER);
    assertEquals(nullValue(), eval(DSL.left(DSL.literal(new ExprStringValue("value")), nullRef)));
  }

  @Test
  void ascii() {
    FunctionExpression expression = DSL.ascii(DSL.literal(new ExprStringValue("hello")));
    assertEquals(INTEGER, expression.type());
    assertEquals(104, eval(expression).integerValue());

    when(nullRef.type()).thenReturn(STRING);
    assertEquals(nullValue(), eval(DSL.ascii(nullRef)));
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(DSL.ascii(missingRef)));
  }

  @Test
  void locate() {
    FunctionExpression expression = DSL.locate(DSL.literal("world"), DSL.literal("helloworld"));
    assertEquals(INTEGER, expression.type());
    assertEquals(6, eval(expression).integerValue());

    expression = DSL.locate(DSL.literal("world"), DSL.literal("helloworldworld"), DSL.literal(7));
    assertEquals(INTEGER, expression.type());
    assertEquals(11, eval(expression).integerValue());

    when(nullRef.type()).thenReturn(STRING);
    assertEquals(nullValue(), eval(DSL.locate(nullRef, DSL.literal("hello"))));
    assertEquals(nullValue(), eval(DSL.locate(nullRef, DSL.literal("hello"), DSL.literal(1))));
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(DSL.locate(missingRef, DSL.literal("hello"))));
    assertEquals(
        missingValue(), eval(DSL.locate(missingRef, DSL.literal("hello"), DSL.literal(1))));
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

    when(nullRef.type()).thenReturn(STRING);
    assertEquals(nullValue(), eval(DSL.position(nullRef, DSL.literal("hello"))));
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(DSL.position(missingRef, DSL.literal("hello"))));
  }

  @Test
  void replace() {
    FunctionExpression expression =
        DSL.replace(DSL.literal("helloworld"), DSL.literal("world"), DSL.literal("opensearch"));
    assertEquals(STRING, expression.type());
    assertEquals("helloopensearch", eval(expression).stringValue());

    when(nullRef.type()).thenReturn(STRING);
    assertEquals(nullValue(), eval(DSL.replace(nullRef, DSL.literal("a"), DSL.literal("b"))));
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(DSL.replace(missingRef, DSL.literal("a"), DSL.literal("b"))));
  }

  @Test
  void reverse() {
    FunctionExpression expression = DSL.reverse(DSL.literal("abcde"));
    assertEquals(STRING, expression.type());
    assertEquals("edcba", eval(expression).stringValue());

    when(nullRef.type()).thenReturn(STRING);
    assertEquals(nullValue(), eval(DSL.reverse(nullRef)));
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(DSL.reverse(missingRef)));
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

  void testLengthString(String str) {
    FunctionExpression expression = DSL.length(DSL.literal(new ExprStringValue(str)));
    assertEquals(INTEGER, expression.type());
    assertEquals(str.getBytes().length, eval(expression).integerValue());
  }

  void testStcmpString(StringPatternPair stringPatternPair) {
    FunctionExpression expression =
        DSL.strcmp(
            DSL.literal(new ExprStringValue(stringPatternPair.getStr())),
            DSL.literal(new ExprStringValue(stringPatternPair.getPatt())));
    assertEquals(INTEGER, expression.type());
    assertEquals(stringPatternPair.strCmpTest(), eval(expression).integerValue());
  }

  void testLowerString(String str) {
    FunctionExpression expression = DSL.lower(DSL.literal(new ExprStringValue(str)));
    assertEquals(STRING, expression.type());
    assertEquals(stringValue(str.toLowerCase()), eval(expression));
  }

  void testUpperString(String str) {
    FunctionExpression expression = DSL.upper(DSL.literal(new ExprStringValue(str)));
    assertEquals(STRING, expression.type());
    assertEquals(stringValue(str.toUpperCase()), eval(expression));
  }

  private ExprValue eval(Expression expression) {
    return expression.valueOf(env);
  }
}
