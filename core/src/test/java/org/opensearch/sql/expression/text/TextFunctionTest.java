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
  @Mock
  Environment<Expression, ExprValue> env;

  @Mock
  Expression nullRef;

  @Mock
  Expression missingRef;


  private static List<SubstringInfo> SUBSTRING_STRINGS = ImmutableList.of(
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
  private static List<String> UPPER_LOWER_STRINGS = ImmutableList.of(
      "test", " test", "test ", " test ", "TesT", "TEST", " TEST", "TEST ", " TEST ", " ", "");
  private static List<StringPatternPair> STRING_PATTERN_PAIRS = ImmutableList.of(
      new StringPatternPair("Michael!", "Michael!"),
      new StringPatternPair("hello", "world"),
      new StringPatternPair("world", "hello"));
  private static List<String> TRIM_STRINGS = ImmutableList.of(
      " test", "     test", "test     ", "test", "     test    ", "", " ");
  private static List<List<String>> CONCAT_STRING_LISTS = ImmutableList.of(
      ImmutableList.of("hello", "world"),
      ImmutableList.of("123", "5325"));

  interface SubstrSubstring {
    FunctionExpression getFunction(SubstringInfo strInfo);
  }

  class Substr implements SubstrSubstring {
    public FunctionExpression getFunction(SubstringInfo strInfo) {
      FunctionExpression expr;
      if (strInfo.getLen() == null) {
        expr = dsl.substr(DSL.literal(strInfo.getExpr()), DSL.literal(strInfo.getStart()));
      } else {
        expr = dsl.substr(DSL.literal(strInfo.getExpr()),
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
        expr = dsl.substring(DSL.literal(strInfo.getExpr()), DSL.literal(strInfo.getStart()));
      } else {
        expr = dsl.substring(DSL.literal(strInfo.getExpr()),
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
    assertEquals(missingValue(), eval(dsl.substr(missingRef, DSL.literal(1))));
    assertEquals(nullValue(), eval(dsl.substr(nullRef, DSL.literal(1))));
    assertEquals(missingValue(), eval(dsl.substring(missingRef, DSL.literal(1))));
    assertEquals(nullValue(), eval(dsl.substring(nullRef, DSL.literal(1))));

    when(nullRef.type()).thenReturn(INTEGER);
    when(missingRef.type()).thenReturn(INTEGER);
    assertEquals(missingValue(), eval(dsl.substr(DSL.literal("hello"), missingRef)));
    assertEquals(nullValue(), eval(dsl.substr(DSL.literal("hello"), nullRef)));
    assertEquals(missingValue(), eval(dsl.substring(DSL.literal("hello"), missingRef)));
    assertEquals(nullValue(), eval(dsl.substring(DSL.literal("hello"), nullRef)));
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
    assertEquals(missingValue(), eval(dsl.ltrim(missingRef)));
    assertEquals(nullValue(), eval(dsl.ltrim(nullRef)));
  }

  @Test
  public void rtrim() {
    TRIM_STRINGS.forEach(this::rtrimString);

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(dsl.ltrim(missingRef)));
    assertEquals(nullValue(), eval(dsl.ltrim(nullRef)));
  }

  @Test
  public void trim() {
    TRIM_STRINGS.forEach(this::trimString);

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(dsl.ltrim(missingRef)));
    assertEquals(nullValue(), eval(dsl.ltrim(nullRef)));
  }

  void ltrimString(String str) {
    FunctionExpression expression = dsl.ltrim(DSL.literal(str));
    assertEquals(STRING, expression.type());
    assertEquals(str.stripLeading(), eval(expression).stringValue());
  }

  void rtrimString(String str) {
    FunctionExpression expression = dsl.rtrim(DSL.literal(str));
    assertEquals(STRING, expression.type());
    assertEquals(str.stripTrailing(), eval(expression).stringValue());
  }

  void trimString(String str) {
    FunctionExpression expression = dsl.trim(DSL.literal(str));
    assertEquals(STRING, expression.type());
    assertEquals(str.trim(), eval(expression).stringValue());
  }

  @Test
  public void lower() {
    UPPER_LOWER_STRINGS.forEach(this::testLowerString);

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(dsl.lower(missingRef)));
    assertEquals(nullValue(), eval(dsl.lower(nullRef)));
  }

  @Test
  public void upper() {
    UPPER_LOWER_STRINGS.forEach(this::testUpperString);

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(dsl.upper(missingRef)));
    assertEquals(nullValue(), eval(dsl.upper(nullRef)));
  }

  @Test
  void concat() {
    CONCAT_STRING_LISTS.forEach(this::testConcatString);

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(
            dsl.concat(missingRef, DSL.literal("1"))));
    assertEquals(nullValue(), eval(
            dsl.concat(nullRef, DSL.literal("1"))));
    assertEquals(missingValue(), eval(
            dsl.concat(DSL.literal("1"), missingRef)));
    assertEquals(nullValue(), eval(
            dsl.concat(DSL.literal("1"), nullRef)));
  }

  @Test
  void concat_ws() {
    CONCAT_STRING_LISTS.forEach(s -> testConcatString(s, ","));

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(
        dsl.concat_ws(missingRef, DSL.literal("1"), DSL.literal("1"))));
    assertEquals(nullValue(), eval(
        dsl.concat_ws(nullRef, DSL.literal("1"), DSL.literal("1"))));
    assertEquals(missingValue(), eval(
        dsl.concat_ws(DSL.literal("1"), missingRef, DSL.literal("1"))));
    assertEquals(nullValue(), eval(
        dsl.concat_ws(DSL.literal("1"), nullRef, DSL.literal("1"))));
    assertEquals(missingValue(), eval(
        dsl.concat_ws(DSL.literal("1"), DSL.literal("1"), missingRef)));
    assertEquals(nullValue(), eval(
        dsl.concat_ws(DSL.literal("1"), DSL.literal("1"), nullRef)));
  }

  @Test
  void length() {
    UPPER_LOWER_STRINGS.forEach(this::testLengthString);

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(dsl.length(missingRef)));
    assertEquals(nullValue(), eval(dsl.length(nullRef)));
  }

  @Test
  void strcmp() {
    STRING_PATTERN_PAIRS.forEach(this::testStcmpString);

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(dsl.strcmp(missingRef, missingRef)));
    assertEquals(nullValue(), eval(dsl.strcmp(nullRef, nullRef)));
    assertEquals(missingValue(), eval(dsl.strcmp(nullRef, missingRef)));
    assertEquals(missingValue(), eval(dsl.strcmp(missingRef, nullRef)));
  }

  @Test
  void right() {
    FunctionExpression expression = dsl.right(
            DSL.literal(new ExprStringValue("foobarbar")),
            DSL.literal(new ExprIntegerValue(4)));
    assertEquals(STRING, expression.type());
    assertEquals("rbar", eval(expression).stringValue());

    expression = dsl.right(DSL.literal("foo"), DSL.literal(10));
    assertEquals(STRING, expression.type());
    assertEquals("foo", eval(expression).value());

    expression = dsl.right(DSL.literal("foo"), DSL.literal(0));
    assertEquals(STRING, expression.type());
    assertEquals("", eval(expression).value());

    expression = dsl.right(DSL.literal(""), DSL.literal(10));
    assertEquals(STRING, expression.type());
    assertEquals("", eval(expression).value());

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(INTEGER);
    assertEquals(missingValue(), eval(dsl.right(nullRef, missingRef)));
    assertEquals(nullValue(), eval(dsl.right(nullRef, DSL.literal(new ExprIntegerValue(1)))));

    when(nullRef.type()).thenReturn(INTEGER);
    assertEquals(nullValue(), eval(dsl.right(DSL.literal(new ExprStringValue("value")), nullRef)));
  }

  @Test
  void left() {
    FunctionExpression expression = dsl.left(
        DSL.literal(new ExprStringValue("helloworld")),
        DSL.literal(new ExprIntegerValue(5)));
    assertEquals(STRING, expression.type());
    assertEquals("hello", eval(expression).stringValue());

    expression = dsl.left(DSL.literal("hello"), DSL.literal(10));
    assertEquals(STRING, expression.type());
    assertEquals("hello", eval(expression).value());

    expression = dsl.left(DSL.literal("hello"), DSL.literal(0));
    assertEquals(STRING, expression.type());
    assertEquals("", eval(expression).value());

    expression = dsl.left(DSL.literal(""), DSL.literal(10));
    assertEquals(STRING, expression.type());
    assertEquals("", eval(expression).value());

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(INTEGER);
    assertEquals(missingValue(), eval(dsl.left(nullRef, missingRef)));
    assertEquals(nullValue(), eval(dsl.left(nullRef, DSL.literal(new ExprIntegerValue(1)))));

    when(nullRef.type()).thenReturn(INTEGER);
    assertEquals(nullValue(), eval(dsl.left(DSL.literal(new ExprStringValue("value")), nullRef)));
  }

  @Test
  void ascii() {
    FunctionExpression expression = dsl.ascii(DSL.literal(new ExprStringValue("hello")));
    assertEquals(INTEGER, expression.type());
    assertEquals(104, eval(expression).integerValue());

    when(nullRef.type()).thenReturn(STRING);
    assertEquals(nullValue(), eval(dsl.ascii(nullRef)));
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(dsl.ascii(missingRef)));
  }

  @Test
  void locate() {
    FunctionExpression expression = dsl.locate(
        DSL.literal("world"),
        DSL.literal("helloworld"));
    assertEquals(INTEGER, expression.type());
    assertEquals(6, eval(expression).integerValue());

    expression = dsl.locate(
        DSL.literal("world"),
        DSL.literal("helloworldworld"),
        DSL.literal(7));
    assertEquals(INTEGER, expression.type());
    assertEquals(11, eval(expression).integerValue());

    when(nullRef.type()).thenReturn(STRING);
    assertEquals(nullValue(), eval(dsl.locate(nullRef, DSL.literal("hello"))));
    assertEquals(nullValue(), eval(dsl.locate(nullRef, DSL.literal("hello"), DSL.literal(1))));
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(dsl.locate(missingRef, DSL.literal("hello"))));
    assertEquals(missingValue(), eval(
        dsl.locate(missingRef, DSL.literal("hello"), DSL.literal(1))));
  }

  @Test
  void replace() {
    FunctionExpression expression = dsl.replace(
        DSL.literal("helloworld"),
        DSL.literal("world"),
        DSL.literal("opensearch"));
    assertEquals(STRING, expression.type());
    assertEquals("helloopensearch", eval(expression).stringValue());

    when(nullRef.type()).thenReturn(STRING);
    assertEquals(nullValue(), eval(dsl.replace(nullRef, DSL.literal("a"), DSL.literal("b"))));
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(missingValue(), eval(dsl.replace(missingRef, DSL.literal("a"), DSL.literal("b"))));
  }

  void testConcatString(List<String> strings) {
    String expected = null;
    if (strings.stream().noneMatch(Objects::isNull)) {
      expected = String.join("", strings);
    }

    FunctionExpression expression = dsl.concat(
        DSL.literal(strings.get(0)), DSL.literal(strings.get(1)));
    assertEquals(STRING, expression.type());
    assertEquals(expected, eval(expression).stringValue());
  }

  void testConcatString(List<String> strings, String delim) {
    String expected = strings.stream()
        .filter(Objects::nonNull).collect(Collectors.joining(","));

    FunctionExpression expression = dsl.concat_ws(
        DSL.literal(delim), DSL.literal(strings.get(0)), DSL.literal(strings.get(1)));
    assertEquals(STRING, expression.type());
    assertEquals(expected, eval(expression).stringValue());
  }

  void testLengthString(String str) {
    FunctionExpression expression = dsl.length(DSL.literal(new ExprStringValue(str)));
    assertEquals(INTEGER, expression.type());
    assertEquals(str.getBytes().length, eval(expression).integerValue());
  }

  void testStcmpString(StringPatternPair stringPatternPair) {
    FunctionExpression expression = dsl.strcmp(
            DSL.literal(new ExprStringValue(stringPatternPair.getStr())),
            DSL.literal(new ExprStringValue(stringPatternPair.getPatt())));
    assertEquals(INTEGER, expression.type());
    assertEquals(stringPatternPair.strCmpTest(), eval(expression).integerValue());
  }

  void testLowerString(String str) {
    FunctionExpression expression = dsl.lower(DSL.literal(new ExprStringValue(str)));
    assertEquals(STRING, expression.type());
    assertEquals(stringValue(str.toLowerCase()), eval(expression));
  }

  void testUpperString(String str) {
    FunctionExpression expression = dsl.upper(DSL.literal(new ExprStringValue(str)));
    assertEquals(STRING, expression.type());
    assertEquals(stringValue(str.toUpperCase()), eval(expression));
  }

  private ExprValue eval(Expression expression) {
    return expression.valueOf(env);
  }
}
