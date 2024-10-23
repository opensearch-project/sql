/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.operator.predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.config.TestConfig.BOOL_TYPE_MISSING_VALUE_FIELD;
import static org.opensearch.sql.config.TestConfig.BOOL_TYPE_NULL_VALUE_FIELD;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;
import static org.opensearch.sql.data.model.ExprValueUtils.booleanValue;
import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.utils.ComparisonUtil.compare;
import static org.opensearch.sql.utils.OperatorUtils.matches;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;

class BinaryPredicateOperatorTest extends ExpressionTestBase {

  private static final List<StringPatternPair> STRING_PATTERN_PAIRS =
      ImmutableList.of(
          new StringPatternPair("Michael!", ".*"),
          new StringPatternPair("new*\\n*line", "new\\\\*.\\\\*line"),
          new StringPatternPair("a", "^[a-d]"),
          new StringPatternPair("helo", "world"),
          new StringPatternPair("a", "A"));

  @AllArgsConstructor
  @Getter
  static class StringPatternPair {
    private final String str;
    private final String patt;

    int regExpTest() {
      return str.matches(patt) ? 1 : 0;
    }
  }

  private static Stream<Arguments> binaryPredicateArguments() {
    List<Boolean> booleans = Arrays.asList(true, false);
    return Lists.cartesianProduct(booleans, booleans).stream()
        .map(list -> Arguments.of(list.get(0), list.get(1)));
  }

  private static List<List<Object>> getValuesForComparisonTests() {
    return List.of(
        List.of(1, 2),
        List.of((byte) 1, (byte) 2),
        List.of((short) 1, (short) 2),
        List.of(1L, 2L),
        List.of(1F, 2F),
        List.of(1D, 2D),
        List.of("str", "str0"),
        List.of(true, false),
        List.of(LocalTime.of(9, 7, 0), LocalTime.of(7, 40, 0)),
        List.of(LocalDate.of(1961, 4, 12), LocalDate.of(1984, 10, 25)),
        List.of(Instant.ofEpochSecond(42), Instant.ofEpochSecond(100500)),
        List.of(LocalDateTime.of(1961, 4, 12, 9, 7, 0), LocalDateTime.of(1984, 10, 25, 7, 40)),
        List.of(LocalDate.of(1961, 4, 12), LocalTime.now().minusHours(1)),
        List.of(LocalDate.of(1961, 4, 12), LocalDateTime.of(1984, 10, 25, 7, 40)),
        List.of(Instant.ofEpochSecond(100500), LocalDate.of(1961, 4, 12)),
        List.of(Instant.ofEpochSecond(100500), LocalTime.of(7, 40, 0)),
        List.of(LocalTime.of(7, 40, 0), LocalDateTime.of(1984, 10, 25, 7, 40)),
        List.of(Instant.ofEpochSecond(42), LocalDateTime.of(1984, 10, 25, 7, 40)));
  }

  private static Stream<Arguments> testEqualArguments() {
    Stream.Builder<Arguments> builder = Stream.builder();
    for (List<Object> argPair : getValuesForComparisonTests()) {
      builder.add(Arguments.of(fromObjectValue(argPair.get(0)), fromObjectValue(argPair.get(0))));
      builder.add(Arguments.of(fromObjectValue(argPair.get(1)), fromObjectValue(argPair.get(1))));
    }
    builder.add(
        Arguments.of(
            fromObjectValue(LocalTime.of(7, 40, 0)),
            fromObjectValue(LocalTime.of(7, 40, 0).atDate(LocalDate.now()))));
    builder.add(
        Arguments.of(
            fromObjectValue(LocalDateTime.of(1970, 1, 1, 0, 0, 42)),
            fromObjectValue(Instant.ofEpochSecond(42))));
    builder.add(
        Arguments.of(
            fromObjectValue(LocalDate.of(1970, 1, 1)), fromObjectValue(Instant.ofEpochSecond(0))));
    builder.add(
        Arguments.of(
            fromObjectValue(LocalDate.of(1984, 10, 25)),
            fromObjectValue(LocalDateTime.of(1984, 10, 25, 0, 0))));
    builder.add(
        Arguments.of(fromObjectValue(LocalTime.of(0, 0, 0)), fromObjectValue(LocalDate.now())));
    builder.add(
        Arguments.of(
            fromObjectValue(LocalTime.of(0, 0, 0)),
            fromObjectValue(LocalDate.now().atStartOfDay(ZoneId.of("UTC")).toInstant())));
    builder.add(
        Arguments.of(fromObjectValue(ImmutableList.of(1)), fromObjectValue(ImmutableList.of(1))));
    builder.add(
        Arguments.of(
            fromObjectValue(ImmutableMap.of("str", 1)),
            fromObjectValue(ImmutableMap.of("str", 1))));
    return builder.build();
  }

  private static Stream<Arguments> testNotEqualArguments() {
    Stream.Builder<Arguments> builder = Stream.builder();
    for (List<Object> argPair : getValuesForComparisonTests()) {
      builder.add(Arguments.of(fromObjectValue(argPair.get(0)), fromObjectValue(argPair.get(1))));
      builder.add(Arguments.of(fromObjectValue(argPair.get(1)), fromObjectValue(argPair.get(0))));
    }
    builder.add(
        Arguments.of(
            fromObjectValue(LocalTime.of(7, 40, 0)),
            fromObjectValue(LocalDateTime.of(1984, 10, 25, 7, 40, 0))));
    builder.add(
        Arguments.of(
            fromObjectValue(LocalDateTime.of(1984, 10, 25, 7, 40, 0)),
            fromObjectValue(Instant.ofEpochSecond(42))));
    builder.add(
        Arguments.of(
            fromObjectValue(LocalDate.of(1984, 10, 25)),
            fromObjectValue(Instant.ofEpochSecond(42))));
    builder.add(
        Arguments.of(
            fromObjectValue(LocalTime.of(7, 40, 0)), fromObjectValue(Instant.ofEpochSecond(42))));
    builder.add(
        Arguments.of(
            fromObjectValue(LocalDate.of(1984, 10, 25)),
            fromObjectValue(LocalDateTime.of(1984, 10, 25, 7, 40))));
    builder.add(
        Arguments.of(
            fromObjectValue(LocalDate.of(1984, 10, 25)), fromObjectValue(LocalTime.of(7, 40, 0))));
    builder.add(
        Arguments.of(
            fromObjectValue(ImmutableList.of(1)), fromObjectValue(ImmutableList.of(1, 2))));
    builder.add(
        Arguments.of(fromObjectValue(ImmutableList.of(1)), fromObjectValue(ImmutableList.of(2))));
    builder.add(
        Arguments.of(
            fromObjectValue(ImmutableMap.of("str", 1)),
            fromObjectValue(ImmutableMap.of("str2", 2))));
    return builder.build();
  }

  private static Stream<Arguments> testCompareValueArguments() {
    Stream.Builder<Arguments> builder = Stream.builder();
    for (List<Object> argPair : getValuesForComparisonTests()) {
      builder.add(Arguments.of(fromObjectValue(argPair.get(0)), fromObjectValue(argPair.get(0))));
      builder.add(Arguments.of(fromObjectValue(argPair.get(0)), fromObjectValue(argPair.get(1))));
      builder.add(Arguments.of(fromObjectValue(argPair.get(1)), fromObjectValue(argPair.get(0))));
    }
    return builder.build();
  }

  private static Stream<Arguments> testLikeArguments() {
    List<List<String>> arguments =
        Arrays.asList(
            Arrays.asList("foo", "foo"),
            Arrays.asList("notFoo", "foo"),
            Arrays.asList("foobar", "%bar"),
            Arrays.asList("bar", "%bar"),
            Arrays.asList("foo", "fo_"),
            Arrays.asList("foo", "foo_"),
            Arrays.asList("foorbar", "%o_ar"),
            Arrays.asList("foobar", "%o_a%"),
            Arrays.asList("fooba%_\\^$.*[]()|+r", "%\\%\\_\\\\\\^\\$\\.\\*\\[\\]\\(\\)\\|\\+_"));
    Stream.Builder<Arguments> builder = Stream.builder();
    for (List<String> argPair : arguments) {
      builder.add(Arguments.of(fromObjectValue(argPair.get(0)), fromObjectValue(argPair.get(1))));
    }
    return builder.build();
  }

  private static Stream<Arguments> getCidrArguments() {
    return Stream.of(
            Arguments.of("10.24.33.5", "10.24.34.0/24", false), // IPv4 less
            Arguments.of("10.24.34.5", "10.24.34.0/24", true),   // IPv4 between
            Arguments.of("10.24.35.5", "10.24.34.0/24", false), // IPv4 greater
            Arguments.of("10.24.35.5", "10.24.34.0/33", false), // IPv4 prefix too long

            Arguments.of("2001:0db7::8329", "2001:0db8::/32", false), // IPv6 less
            Arguments.of("2001:0db8::8329", "2001:0db8::/32", true),  // IPv6 between
            Arguments.of("2001:0db9::8329", "2001:0db8::/32", false), // IPv6 greater
            Arguments.of("2001:0db9::8329", "2001:0db8::/129", false), // IPv6 prefix too long

            Arguments.of("INVALID", "10.24.34.0/24", false), // Invalid argument
            Arguments.of("10.24.34.5", "INVALID", false), // Invalid range
            Arguments.of("10.24.34.5", "10.24.34.0/INVALID", false) // Invalid prefix
    );
  }

  @ParameterizedTest(name = "and({0}, {1})")
  @MethodSource("binaryPredicateArguments")
  public void test_and(Boolean v1, Boolean v2) {
    FunctionExpression and = DSL.and(DSL.literal(booleanValue(v1)), DSL.literal(booleanValue(v2)));
    assertEquals(BOOLEAN, and.type());
    assertEquals(v1 && v2, ExprValueUtils.getBooleanValue(and.valueOf(valueEnv())));
    assertEquals(String.format("and(%s, %s)", v1, v2.toString()), and.toString());
  }

  @Test
  public void test_boolean_and_null() {
    FunctionExpression and =
        DSL.and(DSL.literal(LITERAL_TRUE), DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, and.type());
    assertEquals(LITERAL_NULL, and.valueOf(valueEnv()));

    and = DSL.and(DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN), DSL.literal(LITERAL_TRUE));
    assertEquals(BOOLEAN, and.type());
    assertEquals(LITERAL_NULL, and.valueOf(valueEnv()));

    and = DSL.and(DSL.literal(LITERAL_FALSE), DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, and.type());
    assertEquals(LITERAL_FALSE, and.valueOf(valueEnv()));

    and = DSL.and(DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN), DSL.literal(LITERAL_FALSE));
    assertEquals(BOOLEAN, and.type());
    assertEquals(LITERAL_FALSE, and.valueOf(valueEnv()));
  }

  @Test
  public void test_boolean_and_missing() {
    FunctionExpression and =
        DSL.and(DSL.literal(LITERAL_TRUE), DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, and.type());
    assertEquals(LITERAL_MISSING, and.valueOf(valueEnv()));

    and = DSL.and(DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN), DSL.literal(LITERAL_TRUE));
    assertEquals(BOOLEAN, and.type());
    assertEquals(LITERAL_MISSING, and.valueOf(valueEnv()));

    and = DSL.and(DSL.literal(LITERAL_FALSE), DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, and.type());
    assertEquals(LITERAL_FALSE, and.valueOf(valueEnv()));

    and = DSL.and(DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN), DSL.literal(LITERAL_FALSE));
    assertEquals(BOOLEAN, and.type());
    assertEquals(LITERAL_FALSE, and.valueOf(valueEnv()));
  }

  @Test
  public void test_null_and_missing() {
    FunctionExpression and =
        DSL.and(
            DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN),
            DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, and.type());
    assertEquals(LITERAL_MISSING, and.valueOf(valueEnv()));

    and =
        DSL.and(
            DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN),
            DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, and.type());
    assertEquals(LITERAL_NULL, and.valueOf(valueEnv()));

    and =
        DSL.and(
            DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN),
            DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, and.type());
    assertEquals(LITERAL_MISSING, and.valueOf(valueEnv()));

    and =
        DSL.and(
            DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN),
            DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, and.type());
    assertEquals(LITERAL_MISSING, and.valueOf(valueEnv()));
  }

  @ParameterizedTest(name = "or({0}, {1})")
  @MethodSource("binaryPredicateArguments")
  public void test_or(Boolean v1, Boolean v2) {
    FunctionExpression or = DSL.or(DSL.literal(booleanValue(v1)), DSL.literal(booleanValue(v2)));
    assertEquals(BOOLEAN, or.type());
    assertEquals(v1 || v2, ExprValueUtils.getBooleanValue(or.valueOf(valueEnv())));
    assertEquals(String.format("or(%s, %s)", v1, v2.toString()), or.toString());
  }

  @Test
  public void test_boolean_or_null() {
    FunctionExpression or =
        DSL.or(DSL.literal(LITERAL_TRUE), DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, or.type());
    assertEquals(LITERAL_TRUE, or.valueOf(valueEnv()));

    or = DSL.or(DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN), DSL.literal(LITERAL_TRUE));
    assertEquals(BOOLEAN, or.type());
    assertEquals(LITERAL_TRUE, or.valueOf(valueEnv()));

    or = DSL.or(DSL.literal(LITERAL_FALSE), DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, or.type());
    assertEquals(LITERAL_NULL, or.valueOf(valueEnv()));

    or = DSL.or(DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN), DSL.literal(LITERAL_FALSE));
    assertEquals(BOOLEAN, or.type());
    assertEquals(LITERAL_NULL, or.valueOf(valueEnv()));
  }

  @Test
  public void test_boolean_or_missing() {
    FunctionExpression or =
        DSL.or(DSL.literal(LITERAL_TRUE), DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, or.type());
    assertEquals(LITERAL_TRUE, or.valueOf(valueEnv()));

    or = DSL.or(DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN), DSL.literal(LITERAL_TRUE));
    assertEquals(BOOLEAN, or.type());
    assertEquals(LITERAL_TRUE, or.valueOf(valueEnv()));

    or = DSL.or(DSL.literal(LITERAL_FALSE), DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, or.type());
    assertEquals(LITERAL_MISSING, or.valueOf(valueEnv()));

    or = DSL.or(DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN), DSL.literal(LITERAL_FALSE));
    assertEquals(BOOLEAN, or.type());
    assertEquals(LITERAL_MISSING, or.valueOf(valueEnv()));
  }

  @Test
  public void test_null_or_missing() {
    FunctionExpression or =
        DSL.or(
            DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN),
            DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, or.type());
    assertEquals(LITERAL_MISSING, or.valueOf(valueEnv()));

    or =
        DSL.or(
            DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN),
            DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, or.type());
    assertEquals(LITERAL_NULL, or.valueOf(valueEnv()));

    or =
        DSL.or(
            DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN),
            DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, or.type());
    assertEquals(LITERAL_NULL, or.valueOf(valueEnv()));

    or =
        DSL.or(
            DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN),
            DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, or.type());
    assertEquals(LITERAL_NULL, or.valueOf(valueEnv()));
  }

  @ParameterizedTest(name = "xor({0}, {1})")
  @MethodSource("binaryPredicateArguments")
  public void test_xor(Boolean v1, Boolean v2) {
    FunctionExpression xor = DSL.xor(DSL.literal(booleanValue(v1)), DSL.literal(booleanValue(v2)));
    assertEquals(BOOLEAN, xor.type());
    assertEquals(v1 ^ v2, ExprValueUtils.getBooleanValue(xor.valueOf(valueEnv())));
    assertEquals(String.format("xor(%s, %s)", v1, v2), xor.toString());
  }

  @Test
  public void test_boolean_xor_null() {
    FunctionExpression xor =
        DSL.xor(DSL.literal(LITERAL_TRUE), DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, xor.type());
    assertEquals(LITERAL_TRUE, xor.valueOf(valueEnv()));

    xor = DSL.xor(DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN), DSL.literal(LITERAL_TRUE));
    assertEquals(BOOLEAN, xor.type());
    assertEquals(LITERAL_TRUE, xor.valueOf(valueEnv()));

    xor = DSL.xor(DSL.literal(LITERAL_FALSE), DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, xor.type());
    assertEquals(LITERAL_NULL, xor.valueOf(valueEnv()));

    xor = DSL.xor(DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN), DSL.literal(LITERAL_FALSE));
    assertEquals(BOOLEAN, xor.type());
    assertEquals(LITERAL_NULL, xor.valueOf(valueEnv()));
  }

  @Test
  public void test_boolean_xor_missing() {
    FunctionExpression xor =
        DSL.xor(DSL.literal(LITERAL_TRUE), DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, xor.type());
    assertEquals(LITERAL_TRUE, xor.valueOf(valueEnv()));

    xor = DSL.xor(DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN), DSL.literal(LITERAL_TRUE));
    assertEquals(BOOLEAN, xor.type());
    assertEquals(LITERAL_TRUE, xor.valueOf(valueEnv()));

    xor = DSL.xor(DSL.literal(LITERAL_FALSE), DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, xor.type());
    assertEquals(LITERAL_MISSING, xor.valueOf(valueEnv()));

    xor = DSL.xor(DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN), DSL.literal(LITERAL_FALSE));
    assertEquals(BOOLEAN, xor.type());
    assertEquals(LITERAL_MISSING, xor.valueOf(valueEnv()));
  }

  @Test
  public void test_null_xor_missing() {
    FunctionExpression xor =
        DSL.xor(
            DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN),
            DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, xor.type());
    assertEquals(LITERAL_MISSING, xor.valueOf(valueEnv()));

    xor =
        DSL.xor(
            DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN),
            DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, xor.type());
    assertEquals(LITERAL_NULL, xor.valueOf(valueEnv()));

    xor =
        DSL.xor(
            DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN),
            DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, xor.type());
    assertEquals(LITERAL_NULL, xor.valueOf(valueEnv()));

    xor =
        DSL.xor(
            DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN),
            DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, xor.type());
    assertEquals(LITERAL_NULL, xor.valueOf(valueEnv()));
  }

  @ParameterizedTest(name = "equal({0}, {1})")
  @MethodSource("testEqualArguments")
  public void test_equal(ExprValue v1, ExprValue v2) {
    FunctionExpression equal = DSL.equal(functionProperties, DSL.literal(v1), DSL.literal(v2));
    assertEquals(BOOLEAN, equal.type());
    if (v1.type() == v2.type()) {
      assertEquals(
          v1.value().equals(v2.value()), ExprValueUtils.getBooleanValue(equal.valueOf(valueEnv())));
    }
    if (v1.type() != STRUCT && v1.type() != ARRAY) {
      assertEquals(
          0 == compare(functionProperties, v1, v2),
          ExprValueUtils.getBooleanValue(equal.valueOf(valueEnv())));
    }
    assertStringRepr(v1, v2, "=", equal);
  }

  private void assertStringRepr(
      ExprValue v1, ExprValue v2, String function, FunctionExpression functionExpression) {
    if (v1.type() == v2.type()) {
      assertEquals(String.format("%s(%s, %s)", function, v1, v2), functionExpression.toString());
    } else {
      assertEquals(
          String.format(
              "%s(%s, %s)",
              function, getExpectedStringRepr(TIMESTAMP, v1), getExpectedStringRepr(TIMESTAMP, v2)),
          functionExpression.toString());
    }
  }

  private String getExpectedStringRepr(ExprType widerType, ExprValue value) {
    if (widerType == value.type()) {
      return value.toString();
    }
    return String.format("cast_to_%s(%s)", widerType.toString().toLowerCase(), value);
  }

  @ParameterizedTest(name = "equal({0}, {1})")
  @MethodSource({"testEqualArguments", "testNotEqualArguments"})
  public void test_notequal(ExprValue v1, ExprValue v2) {
    FunctionExpression notequal =
        DSL.notequal(functionProperties, DSL.literal(v1), DSL.literal(v2));
    assertEquals(BOOLEAN, notequal.type());
    if (v1.type() == v2.type()) {
      assertEquals(
          !v1.value().equals(v2.value()),
          ExprValueUtils.getBooleanValue(notequal.valueOf(valueEnv())));
    }
    if (v1.type() != STRUCT && v1.type() != ARRAY) {
      assertEquals(
          0 != compare(functionProperties, v1, v2),
          ExprValueUtils.getBooleanValue(notequal.valueOf(valueEnv())));
    }
    assertStringRepr(v1, v2, "!=", notequal);
  }

  @ParameterizedTest(name = "less({0}, {1})")
  @MethodSource("testCompareValueArguments")
  public void test_less(ExprValue v1, ExprValue v2) {
    FunctionExpression less = DSL.less(functionProperties, DSL.literal(v1), DSL.literal(v2));
    assertEquals(BOOLEAN, less.type());
    assertEquals(
        compare(functionProperties, v1, v2) < 0,
        ExprValueUtils.getBooleanValue(less.valueOf(valueEnv())));
    assertStringRepr(v1, v2, "<", less);
  }

  @ParameterizedTest(name = "lte({0}, {1})")
  @MethodSource("testCompareValueArguments")
  public void test_lte(ExprValue v1, ExprValue v2) {
    FunctionExpression lte = DSL.lte(functionProperties, DSL.literal(v1), DSL.literal(v2));
    assertEquals(BOOLEAN, lte.type());
    assertEquals(
        compare(functionProperties, v1, v2) <= 0,
        ExprValueUtils.getBooleanValue(lte.valueOf(valueEnv())));
    assertStringRepr(v1, v2, "<=", lte);
  }

  @ParameterizedTest(name = "greater({0}, {1})")
  @MethodSource("testCompareValueArguments")
  public void test_greater(ExprValue v1, ExprValue v2) {
    FunctionExpression greater = DSL.greater(functionProperties, DSL.literal(v1), DSL.literal(v2));
    assertEquals(BOOLEAN, greater.type());
    assertEquals(
        compare(functionProperties, v1, v2) > 0,
        ExprValueUtils.getBooleanValue(greater.valueOf(valueEnv())));
    assertStringRepr(v1, v2, ">", greater);
  }

  @ParameterizedTest(name = "gte({0}, {1})")
  @MethodSource("testCompareValueArguments")
  public void test_gte(ExprValue v1, ExprValue v2) {
    FunctionExpression gte = DSL.gte(functionProperties, DSL.literal(v1), DSL.literal(v2));
    assertEquals(BOOLEAN, gte.type());
    assertEquals(
        compare(functionProperties, v1, v2) >= 0,
        ExprValueUtils.getBooleanValue(gte.valueOf(valueEnv())));
    assertStringRepr(v1, v2, ">=", gte);
  }

  @ParameterizedTest(name = "like({0}, {1})")
  @MethodSource("testLikeArguments")
  public void test_like(ExprValue v1, ExprValue v2) {
    FunctionExpression like = DSL.like(DSL.literal(v1), DSL.literal(v2));
    assertEquals(BOOLEAN, like.type());
    assertEquals(matches(v1, v2), like.valueOf(valueEnv()));
    assertEquals(String.format("like(%s, %s)", v1, v2), like.toString());
  }

  @Test
  public void test_not_like() {
    FunctionExpression notLike = DSL.notLike(DSL.literal("bob"), DSL.literal("tom"));
    assertEquals(BOOLEAN, notLike.type());
    assertTrue(notLike.valueOf(valueEnv()).booleanValue());
    assertEquals(String.format("not like(\"%s\", \"%s\")", "bob", "tom"), notLike.toString());

    notLike = DSL.notLike(DSL.literal("bob"), DSL.literal("bo%"));
    assertFalse(notLike.valueOf(valueEnv()).booleanValue());
    assertEquals(String.format("not like(\"%s\", \"%s\")", "bob", "bo%"), notLike.toString());
  }

  @Test
  void test_regexp() {
    STRING_PATTERN_PAIRS.forEach(this::testRegexpString);
  }

  void testRegexpString(StringPatternPair stringPatternPair) {
    FunctionExpression expression =
        DSL.regexp(
            DSL.literal(new ExprStringValue(stringPatternPair.getStr())),
            DSL.literal(new ExprStringValue(stringPatternPair.getPatt())));
    assertEquals(INTEGER, expression.type());
    assertEquals(stringPatternPair.regExpTest(), expression.valueOf(valueEnv()).integerValue());
  }

  /** Todo. remove this test cases after script serialization implemented. */
  @Test
  public void serializationTest() throws Exception {
    Expression expression = DSL.equal(DSL.literal("v1"), DSL.literal("v2"));
    // serialization
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    ObjectOutputStream objectOutput = new ObjectOutputStream(output);
    objectOutput.writeObject(expression);
    objectOutput.flush();
    String source = Base64.getEncoder().encodeToString(output.toByteArray());

    // deserialization
    ByteArrayInputStream input = new ByteArrayInputStream(Base64.getDecoder().decode(source));
    ObjectInputStream objectInput = new ObjectInputStream(input);
    Expression e = (Expression) objectInput.readObject();
    ExprValue exprValue = e.valueOf(valueEnv());

    assertEquals(LITERAL_FALSE, exprValue);
  }

  @Test
  public void compareNumberValueWithDifferentType() {
    FunctionExpression equal = DSL.equal(DSL.literal(1), DSL.literal(1L));
    assertTrue(equal.valueOf(valueEnv()).booleanValue());
  }

  @Test
  public void compare_int_long() {
    FunctionExpression equal = DSL.equal(DSL.literal(1), DSL.literal(1L));
    assertTrue(equal.valueOf(valueEnv()).booleanValue());
  }

  @ParameterizedTest
  @MethodSource("getCidrArguments")
  public void test_cidr(String address, String range, boolean expected) {
    FunctionExpression cidr = DSL.cidr(DSL.literal(address), DSL.literal(range));
    assertEquals(cidr.type(), BOOLEAN);
    assertEquals(cidr.valueOf().booleanValue(), expected);
  }
}
