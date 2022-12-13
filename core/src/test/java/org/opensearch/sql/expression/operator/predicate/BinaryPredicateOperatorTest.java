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
import static org.opensearch.sql.config.TestConfig.INT_TYPE_MISSING_VALUE_FIELD;
import static org.opensearch.sql.config.TestConfig.INT_TYPE_NULL_VALUE_FIELD;
import static org.opensearch.sql.config.TestConfig.STRING_TYPE_MISSING_VALUE_FIELD;
import static org.opensearch.sql.config.TestConfig.STRING_TYPE_NULL_VALUE_FIELD;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;
import static org.opensearch.sql.data.model.ExprValueUtils.booleanValue;
import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.utils.ComparisonUtil.compare;
import static org.opensearch.sql.utils.OperatorUtils.matches;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;

class BinaryPredicateOperatorTest extends ExpressionTestBase {

  private static List<StringPatternPair> STRING_PATTERN_PAIRS = ImmutableList.of(
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

  private static Stream<Arguments> testEqualArguments() {
    Stream.Builder<Arguments> builder = Stream.builder();
    builder.add(Arguments.of(new ExprByteValue(1), new ExprByteValue(1)));
    builder.add(Arguments.of(new ExprShortValue(1), new ExprShortValue(1)));
    builder.add(Arguments.of(new ExprIntegerValue(1), new ExprIntegerValue(1)));
    builder.add(Arguments.of(new ExprLongValue(1L), new ExprLongValue(1L)));
    builder.add(Arguments.of(new ExprFloatValue(1F), new ExprFloatValue(1F)));
    builder.add(Arguments.of(new ExprDoubleValue(1D), new ExprDoubleValue(1D)));
    builder.add(Arguments.of(new ExprStringValue("str"), new ExprStringValue("str")));
    builder.add(Arguments.of(ExprBooleanValue.of(true), ExprBooleanValue.of(true)));
    builder.add(Arguments.of(new ExprCollectionValue(ImmutableList.of(new ExprIntegerValue(1))),
        new ExprCollectionValue(ImmutableList.of(new ExprIntegerValue(1)))));
    builder.add(Arguments.of(ExprTupleValue.fromExprValueMap(ImmutableMap.of("str",
        new ExprIntegerValue(1))),
        ExprTupleValue.fromExprValueMap(ImmutableMap.of("str", new ExprIntegerValue(1)))));
    return builder.build();
  }

  private static Stream<Arguments> testNotEqualArguments() {
    List<List<Object>> arguments = Arrays.asList(
        Arrays.asList((byte) 1, (byte) 2), Arrays.asList(1, 2), Arrays.asList(1L, 2L),
        Arrays.asList(1F, 2F), Arrays.asList(1D, 2D),
        Arrays.asList("str0", "str1"), Arrays.asList(true, false),
        Arrays.asList(ImmutableList.of(1), ImmutableList.of(2)),
        Arrays.asList(ImmutableMap.of("str", 1), ImmutableMap.of("str", 2))
    );
    Stream.Builder<Arguments> builder = Stream.builder();
    for (List<Object> argPair : arguments) {
      builder.add(Arguments.of(fromObjectValue(argPair.get(0)), fromObjectValue(argPair.get(1))));
    }
    return builder.build();
  }

  private static Stream<Arguments> testCompareValueArguments() {
    List<List<Object>> arguments = Arrays.asList(
        Arrays.asList(1, 1), Arrays.asList(1, 2), Arrays.asList(2, 1),
        Arrays.asList(1L, 1L), Arrays.asList(1L, 2L), Arrays.asList(2L, 1L),
        Arrays.asList(1F, 1F), Arrays.asList(1F, 2F), Arrays.asList(2F, 1F),
        Arrays.asList(1D, 1D), Arrays.asList(1D, 2D), Arrays.asList(2D, 1D),
        Arrays.asList("str", "str"), Arrays.asList("str", "str0"), Arrays.asList("str0", "str")
    );
    Stream.Builder<Arguments> builder = Stream.builder();
    for (List<Object> argPair : arguments) {
      builder.add(Arguments.of(fromObjectValue(argPair.get(0)), fromObjectValue(argPair.get(1))));
    }
    builder.add(Arguments.of(new ExprShortValue(1), new ExprShortValue(1)));
    builder.add(Arguments.of(new ExprShortValue(1), new ExprShortValue(2)));
    builder.add(Arguments.of(new ExprShortValue(2), new ExprShortValue(1)));
    return builder.build();
  }

  private static Stream<Arguments> testLikeArguments() {
    List<List<String>> arguments = Arrays.asList(
        Arrays.asList("foo", "foo"), Arrays.asList("notFoo", "foo"),
        Arrays.asList("foobar", "%bar"), Arrays.asList("bar", "%bar"),
        Arrays.asList("foo", "fo_"), Arrays.asList("foo", "foo_"),
        Arrays.asList("foorbar", "%o_ar"), Arrays.asList("foobar", "%o_a%"),
        Arrays.asList("fooba%_\\^$.*[]()|+r", "%\\%\\_\\\\\\^\\$\\.\\*\\[\\]\\(\\)\\|\\+_")
    );
    Stream.Builder<Arguments> builder = Stream.builder();
    for (List<String> argPair : arguments) {
      builder.add(Arguments.of(fromObjectValue(argPair.get(0)), fromObjectValue(argPair.get(1))));
    }
    return builder.build();
  }

  @ParameterizedTest(name = "and({0}, {1})")
  @MethodSource("binaryPredicateArguments")
  public void test_and(Boolean v1, Boolean v2) {
    FunctionExpression and =
        DSL.and(DSL.literal(booleanValue(v1)), DSL.literal(booleanValue(v2)));
    assertEquals(BOOLEAN, and.type());
    assertEquals(v1 && v2, ExprValueUtils.getBooleanValue(and.valueOf(valueEnv())));
    assertEquals(String.format("and(%s, %s)", v1.toString(), v2.toString()), and.toString());
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
    FunctionExpression and = DSL.and(DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN),
        DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, and.type());
    assertEquals(LITERAL_MISSING, and.valueOf(valueEnv()));

    and = DSL.and(DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN),
        DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, and.type());
    assertEquals(LITERAL_NULL, and.valueOf(valueEnv()));

    and = DSL.and(DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN),
        DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, and.type());
    assertEquals(LITERAL_MISSING, and.valueOf(valueEnv()));

    and = DSL.and(DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN),
        DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, and.type());
    assertEquals(LITERAL_MISSING, and.valueOf(valueEnv()));
  }

  @ParameterizedTest(name = "or({0}, {1})")
  @MethodSource("binaryPredicateArguments")
  public void test_or(Boolean v1, Boolean v2) {
    FunctionExpression or =
        DSL.or(DSL.literal(booleanValue(v1)), DSL.literal(booleanValue(v2)));
    assertEquals(BOOLEAN, or.type());
    assertEquals(v1 || v2, ExprValueUtils.getBooleanValue(or.valueOf(valueEnv())));
    assertEquals(String.format("or(%s, %s)", v1.toString(), v2.toString()), or.toString());
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
    FunctionExpression or = DSL.or(DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN),
        DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, or.type());
    assertEquals(LITERAL_MISSING, or.valueOf(valueEnv()));

    or =
        DSL.or(DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN),
            DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, or.type());
    assertEquals(LITERAL_NULL, or.valueOf(valueEnv()));

    or = DSL.or(DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN),
        DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, or.type());
    assertEquals(LITERAL_NULL, or.valueOf(valueEnv()));

    or = DSL.or(DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN),
        DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, or.type());
    assertEquals(LITERAL_NULL, or.valueOf(valueEnv()));
  }


  @ParameterizedTest(name = "xor({0}, {1})")
  @MethodSource("binaryPredicateArguments")
  public void test_xor(Boolean v1, Boolean v2) {
    FunctionExpression xor =
        DSL.xor(DSL.literal(booleanValue(v1)), DSL.literal(booleanValue(v2)));
    assertEquals(BOOLEAN, xor.type());
    assertEquals(v1 ^ v2, ExprValueUtils.getBooleanValue(xor.valueOf(valueEnv())));
    assertEquals(String.format("xor(%s, %s)", v1.toString(), v2.toString()), xor.toString());
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
    FunctionExpression xor = DSL.xor(DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN),
        DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, xor.type());
    assertEquals(LITERAL_MISSING, xor.valueOf(valueEnv()));

    xor = DSL.xor(DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN),
        DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, xor.type());
    assertEquals(LITERAL_NULL, xor.valueOf(valueEnv()));

    xor = DSL.xor(DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN),
        DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, xor.type());
    assertEquals(LITERAL_NULL, xor.valueOf(valueEnv()));

    xor = DSL.xor(DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN),
        DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, xor.type());
    assertEquals(LITERAL_NULL, xor.valueOf(valueEnv()));
  }

  @ParameterizedTest(name = "equal({0}, {1})")
  @MethodSource("testEqualArguments")
  public void test_equal(ExprValue v1, ExprValue v2) {
    FunctionExpression equal = DSL.equal(DSL.literal(v1), DSL.literal(v2));
    assertEquals(BOOLEAN, equal.type());
    assertEquals(v1.value().equals(v2.value()),
        ExprValueUtils.getBooleanValue(equal.valueOf(valueEnv())));
    assertEquals(String.format("=(%s, %s)", v1.toString(), v2.toString()), equal.toString());
  }

  @Test
  public void test_null_equal_missing() {
    FunctionExpression equal = DSL.equal(DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN),
        DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, equal.type());
    assertEquals(LITERAL_MISSING, equal.valueOf(valueEnv()));

    equal = DSL.equal(DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN),
        DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, equal.type());
    assertEquals(LITERAL_NULL, equal.valueOf(valueEnv()));

    equal = DSL.equal(DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN),
        DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, equal.type());
    assertEquals(LITERAL_MISSING, equal.valueOf(valueEnv()));

    equal = DSL.equal(DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN),
        DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, equal.type());
    assertEquals(LITERAL_MISSING, equal.valueOf(valueEnv()));

    equal = DSL.equal(DSL.literal(LITERAL_TRUE), DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, equal.type());
    assertEquals(LITERAL_MISSING, equal.valueOf(valueEnv()));

    equal = DSL.equal(DSL.literal(LITERAL_TRUE), DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, equal.type());
    assertEquals(LITERAL_NULL, equal.valueOf(valueEnv()));

    equal = DSL.equal(DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN), DSL.literal(LITERAL_TRUE));
    assertEquals(BOOLEAN, equal.type());
    assertEquals(LITERAL_MISSING, equal.valueOf(valueEnv()));

    equal = DSL.equal(DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN), DSL.literal(LITERAL_TRUE));
    assertEquals(BOOLEAN, equal.type());
    assertEquals(LITERAL_NULL, equal.valueOf(valueEnv()));
  }

  @ParameterizedTest(name = "equal({0}, {1})")
  @MethodSource({"testEqualArguments", "testNotEqualArguments"})
  public void test_notequal(ExprValue v1, ExprValue v2) {
    FunctionExpression notequal = DSL.notequal(DSL.literal(v1), DSL.literal(v2));
    assertEquals(BOOLEAN, notequal.type());
    assertEquals(!v1.value().equals(v2.value()),
        ExprValueUtils.getBooleanValue(notequal.valueOf(valueEnv())));
    assertEquals(String.format("!=(%s, %s)", v1.toString(), v2.toString()), notequal.toString());
  }

  @Test
  public void test_null_notequal_missing() {
    FunctionExpression notequal = DSL.notequal(DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN),
        DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, notequal.type());
    assertEquals(LITERAL_MISSING, notequal.valueOf(valueEnv()));

    notequal = DSL.notequal(DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN),
        DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, notequal.type());
    assertEquals(LITERAL_NULL, notequal.valueOf(valueEnv()));

    notequal = DSL.notequal(DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN),
        DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, notequal.type());
    assertEquals(LITERAL_MISSING, notequal.valueOf(valueEnv()));

    notequal = DSL.notequal(DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN),
        DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, notequal.type());
    assertEquals(LITERAL_MISSING, notequal.valueOf(valueEnv()));

    notequal = DSL.notequal(DSL.literal(LITERAL_TRUE),
        DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, notequal.type());
    assertEquals(LITERAL_MISSING, notequal.valueOf(valueEnv()));

    notequal = DSL.notequal(DSL.literal(LITERAL_TRUE),
        DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN));
    assertEquals(BOOLEAN, notequal.type());
    assertEquals(LITERAL_NULL, notequal.valueOf(valueEnv()));

    notequal = DSL.notequal(DSL.ref(BOOL_TYPE_MISSING_VALUE_FIELD, BOOLEAN),
        DSL.literal(LITERAL_TRUE));
    assertEquals(BOOLEAN, notequal.type());
    assertEquals(LITERAL_MISSING, notequal.valueOf(valueEnv()));

    notequal = DSL.notequal(DSL.ref(BOOL_TYPE_NULL_VALUE_FIELD, BOOLEAN),
        DSL.literal(LITERAL_TRUE));
    assertEquals(BOOLEAN, notequal.type());
    assertEquals(LITERAL_NULL, notequal.valueOf(valueEnv()));
  }

  @ParameterizedTest(name = "less({0}, {1})")
  @MethodSource("testCompareValueArguments")
  public void test_less(ExprValue v1, ExprValue v2) {
    FunctionExpression less = DSL.less(DSL.literal(v1), DSL.literal(v2));
    assertEquals(BOOLEAN, less.type());
    assertEquals(compare(v1, v2) < 0,
        ExprValueUtils.getBooleanValue(less.valueOf(valueEnv())));
    assertEquals(String.format("<(%s, %s)", v1.toString(), v2.toString()), less.toString());
  }

  @Test
  public void test_less_null() {
    FunctionExpression less = DSL.less(DSL.literal(1),
        DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, less.type());
    assertEquals(LITERAL_NULL, less.valueOf(valueEnv()));

    less = DSL.less(DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER), DSL.literal(1));
    assertEquals(BOOLEAN, less.type());
    assertEquals(LITERAL_NULL, less.valueOf(valueEnv()));

    less = DSL.less(DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER),
        DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, less.type());
    assertEquals(LITERAL_NULL, less.valueOf(valueEnv()));
  }

  @Test
  public void test_less_missing() {
    FunctionExpression less = DSL.less(DSL.literal(1),
        DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, less.type());
    assertEquals(LITERAL_MISSING, less.valueOf(valueEnv()));

    less = DSL.less(DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER), DSL.literal(1));
    assertEquals(BOOLEAN, less.type());
    assertEquals(LITERAL_MISSING, less.valueOf(valueEnv()));

    less = DSL.less(DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER),
        DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, less.type());
    assertEquals(LITERAL_MISSING, less.valueOf(valueEnv()));
  }

  @Test
  public void test_null_less_missing() {
    FunctionExpression less = DSL.less(DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER),
        DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, less.type());
    assertEquals(LITERAL_MISSING, less.valueOf(valueEnv()));

    less = DSL.less(DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER),
        DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, less.type());
    assertEquals(LITERAL_MISSING, less.valueOf(valueEnv()));
  }

  @ParameterizedTest(name = "lte({0}, {1})")
  @MethodSource("testCompareValueArguments")
  public void test_lte(ExprValue v1, ExprValue v2) {
    FunctionExpression lte = DSL.lte(DSL.literal(v1), DSL.literal(v2));
    assertEquals(BOOLEAN, lte.type());
    assertEquals(compare(v1, v2) <= 0,
        ExprValueUtils.getBooleanValue(lte.valueOf(valueEnv())));
    assertEquals(String.format("<=(%s, %s)", v1.toString(), v2.toString()), lte.toString());
  }

  @Test
  public void test_lte_null() {
    FunctionExpression lte = DSL.lte(DSL.literal(1),
        DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, lte.type());
    assertEquals(LITERAL_NULL, lte.valueOf(valueEnv()));

    lte = DSL.lte(DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER), DSL.literal(1));
    assertEquals(BOOLEAN, lte.type());
    assertEquals(LITERAL_NULL, lte.valueOf(valueEnv()));

    lte = DSL.lte(DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER),
        DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, lte.type());
    assertEquals(LITERAL_NULL, lte.valueOf(valueEnv()));
  }

  @Test
  public void test_lte_missing() {
    FunctionExpression lte = DSL.lte(DSL.literal(1),
        DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, lte.type());
    assertEquals(LITERAL_MISSING, lte.valueOf(valueEnv()));

    lte = DSL.lte(DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER), DSL.literal(1));
    assertEquals(BOOLEAN, lte.type());
    assertEquals(LITERAL_MISSING, lte.valueOf(valueEnv()));

    lte = DSL.lte(DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER),
        DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, lte.type());
    assertEquals(LITERAL_MISSING, lte.valueOf(valueEnv()));
  }

  @Test
  public void test_null_lte_missing() {
    FunctionExpression lte = DSL.lte(DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER),
        DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, lte.type());
    assertEquals(LITERAL_MISSING, lte.valueOf(valueEnv()));

    lte = DSL.lte(DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER),
        DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, lte.type());
    assertEquals(LITERAL_MISSING, lte.valueOf(valueEnv()));
  }

  @ParameterizedTest(name = "greater({0}, {1})")
  @MethodSource("testCompareValueArguments")
  public void test_greater(ExprValue v1, ExprValue v2) {
    FunctionExpression greater = DSL.greater(DSL.literal(v1), DSL.literal(v2));
    assertEquals(BOOLEAN, greater.type());
    assertEquals(compare(v1, v2) > 0,
        ExprValueUtils.getBooleanValue(greater.valueOf(valueEnv())));
    assertEquals(String.format(">(%s, %s)", v1.toString(), v2.toString()), greater.toString());
  }

  @Test
  public void test_greater_null() {
    FunctionExpression greater = DSL.greater(DSL.literal(1),
        DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, greater.type());
    assertEquals(LITERAL_NULL, greater.valueOf(valueEnv()));

    greater = DSL.greater(DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER), DSL.literal(1));
    assertEquals(BOOLEAN, greater.type());
    assertEquals(LITERAL_NULL, greater.valueOf(valueEnv()));

    greater = DSL.greater(DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER),
        DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, greater.type());
    assertEquals(LITERAL_NULL, greater.valueOf(valueEnv()));
  }

  @Test
  public void test_greater_missing() {
    FunctionExpression greater = DSL.greater(DSL.literal(1),
        DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, greater.type());
    assertEquals(LITERAL_MISSING, greater.valueOf(valueEnv()));

    greater = DSL.greater(DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER), DSL.literal(1));
    assertEquals(BOOLEAN, greater.type());
    assertEquals(LITERAL_MISSING, greater.valueOf(valueEnv()));

    greater = DSL.greater(DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER),
        DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, greater.type());
    assertEquals(LITERAL_MISSING, greater.valueOf(valueEnv()));
  }

  @Test
  public void test_null_greater_missing() {
    FunctionExpression greater = DSL.greater(DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER),
        DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, greater.type());
    assertEquals(LITERAL_MISSING, greater.valueOf(valueEnv()));

    greater = DSL.greater(DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER),
        DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, greater.type());
    assertEquals(LITERAL_MISSING, greater.valueOf(valueEnv()));
  }

  @ParameterizedTest(name = "gte({0}, {1})")
  @MethodSource("testCompareValueArguments")
  public void test_gte(ExprValue v1, ExprValue v2) {
    FunctionExpression gte = DSL.gte(DSL.literal(v1), DSL.literal(v2));
    assertEquals(BOOLEAN, gte.type());
    assertEquals(compare(v1, v2) >= 0,
        ExprValueUtils.getBooleanValue(gte.valueOf(valueEnv())));
    assertEquals(String.format(">=(%s, %s)", v1.toString(), v2.toString()), gte.toString());
  }

  @Test
  public void test_gte_null() {
    FunctionExpression gte = DSL.gte(DSL.literal(1),
        DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, gte.type());
    assertEquals(LITERAL_NULL, gte.valueOf(valueEnv()));

    gte = DSL.gte(DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER), DSL.literal(1));
    assertEquals(BOOLEAN, gte.type());
    assertEquals(LITERAL_NULL, gte.valueOf(valueEnv()));

    gte = DSL.gte(DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER),
        DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, gte.type());
    assertEquals(LITERAL_NULL, gte.valueOf(valueEnv()));
  }

  @Test
  public void test_gte_missing() {
    FunctionExpression gte = DSL.gte(DSL.literal(1),
        DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, gte.type());
    assertEquals(LITERAL_MISSING, gte.valueOf(valueEnv()));

    gte = DSL.gte(DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER), DSL.literal(1));
    assertEquals(BOOLEAN, gte.type());
    assertEquals(LITERAL_MISSING, gte.valueOf(valueEnv()));

    gte = DSL.gte(DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER),
        DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, gte.type());
    assertEquals(LITERAL_MISSING, gte.valueOf(valueEnv()));
  }

  @Test
  public void test_null_gte_missing() {
    FunctionExpression gte = DSL.gte(DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER),
        DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, gte.type());
    assertEquals(LITERAL_MISSING, gte.valueOf(valueEnv()));

    gte = DSL.gte(DSL.ref(INT_TYPE_MISSING_VALUE_FIELD, INTEGER),
        DSL.ref(INT_TYPE_NULL_VALUE_FIELD, INTEGER));
    assertEquals(BOOLEAN, gte.type());
    assertEquals(LITERAL_MISSING, gte.valueOf(valueEnv()));
  }

  @ParameterizedTest(name = "like({0}, {1})")
  @MethodSource("testLikeArguments")
  public void test_like(ExprValue v1, ExprValue v2) {
    FunctionExpression like = DSL.like(DSL.literal(v1), DSL.literal(v2));
    assertEquals(BOOLEAN, like.type());
    assertEquals(matches(v1, v2), like.valueOf(valueEnv()));
    assertEquals(String.format("like(%s, %s)", v1.toString(), v2.toString()), like.toString());
  }

  @Test
  public void test_like_null() {
    FunctionExpression like =
        DSL.like(DSL.literal("str"), DSL.ref(STRING_TYPE_NULL_VALUE_FIELD, STRING));
    assertEquals(BOOLEAN, like.type());
    assertEquals(LITERAL_NULL, like.valueOf(valueEnv()));

    like = DSL.like(DSL.ref(STRING_TYPE_NULL_VALUE_FIELD, STRING), DSL.literal("str"));
    assertEquals(BOOLEAN, like.type());
    assertEquals(LITERAL_NULL, like.valueOf(valueEnv()));

    like = DSL.like(DSL.ref(STRING_TYPE_NULL_VALUE_FIELD, STRING),
        DSL.ref(STRING_TYPE_NULL_VALUE_FIELD, STRING));
    assertEquals(BOOLEAN, like.type());
    assertEquals(LITERAL_NULL, like.valueOf(valueEnv()));
  }

  @Test
  public void test_like_missing() {
    FunctionExpression like =
        DSL.like(DSL.literal("str"), DSL.ref(STRING_TYPE_MISSING_VALUE_FIELD, STRING));
    assertEquals(BOOLEAN, like.type());
    assertEquals(LITERAL_MISSING, like.valueOf(valueEnv()));

    like = DSL.like(DSL.ref(STRING_TYPE_MISSING_VALUE_FIELD, STRING), DSL.literal("str"));
    assertEquals(BOOLEAN, like.type());
    assertEquals(LITERAL_MISSING, like.valueOf(valueEnv()));

    like = DSL.like(DSL.ref(STRING_TYPE_MISSING_VALUE_FIELD, STRING),
        DSL.ref(STRING_TYPE_MISSING_VALUE_FIELD, STRING));
    assertEquals(BOOLEAN, like.type());
    assertEquals(LITERAL_MISSING, like.valueOf(valueEnv()));
  }

  @Test
  public void test_null_like_missing() {
    FunctionExpression like = DSL.like(DSL.ref(STRING_TYPE_NULL_VALUE_FIELD, STRING),
        DSL.ref(STRING_TYPE_MISSING_VALUE_FIELD, STRING));
    assertEquals(BOOLEAN, like.type());
    assertEquals(LITERAL_MISSING, like.valueOf(valueEnv()));

    like = DSL.like(DSL.ref(STRING_TYPE_MISSING_VALUE_FIELD, STRING),
        DSL.ref(STRING_TYPE_NULL_VALUE_FIELD, STRING));
    assertEquals(BOOLEAN, like.type());
    assertEquals(LITERAL_MISSING, like.valueOf(valueEnv()));
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
    FunctionExpression expression = DSL.regexp(
            DSL.literal(new ExprStringValue(stringPatternPair.getStr())),
            DSL.literal(new ExprStringValue(stringPatternPair.getPatt())));
    assertEquals(INTEGER, expression.type());
    assertEquals(stringPatternPair.regExpTest(), expression
        .valueOf(valueEnv()).integerValue());
  }

  /**
   * Todo. remove this test cases after script serilization implemented.
   */
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

  private boolean eval(Expression expr) {
    return expr.valueOf().booleanValue();
  }
}
