/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class OpenSearchRelOptUtilTest {
  private final RexBuilder rexBuilder;
  private final RelDataTypeFactory typeFactory;
  private RexInputRef inputRef1;
  private RexInputRef inputRef2;
  private RelDataType inputType;

  public OpenSearchRelOptUtilTest() {
    this.typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    this.rexBuilder = new RexBuilder(typeFactory);
  }

  @BeforeEach
  public void setUp() {
    inputType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    inputRef1 = rexBuilder.makeInputRef(inputType, 5);
    inputRef2 = rexBuilder.makeInputRef(inputType, 1);
  }

  @Test
  public void testGetOrderEquivalentInputInfo_InputRef() {
    Optional<Pair<Integer, Boolean>> result =
        OpenSearchRelOptUtil.getOrderEquivalentInputInfo(inputRef1);
    assertExpectedInputInfo(result, 5, false);
  }

  @Test
  public void testGetOrderEquivalentInputInfo_PlusPrefix() {
    RexNode plusPrefix = rexBuilder.makeCall(SqlStdOperatorTable.UNARY_PLUS, inputRef1);
    Optional<Pair<Integer, Boolean>> result =
        OpenSearchRelOptUtil.getOrderEquivalentInputInfo(plusPrefix);
    assertExpectedInputInfo(result, 5, false);
  }

  @Test
  public void testGetOrderEquivalentInputInfo_MinusPrefix() {
    RexNode minusPrefix = rexBuilder.makeCall(SqlStdOperatorTable.UNARY_MINUS, inputRef1);
    Optional<Pair<Integer, Boolean>> result =
        OpenSearchRelOptUtil.getOrderEquivalentInputInfo(minusPrefix);
    assertExpectedInputInfo(result, 5, true);
  }

  @Test
  public void testGetOrderEquivalentInputInfo_MinusPrefixWithAlreadyFlippedInput() {
    RexNode innerMinusPrefix = rexBuilder.makeCall(SqlStdOperatorTable.UNARY_MINUS, inputRef1);
    RexNode outerMinusPrefix =
        rexBuilder.makeCall(SqlStdOperatorTable.UNARY_MINUS, innerMinusPrefix);
    Optional<Pair<Integer, Boolean>> result =
        OpenSearchRelOptUtil.getOrderEquivalentInputInfo(outerMinusPrefix);
    assertExpectedInputInfo(result, 5, false);
  }

  @Test
  public void testGetOrderEquivalentInputInfo_PlusOrMinusWithLiteralSecond() {
    RexNode plus =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS, inputRef1, rexBuilder.makeLiteral(1, inputType));
    Optional<Pair<Integer, Boolean>> result =
        OpenSearchRelOptUtil.getOrderEquivalentInputInfo(plus);
    assertExpectedInputInfo(result, 5, false);

    RexNode minus =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MINUS, inputRef1, rexBuilder.makeLiteral(1, inputType));
    result = OpenSearchRelOptUtil.getOrderEquivalentInputInfo(minus);
    assertExpectedInputInfo(result, 5, false);
  }

  @Test
  public void testGetOrderEquivalentInputInfo_PlusTwoInputs() {
    RexNode plus = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, inputRef1, inputRef2);
    Optional<Pair<Integer, Boolean>> result =
        OpenSearchRelOptUtil.getOrderEquivalentInputInfo(plus);
    assertFalse(result.isPresent());
  }

  @Test
  public void testGetOrderEquivalentInputInfo_PlusTwoLiterals() {
    RexNode plus =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeLiteral(1, inputType),
            rexBuilder.makeLiteral(2, inputType));
    Optional<Pair<Integer, Boolean>> result =
        OpenSearchRelOptUtil.getOrderEquivalentInputInfo(plus);
    assertFalse(result.isPresent());
  }

  @Test
  public void testGetOrderEquivalentInputInfo_PlusWithLiteralFirst() {
    RexNode plus =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS, rexBuilder.makeLiteral(1, inputType), inputRef1);
    Optional<Pair<Integer, Boolean>> result =
        OpenSearchRelOptUtil.getOrderEquivalentInputInfo(plus);
    assertExpectedInputInfo(result, 5, false);
  }

  @Test
  public void testGetOrderEquivalentInputInfo_MinusWithLiteralFirst() {
    RexNode minus =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MINUS, rexBuilder.makeLiteral(1, inputType), inputRef1);
    Optional<Pair<Integer, Boolean>> result =
        OpenSearchRelOptUtil.getOrderEquivalentInputInfo(minus);
    assertExpectedInputInfo(result, 5, true);
  }

  @Test
  public void testGetOrderEquivalentInputInfo_TimesWithPositiveLiteral() {
    RexNode times =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MULTIPLY, inputRef1, rexBuilder.makeLiteral(1, inputType));
    Optional<Pair<Integer, Boolean>> result =
        OpenSearchRelOptUtil.getOrderEquivalentInputInfo(times);
    assertExpectedInputInfo(result, 5, false);
  }

  @Test
  public void testGetOrderEquivalentInputInfo_TimesWithNegativeLiteral() {
    RexNode times =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MULTIPLY, inputRef1, rexBuilder.makeLiteral(-1, inputType));
    Optional<Pair<Integer, Boolean>> result =
        OpenSearchRelOptUtil.getOrderEquivalentInputInfo(times);
    assertExpectedInputInfo(result, 5, true);
  }

  @Test
  public void testGetOrderEquivalentInputInfo_TimesWithZeroOrNullLiteral() {
    RexNode times =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MULTIPLY, inputRef1, rexBuilder.makeLiteral(0, inputType));
    Optional<Pair<Integer, Boolean>> result =
        OpenSearchRelOptUtil.getOrderEquivalentInputInfo(times);
    assertFalse(result.isPresent());

    times =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MULTIPLY, inputRef1, rexBuilder.makeNullLiteral(inputType));
    result = OpenSearchRelOptUtil.getOrderEquivalentInputInfo(times);
    assertFalse(result.isPresent());
  }

  @Test
  public void testGetOrderEquivalentInputInfo_TimesTwoInputs() {
    RexNode times = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, inputRef1, inputRef2);
    Optional<Pair<Integer, Boolean>> result =
        OpenSearchRelOptUtil.getOrderEquivalentInputInfo(times);
    assertFalse(result.isPresent());
  }

  @Test
  public void testGetOrderEquivalentInputInfo_CastOrderPreserving() {
    // Cast from integer to long
    RelDataType srcType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RexNode srcInput = rexBuilder.makeInputRef(srcType, 1);
    RelDataType dstType = typeFactory.createSqlType(SqlTypeName.BIGINT);
    RexNode cast = rexBuilder.makeCast(dstType, srcInput);
    Optional<Pair<Integer, Boolean>> result =
        OpenSearchRelOptUtil.getOrderEquivalentInputInfo(cast);
    assertExpectedInputInfo(result, 1, false);

    // Safe cast from integer to long
    RexNode safeCast = rexBuilder.makeCast(dstType, srcInput, false, true);
    result = OpenSearchRelOptUtil.getOrderEquivalentInputInfo(safeCast);
    assertExpectedInputInfo(result, 1, false);

    // Cast from date to timestamp
    srcType = typeFactory.createSqlType(SqlTypeName.DATE);
    srcInput = rexBuilder.makeInputRef(srcType, 1);
    dstType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    cast = rexBuilder.makeCast(dstType, srcInput);
    result = OpenSearchRelOptUtil.getOrderEquivalentInputInfo(cast);
    assertExpectedInputInfo(result, 1, false);

    // Cast from integer to double
    srcType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    srcInput = rexBuilder.makeInputRef(srcType, 1);
    dstType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
    cast = rexBuilder.makeCast(dstType, srcInput);
    result = OpenSearchRelOptUtil.getOrderEquivalentInputInfo(cast);
    assertExpectedInputInfo(result, 1, false);

    // Cast from integer to float
    srcType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    srcInput = rexBuilder.makeInputRef(srcType, 1);
    dstType = typeFactory.createSqlType(SqlTypeName.FLOAT);
    cast = rexBuilder.makeCast(dstType, srcInput);
    result = OpenSearchRelOptUtil.getOrderEquivalentInputInfo(cast);
    assertFalse(result.isPresent());

    // Cast from low precision to high precision
    srcType = typeFactory.createSqlType(SqlTypeName.DECIMAL);
    srcInput = rexBuilder.makeInputRef(srcType, 1);
    dstType =
        typeFactory.createSqlType(
            SqlTypeName.DECIMAL, srcType.getPrecision() + 4, srcType.getScale() + 4);
    cast = rexBuilder.makeCast(dstType, srcInput);
    result = OpenSearchRelOptUtil.getOrderEquivalentInputInfo(cast);
    assertExpectedInputInfo(result, 1, false);
  }

  @Test
  public void testGetOrderEquivalentInputInfo_UnsupportedOperation() {
    RexNode times = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, inputRef1, inputRef2);
    Optional<Pair<Integer, Boolean>> result =
        OpenSearchRelOptUtil.getOrderEquivalentInputInfo(times);
    assertFalse(result.isPresent());
  }

  private void assertExpectedInputInfo(
      Optional<Pair<Integer, Boolean>> result, int index, boolean flipped) {
    assertTrue(result.isPresent());
    assertEquals(index, result.get().getLeft().intValue());
    assertEquals(flipped, result.get().getRight());
  }

  @Test
  public void testScenario1() {
    List<String> input = Arrays.asList("a_b", "a.b");
    List<String> expected = Arrays.asList("a_b", "a_b0");
    List<String> result = OpenSearchRelOptUtil.resolveColumnNameConflicts(input);
    assertEquals(expected, result);
  }

  @Test
  public void testScenario2() {
    List<String> input = Arrays.asList("a_b", "a_b0", "a.b");
    List<String> expected = Arrays.asList("a_b", "a_b0", "a_b1");
    List<String> result = OpenSearchRelOptUtil.resolveColumnNameConflicts(input);
    assertEquals(expected, result);
  }

  @Test
  public void testScenario3() {
    List<String> input = Arrays.asList("a_b", "a_b1", "a.b");
    List<String> expected = Arrays.asList("a_b", "a_b1", "a_b0");
    List<String> result = OpenSearchRelOptUtil.resolveColumnNameConflicts(input);
    assertEquals(expected, result);
  }

  @Test
  public void testScenario4() {
    List<String> input = Arrays.asList("a_b0", "a.b0", "a.b1");
    List<String> expected = Arrays.asList("a_b0", "a_b00", "a_b1");
    List<String> result = OpenSearchRelOptUtil.resolveColumnNameConflicts(input);
    assertEquals(expected, result);
  }

  @Test
  public void testMultipleDots() {
    List<String> input = Arrays.asList("a.b.c", "a_b_c", "a.b.c");
    List<String> expected = Arrays.asList("a_b_c0", "a_b_c", "a_b_c1");
    List<String> result = OpenSearchRelOptUtil.resolveColumnNameConflicts(input);
    assertEquals(expected, result);
  }

  @Test
  public void testComplexScenario() {
    List<String> input = Arrays.asList("x", "x", "x", "x");
    List<String> expected = Arrays.asList("x", "x", "x", "x");
    List<String> result = OpenSearchRelOptUtil.resolveColumnNameConflicts(input);
    assertEquals(expected, result);
  }

  @Test
  public void testNoConflict() {
    List<String> input = Arrays.asList("col1", "col2", "col3");
    List<String> expected = Arrays.asList("col1", "col2", "col3");
    List<String> result = OpenSearchRelOptUtil.resolveColumnNameConflicts(input);
    assertEquals(expected, result);
  }

  @Test
  public void testMixedConflict() {
    List<String> input = Arrays.asList("a.b", "a_b", "a.b", "a_b0");
    List<String> expected = Arrays.asList("a_b1", "a_b", "a_b2", "a_b0");
    List<String> result = OpenSearchRelOptUtil.resolveColumnNameConflicts(input);
    assertEquals(expected, result);
  }

  @Test
  public void testOriginalNamesPreserved() {
    List<String> input = Arrays.asList("endpoint.ip", "account.id", "timestamp");
    List<String> expected = Arrays.asList("endpoint_ip", "account_id", "timestamp");
    List<String> result = OpenSearchRelOptUtil.resolveColumnNameConflicts(input);
    assertEquals(expected, result);
  }

  @Test
  public void testNoDots() {
    List<String> input = Arrays.asList("col1", "col2", "col3");
    List<String> expected = Arrays.asList("col1", "col2", "col3");
    List<String> result = OpenSearchRelOptUtil.resolveColumnNameConflicts(input);
    assertEquals(expected, result);
  }
}
