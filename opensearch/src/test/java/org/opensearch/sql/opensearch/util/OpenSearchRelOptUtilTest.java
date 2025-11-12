/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.core.Project;
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
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownContext;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownOperation;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownType;
import org.opensearch.sql.opensearch.storage.scan.context.SortExprDigest;

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

  @Test
  public void testSourceCollationSatisfiesTargetCollation_DirectInputRef() {
    // Source collation: col0 ASC
    // Target collation: col0 ASC (output index 0)
    Project project =
        createMockProject(
            Arrays.asList(
                rexBuilder.makeInputRef(inputType, 0), rexBuilder.makeInputRef(inputType, 1)));

    RelFieldCollation sourceCollation = new RelFieldCollation(0, Direction.ASCENDING);
    RelFieldCollation targetCollation = new RelFieldCollation(0, Direction.ASCENDING);

    assertTrue(
        OpenSearchRelOptUtil.sourceCollationSatisfiesTargetCollation(
            sourceCollation, targetCollation, project));
  }

  @Test
  public void testSourceCollationSatisfiesTargetCollation_DirectInputRefDescending() {
    // Source collation: col0 DESC
    // Target collation: col0 DESC (output index 0)
    Project project = createMockProject(Arrays.asList(rexBuilder.makeInputRef(inputType, 0)));

    RelFieldCollation sourceCollation = new RelFieldCollation(0, Direction.DESCENDING);
    RelFieldCollation targetCollation = new RelFieldCollation(0, Direction.DESCENDING);

    assertTrue(
        OpenSearchRelOptUtil.sourceCollationSatisfiesTargetCollation(
            sourceCollation, targetCollation, project));
  }

  @Test
  public void testSourceCollationSatisfiesTargetCollation_DirectionMismatch() {
    // Source collation: col0 ASC
    // Target collation: col0 DESC (output index 0)
    Project project = createMockProject(Arrays.asList(rexBuilder.makeInputRef(inputType, 0)));

    RelFieldCollation sourceCollation = new RelFieldCollation(0, Direction.ASCENDING);
    RelFieldCollation targetCollation = new RelFieldCollation(0, Direction.DESCENDING);

    assertFalse(
        OpenSearchRelOptUtil.sourceCollationSatisfiesTargetCollation(
            sourceCollation, targetCollation, project));
  }

  @Test
  public void testSourceCollationSatisfiesTargetCollation_IndexMismatch() {
    // Source collation: col0 ASC
    // Target collation: col1 ASC (output index 1)
    Project project =
        createMockProject(
            Arrays.asList(
                rexBuilder.makeInputRef(inputType, 0), rexBuilder.makeInputRef(inputType, 1)));

    RelFieldCollation sourceCollation = new RelFieldCollation(0, Direction.ASCENDING);
    RelFieldCollation targetCollation = new RelFieldCollation(1, Direction.ASCENDING);

    assertFalse(
        OpenSearchRelOptUtil.sourceCollationSatisfiesTargetCollation(
            sourceCollation, targetCollation, project));
  }

  @Test
  public void testSourceCollationSatisfiesTargetCollation_UnaryMinus() {
    // Source collation: col0 ASC
    // Target collation: -col0 DESC (output index 0)
    RexNode minusExpr =
        rexBuilder.makeCall(SqlStdOperatorTable.UNARY_MINUS, rexBuilder.makeInputRef(inputType, 0));
    Project project = createMockProject(Arrays.asList(minusExpr));

    RelFieldCollation sourceCollation = new RelFieldCollation(0, Direction.ASCENDING);
    RelFieldCollation targetCollation = new RelFieldCollation(0, Direction.DESCENDING);

    assertTrue(
        OpenSearchRelOptUtil.sourceCollationSatisfiesTargetCollation(
            sourceCollation, targetCollation, project));
  }

  @Test
  public void testSourceCollationSatisfiesTargetCollation_UnaryMinusWrongDirection() {
    // Source collation: col0 ASC
    // Target collation: -col0 ASC (output index 0) - should be DESC
    RexNode minusExpr =
        rexBuilder.makeCall(SqlStdOperatorTable.UNARY_MINUS, rexBuilder.makeInputRef(inputType, 0));
    Project project = createMockProject(Arrays.asList(minusExpr));

    RelFieldCollation sourceCollation = new RelFieldCollation(0, Direction.ASCENDING);
    RelFieldCollation targetCollation = new RelFieldCollation(0, Direction.ASCENDING);

    assertFalse(
        OpenSearchRelOptUtil.sourceCollationSatisfiesTargetCollation(
            sourceCollation, targetCollation, project));
  }

  @Test
  public void testSourceCollationSatisfiesTargetCollation_PlusLiteral() {
    // Source collation: col0 ASC
    // Target collation: (col0 + 10) ASC (output index 0)
    RexNode plusExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(inputType, 0),
            rexBuilder.makeLiteral(10, inputType));
    Project project = createMockProject(Arrays.asList(plusExpr));

    RelFieldCollation sourceCollation = new RelFieldCollation(0, Direction.ASCENDING);
    RelFieldCollation targetCollation = new RelFieldCollation(0, Direction.ASCENDING);

    assertTrue(
        OpenSearchRelOptUtil.sourceCollationSatisfiesTargetCollation(
            sourceCollation, targetCollation, project));
  }

  @Test
  public void testSourceCollationSatisfiesTargetCollation_MinusLiteralFirst() {
    // Source collation: col0 ASC
    // Target collation: (100 - col0) DESC (output index 0)
    RexNode minusExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MINUS,
            rexBuilder.makeLiteral(100, inputType),
            rexBuilder.makeInputRef(inputType, 0));
    Project project = createMockProject(Arrays.asList(minusExpr));

    RelFieldCollation sourceCollation = new RelFieldCollation(0, Direction.ASCENDING);
    RelFieldCollation targetCollation = new RelFieldCollation(0, Direction.DESCENDING);

    assertTrue(
        OpenSearchRelOptUtil.sourceCollationSatisfiesTargetCollation(
            sourceCollation, targetCollation, project));
  }

  @Test
  public void testSourceCollationSatisfiesTargetCollation_MultiplyNegative() {
    // Source collation: col0 ASC
    // Target collation: (col0 * -2) DESC (output index 0)
    RexNode timesExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.MULTIPLY,
            rexBuilder.makeInputRef(inputType, 0),
            rexBuilder.makeLiteral(-2, inputType));
    Project project = createMockProject(Arrays.asList(timesExpr));

    RelFieldCollation sourceCollation = new RelFieldCollation(0, Direction.ASCENDING);
    RelFieldCollation targetCollation = new RelFieldCollation(0, Direction.DESCENDING);

    assertTrue(
        OpenSearchRelOptUtil.sourceCollationSatisfiesTargetCollation(
            sourceCollation, targetCollation, project));
  }

  @Test
  public void testSourceCollationSatisfiesTargetCollation_ComplexExpression() {
    // Source collation: col0 ASC
    // Target collation: (col0 + col1) ASC (output index 0)
    RexNode plusExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(inputType, 0),
            rexBuilder.makeInputRef(inputType, 1));
    Project project = createMockProject(Arrays.asList(plusExpr));

    RelFieldCollation sourceCollation = new RelFieldCollation(0, Direction.ASCENDING);
    RelFieldCollation targetCollation = new RelFieldCollation(0, Direction.ASCENDING);

    assertFalse(
        OpenSearchRelOptUtil.sourceCollationSatisfiesTargetCollation(
            sourceCollation, targetCollation, project));
  }

  @Test
  public void testCanScanProvideSortCollation_EmptySortExprDigests() {
    AbstractCalciteIndexScan scan = createMockScanWithSort(Collections.emptyList());
    Project project = createMockProject(Arrays.asList(rexBuilder.makeInputRef(inputType, 0)));
    RelCollation collation = RelCollations.of(new RelFieldCollation(0, Direction.ASCENDING));

    assertFalse(OpenSearchRelOptUtil.canScanProvideSortCollation(scan, project, collation));
  }

  @Test
  public void testCanScanProvideSortCollation_InsufficientSortExprDigests() {
    // Scan has 1 sort expression, but collation requires 2
    RexNode scanExpr = rexBuilder.makeInputRef(inputType, 0);
    SortExprDigest sortDigest =
        new SortExprDigest(scanExpr, Direction.ASCENDING, NullDirection.LAST);
    AbstractCalciteIndexScan scan = createMockScanWithSort(Arrays.asList(sortDigest));

    Project project =
        createMockProject(
            Arrays.asList(
                rexBuilder.makeInputRef(inputType, 0), rexBuilder.makeInputRef(inputType, 1)));
    RelCollation collation =
        RelCollations.of(
            new RelFieldCollation(0, Direction.ASCENDING),
            new RelFieldCollation(1, Direction.ASCENDING));

    assertFalse(OpenSearchRelOptUtil.canScanProvideSortCollation(scan, project, collation));
  }

  @Test
  public void testCanScanProvideSortCollation_ExactMatch() {
    // Scan sorts by col0 ASC, project outputs col0, collation requires col0 ASC
    RexNode scanExpr = rexBuilder.makeInputRef(inputType, 0);
    RexNode projectExpr = rexBuilder.makeInputRef(inputType, 0);
    SortExprDigest sortDigest =
        new SortExprDigest(scanExpr, Direction.ASCENDING, NullDirection.LAST);
    AbstractCalciteIndexScan scan = createMockScanWithSort(Arrays.asList(sortDigest));

    Project project = createMockProject(Arrays.asList(projectExpr));
    RelCollation collation =
        RelCollations.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));

    assertTrue(OpenSearchRelOptUtil.canScanProvideSortCollation(scan, project, collation));
  }

  @Test
  public void testCanScanProvideSortCollation_DirectionMismatch() {
    // Scan sorts by col0 ASC, but collation requires col0 DESC
    RexNode scanExpr = rexBuilder.makeInputRef(inputType, 0);
    RexNode projectExpr = rexBuilder.makeInputRef(inputType, 0);
    SortExprDigest sortDigest =
        new SortExprDigest(scanExpr, Direction.ASCENDING, NullDirection.LAST);
    AbstractCalciteIndexScan scan = createMockScanWithSort(Arrays.asList(sortDigest));

    Project project = createMockProject(Arrays.asList(projectExpr));
    RelCollation collation =
        RelCollations.of(new RelFieldCollation(0, Direction.DESCENDING, NullDirection.LAST));

    assertFalse(OpenSearchRelOptUtil.canScanProvideSortCollation(scan, project, collation));
  }

  @Test
  public void testCanScanProvideSortCollation_NullDirectionMismatch() {
    // Scan sorts by col0 ASC NULLS LAST, but collation requires NULLS FIRST
    RexNode scanExpr = rexBuilder.makeInputRef(inputType, 0);
    RexNode projectExpr = rexBuilder.makeInputRef(inputType, 0);
    SortExprDigest sortDigest =
        new SortExprDigest(scanExpr, Direction.ASCENDING, NullDirection.LAST);
    AbstractCalciteIndexScan scan = createMockScanWithSort(Arrays.asList(sortDigest));

    Project project = createMockProject(Arrays.asList(projectExpr));
    RelCollation collation =
        RelCollations.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.FIRST));

    assertFalse(OpenSearchRelOptUtil.canScanProvideSortCollation(scan, project, collation));
  }

  @Test
  public void testCanScanProvideSortCollation_ProjectTransformation() {
    // Scan sorts by col0 ASC, project outputs -col0, collation requires -col0 DESC
    RexNode scanExpr = rexBuilder.makeInputRef(inputType, 0);
    RexNode projectExpr =
        rexBuilder.makeCall(SqlStdOperatorTable.UNARY_MINUS, rexBuilder.makeInputRef(inputType, 0));
    SortExprDigest sortDigest =
        new SortExprDigest(scanExpr, Direction.ASCENDING, NullDirection.LAST);
    AbstractCalciteIndexScan scan = createMockScanWithSort(Arrays.asList(sortDigest));

    Project project = createMockProject(Arrays.asList(projectExpr));
    RelCollation collation =
        RelCollations.of(new RelFieldCollation(0, Direction.DESCENDING, NullDirection.LAST));

    assertTrue(OpenSearchRelOptUtil.canScanProvideSortCollation(scan, project, collation));
  }

  @Test
  public void testCanScanProvideSortCollation_ExpressionMismatch() {
    // Scan sorts by col0, but project outputs col1
    RexNode scanExpr = rexBuilder.makeInputRef(inputType, 0);
    RexNode projectExpr = rexBuilder.makeInputRef(inputType, 1);
    SortExprDigest sortDigest =
        new SortExprDigest(scanExpr, Direction.ASCENDING, NullDirection.LAST);
    AbstractCalciteIndexScan scan = createMockScanWithSort(Arrays.asList(sortDigest));

    Project project = createMockProject(Arrays.asList(projectExpr));
    RelCollation collation =
        RelCollations.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));

    assertFalse(OpenSearchRelOptUtil.canScanProvideSortCollation(scan, project, collation));
  }

  @Test
  public void testCanScanProvideSortCollation_ComplexRexCall() {
    // Scan sorts by (col0 + col1) ASC, project outputs (col0 + col1), collation requires (col0 +
    // col1) ASC
    RexNode scanExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(inputType, 0),
            rexBuilder.makeInputRef(inputType, 1));
    RexNode projectExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(inputType, 0),
            rexBuilder.makeInputRef(inputType, 1));
    SortExprDigest sortDigest =
        new SortExprDigest(scanExpr, Direction.ASCENDING, NullDirection.LAST);
    AbstractCalciteIndexScan scan = createMockScanWithSort(Arrays.asList(sortDigest));

    Project project = createMockProject(Arrays.asList(projectExpr));
    RelCollation collation =
        RelCollations.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));

    assertTrue(OpenSearchRelOptUtil.canScanProvideSortCollation(scan, project, collation));
  }

  @Test
  public void testCanScanProvideSortCollation_ComplexRexCall_DifferentExpression() {
    // Scan sorts by (col0 + 10), but project outputs (col0 + 20) - should not match
    RexNode scanExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(inputType, 0),
            rexBuilder.makeLiteral(10, inputType));
    RexNode projectExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(inputType, 0),
            rexBuilder.makeLiteral(20, inputType));
    SortExprDigest sortDigest =
        new SortExprDigest(scanExpr, Direction.ASCENDING, NullDirection.LAST);
    AbstractCalciteIndexScan scan = createMockScanWithSort(Arrays.asList(sortDigest));

    Project project = createMockProject(Arrays.asList(projectExpr));
    RelCollation collation =
        RelCollations.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));

    assertFalse(OpenSearchRelOptUtil.canScanProvideSortCollation(scan, project, collation));
  }

  @Test
  public void testCanScanProvideSortCollation_ComplexRexCall_MixedSimpleAndComplex() {
    // Scan sorts by col0 ASC, (col1 + 5) DESC
    // Project outputs col0, (col1 + 5)
    // Collation requires col0 ASC, (col1 + 5) DESC
    RexNode scanExpr0 = rexBuilder.makeInputRef(inputType, 0);
    RexNode scanExpr1 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(inputType, 1),
            rexBuilder.makeLiteral(5, inputType));

    RexNode projectExpr0 = rexBuilder.makeInputRef(inputType, 0);
    RexNode projectExpr1 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(inputType, 1),
            rexBuilder.makeLiteral(5, inputType));

    SortExprDigest sortDigest0 =
        new SortExprDigest(scanExpr0, Direction.ASCENDING, NullDirection.LAST);
    SortExprDigest sortDigest1 =
        new SortExprDigest(scanExpr1, Direction.DESCENDING, NullDirection.FIRST);
    AbstractCalciteIndexScan scan = createMockScanWithSort(Arrays.asList(sortDigest0, sortDigest1));

    Project project = createMockProject(Arrays.asList(projectExpr0, projectExpr1));
    RelCollation collation =
        RelCollations.of(
            new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST),
            new RelFieldCollation(1, Direction.DESCENDING, NullDirection.FIRST));

    assertTrue(OpenSearchRelOptUtil.canScanProvideSortCollation(scan, project, collation));
  }

  @Test
  public void testCanScanProvideSortCollation_ComplexRexCall_PartialMatch() {
    // Scan sorts by (col0 + 10) ASC, col1 DESC
    // Project outputs (col0 + 10), col1
    // Collation requires only (col0 + 10) ASC - should match (prefix match)
    RexNode scanExpr0 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(inputType, 0),
            rexBuilder.makeLiteral(10, inputType));
    RexNode scanExpr1 = rexBuilder.makeInputRef(inputType, 1);

    RexNode projectExpr0 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(inputType, 0),
            rexBuilder.makeLiteral(10, inputType));
    RexNode projectExpr1 = rexBuilder.makeInputRef(inputType, 1);

    SortExprDigest sortDigest0 =
        new SortExprDigest(scanExpr0, Direction.ASCENDING, NullDirection.LAST);
    SortExprDigest sortDigest1 =
        new SortExprDigest(scanExpr1, Direction.DESCENDING, NullDirection.LAST);
    AbstractCalciteIndexScan scan = createMockScanWithSort(Arrays.asList(sortDigest0, sortDigest1));

    Project project = createMockProject(Arrays.asList(projectExpr0, projectExpr1));
    RelCollation collation =
        RelCollations.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));

    assertTrue(OpenSearchRelOptUtil.canScanProvideSortCollation(scan, project, collation));
  }

  private Project createMockProject(List<RexNode> projects) {
    Project project = mock(Project.class, org.mockito.Mockito.withSettings().lenient());
    when(project.getProjects()).thenReturn(projects);
    return project;
  }

  // Create mock scan with list of sortExprDigest
  private AbstractCalciteIndexScan createMockScanWithSort(List<SortExprDigest> sortDigests) {
    AbstractCalciteIndexScan scan =
        mock(AbstractCalciteIndexScan.class, org.mockito.Mockito.withSettings().lenient());
    PushDownContext context =
        mock(PushDownContext.class, org.mockito.Mockito.withSettings().lenient());
    PushDownOperation sortOperation =
        mock(PushDownOperation.class, org.mockito.Mockito.withSettings().lenient());

    when(scan.getPushDownContext()).thenReturn(context);
    when(context.stream()).thenReturn(Arrays.asList(sortOperation).stream());
    when(sortOperation.type()).thenReturn(PushDownType.SORT_EXPR);
    when(context.getDigestByType(PushDownType.SORT_EXPR)).thenReturn(sortDigests);

    // Mock the cluster and RexBuilder for getEffectiveExpression
    RelOptCluster cluster = mock(RelOptCluster.class, org.mockito.Mockito.withSettings().lenient());
    when(scan.getCluster()).thenReturn(cluster);
    when(cluster.getRexBuilder()).thenReturn(rexBuilder);

    // Mock the row type
    RelDataType rowType =
        typeFactory.builder().add("col0", inputType).add("col1", inputType).build();
    when(scan.getRowType()).thenReturn(rowType);

    return scan;
  }
}
