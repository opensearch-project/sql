/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate.shuttles;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

public class SkipRelValidationShuttleTest {

  private RexBuilder rexBuilder;

  @BeforeEach
  public void setUp() {
    rexBuilder = new RexBuilder(TYPE_FACTORY);
  }

  @Test
  public void testWidthBucketOnDatetimeTriggersSkip() {
    // Create WIDTH_BUCKET call with datetime operand
    RelDataType timestampType = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);
    RexInputRef timestampRef = new RexInputRef(0, timestampType);

    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RexLiteral buckets = rexBuilder.makeExactLiteral(BigDecimal.valueOf(10), intType);
    RexLiteral minVal = rexBuilder.makeExactLiteral(BigDecimal.valueOf(0), intType);
    RexLiteral maxVal = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100), intType);

    RexCall widthBucketCall =
        (RexCall)
            rexBuilder.makeCall(
                PPLBuiltinOperators.WIDTH_BUCKET, timestampRef, minVal, maxVal, buckets);

    // Test the predicate
    boolean shouldSkip =
        SkipRelValidationShuttle.SKIP_CALLS.stream().anyMatch(p -> p.test(widthBucketCall));
    assertTrue(shouldSkip);
  }

  @Test
  public void testWidthBucketOnNumericDoesNotTriggerSkip() {
    // Create WIDTH_BUCKET call with numeric operand
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RexInputRef numericRef = new RexInputRef(0, intType);

    RexLiteral minVal = rexBuilder.makeExactLiteral(BigDecimal.valueOf(0), intType);
    RexLiteral maxVal = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100), intType);
    RexLiteral buckets = rexBuilder.makeExactLiteral(BigDecimal.valueOf(10), intType);

    RexCall widthBucketCall =
        (RexCall)
            rexBuilder.makeCall(
                PPLBuiltinOperators.WIDTH_BUCKET, numericRef, minVal, maxVal, buckets);

    // Test the predicate
    boolean shouldSkip =
        SkipRelValidationShuttle.SKIP_CALLS.stream().anyMatch(p -> p.test(widthBucketCall));
    assertFalse(shouldSkip);
  }

  @Test
  public void testMultipleCaseInGroupByTriggersSkip() {
    // Create mocked LogicalAggregate with multiple CASE expressions in GROUP BY
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    // Create CASE expressions
    RexInputRef ageRef = new RexInputRef(0, intType);
    RexInputRef balanceRef = new RexInputRef(1, intType);
    RexLiteral literal30 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(30), intType);
    RexLiteral literal40 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(40), intType);

    // CASE WHEN age < 30 THEN 'young' ELSE 'old' END
    RexNode caseExpr1 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.CASE,
            rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, ageRef, literal30),
            rexBuilder.makeLiteral("young"),
            rexBuilder.makeLiteral("old"));

    // CASE WHEN balance < 40 THEN 'low' ELSE 'high' END
    RexNode caseExpr2 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.CASE,
            rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, balanceRef, literal40),
            rexBuilder.makeLiteral("low"),
            rexBuilder.makeLiteral("high"));

    // Mock LogicalProject
    LogicalProject project = mock(LogicalProject.class);
    when(project.getProjects()).thenReturn(List.of(caseExpr1, caseExpr2));

    // Mock LogicalAggregate
    LogicalAggregate aggregate = mock(LogicalAggregate.class);
    when(aggregate.getGroupCount()).thenReturn(2);
    when(aggregate.getInput()).thenReturn(project);

    // Test the predicate
    boolean shouldSkip =
        SkipRelValidationShuttle.SKIP_AGGREGATES.stream().anyMatch(p -> p.test(aggregate));
    assertTrue(shouldSkip);
  }

  @Test
  public void testEmptyTuplesTriggersSkip() {
    // Mock LogicalValues with empty tuples
    LogicalValues emptyValues = mock(LogicalValues.class);
    when(emptyValues.getTuples()).thenReturn(ImmutableList.of());

    // Test the predicate
    boolean shouldSkip =
        SkipRelValidationShuttle.SKIP_VALUES.stream().anyMatch(p -> p.test(emptyValues));
    assertTrue(shouldSkip);
  }

  @Test
  public void testNonEmptyTuplesDoesNotTriggerSkip() {
    // Mock LogicalValues with non-empty tuples
    LogicalValues nonEmptyValues = mock(LogicalValues.class);
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RexLiteral literal = rexBuilder.makeExactLiteral(BigDecimal.ONE, intType);
    when(nonEmptyValues.getTuples()).thenReturn(ImmutableList.of(ImmutableList.of(literal)));

    // Test the predicate
    boolean shouldSkip =
        SkipRelValidationShuttle.SKIP_VALUES.stream().anyMatch(p -> p.test(nonEmptyValues));
    assertFalse(shouldSkip);
  }

  @Test
  public void testWidthBucketWithDateTypeTriggersSkip() {
    // Create WIDTH_BUCKET call with date operand
    RelDataType dateType = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);
    RexInputRef dateRef = new RexInputRef(0, dateType);

    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RexLiteral buckets = rexBuilder.makeExactLiteral(BigDecimal.valueOf(10), intType);
    RexLiteral minVal = rexBuilder.makeExactLiteral(BigDecimal.valueOf(0), intType);
    RexLiteral maxVal = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100), intType);

    RexCall widthBucketCall =
        (RexCall)
            rexBuilder.makeCall(PPLBuiltinOperators.WIDTH_BUCKET, dateRef, minVal, maxVal, buckets);

    // Test the predicate
    boolean shouldSkip =
        SkipRelValidationShuttle.SKIP_CALLS.stream().anyMatch(p -> p.test(widthBucketCall));
    assertTrue(shouldSkip);
  }
}
