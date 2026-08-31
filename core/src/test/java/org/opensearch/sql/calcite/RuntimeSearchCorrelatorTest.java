/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class RuntimeSearchCorrelatorTest {

  @Test
  void returnsOriginalFilterWhenConditionHasNoImplicitFormatSubquery() {
    JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
    LogicalValues input = LogicalValues.createOneRow(cluster);
    LogicalFilter filter = LogicalFilter.create(input, rexBuilder.makeLiteral(true));

    RelNode result =
        RuntimeSearchCorrelator.correlate(filter, predicate -> predicate, subquery -> false);

    assertSame(filter, result);
  }

  @Test
  void correlatesMarkedScalarSubqueryWithNonPushdownInput() {
    JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
    RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType rowType = typeFactory.builder().add("search", varcharType).build();
    LogicalValues searchValue =
        LogicalValues.create(
            cluster,
            rowType,
            ImmutableList.of(ImmutableList.of(rexBuilder.makeLiteral("status=500"))));
    RexSubQuery implicitFormat = RexSubQuery.scalar(searchValue);
    LogicalFilter filter =
        LogicalFilter.create(
            searchValue,
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS, implicitFormat, rexBuilder.makeLiteral("status=500")));

    RelNode result =
        RuntimeSearchCorrelator.correlate(
            filter, predicate -> predicate, subquery -> subquery == implicitFormat);

    assertTrue(result instanceof LogicalProject);
    assertTrue(result.getInput(0) instanceof LogicalCorrelate);
  }

  @Test
  void rejectsNonFilterInputWithCustomerSafeMessage() {
    JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                RuntimeSearchCorrelator.correlate(
                    LogicalValues.createOneRow(cluster), predicate -> predicate, subquery -> true));

    assertEquals(
        "The search command could not apply the subsearch result.", exception.getMessage());
  }

  @Test
  void findImplicitFormatSubqueriesIgnoresUnmarkedScalarSubqueries() {
    JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
    RexSubQuery implicitFormat = RexSubQuery.scalar(LogicalValues.createOneRow(cluster));
    RexSubQuery ordinaryScalar = RexSubQuery.scalar(LogicalValues.createOneRow(cluster));
    RelDataType integerType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RexNode condition =
        rexBuilder.makeCall(
            integerType, SqlStdOperatorTable.PLUS, List.of(implicitFormat, ordinaryScalar));

    assertEquals(
        List.of(implicitFormat),
        RuntimeSearchCorrelator.findImplicitFormatSubqueries(
            condition, subquery -> subquery == implicitFormat));
  }

  @Test
  void findImplicitFormatSubqueriesIgnoresInAndExistsSubqueries() {
    JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
    LogicalValues values = LogicalValues.createOneRow(cluster);
    RexSubQuery exists = RexSubQuery.exists(values);
    RexSubQuery in =
        RexSubQuery.in(values, ImmutableList.of(rexBuilder.makeExactLiteral(BigDecimal.ZERO)));

    assertEquals(
        List.of(), RuntimeSearchCorrelator.findImplicitFormatSubqueries(exists, subquery -> false));
    assertEquals(
        List.of(), RuntimeSearchCorrelator.findImplicitFormatSubqueries(in, subquery -> false));
  }

  @Test
  void findImplicitFormatSubqueryAlongsideExistsInConditionalString() {
    JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
    RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType searchRowType = typeFactory.builder().add("search", varcharType).build();
    LogicalValues searchValue =
        LogicalValues.create(
            cluster,
            searchRowType,
            ImmutableList.of(ImmutableList.of(rexBuilder.makeLiteral("( host=\"web-1\" )"))));
    RexSubQuery implicitFormat = RexSubQuery.scalar(searchValue);
    RexSubQuery exists = RexSubQuery.exists(LogicalValues.createOneRow(cluster));
    RexNode conditionalSearch =
        rexBuilder.makeCall(
            varcharType,
            SqlStdOperatorTable.CASE,
            List.of(
                exists,
                rexBuilder.makeLiteral("status:200"),
                rexBuilder.makeLiteral("status:500")));
    RexNode condition =
        rexBuilder.makeCall(
            varcharType,
            SqlStdOperatorTable.CONCAT,
            List.of(
                rexBuilder.makeCall(
                    varcharType,
                    SqlStdOperatorTable.CONCAT,
                    List.of(conditionalSearch, rexBuilder.makeLiteral(" AND "))),
                implicitFormat));

    assertEquals(
        List.of(implicitFormat),
        RuntimeSearchCorrelator.findImplicitFormatSubqueries(
            condition, subquery -> subquery == implicitFormat));
  }

  @Test
  void findImplicitFormatSubqueryAlongsideInSubqueryCastToString() {
    JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
    RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType searchRowType = typeFactory.builder().add("search", varcharType).build();
    LogicalValues searchValue =
        LogicalValues.create(
            cluster,
            searchRowType,
            ImmutableList.of(ImmutableList.of(rexBuilder.makeLiteral("( host=\"web-1\" )"))));
    RexSubQuery implicitFormat = RexSubQuery.scalar(searchValue);
    RexSubQuery in =
        RexSubQuery.in(
            LogicalValues.createOneRow(cluster),
            ImmutableList.of(rexBuilder.makeExactLiteral(BigDecimal.ZERO)));
    RexNode condition =
        rexBuilder.makeCall(
            varcharType,
            SqlStdOperatorTable.CONCAT,
            List.of(rexBuilder.makeCast(varcharType, in), implicitFormat));

    assertEquals(
        List.of(implicitFormat),
        RuntimeSearchCorrelator.findImplicitFormatSubqueries(
            condition, subquery -> subquery == implicitFormat));
  }
}
