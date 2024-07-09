/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.datasource.model.EmptyDataSourceService.getEmptyDataSourceService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanDSL;

class ExpressionReferenceOptimizerTest extends AnalyzerTestBase {

  @Test
  void expression_without_aggregation_should_not_be_replaced() {
    assertEquals(
        DSL.subtract(DSL.ref("age", INTEGER), DSL.literal(1)),
        optimize(DSL.subtract(DSL.ref("age", INTEGER), DSL.literal(1))));
  }

  @Test
  void group_expression_should_be_replaced() {
    assertEquals(DSL.ref("abs(balance)", INTEGER), optimize(DSL.abs(DSL.ref("balance", INTEGER))));
  }

  @Test
  void aggregation_expression_should_be_replaced() {
    assertEquals(DSL.ref("AVG(age)", DOUBLE), optimize(DSL.avg(DSL.ref("age", INTEGER))));
  }

  @Test
  void aggregation_in_expression_should_be_replaced() {
    assertEquals(
        DSL.subtract(DSL.ref("AVG(age)", DOUBLE), DSL.literal(1)),
        optimize(DSL.subtract(DSL.avg(DSL.ref("age", INTEGER)), DSL.literal(1))));
  }

  @Test
  void case_clause_should_be_replaced() {
    Expression caseClause =
        DSL.cases(
            null,
            DSL.when(DSL.equal(DSL.ref("age", INTEGER), DSL.literal(30)), DSL.literal("true")));

    LogicalPlan logicalPlan =
        LogicalPlanDSL.aggregation(
            LogicalPlanDSL.relation("test", table),
            emptyList(),
            ImmutableList.of(
                DSL.named(
                    "CaseClause(whenClauses=[WhenClause(condition==(age, 30), result=\"true\")],"
                        + " defaultResult=null)",
                    caseClause)));

    assertEquals(
        DSL.ref(
            "CaseClause(whenClauses=[WhenClause(condition==(age, 30), result=\"true\")],"
                + " defaultResult=null)",
            STRING),
        optimize(caseClause, logicalPlan));
  }

  @Test
  void aggregation_in_case_when_clause_should_be_replaced() {
    Expression caseClause =
        DSL.cases(
            null,
            DSL.when(
                DSL.equal(DSL.avg(DSL.ref("age", INTEGER)), DSL.literal(30)), DSL.literal("true")));

    LogicalPlan logicalPlan =
        LogicalPlanDSL.aggregation(
            LogicalPlanDSL.relation("test", table),
            ImmutableList.of(DSL.named("AVG(age)", DSL.avg(DSL.ref("age", INTEGER)))),
            ImmutableList.of(DSL.named("name", DSL.ref("name", STRING))));

    assertEquals(
        DSL.cases(
            null,
            DSL.when(DSL.equal(DSL.ref("AVG(age)", DOUBLE), DSL.literal(30)), DSL.literal("true"))),
        optimize(caseClause, logicalPlan));
  }

  @Test
  void aggregation_in_case_else_clause_should_be_replaced() {
    Expression caseClause =
        DSL.cases(
            DSL.avg(DSL.ref("age", INTEGER)),
            DSL.when(DSL.equal(DSL.ref("age", INTEGER), DSL.literal(30)), DSL.literal("true")));

    LogicalPlan logicalPlan =
        LogicalPlanDSL.aggregation(
            LogicalPlanDSL.relation("test", table),
            ImmutableList.of(DSL.named("AVG(age)", DSL.avg(DSL.ref("age", INTEGER)))),
            ImmutableList.of(DSL.named("name", DSL.ref("name", STRING))));

    assertEquals(
        DSL.cases(
            DSL.ref("AVG(age)", DOUBLE),
            DSL.when(DSL.equal(DSL.ref("age", INTEGER), DSL.literal(30)), DSL.literal("true"))),
        optimize(caseClause, logicalPlan));
  }

  @Test
  void window_expression_should_be_replaced() {
    LogicalPlan logicalPlan =
        LogicalPlanDSL.window(
            LogicalPlanDSL.window(
                LogicalPlanDSL.relation("test", table),
                DSL.named(DSL.rank()),
                new WindowDefinition(emptyList(), emptyList())),
            DSL.named(DSL.denseRank()),
            new WindowDefinition(emptyList(), emptyList()));

    assertEquals(DSL.ref("rank()", INTEGER), optimize(DSL.rank(), logicalPlan));
    assertEquals(DSL.ref("dense_rank()", INTEGER), optimize(DSL.denseRank(), logicalPlan));
  }

  Expression optimize(Expression expression) {
    return optimize(expression, logicalPlan());
  }

  Expression optimize(Expression expression, LogicalPlan logicalPlan) {
    BuiltinFunctionRepository functionRepository =
        BuiltinFunctionRepository.getInstance(getEmptyDataSourceService());
    final ExpressionReferenceOptimizer optimizer =
        new ExpressionReferenceOptimizer(functionRepository, logicalPlan);
    return optimizer.optimize(DSL.named(expression), new AnalysisContext());
  }

  LogicalPlan logicalPlan() {
    return LogicalPlanDSL.aggregation(
        LogicalPlanDSL.relation("schema", table),
        ImmutableList.of(
            DSL.named("AVG(age)", DSL.avg(DSL.ref("age", INTEGER))),
            DSL.named("SUM(age)", DSL.sum(DSL.ref("age", INTEGER)))),
        ImmutableList.of(
            DSL.named("balance", DSL.ref("balance", INTEGER)),
            DSL.named("abs(balance)", DSL.abs(DSL.ref("balance", INTEGER)))));
  }
}
