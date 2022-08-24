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

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanDSL;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@Configuration
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {ExpressionConfig.class, AnalyzerTest.class})
class ExpressionReferenceOptimizerTest extends AnalyzerTestBase {

  @Test
  void expression_without_aggregation_should_not_be_replaced() {
    assertEquals(
        dsl.subtract(DSL.ref("age", INTEGER), DSL.literal(1)),
        optimize(dsl.subtract(DSL.ref("age", INTEGER), DSL.literal(1)))
    );
  }

  @Test
  void group_expression_should_be_replaced() {
    assertEquals(
        DSL.ref("abs(balance)", INTEGER),
        optimize(dsl.abs(DSL.ref("balance", INTEGER)))
    );
  }

  @Test
  void aggregation_expression_should_be_replaced() {
    assertEquals(
        DSL.ref("AVG(age)", DOUBLE),
        optimize(dsl.avg(DSL.ref("age", INTEGER)))
    );
  }

  @Test
  void aggregation_in_expression_should_be_replaced() {
    assertEquals(
        dsl.subtract(DSL.ref("AVG(age)", DOUBLE), DSL.literal(1)),
        optimize(dsl.subtract(dsl.avg(DSL.ref("age", INTEGER)), DSL.literal(1)))
    );
  }

  @Test
  void case_clause_should_be_replaced() {
    Expression caseClause = DSL.cases(
        null,
        DSL.when(
            dsl.equal(DSL.ref("age", INTEGER), DSL.literal(30)),
            DSL.literal("true")));

    LogicalPlan logicalPlan =
        LogicalPlanDSL.aggregation(
            LogicalPlanDSL.relation("test", table),
            emptyList(),
            ImmutableList.of(DSL.named(
                "CaseClause(whenClauses=[WhenClause(condition==(age, 30), result=\"true\")],"
                    + " defaultResult=null)",
                caseClause)));

    assertEquals(
        DSL.ref(
            "CaseClause(whenClauses=[WhenClause(condition==(age, 30), result=\"true\")],"
                + " defaultResult=null)", STRING),
        optimize(caseClause, logicalPlan));
  }

  @Test
  void aggregation_in_case_when_clause_should_be_replaced() {
    Expression caseClause = DSL.cases(
        null,
        DSL.when(
            dsl.equal(dsl.avg(DSL.ref("age", INTEGER)), DSL.literal(30)),
            DSL.literal("true")));

    LogicalPlan logicalPlan =
        LogicalPlanDSL.aggregation(
            LogicalPlanDSL.relation("test", table),
            ImmutableList.of(DSL.named("AVG(age)", dsl.avg(DSL.ref("age", INTEGER)))),
            ImmutableList.of(DSL.named("name", DSL.ref("name", STRING))));

    assertEquals(
        DSL.cases(
            null,
            DSL.when(
                dsl.equal(DSL.ref("AVG(age)", DOUBLE), DSL.literal(30)),
                DSL.literal("true"))),
        optimize(caseClause, logicalPlan));
  }

  @Test
  void aggregation_in_case_else_clause_should_be_replaced() {
    Expression caseClause = DSL.cases(
        dsl.avg(DSL.ref("age", INTEGER)),
        DSL.when(
            dsl.equal(DSL.ref("age", INTEGER), DSL.literal(30)),
            DSL.literal("true")));

    LogicalPlan logicalPlan =
        LogicalPlanDSL.aggregation(
            LogicalPlanDSL.relation("test", table),
            ImmutableList.of(DSL.named("AVG(age)", dsl.avg(DSL.ref("age", INTEGER)))),
            ImmutableList.of(DSL.named("name", DSL.ref("name", STRING))));

    assertEquals(
        DSL.cases(
            DSL.ref("AVG(age)", DOUBLE),
            DSL.when(
                dsl.equal(DSL.ref("age", INTEGER), DSL.literal(30)),
                DSL.literal("true"))),
        optimize(caseClause, logicalPlan));
  }

  @Test
  void window_expression_should_be_replaced() {
    LogicalPlan logicalPlan =
        LogicalPlanDSL.window(
            LogicalPlanDSL.window(
                LogicalPlanDSL.relation("test", table),
                DSL.named(dsl.rank()),
                new WindowDefinition(emptyList(), emptyList())),
            DSL.named(dsl.denseRank()),
            new WindowDefinition(emptyList(), emptyList()));

    assertEquals(
        DSL.ref("rank()", INTEGER),
        optimize(dsl.rank(), logicalPlan));
    assertEquals(
        DSL.ref("dense_rank()", INTEGER),
        optimize(dsl.denseRank(), logicalPlan));
  }

  Expression optimize(Expression expression) {
    return optimize(expression, logicalPlan());
  }

  Expression optimize(Expression expression, LogicalPlan logicalPlan) {
    final ExpressionReferenceOptimizer optimizer =
        new ExpressionReferenceOptimizer(functionRepository, logicalPlan);
    return optimizer.optimize(DSL.named(expression), new AnalysisContext());
  }

  LogicalPlan logicalPlan() {
    return LogicalPlanDSL.aggregation(
        LogicalPlanDSL.relation("schema", table),
        ImmutableList
            .of(DSL.named("AVG(age)", dsl.avg(DSL.ref("age", INTEGER))),
                DSL.named("SUM(age)", dsl.sum(DSL.ref("age", INTEGER)))),
        ImmutableList.of(DSL.named("balance", DSL.ref("balance", INTEGER)),
            DSL.named("abs(balance)", dsl.abs(DSL.ref("balance", INTEGER))))
    );
  }
}
