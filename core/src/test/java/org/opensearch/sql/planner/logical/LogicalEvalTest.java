/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.logical;

import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.analysis.AnalyzerTestBase;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@Configuration
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {ExpressionConfig.class, AnalyzerTestBase.class})
@ExtendWith(MockitoExtension.class)
public class LogicalEvalTest extends AnalyzerTestBase {

  @Test
  public void analyze_eval_with_one_field() {
    assertAnalyzeEqual(
        LogicalPlanDSL.eval(
            LogicalPlanDSL.relation("schema"),
            ImmutablePair
                .of(DSL.ref("absValue", INTEGER), dsl.abs(DSL.ref("integer_value", INTEGER)))),
        AstDSL.eval(
            AstDSL.relation("schema"),
            AstDSL.let(AstDSL.field("absValue"), AstDSL.function("abs", field("integer_value")))));
  }

  @Test
  public void analyze_eval_with_two_field() {
    assertAnalyzeEqual(
        LogicalPlanDSL.eval(
            LogicalPlanDSL.relation("schema"),
            ImmutablePair
                .of(DSL.ref("absValue", INTEGER), dsl.abs(DSL.ref("integer_value", INTEGER))),
            ImmutablePair.of(DSL.ref("iValue", INTEGER), dsl.abs(DSL.ref("absValue", INTEGER)))),
        AstDSL.eval(
            AstDSL.relation("schema"),
            AstDSL.let(AstDSL.field("absValue"), AstDSL.function("abs", field("integer_value"))),
            AstDSL.let(AstDSL.field("iValue"), AstDSL.function("abs", field("absValue")))));
  }
}
