/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import static org.opensearch.sql.ast.dsl.AstDSL.argument;
import static org.opensearch.sql.ast.dsl.AstDSL.booleanLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.dedupe;
import static org.opensearch.sql.ast.dsl.AstDSL.defaultDedupArgs;
import static org.opensearch.sql.ast.dsl.AstDSL.exprList;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.analysis.AnalyzerTestBase;
import org.opensearch.sql.expression.DSL;

class LogicalDedupeTest extends AnalyzerTestBase {
  @Test
  public void analyze_dedup_with_two_field_with_default_option() {
    assertAnalyzeEqual(
        LogicalPlanDSL.dedupe(
            LogicalPlanDSL.relation("schema", table),
            DSL.ref("integer_value", INTEGER),
            DSL.ref("double_value", DOUBLE)),
        dedupe(
            relation("schema"), defaultDedupArgs(), field("integer_value"), field("double_value")));
  }

  @Test
  public void analyze_dedup_with_one_field_with_customize_option() {
    assertAnalyzeEqual(
        LogicalPlanDSL.dedupe(
            LogicalPlanDSL.relation("schema", table),
            3,
            false,
            true,
            DSL.ref("integer_value", INTEGER),
            DSL.ref("double_value", DOUBLE)),
        dedupe(
            relation("schema"),
            exprList(
                argument("number", intLiteral(3)),
                argument("keepempty", booleanLiteral(false)),
                argument("consecutive", booleanLiteral(true))),
            field("integer_value"),
            field("double_value")));
  }
}
