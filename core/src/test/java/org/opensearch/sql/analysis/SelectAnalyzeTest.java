/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static org.opensearch.sql.ast.dsl.AstDSL.argument;
import static org.opensearch.sql.ast.dsl.AstDSL.booleanLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.planner.logical.LogicalPlanDSL;

public class SelectAnalyzeTest extends AnalyzerTestBase {

  @Override
  protected Map<String, ExprType> typeMapping() {
    return new ImmutableMap.Builder<String, ExprType>()
        .put("integer_value", ExprCoreType.INTEGER)
        .put("double_value", ExprCoreType.DOUBLE)
        .put("string_value", ExprCoreType.STRING)
        .build();
  }

  @Test
  public void project_all_from_source() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.relation("schema", table),
            DSL.named("integer_value", DSL.ref("integer_value", INTEGER)),
            DSL.named("double_value", DSL.ref("double_value", DOUBLE)),
            DSL.named("integer_value", DSL.ref("integer_value", INTEGER)),
            DSL.named("double_value", DSL.ref("double_value", DOUBLE)),
            DSL.named("string_value", DSL.ref("string_value", STRING))),
        AstDSL.projectWithArg(
            AstDSL.relation("schema"),
            AstDSL.defaultFieldsArgs(),
            AstDSL.field("integer_value"), // Field not wrapped by Alias
            AstDSL.alias("double_value", AstDSL.field("double_value")),
            AllFields.of()));
  }

  @Test
  public void select_and_project_all() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.project(
                LogicalPlanDSL.relation("schema", table),
                DSL.named("integer_value", DSL.ref("integer_value", INTEGER)),
                DSL.named("double_value", DSL.ref("double_value", DOUBLE))),
            DSL.named("integer_value", DSL.ref("integer_value", INTEGER)),
            DSL.named("double_value", DSL.ref("double_value", DOUBLE))),
        AstDSL.projectWithArg(
            AstDSL.projectWithArg(
                AstDSL.relation("schema"),
                AstDSL.defaultFieldsArgs(),
                AstDSL.field("integer_value"),
                AstDSL.field("double_value")),
            AstDSL.defaultFieldsArgs(),
            AllFields.of()));
  }

  @Test
  public void remove_and_project_all() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.remove(
                LogicalPlanDSL.relation("schema", table),
                DSL.ref("integer_value", INTEGER),
                DSL.ref("double_value", DOUBLE)),
            DSL.named("string_value", DSL.ref("string_value", STRING))),
        AstDSL.projectWithArg(
            AstDSL.projectWithArg(
                AstDSL.relation("schema"),
                AstDSL.exprList(argument("exclude", booleanLiteral(true))),
                AstDSL.field("integer_value"),
                AstDSL.field("double_value")),
            AstDSL.defaultFieldsArgs(),
            AllFields.of()));
  }

  @Test
  public void stats_and_project_all() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.aggregation(
                LogicalPlanDSL.relation("schema", table),
                ImmutableList.of(
                    DSL.named("avg(integer_value)", DSL.avg(DSL.ref("integer_value", INTEGER)))),
                ImmutableList.of(DSL.named("string_value", DSL.ref("string_value", STRING)))),
            DSL.named("avg(integer_value)", DSL.ref("avg(integer_value)", DOUBLE)),
            DSL.named("string_value", DSL.ref("string_value", STRING))),
        AstDSL.projectWithArg(
            AstDSL.agg(
                AstDSL.relation("schema"),
                AstDSL.exprList(
                    AstDSL.alias(
                        "avg(integer_value)", AstDSL.aggregate("avg", field("integer_value")))),
                null,
                ImmutableList.of(AstDSL.alias("string_value", field("string_value"))),
                AstDSL.defaultStatsArgs()),
            AstDSL.defaultFieldsArgs(),
            AllFields.of()));
  }

  @Test
  public void rename_and_project_all() {
    assertAnalyzeEqual(
        LogicalPlanDSL.project(
            LogicalPlanDSL.rename(
                LogicalPlanDSL.relation("schema", table),
                ImmutableMap.of(DSL.ref("integer_value", INTEGER), DSL.ref("ivalue", INTEGER))),
            DSL.named("double_value", DSL.ref("double_value", DOUBLE)),
            DSL.named("string_value", DSL.ref("string_value", STRING)),
            DSL.named("ivalue", DSL.ref("ivalue", INTEGER))),
        AstDSL.projectWithArg(
            AstDSL.rename(
                AstDSL.relation("schema"),
                AstDSL.map(AstDSL.field("integer_value"), AstDSL.field("ivalue"))),
            AstDSL.defaultFieldsArgs(),
            AllFields.of()));
  }
}
