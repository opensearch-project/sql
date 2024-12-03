/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_FIRST;
import static org.opensearch.sql.ast.tree.Sort.NullOrder.NULL_LAST;
import static org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_ASC;
import static org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_DESC;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.ASC;
import static org.opensearch.sql.ast.tree.Sort.SortOrder.DESC;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.window.WindowDefinition;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanDSL;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.logical.LogicalSort;

class WindowExpressionAnalyzerTest extends AnalyzerTestBase {

  private LogicalPlan child;

  private WindowExpressionAnalyzer analyzer;

  @BeforeEach
  void setUp() {
    child = new LogicalRelation("test", table);
    analyzer = new WindowExpressionAnalyzer(expressionAnalyzer, child);
  }

  @SuppressWarnings("unchecked")
  @Test
  void should_wrap_child_with_window_and_sort_operator_if_project_item_windowed() {
    assertEquals(
        LogicalPlanDSL.window(
            LogicalPlanDSL.sort(
                LogicalPlanDSL.relation("test", table),
                ImmutablePair.of(DEFAULT_ASC, DSL.ref("string_value", STRING)),
                ImmutablePair.of(DEFAULT_DESC, DSL.ref("integer_value", INTEGER))),
            DSL.named("row_number", DSL.rowNumber()),
            new WindowDefinition(
                ImmutableList.of(DSL.ref("string_value", STRING)),
                ImmutableList.of(
                    ImmutablePair.of(DEFAULT_DESC, DSL.ref("integer_value", INTEGER))))),
        analyzer.analyze(
            AstDSL.alias(
                "row_number",
                AstDSL.window(
                    AstDSL.function("row_number"),
                    ImmutableList.of(AstDSL.qualifiedName("string_value")),
                    ImmutableList.of(
                        ImmutablePair.of(DEFAULT_DESC, AstDSL.qualifiedName("integer_value"))))),
            analysisContext));
  }

  // TODO row_number window function requires window to be ordered.
  @Test
  void should_not_generate_sort_operator_if_no_partition_by_and_order_by_list() {
    assertEquals(
        LogicalPlanDSL.window(
            LogicalPlanDSL.relation("test", table),
            DSL.named("row_number", DSL.rowNumber()),
            new WindowDefinition(ImmutableList.of(), ImmutableList.of())),
        analyzer.analyze(
            AstDSL.alias(
                "row_number",
                AstDSL.window(
                    AstDSL.function("row_number"), ImmutableList.of(), ImmutableList.of())),
            analysisContext));
  }

  @Test
  void can_analyze_without_partition_by() {
    assertEquals(
        LogicalPlanDSL.window(
            LogicalPlanDSL.sort(
                LogicalPlanDSL.relation("test", table),
                ImmutablePair.of(DEFAULT_DESC, DSL.ref("integer_value", INTEGER))),
            DSL.named("row_number", DSL.rowNumber()),
            new WindowDefinition(
                ImmutableList.of(),
                ImmutableList.of(
                    ImmutablePair.of(DEFAULT_DESC, DSL.ref("integer_value", INTEGER))))),
        analyzer.analyze(
            AstDSL.alias(
                "row_number",
                AstDSL.window(
                    AstDSL.function("row_number"),
                    ImmutableList.of(),
                    ImmutableList.of(
                        ImmutablePair.of(DEFAULT_DESC, AstDSL.qualifiedName("integer_value"))))),
            analysisContext));
  }

  @Test
  void should_return_original_child_if_project_item_not_windowed() {
    assertEquals(
        child,
        analyzer.analyze(
            AstDSL.alias("string_value", AstDSL.qualifiedName("string_value")), analysisContext));
  }

  @Test
  void can_analyze_sort_options() {
    // Mapping from input option to expected option after analysis
    ImmutableMap<SortOption, SortOption> expects =
        ImmutableMap.<SortOption, SortOption>builder()
            .put(new SortOption(null, null), DEFAULT_ASC)
            .put(new SortOption(ASC, null), DEFAULT_ASC)
            .put(new SortOption(DESC, null), DEFAULT_DESC)
            .put(new SortOption(null, NULL_FIRST), DEFAULT_ASC)
            .put(new SortOption(null, NULL_LAST), new SortOption(ASC, NULL_LAST))
            .put(new SortOption(ASC, NULL_FIRST), DEFAULT_ASC)
            .put(new SortOption(DESC, NULL_FIRST), new SortOption(DESC, NULL_FIRST))
            .put(new SortOption(DESC, NULL_LAST), DEFAULT_DESC)
            .build();

    expects.forEach(
        (option, expect) -> {
          Alias ast =
              AstDSL.alias(
                  "row_number",
                  AstDSL.window(
                      AstDSL.function("row_number"),
                      Collections.emptyList(),
                      ImmutableList.of(
                          ImmutablePair.of(option, AstDSL.qualifiedName("integer_value")))));

          LogicalPlan plan = analyzer.analyze(ast, analysisContext);
          LogicalSort sort = (LogicalSort) plan.getChild().get(0);
          assertEquals(
              expect,
              sort.getSortList().get(0).getLeft(),
              "Assertion failed on input option: " + option);
        });
  }
}
