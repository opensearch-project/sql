/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import static org.opensearch.sql.ast.dsl.AstDSL.argument;
import static org.opensearch.sql.ast.dsl.AstDSL.booleanLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.defaultSortFieldArgs;
import static org.opensearch.sql.ast.dsl.AstDSL.exprList;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.nullLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.ast.dsl.AstDSL.sort;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.analysis.AnalyzerTestBase;
import org.opensearch.sql.ast.tree.Sort.SortOption;
import org.opensearch.sql.expression.DSL;

class LogicalSortTest extends AnalyzerTestBase {
  @Test
  public void analyze_sort_with_two_field_with_default_option() {
    assertAnalyzeEqual(
        LogicalPlanDSL.sort(
            LogicalPlanDSL.relation("schema", table),
            ImmutablePair.of(SortOption.DEFAULT_ASC, DSL.ref("integer_value", INTEGER)),
            ImmutablePair.of(SortOption.DEFAULT_ASC, DSL.ref("double_value", DOUBLE))),
        sort(
            relation("schema"),
            field("integer_value", defaultSortFieldArgs()),
            field("double_value", defaultSortFieldArgs())));
  }

  @Test
  public void analyze_sort_with_two_field() {
    assertAnalyzeEqual(
        LogicalPlanDSL.sort(
            LogicalPlanDSL.relation("schema", table),
            ImmutablePair.of(SortOption.DEFAULT_DESC, DSL.ref("integer_value", INTEGER)),
            ImmutablePair.of(SortOption.DEFAULT_ASC, DSL.ref("double_value", DOUBLE))),
        sort(
            relation("schema"),
            field(
                "integer_value",
                exprList(argument("asc", booleanLiteral(false)), argument("type", nullLiteral()))),
            field("double_value", defaultSortFieldArgs())));
  }
}
