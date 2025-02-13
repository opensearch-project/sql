/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.analysis.AnalyzerTestBase;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;

@ExtendWith(MockitoExtension.class)
class LogicalExpandTest extends AnalyzerTestBase {

  private static final String TABLE_NAME = "schema";

  @Test
  void testExpandScalar() {
    LogicalPlan expected =
        LogicalPlanDSL.expand(
            LogicalPlanDSL.relation(TABLE_NAME, table), DSL.ref("integer_value", INTEGER));
    LogicalPlan actual =
        analyze(AstDSL.expand(AstDSL.relation(TABLE_NAME), AstDSL.field("integer_value")));
    assertEquals(expected, actual);
  }

  @Test
  void testExpandArray() {
    LogicalPlan expected =
        LogicalPlanDSL.expand(
            LogicalPlanDSL.relation(TABLE_NAME, table), DSL.ref("array_value", ARRAY));
    LogicalPlan actual =
        analyze(AstDSL.expand(AstDSL.relation(TABLE_NAME), AstDSL.field("array_value")));
    assertEquals(expected, actual);
  }

  @Test
  void testExpandInvalidFieldName() {
    UnresolvedPlan unresolved = AstDSL.expand(AstDSL.relation(TABLE_NAME), AstDSL.field("invalid"));
    assertThrows(SemanticCheckException.class, () -> analyze(unresolved));
  }
}
