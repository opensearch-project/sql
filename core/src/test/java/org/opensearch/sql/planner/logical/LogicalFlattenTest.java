/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.analysis.AnalyzerTestBase;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.tree.Flatten;
import org.opensearch.sql.expression.DSL;

@ExtendWith(MockitoExtension.class)
class LogicalFlattenTest extends AnalyzerTestBase {

  @Test
  void testFlatten() {
    String fieldName = "field_name";
    String tableName = "schema";

    LogicalPlan expectedLogicalPlan =
        LogicalPlanDSL.flatten(
            LogicalPlanDSL.relation(tableName, table), DSL.ref(fieldName, STRUCT));

    Flatten actualUnresolvedPlan =
        AstDSL.flatten(AstDSL.relation(tableName), AstDSL.field(fieldName));

    assertAnalyzeEqual(expectedLogicalPlan, actualUnresolvedPlan);
  }
}
