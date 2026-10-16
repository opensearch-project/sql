/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.junit.Test;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.knn.KnnAst;
import org.opensearch.sql.calcite.knn.KnnErrors;
import org.opensearch.sql.exception.SemanticCheckException;

/** The statement check of {@link UnifiedQueryPlanner} runs on the parsed AST before lowering. */
public class UnifiedQueryPlannerStatementCheckTest extends UnifiedQueryTestBase {

  @Test
  public void checkSeesTheParsedStatementAndLoweringProceeds() {
    List<UnresolvedPlan> checked = new ArrayList<>();
    RelNode plan =
        new UnifiedQueryPlanner(context, checked::add).plan("source = catalog.employees");

    assertEquals(1, checked.size());
    assertTrue(checked.getFirst().toString(), containsRelation(checked.getFirst()));
    assertEquals(List.of("id", "name", "age", "department"), plan.getRowType().getFieldNames());
  }

  @Test
  public void checkFailureIsThePlanningError() {
    UnifiedQueryContext knnContext =
        new UnifiedQueryContext(
            context.getPlanContext(),
            context.getSettings(),
            query ->
                AstDSL.knn(
                    AstDSL.relation("catalog.employees"),
                    AstDSL.field("embedding"),
                    AstDSL.vectorLiteral(1, 2, 3)),
            context.getLangSpec());
    UnifiedQueryPlanner planner =
        new UnifiedQueryPlanner(
            knnContext,
            statement -> {
              if (KnnAst.containsKnnStep(statement)) {
                throw KnnErrors.analyticsEngineNotSupported();
              }
            });

    SemanticCheckException e = assertThrows(SemanticCheckException.class, () -> planner.plan("q"));
    assertEquals(KnnErrors.ANALYTICS_ENGINE_NOT_SUPPORTED_MESSAGE, e.getMessage());
  }

  private static boolean containsRelation(UnresolvedPlan plan) {
    if (plan instanceof Relation) {
      return true;
    }
    return plan.getChild().stream()
        .anyMatch(child -> child instanceof UnresolvedPlan p && containsRelation(p));
  }
}
