/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.opensearch.index.query.WrapperQueryBuilder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalValues;

class VectorSearchIndexScanBuilderTest {

  private VectorSearchIndexScanBuilder newScanBuilder() {
    var requestBuilder =
        new OpenSearchRequestBuilder(
            mock(OpenSearchExprValueFactory.class), 10000, mock(Settings.class));
    var queryBuilder =
        new VectorSearchQueryBuilder(
            requestBuilder, new WrapperQueryBuilder("{\"knn\":{}}"), java.util.Map.of("k", "5"));
    return new VectorSearchIndexScanBuilder(queryBuilder, rb -> mock(OpenSearchIndexScan.class));
  }

  private static LogicalProject project(LogicalPlan input) {
    NamedExpression field = DSL.named("id", DSL.ref("id", ExprCoreType.STRING));
    return new LogicalProject(input, ImmutableList.of(field), ImmutableList.of());
  }

  private static LogicalFilter filter(LogicalPlan input) {
    return new LogicalFilter(
        input, DSL.less(DSL.ref("price", ExprCoreType.INTEGER), DSL.literal(150)));
  }

  @Test
  void pushDownAggregationIsRejected() {
    var scanBuilder = newScanBuilder();

    var agg =
        new LogicalAggregation(
            new LogicalValues(Collections.emptyList()),
            Collections.emptyList(),
            Collections.emptyList(),
            false);

    ExpressionEvaluationException ex =
        assertThrows(
            ExpressionEvaluationException.class, () -> scanBuilder.pushDownAggregation(agg));
    assertTrue(
        ex.getMessage().contains("Aggregations are not supported"),
        "Error should state aggregations are not supported; actual: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("vectorSearch"),
        "Error should mention vectorSearch; actual: " + ex.getMessage());
  }

  @Test
  void validatePlanRejectsOuterFilterOverSubqueryProject() {
    // Models: SELECT * FROM (SELECT v.id FROM vs(...) AS v) t WHERE t.price < 150
    // Shape after optimizer: Project(outer) → Filter → Project(inner) → scanBuilder
    var scanBuilder = newScanBuilder();
    LogicalPlan root = project(filter(project(scanBuilder)));

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> scanBuilder.validatePlan(root));
    assertTrue(
        ex.getMessage().contains("Outer WHERE on a vectorSearch() subquery"),
        "Error should mention outer WHERE on subquery; actual: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("silently yield zero rows"),
        "Error should explain silent zero rows; actual: " + ex.getMessage());
  }

  @Test
  void validatePlanRejectsDoubleWrappedOuterFilter() {
    // Models nested subqueries:
    //   SELECT * FROM (SELECT * FROM (SELECT v.id FROM vs(...) AS v) t1) t2 WHERE t2.price < 150
    var scanBuilder = newScanBuilder();
    LogicalPlan root = filter(project(project(scanBuilder)));

    assertThrows(ExpressionEvaluationException.class, () -> scanBuilder.validatePlan(root));
  }

  @Test
  void validatePlanAllowsFilterDirectlyAboveScanBuilder() {
    // Models: SELECT v.id FROM vs(...) AS v WHERE v.gender='M'
    // Here the filter would normally be pushed down and removed, but if it were kept (e.g. a
    // non-pushdownable predicate), validatePlan must not reject it — it is already at the
    // vectorSearch level, not an outer filter.
    var scanBuilder = newScanBuilder();
    LogicalPlan root = project(filter(scanBuilder));

    assertDoesNotThrow(() -> scanBuilder.validatePlan(root));
  }

  @Test
  void validatePlanAllowsInnerFilterWrappedInOuterProject() {
    // Models: SELECT * FROM (SELECT v.id FROM vs(...) AS v WHERE v.gender='M') t
    // After pushdown the inner filter may remain when non-pushdownable; importantly, there is no
    // outer filter — only outer projects wrapping an inner filter directly on scanBuilder.
    var scanBuilder = newScanBuilder();
    LogicalPlan root = project(project(filter(scanBuilder)));

    assertDoesNotThrow(() -> scanBuilder.validatePlan(root));
  }

  @Test
  void validatePlanRejectsFilterProjectFilterShape() {
    // Models: SELECT * FROM (SELECT v.id FROM vs(...) AS v WHERE v.gender='M') t
    //          WHERE t.price < 150
    // Shape: Filter(outer) → Project(subquery) → Filter(inner) → scanBuilder
    // The outer filter is still separated from the scan by the subquery Project; the inner
    // filter sitting between the Project and the scan does not erase that boundary. Without
    // preserving the project marker across the inner filter, the walker would miss this shape.
    var scanBuilder = newScanBuilder();
    LogicalPlan root = filter(project(filter(scanBuilder)));

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> scanBuilder.validatePlan(root));
    assertTrue(
        ex.getMessage().contains("Outer WHERE on a vectorSearch() subquery"),
        "Error should mention outer WHERE on subquery; actual: " + ex.getMessage());
  }

  @Test
  void validatePlanAllowsNoFilterAtAll() {
    // Baseline: no WHERE anywhere. SELECT * FROM (SELECT v.id FROM vs(...) AS v) t
    var scanBuilder = newScanBuilder();
    LogicalPlan root = project(project(scanBuilder));

    assertDoesNotThrow(() -> scanBuilder.validatePlan(root));
  }

  @Test
  void validatePlanAllowsBareScanBuilder() {
    // Defensive: a plan that is just the scan builder itself.
    var scanBuilder = newScanBuilder();

    assertDoesNotThrow(() -> scanBuilder.validatePlan(scanBuilder));
  }
}
