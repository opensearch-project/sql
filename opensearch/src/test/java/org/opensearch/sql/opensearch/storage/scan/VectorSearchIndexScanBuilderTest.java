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
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalSort;
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

  private static LogicalSort sort(LogicalPlan input) {
    return new LogicalSort(
        input,
        ImmutableList.of(
            org.apache.commons.lang3.tuple.Pair.of(
                Sort.SortOption.DEFAULT_DESC, DSL.ref("price", ExprCoreType.INTEGER))));
  }

  private static LogicalLimit limit(LogicalPlan input, int offset) {
    return new LogicalLimit(input, 10, offset);
  }

  private static LogicalAggregation aggregation(LogicalPlan input) {
    return new LogicalAggregation(input, Collections.emptyList(), Collections.emptyList(), false);
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

  @Test
  void validatePlanRejectsOuterSortOverSubqueryProject() {
    // Models: SELECT * FROM (SELECT v.id FROM vs(...) AS v) t ORDER BY t.price
    // Shape: Sort(outer) → Project(subquery) → scanBuilder
    // Outer ORDER BY would be applied only after top-k ANN results, producing an order the user
    // did not ask for (vector distance ordering leaks through when rows are fewer than expected).
    var scanBuilder = newScanBuilder();
    LogicalPlan root = sort(project(scanBuilder));

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> scanBuilder.validatePlan(root));
    assertTrue(
        ex.getMessage().contains("Outer ORDER BY on a vectorSearch() subquery"),
        "Error should mention outer ORDER BY on subquery; actual: " + ex.getMessage());
  }

  @Test
  void validatePlanRejectsOuterOffsetOverSubqueryProject() {
    // Models: SELECT * FROM (SELECT v.id FROM vs(...) AS v) t LIMIT 10 OFFSET 5
    // Outer OFFSET silently skips the top-N nearest rows chosen by ANN, so the remaining rows
    // would be a truncated tail of the k-NN result set rather than the user's intended window.
    var scanBuilder = newScanBuilder();
    LogicalPlan root = limit(project(scanBuilder), 5);

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> scanBuilder.validatePlan(root));
    assertTrue(
        ex.getMessage().contains("Outer OFFSET on a vectorSearch() subquery"),
        "Error should mention outer OFFSET on subquery; actual: " + ex.getMessage());
  }

  @Test
  void validatePlanAllowsOuterLimitWithoutOffsetOverSubquery() {
    // Outer LIMIT with offset=0 just caps row count and is safe over a subquery — reject only
    // non-zero OFFSET. Locks in the offset==0 boundary of the guard.
    var scanBuilder = newScanBuilder();
    LogicalPlan root = limit(project(scanBuilder), 0);

    assertDoesNotThrow(() -> scanBuilder.validatePlan(root));
  }

  @Test
  void validatePlanRejectsOuterAggregationOverSubqueryProject() {
    // Models: SELECT COUNT(*) FROM (SELECT v.id FROM vs(...) AS v) t
    // (Or outer GROUP BY / DISTINCT, both of which rewrite to LogicalAggregation.) The outer
    // aggregation would run on a truncated top-k slice rather than a meaningful population,
    // masking the fact that aggregations are not supported on vectorSearch() in this preview.
    var scanBuilder = newScanBuilder();
    LogicalPlan root = aggregation(project(scanBuilder));

    ExpressionEvaluationException ex =
        assertThrows(ExpressionEvaluationException.class, () -> scanBuilder.validatePlan(root));
    assertTrue(
        ex.getMessage().contains("Outer GROUP BY / aggregation / DISTINCT on a vectorSearch()"),
        "Error should mention outer aggregation on subquery; actual: " + ex.getMessage());
  }
}
