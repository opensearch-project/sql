/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import java.util.function.Function;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalProject;

/**
 * Scan builder for vector search relations.
 *
 * <p>Rejects two planner shapes that the SQL surface cannot express safely:
 *
 * <ul>
 *   <li><b>Aggregations</b> — native OpenSearch k-NN supports aggregations alongside similarity
 *       search, but the SQL layer does not plumb them through, so we fail fast rather than return
 *       silently unaggregated results.
 *   <li><b>Outer WHERE over a vectorSearch() subquery</b> — when vectorSearch() is wrapped in a
 *       subquery (e.g. {@code SELECT * FROM (SELECT v.id FROM vectorSearch(...) AS v) t WHERE
 *       t.price < 150}), the outer predicate does not reach the push-down contract (the inner
 *       {@link LogicalProject} sits between the outer {@link LogicalFilter} and this scan builder,
 *       so the filter never matches the {@code filter(scanBuilder)} push-down pattern). The filter
 *       is then applied in memory, but only <i>after</i> k-NN has already returned the top-k
 *       documents ranked by vector distance, so the outer predicate can silently produce zero rows.
 *       We detect this shape in {@link #validatePlan(LogicalPlan)} and reject with a clear error.
 * </ul>
 */
public class VectorSearchIndexScanBuilder extends OpenSearchIndexScanBuilder {

  public VectorSearchIndexScanBuilder(
      PushDownQueryBuilder translator,
      Function<OpenSearchRequestBuilder, OpenSearchIndexScan> scanFactory) {
    super(translator, scanFactory);
  }

  @Override
  public boolean pushDownAggregation(LogicalAggregation aggregation) {
    throw new ExpressionEvaluationException(
        "Aggregations are not supported on vectorSearch() relations.");
  }

  /**
   * Walk the fully-optimized plan and reject the outer-WHERE-over-subquery shape. We look for a
   * {@link LogicalFilter} whose descendant chain reaches this scan builder through one or more
   * {@link LogicalProject} nodes (the subquery-boundary marker). A filter directly above this scan
   * builder is fine — that WHERE has already gone through the push-down contract.
   */
  @Override
  public void validatePlan(LogicalPlan root) {
    checkForOuterFilter(root, false, false);
  }

  /**
   * Recursive walker with two booleans that together encode the "outer filter separated by a
   * subquery boundary" pattern:
   *
   * <ul>
   *   <li>{@code insideFilter} — true iff some ancestor on the current walk path is a {@link
   *       LogicalFilter}. Projects only matter below a filter, so without a filter ancestor a
   *       project is just the outer SELECT and should not trigger rejection.
   *   <li>{@code sawProjectSinceFilter} — true iff a {@link LogicalProject} has been seen between
   *       <em>any</em> enclosing filter ancestor and the current position. Once an outer Filter has
   *       been separated from the scan by a Project, that separation is permanent — a lower
   *       LogicalFilter below the Project does not undo the outer boundary.
   * </ul>
   *
   * <p>This matters for shapes like {@code Filter(outer) -> Project(subquery) -> Filter(inner) ->
   * Scan}, where the outer predicate is still blocked from reaching the push-down contract by the
   * subquery Project regardless of the inner filter. Resetting {@code sawProjectSinceFilter} when
   * entering the inner filter would make the walker miss this shape.
   */
  private void checkForOuterFilter(
      LogicalPlan node, boolean insideFilter, boolean sawProjectSinceFilter) {
    if (node == this) {
      if (insideFilter && sawProjectSinceFilter) {
        throw new ExpressionEvaluationException(
            "Outer WHERE on a vectorSearch() subquery is not supported: the predicate does not"
                + " push into the k-NN search and would be applied only after top-k results have"
                + " been selected by vector distance, which can silently yield zero rows."
                + " Move the predicate inside the subquery (WHERE directly on vectorSearch()) so"
                + " it can participate in the vectorSearch WHERE pushdown contract.");
      }
      return;
    }
    boolean nextInsideFilter = insideFilter;
    boolean nextSawProject = sawProjectSinceFilter;
    if (node instanceof LogicalFilter) {
      nextInsideFilter = true;
    } else if (node instanceof LogicalProject && insideFilter) {
      nextSawProject = true;
    }
    for (LogicalPlan child : node.getChild()) {
      checkForOuterFilter(child, nextInsideFilter, nextSawProject);
    }
  }
}
