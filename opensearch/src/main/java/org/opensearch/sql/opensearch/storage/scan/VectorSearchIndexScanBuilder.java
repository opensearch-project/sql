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
   *       the <em>nearest enclosing</em> filter and the current position. That project is the
   *       subquery-boundary marker that separates "outer WHERE on a subquery" (bad) from "WHERE
   *       directly on vectorSearch()" (fine, handled by push-down).
   * </ul>
   *
   * <p>Entering a new {@link LogicalFilter} resets {@code sawProjectSinceFilter} to false because
   * the nearest enclosing filter is now this one, and the contract is evaluated from here down.
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
                + " it is applied during the k-NN search.");
      }
      return;
    }
    boolean nextInsideFilter = insideFilter;
    boolean nextSawProject = sawProjectSinceFilter;
    if (node instanceof LogicalFilter) {
      // Entering a new filter: nearest enclosing filter is now this one, reset project marker.
      nextInsideFilter = true;
      nextSawProject = false;
    } else if (node instanceof LogicalProject && insideFilter) {
      // A project between the nearest enclosing filter and a scan builder is the bug marker.
      nextSawProject = true;
    }
    for (LogicalPlan child : node.getChild()) {
      checkForOuterFilter(child, nextInsideFilter, nextSawProject);
    }
  }
}
