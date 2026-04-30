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
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalSort;

/**
 * Scan builder for vector search relations.
 *
 * <p>Rejects planner shapes that the SQL surface cannot express safely:
 *
 * <ul>
 *   <li><b>Aggregations</b> — native OpenSearch k-NN supports aggregations alongside similarity
 *       search, but the SQL layer does not plumb them through, so we fail fast rather than return
 *       silently unaggregated results.
 *   <li><b>Outer operators over a vectorSearch() subquery</b> — when vectorSearch() is wrapped in a
 *       subquery (e.g. {@code SELECT * FROM (SELECT v.id FROM vectorSearch(...) AS v) t WHERE
 *       t.price < 150}), outer WHERE / ORDER BY / OFFSET / GROUP BY / aggregation / DISTINCT do not
 *       participate in the vectorSearch pushdown contract (the inner {@link LogicalProject} sits
 *       between the outer operator and this scan builder, so those nodes never match the
 *       direct-adjacency push-down patterns). They would then be applied in memory <i>after</i>
 *       top-k results have been selected by vector distance, which can silently yield zero rows or
 *       mis-ordered results. We detect these shapes in {@link #validatePlan(LogicalPlan)} and
 *       reject with a clear error.
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
   * Walk the fully-optimized plan and reject outer-operator-over-subquery shapes. We look for an
   * outer {@link LogicalFilter}, {@link LogicalSort}, {@link LogicalLimit} with non-zero offset, or
   * {@link LogicalAggregation} whose descendant chain reaches this scan builder through one or more
   * {@link LogicalProject} nodes (the subquery-boundary marker). An operator directly above this
   * scan builder is fine — those go through the push-down contract in the query builder.
   */
  @Override
  public void validatePlan(LogicalPlan root) {
    checkForOuterOperator(root, null, false);
  }

  /**
   * Recursive walker that tracks the outermost "risky" operator seen on the current walk path and
   * whether a {@link LogicalProject} has been crossed since then:
   *
   * <ul>
   *   <li>{@code outerOp} — name of the outermost filter/sort/offset/aggregation ancestor, or
   *       {@code null} if none. Projects only matter below such an operator — without one, a
   *       project is just the outer SELECT and should not trigger rejection.
   *   <li>{@code sawProjectSinceOuter} — true iff a {@link LogicalProject} has been seen between
   *       the outermost risky ancestor and the current position. Once separation by a Project has
   *       been established, it is permanent — a lower {@link LogicalFilter} below the Project does
   *       not undo the outer boundary.
   * </ul>
   *
   * <p>This matters for shapes like {@code Filter(outer) -> Project(subquery) -> Filter(inner) ->
   * Scan}, where the outer predicate is still blocked from reaching the push-down contract by the
   * subquery Project regardless of the inner filter. Resetting on the inner filter would make the
   * walker miss this shape.
   */
  private void checkForOuterOperator(
      LogicalPlan node, String outerOp, boolean sawProjectSinceOuter) {
    if (node == this) {
      if (outerOp != null && sawProjectSinceOuter) {
        throw new ExpressionEvaluationException(rejectionMessage(outerOp));
      }
      return;
    }
    String nextOuterOp = outerOp;
    boolean nextSawProject = sawProjectSinceOuter;
    if (outerOp == null) {
      String operator = classifyOuterOperator(node);
      if (operator != null) {
        nextOuterOp = operator;
      }
    } else if (node instanceof LogicalProject) {
      nextSawProject = true;
    }
    for (LogicalPlan child : node.getChild()) {
      checkForOuterOperator(child, nextOuterOp, nextSawProject);
    }
  }

  /**
   * Returns a user-facing label for operators that cannot safely sit above a vectorSearch()
   * subquery, or {@code null} for operators that are fine (Project, scan, etc.). {@link
   * LogicalLimit} with {@code offset == 0} is safe — plain LIMIT wrapping a subquery just caps the
   * row count. Non-zero OFFSET skips top-k rows by distance and is rejected.
   */
  private static String classifyOuterOperator(LogicalPlan node) {
    if (node instanceof LogicalFilter) {
      return "WHERE";
    }
    if (node instanceof LogicalSort) {
      return "ORDER BY";
    }
    if (node instanceof LogicalAggregation) {
      return "GROUP BY / aggregation / DISTINCT";
    }
    if (node instanceof LogicalLimit) {
      Integer offset = ((LogicalLimit) node).getOffset();
      if (offset != null && offset != 0) {
        return "OFFSET";
      }
    }
    return null;
  }

  // Operator-specific messages: the generic "move it inside the subquery" advice is only right
  // for WHERE and for ORDER BY _score DESC. OFFSET, aggregation, GROUP BY, and DISTINCT are
  // themselves unsupported on vectorSearch() directly, so the message must not claim a workaround
  // that would only trip the user on a second validation error.
  private static String rejectionMessage(String outerOp) {
    switch (outerOp) {
      case "WHERE":
        return "Outer WHERE on a vectorSearch() subquery is not supported: the predicate does not"
            + " participate in the vectorSearch pushdown contract and would be applied only"
            + " after top-k results have been selected by vector distance, which can silently"
            + " yield zero rows. Move the WHERE into the same SELECT block as vectorSearch() so"
            + " it participates in the vectorSearch WHERE pushdown contract.";
      case "ORDER BY":
        return "Outer ORDER BY on a vectorSearch() subquery is not supported: sorting does not"
            + " participate in the vectorSearch pushdown contract and would be applied only"
            + " after top-k results have been selected by vector distance, which can yield"
            + " mis-ordered results. Use ORDER BY <alias>._score DESC in the same SELECT block"
            + " as vectorSearch(), or omit ORDER BY.";
      case "OFFSET":
        return "Outer OFFSET on a vectorSearch() subquery is not supported. OFFSET is not"
            + " supported on vectorSearch(); use LIMIT only.";
      case "GROUP BY / aggregation / DISTINCT":
        return "Outer GROUP BY / aggregation / DISTINCT on a vectorSearch() subquery is not"
            + " supported. Aggregations and DISTINCT are not supported on vectorSearch()"
            + " relations.";
      default:
        return "Outer " + outerOp + " on a vectorSearch() subquery is not supported.";
    }
  }
}
