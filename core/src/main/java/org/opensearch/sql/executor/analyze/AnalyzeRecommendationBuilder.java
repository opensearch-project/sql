/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.analyze;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.opensearch.sql.executor.AnalyzeResponse.Recommendation;
import org.opensearch.sql.executor.AnalyzeResponse.RecommendationSeverityLevel;
import org.opensearch.sql.monitor.profile.QueryProfile;
import org.opensearch.sql.monitor.profile.QueryProfile.PlanNode;

/**
 * Builds the list of {@link Recommendation}s returned by the PPL {@code analyze} endpoint from the
 * {@link QueryProfile}'s plan-node tree and phase timings.
 *
 * <p>Each rule lives in its own method. Per-node rules ({@code ineffectiveFilter}, {@code
 * joinRowExplosion}, {@code expensiveSort}) scan every plan node and may emit more than one
 * recommendation, so they return a {@link List}. Whole-query rules ({@code bottleneckStage}, {@code
 * optimizePhaseDominates}) emit at most one and return an {@link Optional}. {@link #build()} runs
 * every rule and concatenates the results.
 *
 * <p>Row semantics: a plan node's {@code rows} is its output row count ({@code rows_out}); {@code
 * rows_in} is the sum of its children's {@code rows}.
 *
 * <p>Timing semantics: a node's {@code time_ms} is cumulative wall-time (it includes its
 * descendants' time), not the node's own duration. A node's self-time is therefore {@code time_ms -
 * max(child.time_ms)} (see {@link #duration}). Time-fraction rules use this self-time so they
 * attribute the cost actually spent in the stage rather than the whole subtree beneath it.
 */
public class AnalyzeRecommendationBuilder {

  // Rule thresholds (defaults from the rule spec).

  /** Ineffective filter/project: fires when rows_out / rows_in exceeds this pass-through ratio. */
  private static final double INEFFECTIVE_FILTER_MAX_PASS_RATIO = 0.95;

  /** Join row explosion: fires when rows_out / rows_in exceeds this ratio (WARNING). */
  private static final double JOIN_EXPLOSION_RATIO = 5.0;

  /**
   * Join row explosion escalates to CRITICAL at or above this ratio. Not specified by the rule
   * table; chosen as a higher tier above {@link #JOIN_EXPLOSION_RATIO}.
   */
  private static final double JOIN_EXPLOSION_CRITICAL_RATIO = 20.0;

  /** Expensive sort: fires when the sort's time fraction of execute time exceeds this. */
  private static final double EXPENSIVE_SORT_TIME_FRACTION = 0.20;

  /** Expensive sort: additionally requires at least this many input rows. */
  private static final long EXPENSIVE_SORT_MIN_ROWS = 50_000;

  /** Bottleneck stage: fires when the slowest node's time fraction of execute exceeds this. */
  private static final double BOTTLENECK_TIME_FRACTION = 0.75;

  /** Optimize phase dominates: fires when optimize time exceeds this (ms) and beats execute. */
  private static final double OPTIMIZE_DOMINATES_MIN_MS = 75.0;

  private final QueryProfile profile;

  public AnalyzeRecommendationBuilder(QueryProfile profile) {
    this.profile = profile;
  }

  /** Runs every recommendation rule and concatenates the ones that fired. */
  public List<Recommendation> build() {
    List<Recommendation> recommendations = new ArrayList<>();
    if (profile == null) {
      return recommendations;
    }
    recommendations.addAll(ineffectiveFilter());
    recommendations.addAll(joinRowExplosion());
    recommendations.addAll(expensiveSort());
    bottleneckStage().ifPresent(recommendations::add);
    optimizePhaseDominates().ifPresent(recommendations::add);
    return recommendations;
  }

  /** Flags filter/project stages that barely reduced their input. */
  private List<Recommendation> ineffectiveFilter() {
    List<Recommendation> recommendations = new ArrayList<>();
    for (PlanNode node : planNodes()) {
      String name = node.getNode().toLowerCase(Locale.ROOT);
      if (!name.contains("filter")) {
        continue;
      }
      long rowsIn = rowsIn(node);
      if (rowsIn <= 0) {
        continue;
      }
      double ratio = (double) node.getRows() / rowsIn;
      if (ratio > INEFFECTIVE_FILTER_MAX_PASS_RATIO) {
        long droppedPct = Math.round((1.0 - ratio) * 100);
        recommendations.add(
            Recommendation.builder()
                .severity(RecommendationSeverityLevel.WARNING)
                .rule("Ineffective Filter")
                .message("Filter only dropped " + droppedPct + "% of rows")
                .affected_node(node.getNode())
                .suggestion("Consider removing the filter or making it more selective.")
                .build());
      }
    }
    return recommendations;
  }

  /** Flags joins whose output greatly exceeds their combined input. */
  private List<Recommendation> joinRowExplosion() {
    List<Recommendation> recommendations = new ArrayList<>();
    for (PlanNode node : planNodes()) {
      if (!node.getNode().toLowerCase(Locale.ROOT).contains("join")) {
        continue;
      }
      long rowsIn = rowsIn(node);
      if (rowsIn <= 0) {
        continue;
      }
      double ratio = (double) node.getRows() / rowsIn;
      if (ratio > JOIN_EXPLOSION_RATIO) {
        RecommendationSeverityLevel severity =
            ratio >= JOIN_EXPLOSION_CRITICAL_RATIO
                ? RecommendationSeverityLevel.CRITICAL
                : RecommendationSeverityLevel.WARNING;
        recommendations.add(
            Recommendation.builder()
                .severity(severity)
                .rule("Join Row Explosion")
                .message(
                    "Join expanded "
                        + rowsIn
                        + " rows into "
                        + node.getRows()
                        + " rows ("
                        + String.format(Locale.ROOT, "%.1f", ratio)
                        + "×)")
                .affected_node(node.getNode())
                .suggestion("Add filters to the subqueries before the join to reduce rows.")
                .build());
      }
    }
    return recommendations;
  }

  /** Flags sorts over large inputs that consumed a large share of execution time. */
  private List<Recommendation> expensiveSort() {
    List<Recommendation> recommendations = new ArrayList<>();
    double executeMs = phaseTime("execute");
    if (executeMs <= 0) {
      return recommendations;
    }
    for (PlanNode node : planNodes()) {
      if (!node.getNode().toLowerCase(Locale.ROOT).contains("sort")) {
        continue;
      }
      long rowsIn = rowsIn(node);
      double durationMs = duration(node);
      double timeFraction = durationMs / executeMs;
      if (timeFraction > EXPENSIVE_SORT_TIME_FRACTION && rowsIn > EXPENSIVE_SORT_MIN_ROWS) {
        long pct = Math.round(timeFraction * 100);
        recommendations.add(
            Recommendation.builder()
                .severity(RecommendationSeverityLevel.WARNING)
                .rule("Expensive Sort")
                .message(
                    "Sorting "
                        + rowsIn
                        + " rows took "
                        + durationMs
                        + " ms ("
                        + pct
                        + "% of execution)")
                .affected_node(node.getNode())
                .suggestion("Filter or limit rows before sorting (e.g. add head or a where).")
                .build());
      }
    }
    return recommendations;
  }

  /** Flags the single stage that dominated execution time. */
  private Optional<Recommendation> bottleneckStage() {
    double executeMs = phaseTime("execute");
    if (executeMs <= 0) {
      return Optional.empty();
    }
    PlanNode slowest = null;
    double slowestDuration = 0;
    for (PlanNode node : planNodes()) {
      double durationMs = duration(node);
      if (slowest == null || durationMs > slowestDuration) {
        slowest = node;
        slowestDuration = durationMs;
      }
    }
    if (slowest == null) {
      return Optional.empty();
    }
    double timeFraction = slowestDuration / executeMs;
    if (timeFraction <= BOTTLENECK_TIME_FRACTION) {
      return Optional.empty();
    }
    long pct = Math.round(timeFraction * 100);
    return Optional.of(
        Recommendation.builder()
            .severity(RecommendationSeverityLevel.INFO)
            .rule("Bottleneck Stage")
            .message(
                slowest.getNode() + " took " + slowestDuration + " ms (" + pct + "% of execution)")
            .affected_node(slowest.getNode())
            .build());
  }

  /** Flags queries that spent more time planning than executing. */
  private Optional<Recommendation> optimizePhaseDominates() {
    double executeMs = phaseTime("execute");
    double optimizeMs = phaseTime("optimize");
    if (executeMs < optimizeMs && optimizeMs > OPTIMIZE_DOMINATES_MIN_MS) {
      return Optional.of(
          Recommendation.builder()
              .severity(RecommendationSeverityLevel.INFO)
              .rule("Optimize Phase Dominates")
              .message(
                  "Query planning took " + optimizeMs + " ms vs " + executeMs + " ms executing")
              .build());
    }
    return Optional.empty();
  }

  /** Flattens the profile's plan-node tree into a list (root first, depth-first). */
  private List<PlanNode> planNodes() {
    List<PlanNode> nodes = new ArrayList<>();
    if (profile.getPlan() instanceof PlanNode root) {
      collect(root, nodes);
    }
    return nodes;
  }

  private static void collect(PlanNode node, List<PlanNode> out) {
    out.add(node);
    if (node.getChildren() != null) {
      for (PlanNode child : node.getChildren()) {
        collect(child, out);
      }
    }
  }

  /**
   * Self-time for a node in milliseconds. {@code time_ms} is cumulative wall-time (a node's clock
   * includes its descendants'), so the node's own duration is its time minus the slowest child's
   * time. Clamped at 0 to guard against measurement jitter making a child appear slower than its
   * parent.
   */
  private static double duration(PlanNode node) {
    double maxChild = 0;
    if (node.getChildren() != null) {
      for (PlanNode child : node.getChildren()) {
        maxChild = Math.max(maxChild, child.getTimeMillis());
      }
    }
    return Math.max(0, node.getTimeMillis() - maxChild);
  }

  /** Input rows for a node: the sum of its children's output rows. */
  private static long rowsIn(PlanNode node) {
    if (node.getChildren() == null || node.getChildren().isEmpty()) {
      return 0;
    }
    long sum = 0;
    for (PlanNode child : node.getChildren()) {
      sum += child.getRows();
    }
    return sum;
  }

  /** Millis for a named profile phase, or 0 if absent. */
  private double phaseTime(String phaseName) {
    QueryProfile.Phase phase =
        profile.getPhases() == null ? null : profile.getPhases().get(phaseName);
    return phase == null ? 0 : phase.getTimeMillis();
  }
}
