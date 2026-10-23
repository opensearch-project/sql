/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.analyze;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.sql.monitor.profile.QueryProfile;
import org.opensearch.sql.monitor.profile.QueryProfile.PlanNode;

/**
 * Read-only view over a {@link QueryProfile} shared by all {@link RecommendationRule}s.
 *
 * <p>Row semantics: a plan node's {@code rows} is its output row count ({@code rows_out}); {@code
 * rows_in} is the sum of its children's {@code rows}.
 *
 * <p>Timing semantics: a node's {@code time_ms} is cumulative wall-time (it includes its
 * descendants' time), not the node's own duration. A node's self-time is therefore {@code time_ms -
 * max(child.time_ms)} (see {@link #duration}). Time-fraction rules use this self-time so they
 * attribute the cost actually spent in the stage rather than the whole subtree beneath it.
 */
class ProfileView {

  private final QueryProfile profile;

  ProfileView(QueryProfile profile) {
    this.profile = profile;
  }

  /** Flattens the profile's plan-node tree into a list (root first, depth-first). */
  List<PlanNode> planNodes() {
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
  static double duration(PlanNode node) {
    double maxChild = 0;
    if (node.getChildren() != null) {
      for (PlanNode child : node.getChildren()) {
        maxChild = Math.max(maxChild, child.getTimeMillis());
      }
    }
    return Math.max(0, node.getTimeMillis() - maxChild);
  }

  /** Input rows for a node: the sum of its children's output rows. */
  static long rowsIn(PlanNode node) {
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
  double phaseTime(String phaseName) {
    QueryProfile.Phase phase =
        profile.getPhases() == null ? null : profile.getPhases().get(phaseName);
    return phase == null ? 0 : phase.getTimeMillis();
  }
}
