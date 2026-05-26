/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Mutable plan node used while profiling a query. */
public final class ProfilePlanNode {

  private final String nodeName;
  private final List<ProfilePlanNode> children;
  private final ProfilePlanNodeMetrics metrics = new ProfilePlanNodeMetrics();

  public ProfilePlanNode(String nodeName, List<ProfilePlanNode> children) {
    this.nodeName = Objects.requireNonNull(nodeName, "nodeName");
    this.children = List.copyOf(Objects.requireNonNull(children, "children"));
  }

  public ProfilePlanNodeMetrics metrics() {
    return metrics;
  }

  public QueryProfile.PlanNode snapshot() {
    List<QueryProfile.PlanNode> snapshotChildren = new ArrayList<>();
    for (ProfilePlanNode child : children) {
      snapshotChildren.add(child.snapshot());
    }
    List<QueryProfile.PlanNode> outputChildren =
        snapshotChildren.isEmpty() ? null : List.copyOf(snapshotChildren);
    return new QueryProfile.PlanNode(
        nodeName, ProfileUtils.roundToMillis(metrics.timeNanos()), metrics.rows(), outputChildren);
  }
}
