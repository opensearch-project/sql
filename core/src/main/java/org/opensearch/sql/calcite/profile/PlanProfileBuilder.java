/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.profile;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.calcite.plan.Scannable;
import org.opensearch.sql.monitor.profile.ProfilePlanNode;

/** Builds a profiled EnumerableRel plan tree and matching plan node structure. */
public final class PlanProfileBuilder {

  private PlanProfileBuilder() {}

  public static ProfilePlan profile(RelNode root) {
    Objects.requireNonNull(root, "root");
    return profileRel(root);
  }

  private static ProfilePlan profileRel(RelNode rel) {
    List<ProfilePlan> childPlans = new ArrayList<>();
    for (RelNode input : rel.getInputs()) {
      childPlans.add(profileRel(input));
    }

    List<RelNode> newInputs =
        childPlans.stream().map(ProfilePlan::rel).collect(Collectors.toList());
    List<ProfilePlanNode> childNodes =
        childPlans.stream().map(ProfilePlan::planRoot).collect(Collectors.toList());

    ProfilePlanNode planNode = new ProfilePlanNode(nodeName(rel), childNodes);
    RelNode wrappedRel = wrap(rel, newInputs, planNode);
    return new ProfilePlan(wrappedRel, planNode);
  }

  private static RelNode wrap(RelNode rel, List<RelNode> inputs, ProfilePlanNode planNode) {
    if (!(rel instanceof EnumerableRel)) {
      try {
        return rel.copy(rel.getTraitSet(), inputs);
      } catch (UnsupportedOperationException e) {
        return rel;
      }
    }
    if (rel instanceof Scannable) {
      return new ProfileScannableRel((EnumerableRel) rel, inputs, planNode);
    }
    return new ProfileEnumerableRel((EnumerableRel) rel, inputs, planNode);
  }

  private static String nodeName(RelNode rel) {
    return rel.getRelTypeName();
  }

  /** Pair of the profiled RelNode tree and its root plan node. */
  public static final class ProfilePlan {
    private final RelNode rel;
    private final ProfilePlanNode planRoot;

    public ProfilePlan(RelNode rel, ProfilePlanNode planRoot) {
      this.rel = rel;
      this.planRoot = planRoot;
    }

    public RelNode rel() {
      return rel;
    }

    public ProfilePlanNode planRoot() {
      return planRoot;
    }
  }
}
