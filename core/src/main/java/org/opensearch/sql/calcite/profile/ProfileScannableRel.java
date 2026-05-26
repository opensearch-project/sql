/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.profile;

import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.plan.Scannable;
import org.opensearch.sql.monitor.profile.ProfilePlanNode;

/** EnumerableRel wrapper that also supports Scannable plans. */
public final class ProfileScannableRel extends ProfileEnumerableRel implements Scannable {

  private final Scannable scannable;

  public ProfileScannableRel(
      EnumerableRel delegate, List<RelNode> inputs, ProfilePlanNode planNode) {
    super(delegate, inputs, planNode);
    this.scannable = (Scannable) delegate;
  }

  @Override
  public Enumerable<@Nullable Object> scan() {
    return profile(scannable.scan(), planNode.metrics());
  }
}
