/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexNode;

public class TimeWindow extends SingleRel {
  private final RexNode timeColumn;
  private final RexNode windowDuration;
  private final RexNode slideDuration;
  private final RexNode startTime;

  public TimeWindow(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RexNode timeColumn,
      RexNode windowDuration,
      RexNode slideDuration,
      RexNode startTime) {
    super(cluster, traits, input);
    this.timeColumn = timeColumn;
    this.windowDuration = windowDuration;
    this.slideDuration = slideDuration;
    this.startTime = startTime;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new TimeWindow(
        getCluster(), traitSet, sole(inputs), timeColumn, windowDuration, slideDuration, startTime);
  }
}
