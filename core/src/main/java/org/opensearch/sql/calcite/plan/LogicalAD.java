/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

public class LogicalAD extends AD {

  /**
   * Creates a LogicalAD operator
   *
   * @param cluster Cluster this relational expression belongs to
   * @param traits collation traits of the operator, usually NONE for ad
   * @param input Input relational expression
   * @param arguments an argument mapping of parameter keys and values
   */
  public LogicalAD(
      RelOptCluster cluster, RelTraitSet traits, RelNode input, Map<String, Object> arguments) {
    super(cluster, traits, input, arguments);
  }

  @Override
  public final LogicalAD copy(RelTraitSet traitSet, RelNode input) {
    return new LogicalAD(getCluster(), traitSet, input, getArguments());
  }
}
