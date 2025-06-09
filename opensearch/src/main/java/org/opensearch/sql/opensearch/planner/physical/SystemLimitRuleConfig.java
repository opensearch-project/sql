/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.adapter.enumerable.EnumerableCorrelate;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.tools.RelBuilderFactory;

@EqualsAndHashCode
@RequiredArgsConstructor
public class SystemLimitRuleConfig implements RelRule.Config {
  private final int limit;
  private RelBuilderFactory relBuilderFactory = relBuilderFactory();
  private String description;
  private RelRule.OperandTransform operandSupplier;

  @Override
  public RelOptRule toRule() {
    return new OpenSearchSystemLimitRule(this, limit);
  }

  @Override
  public RelRule.Config withRelBuilderFactory(RelBuilderFactory factory) {
    this.relBuilderFactory = factory;
    return this;
  }

  @Override
  public RelRule.Config withDescription(String description) {
    this.description = description;
    return this;
  }

  @Override
  public RelRule.Config withOperandSupplier(RelRule.OperandTransform transform) {
    this.operandSupplier = transform;
    return this;
  }

  @Override
  public RelRule.OperandTransform operandSupplier() {
    return this.operandSupplier;
  }

  @Override
  public String description() {
    return this.description;
  }

  /** Operand supplier to specify physical join operator */
  public static RelRule.OperandTransform SYSTEM_LIMIT_JOIN_TRANSFORM =
      b0 ->
          b0.operand(Join.class)
              .predicate(j -> j instanceof EnumerableRel)
              .predicate(j -> j.getJoinType().projectsRight()) // semi anti won't cause table bloat
              .inputs(
                  b2 -> b2.operand(RelNode.class).anyInputs(),
                  b2 -> b2.operand(RelNode.class).anyInputs());

  /** Operand supplier to specify physical correlate operator */
  public static RelRule.OperandTransform SYSTEM_LIMIT_CORRELATE_TRANSFORM =
      b0 ->
          b0.operand(EnumerableCorrelate.class)
              .predicate(j -> j.getJoinType().projectsRight()) // semi anti won't cause table bloat
              .inputs(
                  b2 -> b2.operand(RelNode.class).anyInputs(),
                  b2 -> b2.operand(RelNode.class).anyInputs());
}
