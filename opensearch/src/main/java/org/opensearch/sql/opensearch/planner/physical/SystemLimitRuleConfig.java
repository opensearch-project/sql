/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import java.util.function.Predicate;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
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

  public static boolean isLogicalLimit(RelNode relNode) {
    return relNode instanceof LogicalSort sort && sort.getCollation() == RelCollations.EMPTY;
  }

  public static boolean canPushSystemLimitToRight(LogicalJoin join) {
    JoinRelType joinRelType = join.getJoinType();
    return joinRelType == JoinRelType.INNER
        || joinRelType == JoinRelType.LEFT
        || joinRelType == JoinRelType.FULL;
  }

  public static boolean canPushSystemLimitToLeft(LogicalJoin join) {
    JoinRelType joinRelType = join.getJoinType();
    return joinRelType == JoinRelType.RIGHT || joinRelType == JoinRelType.FULL;
  }

  public static RelRule.OperandTransform PUSHDOWN_SYSTEM_LIMIT_TO_RIGHT_TRANSFORM =
      b0 ->
          b0.operand(LogicalJoin.class)
              .predicate(SystemLimitRuleConfig::canPushSystemLimitToRight)
              .inputs(
                  b2 -> b2.operand(RelNode.class).anyInputs(),
                  b2 ->
                      b2.operand(RelNode.class)
                          .predicate(Predicate.not(SystemLimitRuleConfig::isLogicalLimit))
                          .anyInputs());

  public static RelRule.OperandTransform PUSHDOWN_SYSTEM_LIMIT_TO_LEFT_TRANSFORM =
      b0 ->
          b0.operand(LogicalJoin.class)
              .predicate(SystemLimitRuleConfig::canPushSystemLimitToLeft)
              .inputs(
                  b2 ->
                      b2.operand(RelNode.class)
                          .predicate(Predicate.not(SystemLimitRuleConfig::isLogicalLimit))
                          .anyInputs());
}
