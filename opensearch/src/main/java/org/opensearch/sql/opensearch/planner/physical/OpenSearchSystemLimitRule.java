/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import org.apache.calcite.adapter.enumerable.EnumerableAggregate;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan;

/** Pushdown system limit to index scan rule */
public class OpenSearchSystemLimitRule extends RelRule<SystemLimitRuleConfig> {
  private final int limit;

  OpenSearchSystemLimitRule(SystemLimitRuleConfig config, int limit) {
    super(config);
    this.limit = limit;
  }

  private boolean isLogicalLimit(RelNode relNode) {
    return relNode instanceof LogicalSort sort && sort.getCollation() == RelCollations.EMPTY;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);
    final RelNode leftJoinChild = call.rel(1);
    final RelNode rightJoinChild = call.rel(2);
    final JoinRelType joinType = join.getJoinType();
    final RelBuilder builder = call.builder();
    RelNode newLeft = leftJoinChild;
    RelNode newRight = rightJoinChild;
    switch (joinType) {
      case INNER:
      case LEFT:
        if (isLogicalLimit(rightJoinChild)) {
          return;
        }
        newRight = builder.push(rightJoinChild).limit(0, limit).build();
        break;
      case RIGHT:
        if (isLogicalLimit(leftJoinChild)) {
          return;
        }
        newLeft = builder.push(leftJoinChild).limit(0, limit).build();
        break;
      case FULL:
        if (isLogicalLimit(leftJoinChild) && isLogicalLimit(rightJoinChild)) {
          return;
        }
        newLeft = builder.push(leftJoinChild).limit(0, limit).build();
        newRight = builder.push(rightJoinChild).limit(0, limit).build();
        break;
      default:
        return;
    }
    RelNode newJoin =
        builder
            .push(newLeft)
            .push(newRight)
            .join(joinType, join.getCondition(), join.getVariablesSet())
            .build();
    call.transformTo(newJoin);
  }

  /** pushdown system limit to {@link CalciteEnumerableIndexScan} */
  public void pushdownSystemLimit(RelNode rel) {
    rel.accept(
        new RelShuttleImpl() {
          @Override
          public RelNode visit(RelNode node) {
            if (node instanceof CalciteEnumerableIndexScan scan) {
              scan.pushDownSystemLimit(limit, 0);
              return scan;
            }
            if (node instanceof EnumerableAggregate) {
              // unpushed aggregation, stop visiting
              return node;
            }
            if (node instanceof EnumerableLimit userLimit) {
              // stop visiting child if user's limit is less than system limit
              if (PlanUtils.intValue(userLimit.fetch, Integer.MAX_VALUE) <= limit) {
                return node;
              }
            }
            if (node instanceof RelSubset subset) {
              return subset.getBest() == null ? node : visit(subset.getBest());
            }
            // visit children
            return super.visit(node);
          }
        });
  }
}
