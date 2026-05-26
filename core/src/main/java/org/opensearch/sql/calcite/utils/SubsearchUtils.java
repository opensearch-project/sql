/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.UtilityClass;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;

@UtilityClass
public class SubsearchUtils {

  /** Insert a system_limit under correlate conditions. */
  private static RelNode insertSysLimitUnderCorrelateConditions(
      LogicalFilter logicalFilter, CalcitePlanContext context) {
    // Before:
    // LogicalFilter(condition=[AND(=($cor0.SAL, $2), >($1, 1000.0:DECIMAL(5, 1)))])
    // After:
    // LogicalFilter(condition=[=($cor0.SAL, $2)])
    //   LogicalSystemLimit(fetch=[1], type=[SUBSEARCH_MAXOUT])
    //     LogicalFilter(condition=[>($1, 1000.0:DECIMAL(5, 1))])
    RexNode originalCondition = logicalFilter.getCondition();
    List<RexNode> conditions = RelOptUtil.conjunctions(originalCondition);
    Pair<List<RexNode>, List<RexNode>> result =
        CalciteUtils.partition(conditions, PlanUtils::containsCorrelVariable);
    if (result.getLeft().isEmpty()) {
      return logicalFilter;
    }

    RelNode input = logicalFilter.getInput();
    if (!result.getRight().isEmpty()) {
      RexNode nonCorrelCondition =
          RexUtil.composeConjunction(context.rexBuilder, result.getRight());
      input = LogicalFilter.create(input, nonCorrelCondition);
    }
    input =
        LogicalSystemLimit.create(
            LogicalSystemLimit.SystemLimitType.SUBSEARCH_MAXOUT,
            input,
            context.relBuilder.literal(context.sysLimit.subsearchLimit()));
    if (!result.getLeft().isEmpty()) {
      RexNode correlCondition = RexUtil.composeConjunction(context.rexBuilder, result.getLeft());
      input = LogicalFilter.create(input, correlCondition);
    }
    return input;
  }

  /** Insert a system_limit under correlated conditions by visiting a plan tree. */
  @RequiredArgsConstructor
  public static class SystemLimitInsertionShuttle extends RelShuttleImpl {

    private final CalcitePlanContext context;
    @Getter private boolean correlatedConditionFound = false;

    @Override
    public RelNode visit(LogicalFilter filter) {
      RelNode newFilter = insertSysLimitUnderCorrelateConditions(filter, context);
      if (newFilter != filter) {
        correlatedConditionFound = true;
        return newFilter;
      }
      return super.visitChildren(filter);
    }

    @Override
    public RelNode visit(LogicalJoin node) {
      return node;
    }

    @Override
    public RelNode visit(LogicalCorrelate node) {
      return node;
    }

    @Override
    public RelNode visit(LogicalUnion node) {
      return node;
    }

    @Override
    public RelNode visit(LogicalIntersect node) {
      return node;
    }

    @Override
    public RelNode visit(LogicalMinus node) {
      return node;
    }
  }
}
