/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableAggregate;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

@Value.Enclosing
class PPLAggregateHintConvertRule extends ConverterRule {
  /** Default configuration. */
  static final Config DEFAULT_CONFIG = Config.INSTANCE
      .withConversion(LogicalAggregate.class, PPLAggregateHintConvertRule::containsStatsArgsHint,
          Convention.NONE, EnumerableConvention.INSTANCE, "PPLAggregateHintConvertRule")
      .withRuleFactory(PPLAggregateHintConvertRule::new);

  /** Called from the Config. */
  protected PPLAggregateHintConvertRule(Config config) {
    super(config);
  }

  @Override public @Nullable RelNode convert(RelNode rel) {
    final LogicalAggregate agg = (LogicalAggregate) rel;
    RelNode input = agg.getInput();
    RexBuilder rexBuilder = agg.getCluster().getRexBuilder();
    List<RexNode> predicates = agg.getGroupSet().asList().stream().map(index -> rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, rexBuilder.makeInputRef(input, index))).toList();
    RexNode condition = RexUtil.composeConjunction(rexBuilder, predicates);
    final LogicalFilter filter = LogicalFilter.create(input, condition);
    final RelTraitSet traitSet = rel.getCluster()
        .traitSet().replace(EnumerableConvention.INSTANCE);
    try {
      return new EnumerableAggregate(
          rel.getCluster(),
          traitSet,
          convert(filter, traitSet),
          agg.getGroupSet(),
          agg.getGroupSets(),
          agg.getAggCallList());
    } catch (InvalidRelException e) {
      return null;
    }
  }

  public static boolean containsStatsArgsHint(LogicalAggregate agg) {
    return agg.getHints().stream().anyMatch(hint -> hint.hintName.equals("stats_args"));
  }
}
