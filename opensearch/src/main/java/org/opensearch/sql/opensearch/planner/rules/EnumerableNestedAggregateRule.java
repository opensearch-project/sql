/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.utils.PPLHintUtils;
import org.opensearch.sql.opensearch.planner.physical.CalciteEnumerableNestedAggregate;

/** Rule to convert {@link LogicalAggregate} to {@link CalciteEnumerableNestedAggregate}. */
public class EnumerableNestedAggregateRule extends ConverterRule {
  private static final Logger LOG = LogManager.getLogger();

  /** Default configuration. */
  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(
              LogicalAggregate.class,
              Convention.NONE,
              EnumerableConvention.INSTANCE,
              "EnumerableNestedAggregateRule")
          .withRuleFactory(EnumerableNestedAggregateRule::new);

  /** Called from the Config. */
  protected EnumerableNestedAggregateRule(Config config) {
    super(config);
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    final Aggregate agg = (Aggregate) rel;
    if (PPLHintUtils.hasNestedAggCall(agg)) {
      final RelTraitSet traitSet =
          rel.getCluster().traitSet().replace(EnumerableConvention.INSTANCE);
      try {
        return new CalciteEnumerableNestedAggregate(
            rel.getCluster(),
            traitSet,
            agg.getHints(),
            convert(agg.getInput(), traitSet),
            agg.getGroupSet(),
            agg.getGroupSets(),
            agg.getAggCallList());
      } catch (InvalidRelException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(e.toString());
        }
        return null;
      }
    }
    return null;
  }
}
