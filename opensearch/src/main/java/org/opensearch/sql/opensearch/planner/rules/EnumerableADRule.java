/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.rules;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.plan.LogicalAD;
import org.opensearch.sql.opensearch.storage.ml.EnumerableAD;

public class EnumerableADRule extends ConverterRule {

  public static final Config DEFAULT_CONFIG =
      Config.INSTANCE
          .as(Config.class)
          .withConversion(
              LogicalAD.class,
              ad -> ad.getInput() != null,
              Convention.NONE,
              EnumerableConvention.INSTANCE,
              "EnumerableADRule")
          .withRuleFactory(EnumerableADRule::new);

  protected EnumerableADRule(Config config) {
    super(config);
  }

  @Override
  public @Nullable RelNode convert(RelNode rel) {
    final LogicalAD ad = (LogicalAD) rel;

    return new EnumerableAD(
        ad.getCluster(),
        ad.getTraitSet().plus(EnumerableConvention.INSTANCE),
        convert(ad.getInput(), ad.getInput().getTraitSet().replace(EnumerableConvention.INSTANCE)),
        ad.getArguments());
  }
}
