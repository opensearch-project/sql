/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.metadata;

import com.google.common.collect.ImmutableList;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.enumerable.EnumerableValues;
import org.apache.calcite.adapter.enumerable.EnumerableValuesRule;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Volcano rules that create empty {@code Values} without deriving their collation trait.
 *
 * <p>Calcite's {@link PruneEmptyRules} build their result with {@code RelBuilder.empty()} through
 * the default values factory, and {@link EnumerableValuesRule} converts with {@link
 * EnumerableValues#create}. Both derive the collation trait with {@code RelMdCollation.values},
 * which is O(n^2) in the number of output fields for an empty {@code Values}
 * (opensearch-project/sql#5952). {@link #register} replaces them with equivalents that skip that
 * derivation for empty values. Non-empty values are handled exactly as before.
 */
public final class EmptyValuesRules {

  private static final RelBuilderFactory EMPTY_VALUES_BUILDER =
      RelBuilder.proto(Contexts.of(EmptyValuesFactory.INSTANCE));

  private static final List<RelOptRule> PRUNE_EMPTY_RULES =
      List.of(
          PruneEmptyRules.UNION_INSTANCE,
          PruneEmptyRules.MINUS_INSTANCE,
          PruneEmptyRules.INTERSECT_INSTANCE,
          PruneEmptyRules.EMPTY_TABLE_INSTANCE,
          PruneEmptyRules.PROJECT_INSTANCE,
          PruneEmptyRules.FILTER_INSTANCE,
          PruneEmptyRules.SORT_INSTANCE,
          PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE,
          PruneEmptyRules.AGGREGATE_INSTANCE,
          PruneEmptyRules.WINDOW_INSTANCE,
          PruneEmptyRules.JOIN_LEFT_INSTANCE,
          PruneEmptyRules.JOIN_RIGHT_INSTANCE,
          PruneEmptyRules.CORRELATE_RIGHT_INSTANCE,
          PruneEmptyRules.CORRELATE_LEFT_INSTANCE);

  static final RelOptRule ENUMERABLE_VALUES_RULE =
      EnumerableValuesRule.DEFAULT_CONFIG
          .withRuleFactory(EmptyEnumerableValuesRule::new)
          .toRule(EmptyEnumerableValuesRule.class);

  private EmptyValuesRules() {}

  /** Replaces Calcite's empty-pruning and values conversion rules that the planner has. */
  public static void register(RelOptPlanner planner) {
    for (RelOptRule rule : PRUNE_EMPTY_RULES) {
      if (planner.removeRule(rule)) {
        planner.addRule(withEmptyValuesBuilder(rule));
      }
    }
    if (planner.removeRule(EnumerableRules.ENUMERABLE_VALUES_RULE)) {
      planner.addRule(ENUMERABLE_VALUES_RULE);
    }
  }

  private static RelOptRule withEmptyValuesBuilder(RelOptRule rule) {
    return ((RelRule<?>) rule).config.withRelBuilderFactory(EMPTY_VALUES_BUILDER).toRule();
  }

  /** Converts an empty {@code Values} to {@link EnumerableValues} without deriving collations. */
  public static class EmptyEnumerableValuesRule extends EnumerableValuesRule {
    // EnumerableValues has only a private constructor besides create(), which derives collations.
    private static final @Nullable MethodHandle CONSTRUCTOR = findConstructor();

    protected EmptyEnumerableValuesRule(Config config) {
      super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
      Values values = (Values) rel;
      if (!values.getTuples().isEmpty() || CONSTRUCTOR == null) {
        return super.convert(rel);
      }
      // Same traits as EnumerableValues.create, minus the O(n^2) collation derivation.
      RelTraitSet traits =
          values
              .getCluster()
              .traitSetOf(EnumerableConvention.INSTANCE)
              .replace(RelCollations.EMPTY)
              .replaceIf(
                  RelDistributionTraitDef.INSTANCE,
                  () -> RelMdDistribution.values(values.getRowType(), values.getTuples()));
      try {
        return (RelNode)
            CONSTRUCTOR.invoke(
                values.getCluster(), values.getRowType(), values.getTuples(), traits);
      } catch (Throwable e) {
        return super.convert(rel);
      }
    }

    private static @Nullable MethodHandle findConstructor() {
      try {
        return MethodHandles.privateLookupIn(EnumerableValues.class, MethodHandles.lookup())
            .findConstructor(
                EnumerableValues.class,
                MethodType.methodType(
                    void.class,
                    RelOptCluster.class,
                    RelDataType.class,
                    ImmutableList.class,
                    RelTraitSet.class));
      } catch (ReflectiveOperationException | RuntimeException e) {
        return null;
      }
    }
  }
}
