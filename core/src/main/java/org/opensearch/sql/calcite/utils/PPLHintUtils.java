/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import com.google.common.base.Suppliers;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import lombok.experimental.UtilityClass;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.tools.RelBuilder;

@UtilityClass
public class PPLHintUtils {
  private static final String HINT_AGG_ARGUMENTS = "AGG_ARGS";
  private static final String KEY_IGNORE_NULL_BUCKET = "ignoreNullBucket";
  private static final String KEY_HAS_NESTED_AGG_CALL = "hasNestedAggCall";
  private static final String KEY_DEDUP_SORT_FIELD = "dedupSortField";
  private static final String KEY_DEDUP_SORT_ORDER = "dedupSortOrder";

  private static final Supplier<HintStrategyTable> HINT_STRATEGY_TABLE =
      Suppliers.memoize(
          () ->
              HintStrategyTable.builder()
                  .hintStrategy(
                      HINT_AGG_ARGUMENTS,
                      (hint, rel) -> {
                        return rel instanceof LogicalAggregate;
                      })
                  // add more here
                  .build());

  /**
   * Add hint to aggregate to indicate that the aggregate will ignore null value bucket. Notice, the
   * current peek of relBuilder is expected to be LogicalAggregate.
   */
  public static void addIgnoreNullBucketHintToAggregate(RelBuilder relBuilder) {
    assert relBuilder.peek() instanceof LogicalAggregate
        : "Hint HINT_AGG_ARGUMENTS can be added to LogicalAggregate only";
    final RelHint statHint =
        RelHint.builder(HINT_AGG_ARGUMENTS).hintOption(KEY_IGNORE_NULL_BUCKET, "true").build();
    relBuilder.hints(statHint);
    if (relBuilder.getCluster().getHintStrategies() == HintStrategyTable.EMPTY) {
      relBuilder.getCluster().setHintStrategies(HINT_STRATEGY_TABLE.get());
    }
  }

  /**
   * Add hint to aggregate to indicate that the aggregate has nested agg call. Notice, the current
   * peek of relBuilder is expected to be LogicalAggregate.
   */
  public static void addNestedAggCallHintToAggregate(RelBuilder relBuilder) {
    assert relBuilder.peek() instanceof LogicalAggregate
        : "Hint HINT_AGG_ARGUMENTS can be added to LogicalAggregate only";
    final RelHint statHint =
        RelHint.builder(HINT_AGG_ARGUMENTS).hintOption(KEY_HAS_NESTED_AGG_CALL, "true").build();
    relBuilder.hints(statHint);
    if (relBuilder.getCluster().getHintStrategies() == HintStrategyTable.EMPTY) {
      relBuilder.getCluster().setHintStrategies(HINT_STRATEGY_TABLE.get());
    }
  }

  /** Return true if the aggregate will ignore null value bucket. */
  public static boolean ignoreNullBucket(Aggregate aggregate) {
    return aggregate.getHints().stream()
        .anyMatch(
            hint ->
                hint.hintName.equals(PPLHintUtils.HINT_AGG_ARGUMENTS)
                    && hint.kvOptions.getOrDefault(KEY_IGNORE_NULL_BUCKET, "false").equals("true"));
  }

  /** Return true if the aggregate has any nested agg call. */
  public static boolean hasNestedAggCall(Aggregate aggregate) {
    return aggregate.getHints().stream()
        .anyMatch(
            hint ->
                hint.hintName.equals(PPLHintUtils.HINT_AGG_ARGUMENTS)
                    && hint.kvOptions
                        .getOrDefault(KEY_HAS_NESTED_AGG_CALL, "false")
                        .equals("true"));
  }

  /**
   * Add dedup sort info hint to aggregate so that AggregateAnalyzer can set top_hits sort. Only the
   * first field collation is used since top_hits supports a single sort field.
   */
  public static void addDedupSortHintToAggregate(
      RelBuilder relBuilder, RelCollation collation, java.util.List<String> fieldNames) {
    assert relBuilder.peek() instanceof LogicalAggregate
        : "Hint HINT_AGG_ARGUMENTS can be added to LogicalAggregate only";
    RelFieldCollation fc = collation.getFieldCollations().get(0);
    String sortField = fieldNames.get(fc.getFieldIndex());
    String sortOrder = fc.direction.isDescending() ? "DESC" : "ASC";
    final RelHint sortHint =
        RelHint.builder(HINT_AGG_ARGUMENTS)
            .hintOption(KEY_DEDUP_SORT_FIELD, sortField)
            .hintOption(KEY_DEDUP_SORT_ORDER, sortOrder)
            .build();
    relBuilder.hints(sortHint);
    if (relBuilder.getCluster().getHintStrategies() == HintStrategyTable.EMPTY) {
      relBuilder.getCluster().setHintStrategies(HINT_STRATEGY_TABLE.get());
    }
  }

  /** Return the dedup sort field name from aggregate hints, or null if not present. */
  public static @Nullable String getDedupSortField(Aggregate aggregate) {
    return aggregate.getHints().stream()
        .filter(hint -> hint.hintName.equals(HINT_AGG_ARGUMENTS))
        .map(hint -> hint.kvOptions.get(KEY_DEDUP_SORT_FIELD))
        .filter(java.util.Objects::nonNull)
        .findFirst()
        .orElse(null);
  }

  /** Return the dedup sort order from aggregate hints, or null if not present. */
  public static @Nullable String getDedupSortOrder(Aggregate aggregate) {
    return aggregate.getHints().stream()
        .filter(hint -> hint.hintName.equals(HINT_AGG_ARGUMENTS))
        .map(hint -> hint.kvOptions.get(KEY_DEDUP_SORT_ORDER))
        .filter(java.util.Objects::nonNull)
        .findFirst()
        .orElse(null);
  }
}
