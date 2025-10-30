/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.opensearch.sql.common.setting.Settings.Key.CALCITE_ENGINE_ENABLED;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

@UtilityClass
public class CalciteUtils {

  public static UnsupportedOperationException getOnlyForCalciteException(String feature) {
    return new UnsupportedOperationException(
        feature + " is supported only when " + CALCITE_ENGINE_ENABLED.getKeyValue() + "=true");
  }

  public static <T> Pair<List<T>, List<T>> partition(
      Collection<T> collection, Predicate<T> predicate) {
    Map<Boolean, List<T>> map = collection.stream().collect(Collectors.partitioningBy(predicate));
    return new ImmutablePair<>(map.get(true), map.get(false));
  }

  public static void buildAggregateWithTrimUnusedFields(
      ImmutableBitSet groupSet,
      List<AggregateCall> aggCalls,
      int sourceFieldCount,
      RelBuilder relBuilder) {
    /* Eliminate unused fields in the child project */
    final Set<Integer> fieldsUsed = RelOptUtil.getAllFields2(groupSet, aggCalls);
    if (fieldsUsed.size() < sourceFieldCount) {
      // Some fields are computed but not used. Prune them.
      final Map<Integer, Integer> sourceFieldToTargetFieldMap = new HashMap<>();
      for (int source : fieldsUsed) {
        sourceFieldToTargetFieldMap.put(source, sourceFieldToTargetFieldMap.size());
      }
      groupSet = groupSet.permute(sourceFieldToTargetFieldMap);
      final Mappings.TargetMapping targetMapping =
          Mappings.target(sourceFieldToTargetFieldMap, sourceFieldCount, fieldsUsed.size());
      aggCalls = aggCalls.stream().map(aggCall -> aggCall.transform(targetMapping)).toList();
      // Project the used fields
      relBuilder.project(relBuilder.fields(fieldsUsed.stream().toList()));
    }
    relBuilder.aggregate(relBuilder.groupKey(groupSet, ImmutableList.of(groupSet)), aggCalls);
  }
}
