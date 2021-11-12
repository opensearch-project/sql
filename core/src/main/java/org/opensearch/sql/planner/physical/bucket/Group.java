/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.bucket;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.aggregation.AggregationState;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.storage.bindingtuple.BindingTuple;

@VisibleForTesting
@RequiredArgsConstructor
public class Group {
  @Getter
  private final List<NamedAggregator> aggregatorList;
  @Getter
  private final List<NamedExpression> groupByExprList;

  protected final Map<Key, List<Map.Entry<NamedAggregator, AggregationState>>> groupListMap =
      new HashMap<>();

  /**
   * Push the BindingTuple to Group. Two functions will be applied to each BindingTuple to
   * generate the {@link Key} and {@link AggregationState}
   * Key = GroupKey(bindingTuple), State = Aggregator(bindingTuple)
   */
  public void push(ExprValue inputValue) {
    Key groupKey = new Key(inputValue, groupByExprList);
    groupListMap.computeIfAbsent(groupKey, k ->
        aggregatorList.stream()
            .map(aggregator -> new AbstractMap.SimpleEntry<>(aggregator,
                aggregator.create()))
            .collect(Collectors.toList())
    );
    groupListMap.computeIfPresent(groupKey, (key, aggregatorList) -> {
      aggregatorList
          .forEach(entry -> entry.getKey().iterate(inputValue.bindingTuples(), entry.getValue()));
      return aggregatorList;
    });
  }

  /**
   * Get the list of {@link BindingTuple} for each group.
   */
  public List<ExprValue> result() {
    ImmutableList.Builder<ExprValue> resultBuilder = new ImmutableList.Builder<>();
    for (Map.Entry<Key, List<Map.Entry<NamedAggregator, AggregationState>>>
        entry : groupListMap.entrySet()) {
      LinkedHashMap<String, ExprValue> map = new LinkedHashMap<>(entry.getKey().groupKeyMap());
      for (Map.Entry<NamedAggregator, AggregationState> stateEntry : entry.getValue()) {
        map.put(stateEntry.getKey().getName(), stateEntry.getValue().result());
      }
      resultBuilder.add(ExprTupleValue.fromExprValueMap(map));
    }
    return resultBuilder.build();
  }

  /**
   * Group Key.
   */
  @EqualsAndHashCode
  @VisibleForTesting
  public static class Key {
    private final List<ExprValue> groupByValueList;
    private final List<NamedExpression> groupByExprList;

    /**
     * GroupKey constructor.
     */
    public Key(ExprValue value, List<NamedExpression> groupByExprList) {
      this.groupByValueList = new ArrayList<>();
      this.groupByExprList = groupByExprList;
      for (Expression groupExpr : groupByExprList) {
        this.groupByValueList.add(groupExpr.valueOf(value.bindingTuples()));
      }
    }

    /**
     * Return the Map of group field and group field value.
     */
    public LinkedHashMap<String, ExprValue> groupKeyMap() {
      LinkedHashMap<String, ExprValue> map = new LinkedHashMap<>();
      for (int i = 0; i < groupByExprList.size(); i++) {
        map.put(groupByExprList.get(i).getNameOrAlias(), groupByValueList.get(i));
      }
      return map;
    }
  }
}
