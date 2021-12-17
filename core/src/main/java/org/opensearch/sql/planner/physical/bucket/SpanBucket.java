/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.bucket;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.aggregation.AggregationState;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.expression.span.SpanExpression;

public class SpanBucket extends Group {
  private final List<NamedAggregator> aggregatorList;
  private final List<NamedExpression> groupByExprList;
  private final Rounding<?> rounding;

  /**
   * SpanBucket Constructor.
   */
  public SpanBucket(List<NamedAggregator> aggregatorList, List<NamedExpression> groupByExprList) {
    super(aggregatorList, groupByExprList);
    this.aggregatorList = aggregatorList;
    this.groupByExprList = groupByExprList;
    rounding = Rounding.createRounding(((SpanExpression) groupByExprList.get(0).getDelegated()));
  }

  @Override
  public void push(ExprValue inputValue) {
    Key spanKey = new Key(inputValue, groupByExprList, rounding);
    groupListMap.computeIfAbsent(spanKey, k ->
        aggregatorList.stream()
            .map(aggregator -> new AbstractMap.SimpleEntry<>(aggregator,
                aggregator.create()))
            .collect(Collectors.toList())
    );
    groupListMap.computeIfPresent(spanKey, (key, aggregatorList) -> {
      aggregatorList
          .forEach(entry -> entry.getKey().iterate(inputValue.bindingTuples(), entry.getValue()));
      return aggregatorList;
    });
  }

  @Override
  public List<ExprValue> result() {
    ExprValue[] buckets = rounding.createBuckets();
    LinkedHashMap<String, ExprValue> emptyBucketTuple = new LinkedHashMap<>();
    String spanKey = null;
    for (Map.Entry<Group.Key, List<Map.Entry<NamedAggregator, AggregationState>>>
        entry : groupListMap.entrySet()) {
      LinkedHashMap<String, ExprValue> tupleMap = new LinkedHashMap<>(entry.getKey().groupKeyMap());
      if (spanKey == null) {
        spanKey = ((Key) entry.getKey()).namedSpan.getNameOrAlias();
      }
      for (Map.Entry<NamedAggregator, AggregationState> stateEntry : entry.getValue()) {
        tupleMap.put(stateEntry.getKey().getName(), stateEntry.getValue().result());
        if (emptyBucketTuple.isEmpty()) {
          entry.getKey().groupKeyMap().keySet().forEach(key -> emptyBucketTuple.put(key, null));
          emptyBucketTuple.put(stateEntry.getKey().getName(), ExprValueUtils.fromObjectValue(0));
        }
      }
      int index = rounding.locate(((SpanBucket.Key) entry.getKey()).getRoundedValue());
      buckets[index] = ExprTupleValue.fromExprValueMap(tupleMap);
    }
    return ImmutableList.copyOf(rounding.fillBuckets(buckets, emptyBucketTuple, spanKey));
  }

  @EqualsAndHashCode(callSuper = false)
  @VisibleForTesting
  public static class Key extends Group.Key {
    @Getter
    private final ExprValue roundedValue;
    private final NamedExpression namedSpan;

    /**
     * SpanBucket.Key Constructor.
     */
    public Key(ExprValue value, List<NamedExpression> groupByExprList, Rounding<?> rounding) {
      super(value, groupByExprList);
      namedSpan = groupByExprList.get(0);
      ExprValue actualValue =  ((SpanExpression) namedSpan.getDelegated()).getField()
          .valueOf(value.bindingTuples());
      roundedValue = rounding.round(actualValue);
    }

    /**
     * Return the Map of span key and its actual value.
     */
    @Override
    public LinkedHashMap<String, ExprValue> groupKeyMap() {
      LinkedHashMap<String, ExprValue> map = new LinkedHashMap<>();
      map.put(namedSpan.getNameOrAlias(), roundedValue);
      return map;
    }
  }

}
