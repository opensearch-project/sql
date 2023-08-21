/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.collector;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.aggregation.AggregationState;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.storage.bindingtuple.BindingTuple;

/**
 * Each {@link NamedAggregator} defined in aggregators collect metrics from {@link BindingTuple}.
 */
public class MetricCollector implements Collector {

  /** List of {@link NamedAggregator}. */
  private final List<Map.Entry<NamedAggregator, AggregationState>> aggregators;

  /**
   * Constructor of {@link MetricCollector}.
   *
   * @param aggregators aggregators.
   */
  public MetricCollector(List<NamedAggregator> aggregators) {
    this.aggregators =
        aggregators.stream()
            .map(aggregator -> new AbstractMap.SimpleEntry<>(aggregator, aggregator.create()))
            .collect(Collectors.toList());
  }

  /**
   * Collect Metrics from BindingTuple.
   *
   * @param input {@link BindingTuple}.
   */
  public void collect(BindingTuple input) {
    aggregators.forEach(
        agg -> {
          agg.getKey().iterate(input, agg.getValue());
        });
  }

  /**
   * Get aggregation result from aggregators.
   *
   * @return List of {@link ExprValue}.
   */
  public List<ExprValue> results() {
    LinkedHashMap<String, ExprValue> map = new LinkedHashMap<>();
    aggregators.forEach(agg -> map.put(agg.getKey().getName(), agg.getValue().result()));
    return Collections.singletonList(ExprTupleValue.fromExprValueMap(map));
  }
}
