/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.physical.collector;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.storage.bindingtuple.BindingTuple;

/**
 * Interface of {@link BindingTuple} Collector.
 */
public interface Collector {

  /**
   * Collect from {@link BindingTuple}.
   *
   * @param tuple {@link BindingTuple}.
   */
  void collect(BindingTuple tuple);

  /**
   * Get Result from Collector.
   *
   * @return list of {@link ExprValue}.
   */
  List<ExprValue> results();

  /**
   * {@link Collector} tree builder.
   */
  @UtilityClass
  class Builder {
    /**
     * build {@link Collector}.
     */
    public static Collector build(List<NamedExpression> buckets,
                                  List<NamedAggregator> aggregators) {
      if (buckets.isEmpty()) {
        return new MetricCollector(aggregators);
      } else {
        return new BucketCollector(
            buckets.get(0),
            () -> build(ImmutableList.copyOf(buckets.subList(1, buckets.size())), aggregators));
      }
    }
  }
}
