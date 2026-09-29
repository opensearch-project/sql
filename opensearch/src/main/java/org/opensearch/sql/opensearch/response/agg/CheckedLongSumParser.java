/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.NumericMetricsAggregation;

/**
 * Narrows OpenSearch's double-based native sum to the BIGINT type declared by CHECKED_LONG_SUM.
 *
 * <p>OpenSearch may already have lost low-order precision before this parser receives the result. A
 * double at the positive BIGINT boundary is also ambiguous because {@code Long.MAX_VALUE} rounds to
 * {@code 2^63}; Java's narrowing conversion saturates that value to {@code Long.MAX_VALUE}.
 */
@EqualsAndHashCode
@RequiredArgsConstructor
public class CheckedLongSumParser implements MetricParser {

  private static final double TWO_POW_63 = 0x1p63;

  @Getter private final String name;

  @Override
  public List<Map<String, Object>> parse(Aggregation aggregation) {
    double value = ((NumericMetricsAggregation.SingleValue) aggregation).value();
    Long narrowed = Double.isNaN(value) ? null : narrow(value);
    return Collections.singletonList(
        new HashMap<>(Collections.singletonMap(aggregation.getName(), narrowed)));
  }

  static long narrow(double value) {
    if (!Double.isFinite(value) || value > TWO_POW_63 || value < -TWO_POW_63) {
      throw new ArithmeticException("BIGINT overflow in SUM");
    }
    return (long) value;
  }
}
