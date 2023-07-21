/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.response.agg;

import static org.opensearch.sql.opensearch.response.agg.Utils.handleNanInfValue;

import java.util.Collections;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.NumericMetricsAggregation;

/**
 * {@link NumericMetricsAggregation.SingleValue} metric parser.
 */
@EqualsAndHashCode
@RequiredArgsConstructor
public class SingleValueParser implements MetricParser {

  @Getter private final String name;

  @Override
  public Map<String, Object> parse(Aggregation agg) {
    return Collections.singletonMap(
        agg.getName(),
        handleNanInfValue(((NumericMetricsAggregation.SingleValue) agg).value()));
  }
}
