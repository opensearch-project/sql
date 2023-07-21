/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.response.agg;

import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.bucket.filter.Filter;

/**
 * {@link Filter} Parser.
 * The current use case is filter aggregation, e.g. avg(age) filter(balance>0). The filter parser
 * do nothing and return the result from metricsParser.
 */
@Builder
@EqualsAndHashCode
public class FilterParser implements MetricParser {

  private final MetricParser metricsParser;

  @Getter private final String name;

  @Override
  public Map<String, Object> parse(Aggregation aggregations) {
    return metricsParser.parse(((Filter) aggregations).getAggregations().asList().get(0));
  }
}
