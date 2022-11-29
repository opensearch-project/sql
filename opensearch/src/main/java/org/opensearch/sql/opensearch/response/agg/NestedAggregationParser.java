/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.nested.InternalNested;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;

/**
 * Nested Aggregation Parser which include nested aggregation and metric parsers.
 */
public class NestedAggregationParser implements OpenSearchAggregationResponseParser {

  private final MetricParserHelper metricsParser;

  public NestedAggregationParser(MetricParser... metricParserList) {
    metricsParser = new MetricParserHelper(Arrays.asList(metricParserList));
  }

  public NestedAggregationParser(List<MetricParser> metricParserList) {
    metricsParser = new MetricParserHelper(metricParserList);
  }

  @Override
  public List<Map<String, Object>> parse(Aggregations aggregations) {
    List<Map<String, Object>> resultMap = new ArrayList<>();
    for(Map.Entry<String, Aggregation> entry :
        ((InternalNested) aggregations.asList().get(0)).getAggregations().getAsMap().entrySet()) {
      if (entry.getValue() instanceof StringTerms) {
        ((StringTerms)entry.getValue()).getBuckets().stream()
            .forEach(k -> resultMap.add(parse(k, entry.getKey())));
      }
    }

  return resultMap;
  }

  private Map<String, Object> parse(StringTerms.Bucket bucket, String key) {
    Map<String, Object> resultMap = new HashMap<>();
    resultMap.put(key, bucket.getKeyAsString());
    resultMap.putAll(metricsParser.parse(bucket.getAggregations()));
    return resultMap;
  }
}
