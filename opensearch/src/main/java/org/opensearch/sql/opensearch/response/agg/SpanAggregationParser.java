/*
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 *
 */

package org.opensearch.sql.opensearch.response.agg;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;

public class SpanAggregationParser implements OpenSearchAggregationResponseParser {
  private final MetricParserHelper metricsParser;

  public SpanAggregationParser(MetricParser metricParser) {
    this.metricsParser = new MetricParserHelper(Collections.singletonList(metricParser));
  }

  @Override
  public List<Map<String, Object>> parse(Aggregations aggregations) {
    ImmutableList.Builder<Map<String, Object>> list = ImmutableList.builder();
    aggregations.asList().forEach(aggregation -> list
        .addAll(parseHistogram((Histogram) aggregation)));
    return list.build();
  }

  private List<Map<String, Object>> parseHistogram(Histogram histogram) {
    ImmutableList.Builder<Map<String, Object>> mapList = ImmutableList.builder();
    histogram.getBuckets().forEach(bucket -> {
      Map<String, Object> map = new HashMap<>();
      map.put(histogram.getName(), bucket.getKey().toString());
      Aggregation aggregation = bucket.getAggregations().asList().get(0);
      Map<String, Object> metricsAggMap = metricsParser.parse(bucket.getAggregations());
      map.put(aggregation.getName(), metricsAggMap.get(aggregation.getName()));
      mapList.add(map);
    });
    return mapList.build();
  }

}
