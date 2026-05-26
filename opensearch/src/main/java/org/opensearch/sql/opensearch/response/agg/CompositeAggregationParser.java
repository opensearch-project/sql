/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;

/**
 * Composite Aggregation Parser which include composite aggregation and metric parsers. This is only
 * for the aggregation with multiple group-by keys. Use {@link BucketAggregationParser} when there
 * is only one group-by key.
 */
@Getter
@EqualsAndHashCode
public class CompositeAggregationParser implements OpenSearchAggregationResponseParser {

  private final MetricParserHelper metricsParser;

  public CompositeAggregationParser(MetricParser... metricParserList) {
    metricsParser = new MetricParserHelper(Arrays.asList(metricParserList));
  }

  public CompositeAggregationParser(List<MetricParser> metricParserList) {
    metricsParser = new MetricParserHelper(metricParserList);
  }

  @Override
  public List<Map<String, Object>> parse(Aggregations aggregations) {
    return ((CompositeAggregation) aggregations.asList().get(0))
        .getBuckets().stream().map(this::parse).flatMap(Collection::stream).toList();
  }

  private List<Map<String, Object>> parse(CompositeAggregation.Bucket bucket) {
    List<Map<String, Object>> resultMapList = new ArrayList<>();
    resultMapList.add(new HashMap<>(bucket.getKey()));
    resultMapList.addAll(metricsParser.parse(bucket.getAggregations()));
    return resultMapList;
  }

  @Override
  public List<Map<String, Object>> parse(SearchHits hits) {
    throw new UnsupportedOperationException(
        "CompositeAggregationParser doesn't support parse(SearchHits)");
  }
}
