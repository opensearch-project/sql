/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;

/**
 * Use BucketAggregationParser only when there is a single group-by key, it returns multiple
 * buckets. {@link CompositeAggregationParser} is used for multiple group by keys
 */
@EqualsAndHashCode
public class BucketAggregationParser implements OpenSearchAggregationResponseParser {
  private final MetricParserHelper metricsParser;

  public BucketAggregationParser(MetricParser... metricParserList) {
    metricsParser = new MetricParserHelper(Arrays.asList(metricParserList));
  }

  public BucketAggregationParser(List<MetricParser> metricParserList) {
    metricsParser = new MetricParserHelper(metricParserList);
  }

  @Override
  public List<Map<String, Object>> parse(Aggregations aggregations) {
    Aggregation agg = aggregations.asList().getFirst();
    return ((MultiBucketsAggregation) agg)
        .getBuckets().stream().map(b -> parse(b, agg.getName())).collect(Collectors.toList());
  }

  private Map<String, Object> parse(MultiBucketsAggregation.Bucket bucket, String keyName) {
    Map<String, Object> resultMap = new LinkedHashMap<>();
    resultMap.put(keyName, bucket.getKey());
    resultMap.putAll(metricsParser.parse(bucket.getAggregations()));
    return resultMap;
  }
}
