/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.range.Range;

/**
 * Use BucketAggregationParser only when there is a single group-by key, it returns multiple
 * buckets. {@link CompositeAggregationParser} is used for multiple group by keys
 */
@EqualsAndHashCode
public class LeafBucketAggregationParser implements OpenSearchAggregationResponseParser {
  @Getter private final MetricParserHelper metricsParser;

  public LeafBucketAggregationParser(MetricParser... metricParserList) {
    metricsParser = new MetricParserHelper(Arrays.asList(metricParserList));
  }

  public LeafBucketAggregationParser(List<MetricParser> metricParserList) {
    metricsParser = new MetricParserHelper(metricParserList);
  }

  @Override
  public List<Map<String, Object>> parse(Aggregations aggregations) {
    Aggregation agg = aggregations.asList().getFirst();
    return ((MultiBucketsAggregation) agg)
        .getBuckets().stream()
            .map(b -> parse(b, agg.getName()))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
  }

  @Override
  public List<Map<String, Object>> parse(SearchHits hits) {
    throw new UnsupportedOperationException(
        "LeafBucketAggregationParser doesn't support parse(SearchHits)");
  }

  private Map<String, Object> parse(MultiBucketsAggregation.Bucket bucket, String name) {
    if (bucket instanceof CompositeAggregation.Bucket compositeBucket) {
      return parse(compositeBucket);
    }
    if (bucket instanceof Range.Bucket && bucket.getDocCount() == 0) {
      return null;
    }
    Map<String, Object> resultMap = new LinkedHashMap<>();
    resultMap.put(name, bucket.getKey());
    resultMap.putAll(metricsParser.parse(bucket.getAggregations()));
    return resultMap;
  }

  private Map<String, Object> parse(CompositeAggregation.Bucket bucket) {
    Map<String, Object> resultMap = new HashMap<>();
    resultMap.putAll(bucket.getKey());
    resultMap.putAll(metricsParser.parse(bucket.getAggregations()));
    return resultMap;
  }
}
