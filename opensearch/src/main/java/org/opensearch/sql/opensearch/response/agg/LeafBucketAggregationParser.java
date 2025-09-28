/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.Arrays;
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
  // countAggNameList dedicated the list of count aggregations which are filled by doc_count
  private List<String> countAggNameList = List.of();

  public LeafBucketAggregationParser(MetricParser... metricParserList) {
    metricsParser = new MetricParserHelper(Arrays.asList(metricParserList));
  }

  public LeafBucketAggregationParser(List<MetricParser> metricParserList) {
    metricsParser = new MetricParserHelper(metricParserList);
  }

  /** CompositeAggregationParser with count aggregation name list, used in v3 */
  public LeafBucketAggregationParser(
      List<MetricParser> metricParserList, List<String> countAggNameList) {
    metricsParser = new MetricParserHelper(metricParserList);
    this.countAggNameList = countAggNameList;
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
    Map<String, Object> result = metricsParser.parse(bucket.getAggregations());
    if (bucket instanceof CompositeAggregation.Bucket compositeBucket) {
      result.putAll(compositeBucket.getKey());
    } else if (bucket instanceof Range.Bucket && bucket.getDocCount() == 0) {
      return null;
    }
    result.put(name, bucket.getKey());
    countAggNameList.forEach(n -> result.put(n, bucket.getDocCount()));
    return result;
  }
}
