/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import org.opensearch.search.SearchHits;
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
  // countAggNameList dedicated the list of count aggregations which are filled by doc_count
  private List<String> countAggNameList = List.of();

  public BucketAggregationParser(MetricParser... metricParserList) {
    metricsParser = new MetricParserHelper(Arrays.asList(metricParserList));
  }

  public BucketAggregationParser(List<MetricParser> metricParserList) {
    metricsParser = new MetricParserHelper(metricParserList);
  }

  public BucketAggregationParser(
      List<MetricParser> metricParserList, List<String> countAggNameList) {
    metricsParser = new MetricParserHelper(metricParserList, countAggNameList);
    this.countAggNameList = countAggNameList;
  }

  @Override
  public List<Map<String, Object>> parse(Aggregations aggregations) {
    Aggregation agg = aggregations.asList().getFirst();
    return ((MultiBucketsAggregation) agg)
        .getBuckets().stream()
            .map(b -> parseBucket(b, agg.getName()))
            .flatMap(List::stream)
            .toList();
  }

  private List<Map<String, Object>> parseBucket(
      MultiBucketsAggregation.Bucket bucket, String name) {
    Aggregations aggregations = bucket.getAggregations();
    List<Map<String, Object>> results =
        isLeafAgg(aggregations)
            ? parseLeafAgg(aggregations, bucket.getDocCount())
            : parse(aggregations);
    for (Map<String, Object> r : results) {
      r.put(name, bucket.getKey());
    }
    return results;
  }

  private boolean isLeafAgg(Aggregations aggregations) {
    return !(aggregations.asList().size() == 1
        && aggregations.asList().get(0) instanceof MultiBucketsAggregation);
  }

  private List<Map<String, Object>> parseLeafAgg(Aggregations aggregations, long docCount) {
    Map<String, Object> resultMap = metricsParser.parse(aggregations);
    countAggNameList.forEach(countAggName -> resultMap.put(countAggName, docCount));
    return List.of(resultMap);
  }

  @Override
  public List<Map<String, Object>> parse(SearchHits hits) {
    throw new UnsupportedOperationException(
        "BucketAggregationParser doesn't support parse(SearchHits)");
  }
}
