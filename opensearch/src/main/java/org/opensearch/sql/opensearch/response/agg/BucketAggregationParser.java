/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
public class BucketAggregationParser implements OpenSearchAggregationResponseParser {
  @Getter private final MetricParserHelper metricsParser;
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
            .filter(Objects::nonNull)
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

    if (bucket instanceof CompositeAggregation.Bucket compositeBucket) {
      Map<String, Object> common = extract(compositeBucket);
      for (Map<String, Object> r : results) {
        r.putAll(common);
      }
    } else if (bucket instanceof Range.Bucket) {
      // return null so that an empty range will be filtered out
      if (bucket.getDocCount() == 0) {
        return null;
      }
      // the content of the range bucket is extracted with `r.put(name, bucket.getKey())` below
    }
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

  /**
   * Extracts key-value pairs from a composite aggregation bucket without processing its
   * sub-aggregations.
   *
   * <p>For example, for the following CompositeAggregation bucket in response:
   *
   * <pre>{@code
   * {
   *   "key": {
   *     "firstname": "William",
   *     "lastname": "Shakespeare"
   *   },
   *   "sub_agg_name": {
   *     "buckets": []
   *   }
   * }
   * }</pre>
   *
   * It returns {@code {"firstname": "William", "lastname": "Shakespeare"}} as the response.
   *
   * @param bucket the composite aggregation bucket to extract data from
   * @return a map containing the bucket's key-value pairs
   */
  protected Map<String, Object> extract(CompositeAggregation.Bucket bucket) {
    return bucket.getKey();
  }
}
