/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram;
import org.opensearch.search.aggregations.bucket.nested.InternalNested;
import org.opensearch.search.aggregations.bucket.range.Range;
import org.opensearch.search.aggregations.bucket.terms.InternalMultiTerms;

/**
 * Use BucketAggregationParser for {@link MultiBucketsAggregation}, where it returns multiple
 * buckets.
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
    Aggregation agg;
    if (aggregations.asList().getFirst() instanceof InternalNested) {
      agg = ((InternalNested) aggregations.asList().getFirst()).getAggregations().iterator().next();
    } else {
      agg = aggregations.asList().getFirst();
    }
    return ((MultiBucketsAggregation) agg)
        .getBuckets().stream()
            .map(b -> parseBucket(b, agg.getName()))
            .filter(Objects::nonNull)
            .flatMap(List::stream)
            .toList();
  }

  private List<Map<String, Object>> parseBucket(
      MultiBucketsAggregation.Bucket bucket, String name) {
    // return null so that an empty bucket of range or date span will be filtered out
    if (bucket instanceof Range.Bucket || bucket instanceof InternalAutoDateHistogram.Bucket) {
      if (bucket.getDocCount() == 0) {
        return null;
      }
    }

    Aggregations aggregations = bucket.getAggregations();
    List<Map<String, Object>> results =
        isLeafAgg(aggregations)
            ? parseLeafAgg(aggregations, bucket.getDocCount())
            : parse(aggregations);

    Optional<Map<String, Object>> common = extract(bucket, name);
    common.ifPresent(commonMap -> results.forEach(r -> r.putAll(commonMap)));
    return results;
  }

  private boolean isLeafAgg(Aggregations aggregations) {
    return !(aggregations.asList().size() == 1
        && aggregations.asList().get(0) instanceof MultiBucketsAggregation);
  }

  private List<Map<String, Object>> parseLeafAgg(Aggregations aggregations, long docCount) {
    List<Map<String, Object>> resultMapList = metricsParser.parse(aggregations);
    List<Map<String, Object>> maps =
        resultMapList.isEmpty() ? List.of(new HashMap<>()) : resultMapList;
    countAggNameList.forEach(countAggName -> maps.forEach(map -> map.put(countAggName, docCount)));
    return maps;
  }

  @Override
  public List<Map<String, Object>> parse(SearchHits hits) {
    throw new UnsupportedOperationException(
        "BucketAggregationParser doesn't support parse(SearchHits)");
  }

  /**
   * Extracts key-value pairs from different types of aggregation buckets without processing their
   * sub-aggregations.
   *
   * <p>For CompositeAggregation buckets, it extracts all key-value pairs from the bucket's key. For
   * example, for the following CompositeAggregation bucket in response:
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
   * It returns {@code {"firstname": "William", "lastname": "Shakespeare"}}.
   *
   * <p>For Range buckets, it creates a single key-value pair using the provided name and the
   * bucket's key.
   *
   * @param bucket the aggregation bucket to extract data from
   * @param name the aggregation name
   * @return an Optional containing the extracted key-value pairs
   */
  protected Optional<Map<String, Object>> extract(
      MultiBucketsAggregation.Bucket bucket, String name) {
    Map<String, Object> extracted;
    if (bucket instanceof CompositeAggregation.Bucket compositeBucket) {
      extracted = compositeBucket.getKey();
    } else if (bucket instanceof InternalMultiTerms.Bucket) {
      List<String> keys = Arrays.asList(name.split("\\|"));
      extracted =
          IntStream.range(0, keys.size())
              .boxed()
              .collect(Collectors.toMap(keys::get, ((List<Object>) bucket.getKey())::get));
    } else {
      extracted = Map.of(name, bucket.getKey());
    }
    return Optional.ofNullable(extracted);
  }
}
