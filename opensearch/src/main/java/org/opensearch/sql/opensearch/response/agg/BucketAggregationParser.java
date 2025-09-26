/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;

public class BucketAggregationParser implements OpenSearchAggregationResponseParser {
  private final OpenSearchAggregationResponseParser subAggParser;

  public BucketAggregationParser(OpenSearchAggregationResponseParser subAggParser) {
    this.subAggParser = subAggParser;
  }

  @Override
  public List<Map<String, Object>> parse(Aggregations aggregations) {
    if (subAggParser instanceof BucketAggregationParser) {
      return aggregations.asList().stream()
          .map(
              aggregation -> {
                if (aggregation instanceof CompositeAggregation) {
                  return (CompositeAggregation) aggregation;
                } else {
                  return (MultiBucketsAggregation) aggregation;
                }
              })
          .map(MultiBucketsAggregation::getBuckets)
          .flatMap(List::stream)
          .map(this::parse)
          .flatMap(List::stream)
          .collect(Collectors.toList());
    } else if (subAggParser instanceof LeafBucketAggregationParser) {
      return subAggParser.parse(aggregations);
    } else {
      throw new IllegalStateException(
          "Sub parsers of a BucketAggregationParser can only be either BucketAggregationParser or"
              + " LeafBucketAggregationParser");
    }
  }

  private List<Map<String, Object>> parse(MultiBucketsAggregation.Bucket bucket) {
    if (bucket instanceof CompositeAggregation.Bucket compositeBucket) {
      return parse(compositeBucket);
    }
    List<Map<String, Object>> results = new ArrayList<>();
    for (Aggregation subAgg : bucket.getAggregations()) {
      var sub = (Aggregations) subAgg;
      results.addAll(subAggParser.parse(sub));
    }
    return results;
  }

  private List<Map<String, Object>> parse(CompositeAggregation.Bucket bucket) {
    Map<String, Object> common = new HashMap<>(bucket.getKey());
    List<Map<String, Object>> results = subAggParser.parse(bucket.getAggregations());
    for (Map<String, Object> r : results) {
      r.putAll(common);
    }
    return results;
  }
}
