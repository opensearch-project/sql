/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.range.Range;

@Getter
@EqualsAndHashCode
public class BucketAggregationParser implements OpenSearchAggregationResponseParser {
  private final OpenSearchAggregationResponseParser subAggParser;

  public BucketAggregationParser(OpenSearchAggregationResponseParser subAggParser) {
    this.subAggParser = subAggParser;
  }

  @Override
  public List<Map<String, Object>> parse(Aggregations aggregations) {
    if (subAggParser instanceof BucketAggregationParser) {
      Aggregation aggregation = aggregations.asList().getFirst();
      if (!(aggregation instanceof MultiBucketsAggregation)) {
        throw new IllegalStateException(
            "BucketAggregationParser can only be used with MultiBucketsAggregation");
      }
      return ((MultiBucketsAggregation) aggregation)
          .getBuckets().stream()
              .map(b -> parse(b, aggregation.getName()))
              .flatMap(List::stream)
              .toList();
    } else if (subAggParser instanceof LeafBucketAggregationParser) {
      return subAggParser.parse(aggregations);
    } else {
      throw new IllegalStateException(
          "Sub parsers of a BucketAggregationParser can only be either BucketAggregationParser or"
              + " LeafBucketAggregationParser");
    }
  }

  private List<Map<String, Object>> parse(MultiBucketsAggregation.Bucket bucket, String name) {
    List<Map<String, Object>> results = subAggParser.parse(bucket.getAggregations());
    if (bucket instanceof CompositeAggregation.Bucket compositeBucket) {
      Map<String, Object> common = new HashMap<>(compositeBucket.getKey());
      for (Map<String, Object> r : results) {
        r.putAll(common);
      }
    } else if (bucket instanceof Range.Bucket) {
      for (Map<String, Object> r : results) {
        r.put(name, bucket.getKey());
      }
    }
    return results;
  }

  @Override
  public List<Map<String, Object>> parse(SearchHits hits) {
    throw new UnsupportedOperationException(
        "BucketAggregationParser doesn't support parse(SearchHits)");
  }
}
