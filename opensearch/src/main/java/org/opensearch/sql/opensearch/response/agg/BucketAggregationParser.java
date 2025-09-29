/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.range.Range;

/**
 * Parser for bucket aggregations that contain sub-aggregations. This parser handles multiple levels
 * of multi-bucket aggregations by delegates sublevels to sub-parsers.
 *
 * <p>Please note that it does not handle metric or value count responses -- they should be parsed
 * only in {@link LeafBucketAggregationParser}.
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class BucketAggregationParser extends AbstractBucketAggregationParser {
  /** The sub-aggregation parser used to process nested aggregations within each bucket. */
  private final AbstractBucketAggregationParser subAggParser;

  /**
   * Constructs a new BucketAggregationParser with the specified sub-aggregation parser.
   *
   * @param subAggParser the parser to handle sublevel multi-bucket aggregations within each bucket
   */
  public BucketAggregationParser(AbstractBucketAggregationParser subAggParser) {
    this.subAggParser = subAggParser;
  }

  /**
   * Parses the provided aggregations into a list of maps containing the aggregated data. This
   * method handles multi-bucket aggregations by processing each bucket and merging the results with
   * bucket-specific key information.
   *
   * @param aggregations the aggregations to parse
   * @return a list of maps containing the parsed aggregation data
   * @throws IllegalStateException if the aggregation type is not supported or if the sub-parser
   *     type is invalid
   */
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
      Map<String, Object> common = extract(compositeBucket);
      for (Map<String, Object> r : results) {
        r.putAll(common);
      }
    } else if (bucket instanceof Range.Bucket rangeBucket) {
      Map<String, Object> common = extract(rangeBucket, name);
      for (Map<String, Object> r : results) {
        r.putAll(common);
      }
    }
    return results;
  }
}
