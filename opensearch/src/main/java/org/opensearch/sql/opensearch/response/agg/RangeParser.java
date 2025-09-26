/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.bucket.range.Range;

/**
 * Parser for {@link Range} aggregations
 * (org.opensearch.search.aggregations.bucket.range.InternalRange). Parses range bucket aggregations
 * and returns a map with range keys and their document counts.
 */
@EqualsAndHashCode
@RequiredArgsConstructor
public class RangeParser implements MetricParser {

  @Getter private final String name;

  @Override
  public Map<String, Object> parse(Aggregation aggregation) {
    Range rangeAgg = (Range) aggregation;
    Map<String, Object> result = new LinkedHashMap<>();

    for (Range.Bucket bucket : rangeAgg.getBuckets()) {
      String key = bucket.getKeyAsString();
      if (key == null || key.isEmpty()) {
        // Generate key from range bounds if no explicit key
        key = generateRangeKey(bucket);
      }
      if (bucket.getAggregations() != null && bucket.getDocCount() > 0) {
        result.put(key, bucket.getAggregations());
      }
    }
    return result;
  }

  /**
   * Generates a human-readable key for range buckets without explicit keys.
   *
   * @param bucket the range bucket
   * @return formatted range key (e.g., "10.0-20.0", "*-10.0", "20.0-*")
   */
  private String generateRangeKey(Range.Bucket bucket) {
    Object from = bucket.getFrom();
    Object to = bucket.getTo();

    String fromStr = (from == null) ? "*" : from.toString();
    String toStr = (to == null) ? "*" : to.toString();

    return fromStr + "-" + toStr;
  }
}
