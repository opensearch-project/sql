/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.bucket.terms.Terms;

/** {@link Terms} aggregation parser for list() and values() functions. */
@EqualsAndHashCode
@RequiredArgsConstructor
public class TermsParser implements MetricParser {

  @Getter private final String name;

  @Override
  public Map<String, Object> parse(Aggregation agg) {
    Terms terms = (Terms) agg;
    List<String> values =
        terms.getBuckets().stream()
            .map(bucket -> String.valueOf(bucket.getKey())) // Convert all keys to strings
            .collect(Collectors.toList());

    // Note: OpenSearch terms aggregation returns results in order by doc_count (desc) by default
    // For list() function: preserve this order and limit to 100 (handled by .size(100) in builder)
    // For values() function: sort lexicographically (OpenSearch doesn't guarantee lexicographic
    // order by default)
    // The specific behavior (sort/no-sort) is determined by the aggregation builder configuration

    return Collections.singletonMap(agg.getName(), values);
  }
}
