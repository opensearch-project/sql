/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import com.google.common.collect.Streams;
import java.util.Collections;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.Percentiles;

@EqualsAndHashCode
@RequiredArgsConstructor
public class SinglePercentileParser implements MetricParser {

  @Getter private final String name;

  @Override
  public Map<String, Object> parse(Aggregation agg) {
    return Collections.singletonMap(
        agg.getName(),
        // TODO `Percentiles` implements interface
        // `org.opensearch.search.aggregations.metrics.MultiValue`, but there is not
        // method `values()` available in this interface. So we
        Streams.stream(((Percentiles) agg).iterator()).findFirst().get().getValue());
  }
}
