/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import com.google.common.collect.Streams;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.Percentile;
import org.opensearch.search.aggregations.metrics.Percentiles;

@EqualsAndHashCode
@RequiredArgsConstructor
public class PercentilesParser implements MetricParser {

  @Getter private final String name;

  @Override
  public List<Map<String, Object>> parse(Aggregation agg) {
    return Collections.singletonList(
        new HashMap<>(
            Collections.singletonMap(
                agg.getName(),
                // TODO a better implementation here is providing a class `MultiValueParser`
                // similar to `SingleValueParser`. However, there is no method `values()` available
                // in `org.opensearch.search.aggregations.metrics.MultiValue`.
                Streams.stream(((Percentiles) agg).iterator())
                    .map(Percentile::getValue)
                    .collect(Collectors.toList()))));
  }
}
