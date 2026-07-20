/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.sql.opensearch.aggregation.InternalCheckedLongSum;

/** Parser for the exact native BIGINT sum aggregation. */
@EqualsAndHashCode
@RequiredArgsConstructor
public class CheckedLongSumParser implements MetricParser {

  @Getter private final String name;

  @Override
  public List<Map<String, Object>> parse(Aggregation aggregation) {
    return Collections.singletonList(
        new HashMap<>(
            Collections.singletonMap(
                aggregation.getName(), ((InternalCheckedLongSum) aggregation).longValue())));
  }
}
