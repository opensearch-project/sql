/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.sql.common.utils.StringUtils;

/** Parse multiple metrics in one bucket. */
@Getter
@EqualsAndHashCode
@RequiredArgsConstructor
public class MetricParserHelper {

  private final Map<String, MetricParser> metricParserMap;
  // countAggNameList dedicated the list of count aggregations which are filled by doc_count
  private final List<String> countAggNameList;

  public MetricParserHelper(List<MetricParser> metricParserList) {
    metricParserMap =
        metricParserList.stream().collect(Collectors.toMap(MetricParser::getName, m -> m));
    this.countAggNameList = List.of();
  }

  /** MetricParserHelper with count aggregation name list, used in v3 */
  public MetricParserHelper(List<MetricParser> metricParserList, List<String> countAggNameList) {
    metricParserMap =
        metricParserList.stream().collect(Collectors.toMap(MetricParser::getName, m -> m));
    this.countAggNameList = countAggNameList;
  }

  /**
   * Parse {@link Aggregations}.
   *
   * @param aggregations {@link Aggregations}
   * @return the map between metric name and metric value.
   */
  public Map<String, Object> parse(Aggregations aggregations) {
    Map<String, Object> resultMap = new HashMap<>();
    for (Aggregation aggregation : aggregations) {
      if (metricParserMap.containsKey(aggregation.getName())) {
        resultMap.putAll(metricParserMap.get(aggregation.getName()).parse(aggregation));
      } else {
        throw new RuntimeException(
            StringUtils.format(
                "couldn't parse field %s in aggregation response", aggregation.getName()));
      }
    }
    return resultMap;
  }
}
