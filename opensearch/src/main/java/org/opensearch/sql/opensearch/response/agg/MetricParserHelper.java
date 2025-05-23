/*
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
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

  public MetricParserHelper(List<MetricParser> metricParserList) {
    metricParserMap =
        metricParserList.stream().collect(Collectors.toMap(MetricParser::getName, m -> m));
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
