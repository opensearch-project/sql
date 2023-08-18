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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;

/** Composite Aggregation Parser which include composite aggregation and metric parsers. */
@EqualsAndHashCode
public class CompositeAggregationParser implements OpenSearchAggregationResponseParser {

  private final MetricParserHelper metricsParser;

  public CompositeAggregationParser(MetricParser... metricParserList) {
    metricsParser = new MetricParserHelper(Arrays.asList(metricParserList));
  }

  public CompositeAggregationParser(List<MetricParser> metricParserList) {
    metricsParser = new MetricParserHelper(metricParserList);
  }

  @Override
  public List<Map<String, Object>> parse(Aggregations aggregations) {
    return ((CompositeAggregation) aggregations.asList().get(0))
        .getBuckets().stream().map(this::parse).collect(Collectors.toList());
  }

  private Map<String, Object> parse(CompositeAggregation.Bucket bucket) {
    Map<String, Object> resultMap = new HashMap<>();
    resultMap.putAll(bucket.getKey());
    resultMap.putAll(metricsParser.parse(bucket.getAggregations()));
    return resultMap;
  }
}
