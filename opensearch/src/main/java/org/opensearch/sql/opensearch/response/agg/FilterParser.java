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

import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.bucket.filter.Filter;

/**
 * {@link Filter} Parser. The current use case is filter aggregation, e.g. avg(age)
 * filter(balance>0). The filter parser do nothing and return the result from metricsParser.
 */
@Builder
@EqualsAndHashCode
public class FilterParser implements MetricParser {

  private final MetricParser metricsParser;

  @Getter private final String name;

  @Override
  public Map<String, Object> parse(Aggregation aggregations) {
    return metricsParser.parse(((Filter) aggregations).getAggregations().asList().get(0));
  }
}
