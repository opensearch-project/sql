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
