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
import org.opensearch.search.aggregations.Aggregation;

/** Metric Aggregation Parser. */
public interface MetricParser {

  /** Get the name of metric parser. */
  String getName();

  /**
   * Parse the {@link Aggregation}.
   *
   * @param aggregation {@link Aggregation}
   * @return the map between metric name and metric value.
   */
  Map<String, Object> parse(Aggregation aggregation);
}
