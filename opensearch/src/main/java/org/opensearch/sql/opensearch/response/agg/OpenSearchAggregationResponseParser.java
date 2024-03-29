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

import java.util.List;
import java.util.Map;
import org.opensearch.search.aggregations.Aggregations;

/** OpenSearch Aggregation Response Parser. */
public interface OpenSearchAggregationResponseParser {

  /**
   * Parse the OpenSearch Aggregation Response.
   *
   * @param aggregations Aggregations.
   * @return aggregation result.
   */
  List<Map<String, Object>> parse(Aggregations aggregations);
}
