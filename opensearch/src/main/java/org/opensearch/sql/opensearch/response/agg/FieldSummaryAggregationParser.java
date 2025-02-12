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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.sql.data.type.ExprType;

/** No Bucket Aggregation Parser which include only metric parsers. */
public class FieldSummaryAggregationParser implements OpenSearchAggregationResponseParser {

  private final MetricParserHelper metricsParser;
  private Map<String, Map.Entry<String, ExprType>> aggregationToFieldNameMap;

  public FieldSummaryAggregationParser(
      List<MetricParser> metricParserList,
      Map<String, Map.Entry<String, ExprType>> aggregationToFieldNameMap) {
    metricParserList.addAll(
        Arrays.asList(
            new SingleValueParser("Field"),
            new SingleValueParser("Count"),
            new SingleValueParser("Distinct"),
            new SingleValueParser("Avg"),
            new SingleValueParser("Max"),
            new SingleValueParser("Min"),
            new SingleValueParser("Type")));
    metricsParser = new MetricParserHelper(metricParserList);
    this.aggregationToFieldNameMap = aggregationToFieldNameMap;
  }

  @Override
  public List<Map<String, Object>> parse(Aggregations aggregations) {
    List<Map<String, Object>> summaryTable = new ArrayList<>();

    Map<Map.Entry<String, ExprType>, List<Aggregation>> aggregationsByField = new HashMap<>();

    for (Aggregation aggregation : aggregations.asList()) {
      String aggregationName = aggregation.getName();
      Map.Entry<String, ExprType> aggregationField = aggregationToFieldNameMap.get(aggregationName);

      aggregationsByField.putIfAbsent(aggregationField, new ArrayList<Aggregation>());
      aggregationsByField.get(aggregationField).add(aggregation);
    }

    for (Map.Entry<Map.Entry<String, ExprType>, List<Aggregation>> entry :
        aggregationsByField.entrySet()) {
      Map<String, Object> row = new HashMap<>();
      row.put("Field", entry.getKey().getKey());
      row.putAll(transformKeys(metricsParser.parse(new Aggregations(entry.getValue()))));
      row.put("Type", entry.getKey().getValue().toString());
      summaryTable.add(row);
    }

    return summaryTable;
  }

  private static Map<String, Object> transformKeys(Map<String, Object> map) {
    Map<String, Object> newMap = new HashMap<>();

    for (Map.Entry<String, Object> entry : map.entrySet()) {
      String originalKey = entry.getKey();
      String newKey = extractAggregationType(originalKey);
      newMap.put(newKey, entry.getValue());
    }

    return newMap;
  }

  private static String extractAggregationType(String key) {
    String[] aggregationTypes = {"Count", "Avg", "Min", "Max", "Distinct"};

    for (String type : aggregationTypes) {
      if (key.startsWith(type)) {
        return type;
      }
    }

    return "Unknown";
  }
}
