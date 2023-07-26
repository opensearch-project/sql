/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.functions.response;

import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.LABELS;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TIMESTAMP;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.VALUE;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine;

/**
 * Default implementation of QueryRangeFunctionResponseHandle.
 */
public class DefaultQueryRangeFunctionResponseHandle implements QueryRangeFunctionResponseHandle {

  private final JSONObject responseObject;
  private Iterator<ExprValue> responseIterator;
  private ExecutionEngine.Schema schema;

  /**
   * Constructor.
   *
   * @param responseObject Prometheus responseObject.
   */
  public DefaultQueryRangeFunctionResponseHandle(JSONObject responseObject) {
    this.responseObject = responseObject;
    constructSchema();
    constructIterator();
  }

  private void constructIterator() {
    List<ExprValue> result = new ArrayList<>();
    if ("matrix".equals(responseObject.getString("resultType"))) {
      JSONArray itemArray = responseObject.getJSONArray("result");
      for (int i = 0; i < itemArray.length(); i++) {
        LinkedHashMap<String, ExprValue> linkedHashMap = new LinkedHashMap<>();
        JSONObject item = itemArray.getJSONObject(i);
        linkedHashMap.put(LABELS, extractLabels(item.getJSONObject("metric")));
        extractTimestampAndValues(item.getJSONArray("values"), linkedHashMap);
        result.add(new ExprTupleValue(linkedHashMap));
      }
    } else {
      throw new RuntimeException(String.format("Unexpected Result Type: %s during Prometheus "
              + "Response Parsing. 'matrix' resultType is expected",
          responseObject.getString("resultType")));
    }
    this.responseIterator = result.iterator();
  }

  private static void extractTimestampAndValues(JSONArray values,
                                                LinkedHashMap<String, ExprValue> linkedHashMap) {
    List<ExprValue> timestampList = new ArrayList<>();
    List<ExprValue> valueList = new ArrayList<>();
    for (int j = 0; j < values.length(); j++) {
      JSONArray value = values.getJSONArray(j);
      timestampList.add(new ExprTimestampValue(
          Instant.ofEpochMilli((long) (value.getDouble(0) * 1000))));
      valueList.add(new ExprDoubleValue(value.getDouble(1)));
    }
    linkedHashMap.put(TIMESTAMP,
        new ExprCollectionValue(timestampList));
    linkedHashMap.put(VALUE, new ExprCollectionValue(valueList));
  }

  private void constructSchema() {
    this.schema = new ExecutionEngine.Schema(getColumnList());
  }

  private ExprValue extractLabels(JSONObject metric) {
    LinkedHashMap<String, ExprValue> labelsMap = new LinkedHashMap<>();
    metric.keySet().forEach(key
        -> labelsMap.put(key, new ExprStringValue(metric.getString(key))));
    return new ExprTupleValue(labelsMap);
  }


  private List<ExecutionEngine.Schema.Column> getColumnList() {
    List<ExecutionEngine.Schema.Column> columnList = new ArrayList<>();
    columnList.add(new ExecutionEngine.Schema.Column(TIMESTAMP, TIMESTAMP, ExprCoreType.ARRAY));
    columnList.add(new ExecutionEngine.Schema.Column(VALUE, VALUE, ExprCoreType.ARRAY));
    columnList.add(new ExecutionEngine.Schema.Column(LABELS, LABELS, ExprCoreType.STRUCT));
    return columnList;
  }

  @Override
  public boolean hasNext() {
    return responseIterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return responseIterator.next();
  }

  @Override
  public ExecutionEngine.Schema schema() {
    return schema;
  }
}
