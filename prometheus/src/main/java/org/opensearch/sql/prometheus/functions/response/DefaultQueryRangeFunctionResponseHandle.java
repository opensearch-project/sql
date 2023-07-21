/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.functions.response;

import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.VALUE;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants;

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
    constructIteratorAndSchema();
  }

  private void constructIteratorAndSchema() {
    List<ExprValue> result = new ArrayList<>();
    List<ExecutionEngine.Schema.Column> columnList = new ArrayList<>();
    if ("matrix".equals(responseObject.getString("resultType"))) {
      JSONArray itemArray = responseObject.getJSONArray("result");
      for (int i = 0; i < itemArray.length(); i++) {
        JSONObject item = itemArray.getJSONObject(i);
        JSONObject metric = item.getJSONObject("metric");
        JSONArray values = item.getJSONArray("values");
        if (i == 0) {
          columnList = getColumnList(metric);
        }
        for (int j = 0; j < values.length(); j++) {
          LinkedHashMap<String, ExprValue> linkedHashMap =
              extractRow(metric, values.getJSONArray(j), columnList);
          result.add(new ExprTupleValue(linkedHashMap));
        }
      }
    } else {
      throw new RuntimeException(String.format("Unexpected Result Type: %s during Prometheus "
              + "Response Parsing. 'matrix' resultType is expected",
          responseObject.getString("resultType")));
    }
    this.schema = new ExecutionEngine.Schema(columnList);
    this.responseIterator = result.iterator();
  }

  @NotNull
  private static LinkedHashMap<String, ExprValue> extractRow(JSONObject metric,
         JSONArray values, List<ExecutionEngine.Schema.Column> columnList) {
    LinkedHashMap<String, ExprValue> linkedHashMap = new LinkedHashMap<>();
    for (ExecutionEngine.Schema.Column column : columnList) {
      if (PrometheusFieldConstants.TIMESTAMP.equals(column.getName())) {
        linkedHashMap.put(PrometheusFieldConstants.TIMESTAMP,
            new ExprTimestampValue(Instant.ofEpochMilli((long) (values.getDouble(0) * 1000))));
      } else if (column.getName().equals(VALUE)) {
        linkedHashMap.put(VALUE, new ExprDoubleValue(values.getDouble(1)));
      } else {
        linkedHashMap.put(column.getName(),
            new ExprStringValue(metric.getString(column.getName())));
      }
    }
    return linkedHashMap;
  }


  private List<ExecutionEngine.Schema.Column> getColumnList(JSONObject metric) {
    List<ExecutionEngine.Schema.Column> columnList = new ArrayList<>();
    columnList.add(new ExecutionEngine.Schema.Column(PrometheusFieldConstants.TIMESTAMP,
        PrometheusFieldConstants.TIMESTAMP, ExprCoreType.TIMESTAMP));
    columnList.add(new ExecutionEngine.Schema.Column(VALUE, VALUE, ExprCoreType.DOUBLE));
    for (String key : metric.keySet()) {
      columnList.add(new ExecutionEngine.Schema.Column(key, key, ExprCoreType.STRING));
    }
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
