/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.functions.response;

import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.EXEMPLARS_KEY;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.LABELS_KEY;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.SERIES_LABELS_KEY;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TIMESTAMP_KEY;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TRACE_ID_KEY;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.VALUE_KEY;

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

public class QueryExemplarsFunctionResponseHandle implements PrometheusFunctionResponseHandle {
  private Iterator<ExprValue> responseIterator;
  private ExecutionEngine.Schema schema;

  /**
   * Constructor.
   *
   * @param responseArray Query Exemplars responseObject.
   */
  public QueryExemplarsFunctionResponseHandle(JSONArray responseArray) {
    constructIteratorAndSchema(responseArray);
  }

  private void constructIteratorAndSchema(JSONArray responseArray) {
    List<ExecutionEngine.Schema.Column> columnList = new ArrayList<>();
    columnList.add(new ExecutionEngine.Schema.Column(SERIES_LABELS_KEY, SERIES_LABELS_KEY, STRUCT));
    columnList.add(new ExecutionEngine.Schema.Column(EXEMPLARS_KEY, EXEMPLARS_KEY,
        ExprCoreType.ARRAY));
    this.schema = new ExecutionEngine.Schema(columnList);
    List<ExprValue> result = new ArrayList<>();
    for (int i = 0; i < responseArray.length(); i++) {
      JSONObject rowObject = responseArray.getJSONObject(i);
      LinkedHashMap<String, ExprValue> rowMap = new LinkedHashMap<>();
      JSONObject seriesLabels = rowObject.getJSONObject(SERIES_LABELS_KEY);
      rowMap.put(SERIES_LABELS_KEY, constructSeriesLabels(seriesLabels));
      JSONArray exemplars = rowObject.getJSONArray(EXEMPLARS_KEY);
      rowMap.put(EXEMPLARS_KEY, constructExemplarList(exemplars));
      result.add(new ExprTupleValue(rowMap));
    }
    this.responseIterator = result.iterator();
  }

  private ExprValue constructSeriesLabels(JSONObject seriesLabels) {
    LinkedHashMap<String, ExprValue> seriesLabelsMap = new LinkedHashMap<>();
    seriesLabels.keySet()
        .forEach(key -> seriesLabelsMap.put(key, new ExprStringValue(seriesLabels.getString(key))));
    return new ExprTupleValue(seriesLabelsMap);
  }

  private ExprValue constructExemplarList(JSONArray exemplars) {
    List<ExprValue> exemplarsList = new ArrayList<>();
    for (int i = 0; i < exemplars.length(); i++) {
      JSONObject exemplarsJSONObject = exemplars.getJSONObject(i);
      exemplarsList.add(constructExemplar(exemplarsJSONObject));
    }
    return new ExprCollectionValue(exemplarsList);
  }

  private ExprValue constructExemplar(JSONObject exemplarsJSONObject) {
    LinkedHashMap<String, ExprValue> exemplarHashMap = new LinkedHashMap<>();
    exemplarHashMap.put(LABELS_KEY,
        constructLabelsInExemplar(exemplarsJSONObject.getJSONObject(LABELS_KEY)));
    exemplarHashMap.put(TIMESTAMP_KEY,
        new ExprTimestampValue(Instant.ofEpochMilli((long)(
            exemplarsJSONObject.getDouble(TIMESTAMP_KEY) * 1000))));
    exemplarHashMap.put(VALUE_KEY,
        new ExprDoubleValue(exemplarsJSONObject.getDouble(VALUE_KEY)));
    return new ExprTupleValue(exemplarHashMap);
  }

  private ExprValue constructLabelsInExemplar(JSONObject labelsObject) {
    LinkedHashMap<String, ExprValue> labelsMap = new LinkedHashMap<>();
    for (String key : labelsObject.keySet()) {
      labelsMap.put(key, new ExprStringValue(labelsObject.getString(key)));
    }
    return new ExprTupleValue(labelsMap);
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
