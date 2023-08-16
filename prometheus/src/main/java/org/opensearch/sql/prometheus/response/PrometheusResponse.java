/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.response;

import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.LABELS;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.MATRIX_KEY;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.METRIC_KEY;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.RESULT_KEY;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.RESULT_TYPE_KEY;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.VALUES_KEY;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import lombok.NonNull;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.prometheus.storage.model.PrometheusResponseFieldNames;

public class PrometheusResponse implements Iterable<ExprValue> {

  private final JSONObject responseObject;

  private final PrometheusResponseFieldNames prometheusResponseFieldNames;

  /**
   * Constructor.
   *
   * @param responseObject Prometheus responseObject.
   * @param prometheusResponseFieldNames data model which contains field names for the metric
   *     measurement and timestamp fieldName.
   */
  public PrometheusResponse(
      JSONObject responseObject, PrometheusResponseFieldNames prometheusResponseFieldNames) {
    this.responseObject = responseObject;
    this.prometheusResponseFieldNames = prometheusResponseFieldNames;
  }

  @NonNull
  @Override
  public Iterator<ExprValue> iterator() {
    List<ExprValue> result = new ArrayList<>();
    if (MATRIX_KEY.equals(responseObject.getString(RESULT_TYPE_KEY))) {
      JSONArray itemArray = responseObject.getJSONArray(RESULT_KEY);
      for (int i = 0; i < itemArray.length(); i++) {
        JSONObject item = itemArray.getJSONObject(i);
        JSONObject metric = item.getJSONObject(METRIC_KEY);
        JSONArray values = item.getJSONArray(VALUES_KEY);
        for (int j = 0; j < values.length(); j++) {
          LinkedHashMap<String, ExprValue> linkedHashMap = new LinkedHashMap<>();
          JSONArray val = values.getJSONArray(j);
          linkedHashMap.put(
              prometheusResponseFieldNames.getTimestampFieldName(),
              new ExprTimestampValue(Instant.ofEpochMilli((long) (val.getDouble(0) * 1000))));
          linkedHashMap.put(
              prometheusResponseFieldNames.getValueFieldName(),
              getValue(val, 1, prometheusResponseFieldNames.getValueType()));
          insertLabels(linkedHashMap, metric);
          result.add(new ExprTupleValue(linkedHashMap));
        }
      }
    } else {
      throw new RuntimeException(
          String.format(
              "Unexpected Result Type: %s during Prometheus "
                  + "Response Parsing. 'matrix' resultType is expected",
              responseObject.getString(RESULT_TYPE_KEY)));
    }
    return result.iterator();
  }

  private void insertLabels(LinkedHashMap<String, ExprValue> linkedHashMap, JSONObject metric) {
    for (String key : metric.keySet()) {
      linkedHashMap.put(getKey(key), new ExprStringValue(metric.getString(key)));
    }
  }

  private ExprValue getValue(JSONArray jsonArray, Integer index, ExprType exprType) {
    if (INTEGER.equals(exprType)) {
      return new ExprIntegerValue(jsonArray.getInt(index));
    } else if (LONG.equals(exprType)) {
      return new ExprLongValue(jsonArray.getLong(index));
    }
    return new ExprDoubleValue(jsonArray.getDouble(index));
  }

  private String getKey(String key) {
    if (this.prometheusResponseFieldNames.getGroupByList() == null) {
      return key;
    } else {
      return this.prometheusResponseFieldNames.getGroupByList().stream()
          .filter(expression -> expression.getDelegated() instanceof ReferenceExpression)
          .filter(
              expression -> ((ReferenceExpression) expression.getDelegated()).getAttr().equals(key))
          .findFirst()
          .map(NamedExpression::getName)
          .orElse(key);
    }
  }
}
