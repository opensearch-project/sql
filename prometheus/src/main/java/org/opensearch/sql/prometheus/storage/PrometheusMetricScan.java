/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.storage;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.config.PrometheusConfig;
import org.opensearch.sql.prometheus.data.value.PrometheusExprValueFactory;
import org.opensearch.sql.prometheus.request.PrometheusQueryRequest;
import org.opensearch.sql.storage.TableScanOperator;

/**
 * OpenSearch index scan operator.
 */
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class PrometheusMetricScan extends TableScanOperator {

  private final PrometheusClient prometheusService;

  /**
   * Search request.
   */
  @EqualsAndHashCode.Include
  @Getter
  @ToString.Include
  private final PrometheusQueryRequest request;

  /**
   * Search response for current batch.
   */
  private Iterator<ExprValue> iterator;

  private static final Logger LOG = LogManager.getLogger();

  /**
   * Constructor.
   */
  public PrometheusMetricScan(PrometheusClient prometheusService, PrometheusConfig prometheusConfig,
                              String metricName,
                              PrometheusExprValueFactory exprValueFactory) {
    this.prometheusService = prometheusService;
    this.request = new PrometheusQueryRequest(metricName, exprValueFactory);
  }

  @Override
  public void open() {
    super.open();

    JSONObject responseObject = AccessController.doPrivileged((PrivilegedAction<JSONObject>) () -> {
      try {
        return prometheusService.queryRange(
            request.getPrometheusQueryBuilder().toString(),
            request.getStartTime(), request.getEndTime(), request.getStep());
      } catch (IOException e) {
        LOG.error(e.getMessage());
        throw new RuntimeException("Error fetching data from prometheus server. " + e.getMessage());
      }
    });
    List<ExprValue> result = new ArrayList<>();
    PrometheusExprValueFactory exprValueFactory = this.request.getExprValueFactory();
    if ("matrix".equals(responseObject.getString("resultType"))) {
      JSONArray itemArray = responseObject.getJSONArray("result");
      for (int i = 0; i < itemArray.length(); i++) {
        JSONObject item = itemArray.getJSONObject(i);
        JSONObject metric = item.getJSONObject("metric");
        JSONArray values = item.getJSONArray("values");
        for (int j = 0; j < values.length(); j++) {
          LinkedHashMap<String, ExprValue> linkedHashMap = new LinkedHashMap<>();
          JSONArray val = values.getJSONArray(j);
          linkedHashMap.put("@timestamp",
              exprValueFactory.construct("@timestamp", val.getLong(0) * 1000));
          linkedHashMap.put("@value", new ExprDoubleValue(val.getDouble(1)));
          linkedHashMap.put("metric", new ExprStringValue(metric.toString()));
          result.add(new ExprTupleValue(linkedHashMap));
        }
      }
    }
    iterator = result.iterator();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }

  @Override
  public void close() {
    super.close();
  }

  @Override
  public String explain() {
    return getRequest().toString();
  }
}
