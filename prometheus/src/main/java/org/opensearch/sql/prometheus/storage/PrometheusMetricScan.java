/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.storage;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.request.PrometheusQueryRequest;
import org.opensearch.sql.prometheus.response.PrometheusResponse;
import org.opensearch.sql.prometheus.storage.model.PrometheusResponseFieldNames;
import org.opensearch.sql.storage.TableScanOperator;

/**
 * Prometheus metric scan operator.
 */
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class PrometheusMetricScan extends TableScanOperator {

  private final PrometheusClient prometheusClient;

  @EqualsAndHashCode.Include
  @Getter
  @Setter
  @ToString.Include
  private PrometheusQueryRequest request;

  private Iterator<ExprValue> iterator;

  @Setter
  @Getter
  private Boolean isQueryRangeFunctionScan = Boolean.FALSE;

  @Setter
  private PrometheusResponseFieldNames prometheusResponseFieldNames;


  private static final Logger LOG = LogManager.getLogger();

  /**
   * Constructor.
   *
   * @param prometheusClient prometheusClient.
   */
  public PrometheusMetricScan(PrometheusClient prometheusClient) {
    this.prometheusClient = prometheusClient;
    this.request = new PrometheusQueryRequest();
    this.prometheusResponseFieldNames = new PrometheusResponseFieldNames();
  }

  @Override
  public void open() {
    super.open();
    this.iterator = AccessController.doPrivileged((PrivilegedAction<Iterator<ExprValue>>) () -> {
      try {
        JSONObject responseObject = prometheusClient.queryRange(
            request.getPromQl(),
            request.getStartTime(), request.getEndTime(), request.getStep());
        return new PrometheusResponse(responseObject, prometheusResponseFieldNames,
            isQueryRangeFunctionScan).iterator();
      } catch (IOException e) {
        LOG.error(e.getMessage());
        throw new RuntimeException("Error fetching data from prometheus server. " + e.getMessage());
      }
    });
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
  public String explain() {
    return getRequest().toString();
  }
}
