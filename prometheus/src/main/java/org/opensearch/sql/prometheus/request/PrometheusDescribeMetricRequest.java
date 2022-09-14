/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.request;

import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.METRIC;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TIMESTAMP;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.VALUE;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.prometheus.client.PrometheusClient;

/**
 * Describe index meta data request.
 */
public class PrometheusDescribeMetricRequest {

  private final PrometheusClient prometheusClient;

  private final String metricName;

  private static final Logger LOG = LogManager.getLogger();


  public PrometheusDescribeMetricRequest(PrometheusClient prometheusClient,
                                         String metricName) {
    this.prometheusClient = prometheusClient;
    this.metricName = metricName;
  }

  /**
   * Get the mapping of field and type.
   *
   * @return mapping of field and type.
   */
  public Map<String, ExprType> getFieldTypes() {
    Map<String, ExprType> fieldTypes = new HashMap<>();

    String[] labels = AccessController.doPrivileged((PrivilegedAction<String[]>) () -> {
      if (metricName != null) {
        try {
          return prometheusClient.getLabels(metricName);
        } catch (IOException e) {
          LOG.error("Error connecting prometheus., {}", e.getMessage());
          throw new RuntimeException("Error connecting prometheus. " + e.getMessage());
        }
      }
      return null;
    });
    if (labels != null) {
      for (String label : labels) {
        fieldTypes.put(label, ExprCoreType.STRING);
      }
    }
    fieldTypes.put(VALUE, ExprCoreType.DOUBLE);
    fieldTypes.put(TIMESTAMP, ExprCoreType.TIMESTAMP);
    fieldTypes.put(METRIC, ExprCoreType.STRING);
    return fieldTypes;
  }


  @Override
  public String toString() {
    return "PrometheusDescribeMetricRequest{"
        + "metricName='" + metricName + '\''
        + '}';
  }
}
