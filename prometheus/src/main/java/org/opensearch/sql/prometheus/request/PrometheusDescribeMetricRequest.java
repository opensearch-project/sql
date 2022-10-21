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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.prometheus.client.PrometheusClient;

/**
 * Describe Metric metadata request.
 * This is triggered in case of both query range table function and relation.
 * In case of table function metric name is null.
 */
@ToString(onlyExplicitlyIncluded = true)
public class PrometheusDescribeMetricRequest {

  private final PrometheusClient prometheusClient;

  @ToString.Include
  private final Optional<String> metricName;

  private static final Logger LOG = LogManager.getLogger();


  public PrometheusDescribeMetricRequest(PrometheusClient prometheusClient,
                                         String metricName) {
    this.prometheusClient = prometheusClient;
    this.metricName = Optional.ofNullable(metricName);
  }


  /**
   * Get the mapping of field and type.
   *
   * @return mapping of field and type.
   */
  public Map<String, ExprType> getFieldTypes() {
    Map<String, ExprType> fieldTypes = new HashMap<>();
    AccessController.doPrivileged((PrivilegedAction<List<Void>>) () -> {
      if (metricName.isPresent()) {
        try {
          prometheusClient.getLabels(metricName.get())
              .forEach(label -> fieldTypes.put(label, ExprCoreType.STRING));
        } catch (IOException e) {
          LOG.error("Error while fetching labels for {} from prometheus: {}",
              metricName, e.getMessage());
          throw new RuntimeException(String.format("Error while fetching labels "
              + "for %s from prometheus: %s", metricName.get(), e.getMessage()));
        }
      }
      return null;
    });
    fieldTypes.put(VALUE, ExprCoreType.DOUBLE);
    fieldTypes.put(TIMESTAMP, ExprCoreType.TIMESTAMP);
    fieldTypes.put(METRIC, ExprCoreType.STRING);
    return fieldTypes;
  }

}
