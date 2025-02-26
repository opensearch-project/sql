/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.request.system;

import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.exceptions.PrometheusClientException;
import org.opensearch.sql.prometheus.storage.PrometheusMetricDefaultSchema;

/**
 * Describe Metric metadata request. This is triggered in case of both query range table function
 * and relation. In case of table function metric name is null.
 */
@ToString(onlyExplicitlyIncluded = true)
public class PrometheusDescribeMetricRequest implements PrometheusSystemRequest {

  private final PrometheusClient prometheusClient;

  @ToString.Include private final String metricName;

  private final DataSourceSchemaName dataSourceSchemaName;

  private static final Logger LOG = LogManager.getLogger();

  /**
   * Constructor for Prometheus Describe Metric Request. In case of pass through queries like
   * query_range function, metric names are optional.
   *
   * @param prometheusClient prometheusClient.
   * @param dataSourceSchemaName dataSourceSchemaName.
   * @param metricName metricName.
   */
  public PrometheusDescribeMetricRequest(
      PrometheusClient prometheusClient,
      DataSourceSchemaName dataSourceSchemaName,
      @NonNull String metricName) {
    this.prometheusClient = prometheusClient;
    this.metricName = metricName;
    this.dataSourceSchemaName = dataSourceSchemaName;
  }

  /**
   * Get the mapping of field and type. Returns labels and default schema fields.
   *
   * @return mapping of field and type.
   */
  public Map<String, ExprType> getFieldTypes() {
    Map<String, ExprType> fieldTypes = new HashMap<>();
    AccessController.doPrivileged(
        (PrivilegedAction<List<Void>>)
            () -> {
              try {
                prometheusClient
                    .getLabels(metricName)
                    .forEach(label -> fieldTypes.put(label, ExprCoreType.STRING));
              } catch (IOException e) {
                LOG.error(
                    "Error while fetching labels for {} from prometheus: {}",
                    metricName,
                    e.getMessage());
                throw new PrometheusClientException(
                    String.format(
                        "Error while fetching labels " + "for %s from prometheus: %s",
                        metricName, e.getMessage()));
              }
              return null;
            });
    fieldTypes.putAll(PrometheusMetricDefaultSchema.DEFAULT_MAPPING.getMapping());
    return fieldTypes;
  }

  @Override
  public List<ExprValue> search() {
    List<ExprValue> results = new ArrayList<>();
    for (Map.Entry<String, ExprType> entry : getFieldTypes().entrySet()) {
      results.add(
          row(
              entry.getKey(),
              entry.getValue().typeName().toLowerCase(),
              dataSourceSchemaName));
    }
    return results;
  }

  private ExprTupleValue row(
      String fieldName, String fieldType, DataSourceSchemaName dataSourceSchemaName) {
    LinkedHashMap<String, ExprValue> valueMap = new LinkedHashMap<>();
    valueMap.put("TABLE_CATALOG", stringValue(dataSourceSchemaName.getDataSourceName()));
    valueMap.put("TABLE_SCHEMA", stringValue(dataSourceSchemaName.getSchemaName()));
    valueMap.put("TABLE_NAME", stringValue(metricName));
    valueMap.put("COLUMN_NAME", stringValue(fieldName));
    valueMap.put("DATA_TYPE", stringValue(fieldType));
    return new ExprTupleValue(valueMap);
  }
}
