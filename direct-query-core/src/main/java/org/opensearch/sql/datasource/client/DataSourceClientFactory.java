/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.inject.Inject;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.client.exceptions.DataSourceClientException;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.client.PrometheusClientImpl;
import org.opensearch.sql.prometheus.utils.PrometheusClientUtils;

/** Factory for creating data source clients based on the data source type. */
public class DataSourceClientFactory {

  public static final String URI = "prometheus.uri";

  private static final Logger LOG = LogManager.getLogger();

  private final Settings settings;
  private final DataSourceService dataSourceService;

  @Inject
  public DataSourceClientFactory(DataSourceService dataSourceService, Settings settings) {
    this.settings = settings;
    this.dataSourceService = dataSourceService;
  }

  /**
   * Creates a client for the specified data source with appropriate type.
   *
   * @param <T> The type of client to create
   * @param dataSourceName The name of the data source
   * @return The appropriate client for the data source type
   * @throws DataSourceClientException If client creation fails
   */
  @SuppressWarnings("unchecked")
  public <T> T createClient(String dataSourceName) throws DataSourceClientException {
    try {
      if (!dataSourceService.dataSourceExists(dataSourceName)) {
        throw new DataSourceClientException("Data source does not exist: " + dataSourceName);
      }

      DataSourceMetadata metadata = dataSourceService.getDataSourceMetadata(dataSourceName);
      DataSourceType dataSourceType = metadata.getConnector();

      return (T) createClientForType(dataSourceType.name(), metadata);
    } catch (Exception e) {
      if (e instanceof DataSourceClientException) {
        throw e;
      }
      LOG.error("Failed to create client for data source: " + dataSourceName, e);
      throw new DataSourceClientException(
          "Failed to create client for data source: " + dataSourceName, e);
    }
  }

  /**
   * Gets the data source type for a given data source name.
   *
   * @param dataSourceName The name of the data source
   * @return The type of the data source
   * @throws DataSourceClientException If the data source doesn't exist
   */
  public DataSourceType getDataSourceType(String dataSourceName) throws DataSourceClientException {
    if (!dataSourceService.dataSourceExists(dataSourceName)) {
      throw new DataSourceClientException("Data source does not exist: " + dataSourceName);
    }

    return dataSourceService.getDataSourceMetadata(dataSourceName).getConnector();
  }

  private Object createClientForType(String dataSourceType, DataSourceMetadata metadata)
      throws DataSourceClientException {
    switch (dataSourceType) {
      case "PROMETHEUS":
        return createPrometheusClient(metadata);
        // Add cases for other data source types as needed
      default:
        throw new DataSourceClientException("Unsupported data source type: " + dataSourceType);
    }
  }

  private PrometheusClient createPrometheusClient(DataSourceMetadata metadata) {
    try {
      // replace this with validate properties in PrometheusStorageFactory
      String host = metadata.getProperties().get(URI);
      if (Objects.isNull(host)) {
        throw new DataSourceClientException("Host is required for Prometheus data source");
      }

      URI uri = new URI(host);
      return new PrometheusClientImpl(
          PrometheusClientUtils.getHttpClient(metadata.getProperties(), settings), uri);
    } catch (URISyntaxException e) {
      throw new DataSourceClientException("Invalid Prometheus URI", e);
    }
  }
}
