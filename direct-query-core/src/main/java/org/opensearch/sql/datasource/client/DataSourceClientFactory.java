/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.client;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.inject.Inject;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.client.exceptions.DataSourceClientException;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.prometheus.utils.PrometheusClientUtils;

/**
 * Factory for creating data source clients based on the data source type.
 *
 * @opensearch.experimental
 */
public class DataSourceClientFactory {

  private static final Logger LOG = LogManager.getLogger();

  private final Settings settings;
  private final DataSourceService dataSourceService;

  /**
   * Clients are cached because building one is expensive and stateful: each carries its own OkHttp
   * connection pool and, for OAuth2 data sources, its own bearer token cache. Creating a client per
   * request meant every query minted a fresh token, which trips IdP rate limits on a dashboard
   * refreshing several panels.
   *
   * <p>Keyed on the whole {@link DataSourceMetadata} rather than the name, mirroring {@code
   * DataSourceLoaderCacheImpl}: any change to the metadata - a rotated client secret, a new URI -
   * produces a different key, so a client built from stale configuration is never served.
   */
  private final Cache<DataSourceMetadata, DataSourceClient> clientCache;

  @Inject
  public DataSourceClientFactory(DataSourceService dataSourceService, Settings settings) {
    this.settings = settings;
    this.dataSourceService = dataSourceService;
    this.clientCache =
        CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterAccess(24, TimeUnit.HOURS)
            // Each cached client owns an OkHttp dispatcher thread pool and connection pool. Once
            // evicted - by size, by expiry, or by being replaced when the metadata changes - the
            // client is unreachable, so without this its threads and sockets survive until
            // OkHttp's own idle timers fire. Guava calls this during later cache operations, not
            // on a timer, which is why close() below also drains it explicitly.
            .removalListener(DataSourceClientFactory::closeEvicted)
            .build();
  }

  /**
   * Creates a client for the specified data source with appropriate type.
   *
   * @param <T> The type of client to create, must implement DataSourceClient
   * @param dataSourceName The name of the data source
   * @return The appropriate client for the data source type
   * @throws DataSourceClientException If client creation fails
   */
  @SuppressWarnings("unchecked")
  public <T extends DataSourceClient> T createClient(String dataSourceName)
      throws DataSourceClientException {
    try {
      if (!dataSourceService.dataSourceExists(dataSourceName)) {
        throw new DataSourceClientException("Data source does not exist: " + dataSourceName);
      }

      DataSourceMetadata metadata =
          dataSourceService.verifyDataSourceAccessAndGetRawMetadata(dataSourceName, null);
      return (T) getOrCreateClient(metadata);
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

  private DataSourceClient getOrCreateClient(DataSourceMetadata metadata)
      throws DataSourceClientException {
    try {
      // get(key, loader) rather than getIfPresent + put: the latter is not atomic, so a cold
      // burst - a dashboard refreshing several panels at once, which is the case this cache
      // exists for - would have every request miss, build its own client, and mint its own
      // token, discarding all but one client without shutting it down.
      return clientCache.get(
          metadata, () -> createClientForType(metadata.getConnector().name(), metadata));
    } catch (ExecutionException | UncheckedExecutionException e) {
      // Guava splits loader failures by kind: checked ones arrive as ExecutionException, unchecked
      // ones as UncheckedExecutionException. An incomplete OAuth2 block makes the interceptor
      // constructor throw IllegalArgumentException, so the unchecked case is a normal
      // misconfiguration path, not a theoretical one - both have to be unwrapped or the reason is
      // lost behind a cache-internal wrapper.
      Throwable cause = e.getCause();
      if (cause instanceof DataSourceClientException) {
        throw (DataSourceClientException) cause;
      }
      throw new DataSourceClientException(
          "Failed to create client for data source: " + metadata.getName(), cause);
    }
  }

  private static void closeEvicted(
      RemovalNotification<DataSourceMetadata, DataSourceClient> notification) {
    try {
      // Guava never stores null values and this cache uses neither weak nor soft values, so the
      // evicted client is always present - a null check here would be an uncoverable branch.
      notification.getValue().close();
    } catch (RuntimeException e) {
      // A client that fails to release its resources must not propagate into whichever cache
      // operation happened to trigger the eviction.
      LOG.warn("Failed to release resources for an evicted data source client", e);
    }
  }

  /**
   * Releases every cached client. Guava runs removal listeners during subsequent cache operations,
   * so a plugin shutting down without this would leave the last clients' thread pools running.
   */
  public void close() {
    clientCache.invalidateAll();
    clientCache.cleanUp();
  }

  private DataSourceClient createClientForType(String dataSourceType, DataSourceMetadata metadata)
      throws DataSourceClientException {
    switch (dataSourceType) {
      case "PROMETHEUS":
        return PrometheusClientUtils.createPrometheusClient(metadata, settings);
      default:
        throw new DataSourceClientException("Unsupported data source type: " + dataSourceType);
    }
  }
}
