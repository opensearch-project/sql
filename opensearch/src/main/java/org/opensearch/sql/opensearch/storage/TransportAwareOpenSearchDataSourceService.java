/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.RequestContext;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;

/**
 * Preserves the plugin-wide data source registry while replacing local OpenSearch storage engines
 * with one backed by the transport-aware client used by the PPL transport action.
 *
 * <p>The plugin-wide {@link DataSourceService} is created before transport actions and therefore
 * cannot receive {@code TransportService}. Delegating first preserves its authorization, status,
 * metadata, and caching behavior. Only the returned local OpenSearch storage engine is replaced so
 * Calcite searches use the request-scoped progress-aware node client.
 */
@RequiredArgsConstructor
public class TransportAwareOpenSearchDataSourceService implements DataSourceService {
  private final DataSourceService delegate;
  private final OpenSearchClient client;
  private final Settings settings;

  @Override
  public DataSource getDataSource(String dataSourceName) {
    DataSource dataSource = delegate.getDataSource(dataSourceName);
    if (dataSource.getConnectorType() != DataSourceType.OPENSEARCH) {
      return dataSource;
    }
    return new DataSource(
        dataSource.getName(),
        dataSource.getConnectorType(),
        new OpenSearchStorageEngine(client, settings));
  }

  @Override
  public Set<DataSourceMetadata> getDataSourceMetadata(boolean isDefaultDataSourceRequired) {
    return delegate.getDataSourceMetadata(isDefaultDataSourceRequired);
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata(String name) {
    return delegate.getDataSourceMetadata(name);
  }

  @Override
  public void createDataSource(DataSourceMetadata metadata) {
    delegate.createDataSource(metadata);
  }

  @Override
  public void updateDataSource(DataSourceMetadata dataSourceMetadata) {
    delegate.updateDataSource(dataSourceMetadata);
  }

  @Override
  public void patchDataSource(Map<String, Object> dataSourceData) {
    delegate.patchDataSource(dataSourceData);
  }

  @Override
  public void deleteDataSource(String dataSourceName) {
    delegate.deleteDataSource(dataSourceName);
  }

  @Override
  public Boolean dataSourceExists(String dataSourceName) {
    return delegate.dataSourceExists(dataSourceName);
  }

  @Override
  public DataSourceMetadata verifyDataSourceAccessAndGetRawMetadata(
      String dataSourceName, RequestContext context) {
    return delegate.verifyDataSourceAccessAndGetRawMetadata(dataSourceName, context);
  }
}
