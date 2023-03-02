/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource;

import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.storage.DataSourceFactory;

/**
 * Default implementation of {@link DataSourceService}. It is per-jvm single instance.
 *
 * <p>{@link DataSourceService} is constructed by the list of {@link DataSourceFactory} at service
 * bootstrap time. The set of {@link DataSourceFactory} is immutable. Client could add {@link
 * DataSource} defined by {@link DataSourceMetadata} at any time. {@link DataSourceService} use
 * {@link DataSourceFactory} to create {@link DataSource}.
 */
public class DataSourceServiceImpl implements DataSourceService {

  private final ConcurrentHashMap<DataSourceMetadata, DataSource> dataSourceMap;

  private final Map<DataSourceType, DataSourceFactory> dataSourceFactoryMap;

  private final DataSourceMetadataStorage dataSourceMetadataStorage;

  private final String clusterName;

  /**
   * Construct from the set of {@link DataSourceFactory} at bootstrap time.
   */
  public DataSourceServiceImpl(DataSourceMetadataStorage dataSourceMetadataStorage,
                               Set<DataSourceFactory> dataSourceFactories,
                               String clusterName) {
    this.dataSourceMetadataStorage = dataSourceMetadataStorage;
    dataSourceFactoryMap =
        dataSourceFactories.stream()
            .collect(Collectors.toMap(DataSourceFactory::getDataSourceType, f -> f));
    dataSourceMap = new ConcurrentHashMap<>();
    this.clusterName = clusterName;

  }

  @Override
  public Set<DataSourceMetadata> getMaskedDataSourceMetadataInfo() {
    return new HashSet<>(this.dataSourceMetadataStorage.getDataSourceMetadata());
  }

  @Override
  public DataSource getDataSource(String dataSourceName) {
    Optional<DataSourceMetadata> dataSourceMetadataOptional
        = this.dataSourceMetadataStorage.getDataSourceMetadata(dataSourceName);
    if (dataSourceMetadataOptional.isEmpty()) {
      if (dataSourceName.equals(DEFAULT_DATASOURCE_NAME)) {
        return dataSourceMap.get(DataSourceMetadata.defaultOpenSearchDataSourceMetadata());
      } else {
        throw new IllegalArgumentException(
            String.format("DataSource with name %s doesn't exist.", dataSourceName));
      }
    } else if (!dataSourceMap.containsKey(dataSourceMetadataOptional.get())) {
      clearDataSource(dataSourceMetadataOptional.get());
      addDataSource(dataSourceMetadataOptional.get());
    }
    return dataSourceMap.get(dataSourceMetadataOptional.get());
  }

  private void clearDataSource(DataSourceMetadata dataSourceMetadata) {
    dataSourceMap.entrySet()
        .removeIf(entry -> entry.getKey().getName().equals(dataSourceMetadata.getName()));
  }

  @Override
  public void addDataSource(DataSourceMetadata... metadatas) {
    for (DataSourceMetadata metadata : metadatas) {
      AccessController.doPrivileged((PrivilegedAction<DataSource>) () -> dataSourceMap.put(
          metadata,
          dataSourceFactoryMap.get(metadata.getConnector()).createDataSource(metadata, clusterName)));
    }
  }

  @Override
  public void clear() {
    dataSourceMap.clear();
  }
}
