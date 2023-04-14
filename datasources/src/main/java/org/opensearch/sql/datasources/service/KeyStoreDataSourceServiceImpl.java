/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasources.service;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceInterfaceType;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.exceptions.DataSourceNotFoundException;
import org.opensearch.sql.storage.DataSourceFactory;

/**
 * Default implementation of {@link DataSourceService}. It is per-jvm single instance.
 *
 * <p>{@link DataSourceService} is constructed by the list of {@link DataSourceFactory} at service
 * bootstrap time. The set of {@link DataSourceFactory} is immutable. Client could add {@link
 * DataSource} defined by {@link DataSourceMetadata} at any time. {@link DataSourceService} use
 * {@link DataSourceFactory} to create {@link DataSource}.
 */
public class KeyStoreDataSourceServiceImpl implements DataSourceService {

  private static String DATASOURCE_NAME_REGEX = "[@*A-Za-z]+?[*a-zA-Z_\\-0-9]*";

  private final ConcurrentHashMap<String, DataSource> dataSourceMap;

  private final ConcurrentHashMap<String, DataSourceMetadata> dataSourceMetadataMap;

  private final Map<DataSourceType, DataSourceFactory> dataSourceFactoryMap;

  /**
   * Construct from the set of {@link DataSourceFactory} at bootstrap time.
   */
  public KeyStoreDataSourceServiceImpl(Set<DataSourceFactory> dataSourceFactories) {
    dataSourceFactoryMap =
        dataSourceFactories.stream()
            .collect(Collectors.toMap(DataSourceFactory::getDataSourceType, f -> f));
    dataSourceMap = new ConcurrentHashMap<>();
    dataSourceMetadataMap = new ConcurrentHashMap<>();
  }

  @Override
  public DataSource getDataSource(String dataSourceName) {
    if (!dataSourceMap.containsKey(dataSourceName)) {
      throw new DataSourceNotFoundException(
          String.format("DataSource with name %s doesn't exist.", dataSourceName));
    }
    return dataSourceMap.get(dataSourceName);
  }

  @Override
  public Set<DataSourceMetadata> getDataSourceMetadata(boolean isDefaultDataSourceRequired) {
    return Set.copyOf(this.dataSourceMetadataMap.values());
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata(String name) {
    if (!this.dataSourceMetadataMap.containsKey(name)) {
      throw new IllegalArgumentException("DataSourceMetadata with name: " + name
          + " doesn't exist.");
    }
    return this.dataSourceMetadataMap.get(name);
  }

  @Override
  public void createDataSource(DataSourceMetadata metadata) {
    validateDataSourceMetaData(metadata);
    dataSourceMap.put(
        metadata.getName(),
        dataSourceFactoryMap.get(metadata.getConnector()).createDataSource(metadata));
    removeAuthInfo(metadata);
    dataSourceMetadataMap.put(metadata.getName(), metadata);
  }

  @Override
  public void updateDataSource(DataSourceMetadata dataSourceMetadata) {
    throw new UnsupportedOperationException(
        "Update operation is not supported in KeyStoreDataService");
  }

  @Override
  public void deleteDataSource(String dataSourceName) {
    dataSourceMap.remove(dataSourceName);
    dataSourceMetadataMap.remove(dataSourceName);
  }

  @Override
  public Boolean dataSourceExists(String dataSourceName) {
    return dataSourceMap.containsKey(dataSourceName);
  }

  @Override
  public DataSourceInterfaceType datasourceInterfaceType() {
    return DataSourceInterfaceType.KEYSTORE;
  }

  /**
   * This can be moved to a different validator class when we introduce more connectors.
   *
   * @param metadata {@link DataSourceMetadata}.
   */
  private void validateDataSourceMetaData(DataSourceMetadata metadata) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(metadata.getName()),
        "Missing Name Field from a DataSource. Name is a required parameter.");
    Preconditions.checkArgument(
        !dataSourceMap.containsKey(metadata.getName()),
        StringUtils.format(
            "Datasource name should be unique, Duplicate datasource found %s.",
            metadata.getName()));
    Preconditions.checkArgument(
        metadata.getName().matches(DATASOURCE_NAME_REGEX),
        StringUtils.format(
            "DataSource Name: %s contains illegal characters. Allowed characters: a-zA-Z0-9_-*@.",
            metadata.getName()));
    Preconditions.checkArgument(
        !Objects.isNull(metadata.getProperties()),
        "Missing properties field in catalog configuration. Properties are required parameters.");
  }

  // It is advised to avoid storing any kind credentials.
  private void removeAuthInfo(DataSourceMetadata dataSourceMetadata) {
    HashMap<String, String> safeProperties
        = new HashMap<>(dataSourceMetadata.getProperties());
    safeProperties
        .entrySet()
        .removeIf(entry -> entry.getKey().contains("auth"));
    dataSourceMetadata.setProperties(safeProperties);
  }

  public void clear() {
    this.dataSourceMap.clear();
    this.dataSourceMetadataMap.clear();
  }

}