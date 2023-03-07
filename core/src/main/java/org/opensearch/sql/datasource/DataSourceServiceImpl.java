/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.opensearch.sql.common.utils.StringUtils;
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

  private static String DATASOURCE_NAME_REGEX = "[@*A-Za-z]+?[*a-zA-Z_\\-0-9]*";

  private final ConcurrentHashMap<String, DataSource> dataSourceMap;

  private final Map<DataSourceType, DataSourceFactory> dataSourceFactoryMap;

  /**
   * Construct from the set of {@link DataSourceFactory} at bootstrap time.
   */
  public DataSourceServiceImpl(Set<DataSourceFactory> dataSourceFactories) {
    dataSourceFactoryMap =
        dataSourceFactories.stream()
            .collect(Collectors.toMap(DataSourceFactory::getDataSourceType, f -> f));
    dataSourceMap = new ConcurrentHashMap<>();
  }

  @Override
  public Set<DataSourceMetadata> getDataSourceMetadataSet() {
    return dataSourceMap.values().stream()
        .map(dataSource
            -> new DataSourceMetadata(dataSource.getName(),
            dataSource.getConnectorType(), ImmutableMap.of()))
        .collect(Collectors.toSet());
  }

  @Override
  public DataSource getDataSource(String dataSourceName) {
    if (!dataSourceMap.containsKey(dataSourceName)) {
      throw new IllegalArgumentException(
          String.format("DataSource with name %s doesn't exist.", dataSourceName));
    }
    return dataSourceMap.get(dataSourceName);
  }

  @Override
  public void createDataSource(DataSourceMetadata... metadatas) {
    for (DataSourceMetadata metadata : metadatas) {
      validateDataSourceMetaData(metadata);
      dataSourceMap.put(
          metadata.getName(),
          dataSourceFactoryMap.get(metadata.getConnector()).createDataSource(metadata));
    }
  }

  @Override
  public void updateDataSource(DataSourceMetadata dataSourceMetadata) {
    throw new UnsupportedOperationException("will be supported in future");
  }

  @Override
  public void deleteDataSource(String dataSourceName) {
    throw new UnsupportedOperationException("will be supported in future");
  }

  @Override
  public void bootstrapDataSources() {
    throw new UnsupportedOperationException("will be supported in future");
  }

  @Override
  public void clear() {
    dataSourceMap.clear();
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
}
