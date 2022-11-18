/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
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
  public Set<DataSource> getDataSources() {
    return new HashSet<>(dataSourceMap.values());
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
  public void addDataSource(DataSourceMetadata metadata) {
    validateDataSource(metadata);
    dataSourceMap.put(
        metadata.getName(),
        dataSourceFactoryMap.get(metadata.getConnector()).createDataSource(metadata));
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
  private void validateDataSource(DataSourceMetadata metadata) {
    if (StringUtils.isEmpty(metadata.getName())) {
      throw new IllegalArgumentException(
          "Missing Name Field from a DataSource. Name is a required parameter.");
    }
    if (!metadata.getName().matches(DATASOURCE_NAME_REGEX)) {
      throw new IllegalArgumentException(
          String.format(
              "DataSource Name: %s contains illegal characters. Allowed characters: a-zA-Z0-9_-*@.",
              metadata.getName()));
    }
    if (Objects.isNull(metadata.getProperties())) {
      throw new IllegalArgumentException(
          "Missing properties field in catalog configuration. Properties are required parameters.");
    }
  }
}
