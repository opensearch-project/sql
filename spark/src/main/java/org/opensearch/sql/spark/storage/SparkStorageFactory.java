/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.storage;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.opensearch.client.Client;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.sql.storage.StorageEngine;

/**
 * Storage factory implementation for spark connector.
 */
@RequiredArgsConstructor
public class SparkStorageFactory implements DataSourceFactory {
  private final Client client;
  private final Settings settings;

  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.SPARK;
  }

  @Override
  public DataSource createDataSource(DataSourceMetadata metadata) {
    return new DataSource(
        metadata.getName(),
        DataSourceType.SPARK,
        getStorageEngine(metadata.getProperties()));
  }

  /**
   * This function gets spark storage engine.
   *
   * @param requiredConfig spark config options
   * @return               spark storage engine object
   */
  StorageEngine getStorageEngine(Map<String, String> requiredConfig) {
    SparkClient sparkClient = null;
    //TODO: Initialize spark client
    return new SparkStorageEngine(sparkClient);
  }
}