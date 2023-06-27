/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.storage;

import java.util.HashMap;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.Client;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.storage.StorageEngine;

@ExtendWith(MockitoExtension.class)
public class SparkStorageFactoryTest {
  @Mock
  private Settings settings;

  @Mock
  private Client client;

  @Test
  void testGetConnectorType() {
    SparkStorageFactory sparkStorageFactory = new SparkStorageFactory(client, settings);
    Assertions.assertEquals(
        DataSourceType.SPARK, sparkStorageFactory.getDataSourceType());
  }

  @Test
  @SneakyThrows
  void testGetStorageEngine() {
    SparkStorageFactory sparkStorageFactory = new SparkStorageFactory(client, settings);
    StorageEngine storageEngine
        = sparkStorageFactory.getStorageEngine(new HashMap<>());
    Assertions.assertTrue(storageEngine instanceof SparkStorageEngine);
  }

  @Test
  void createDataSourceSuccessWithLocalhost() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("spark.connector", "emr");
    properties.put("emr.cluster", "j-abc123");
    properties.put("emr.auth.type", "sigv4");
    properties.put("spark.datasource.flint.host", "localhost");
    properties.put("spark.datasource.flint.port", "9200");
    properties.put("spark.datasource.flint.scheme", "http");
    properties.put("spark.datasource.flint.auth", "false");
    properties.put("spark.datasource.flint.region", "us-west-2");

    DataSourceMetadata metadata = new DataSourceMetadata();
    metadata.setName("spark");
    metadata.setConnector(DataSourceType.SPARK);
    metadata.setProperties(properties);

    DataSource dataSource = new SparkStorageFactory(client, settings).createDataSource(metadata);
    Assertions.assertTrue(dataSource.getStorageEngine() instanceof SparkStorageEngine);
  }

}
