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

}
