/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.spark.storage;

import static org.opensearch.sql.spark.constants.TestConstants.EMR_CLUSTER_ID;

import java.security.InvalidParameterException;
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
    HashMap<String, String> properties = new HashMap<>();
    properties.put("spark.connector", "emr");
    properties.put("emr.cluster", EMR_CLUSTER_ID);
    properties.put("emr.auth.type", "awssigv4");
    properties.put("emr.auth.access_key", "access_key");
    properties.put("emr.auth.secret_key", "secret_key");
    properties.put("emr.auth.region", "region");
    SparkStorageFactory sparkStorageFactory = new SparkStorageFactory(client, settings);
    StorageEngine storageEngine
        = sparkStorageFactory.getStorageEngine(properties);
    Assertions.assertTrue(storageEngine instanceof SparkStorageEngine);
  }

  @Test
  @SneakyThrows
  void testInvalidConnectorType() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("spark.connector", "random");
    SparkStorageFactory sparkStorageFactory = new SparkStorageFactory(client, settings);
    InvalidParameterException exception = Assertions.assertThrows(InvalidParameterException.class,
        () -> sparkStorageFactory.getStorageEngine(properties));
    Assertions.assertEquals("Spark connector type is invalid.",
        exception.getMessage());
  }

  @Test
  @SneakyThrows
  void testMissingAuth() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("spark.connector", "emr");
    properties.put("emr.cluster", EMR_CLUSTER_ID);
    SparkStorageFactory sparkStorageFactory = new SparkStorageFactory(client, settings);
    IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
        () -> sparkStorageFactory.getStorageEngine(properties));
    Assertions.assertEquals("EMR config properties are missing.",
        exception.getMessage());
  }

  @Test
  @SneakyThrows
  void testUnsupportedEmrAuth() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("spark.connector", "emr");
    properties.put("emr.cluster", EMR_CLUSTER_ID);
    properties.put("emr.auth.type", "basic");
    SparkStorageFactory sparkStorageFactory = new SparkStorageFactory(client, settings);
    IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
        () -> sparkStorageFactory.getStorageEngine(properties));
    Assertions.assertEquals("Invalid auth type.",
        exception.getMessage());
  }

  @Test
  @SneakyThrows
  void testMissingCluster() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("spark.connector", "emr");
    properties.put("emr.auth.type", "awssigv4");
    SparkStorageFactory sparkStorageFactory = new SparkStorageFactory(client, settings);
    IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
        () -> sparkStorageFactory.getStorageEngine(properties));
    Assertions.assertEquals("EMR config properties are missing.",
        exception.getMessage());
  }

  @Test
  @SneakyThrows
  void testMissingAuthKeys() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("spark.connector", "emr");
    properties.put("emr.cluster", EMR_CLUSTER_ID);
    properties.put("emr.auth.type", "awssigv4");
    SparkStorageFactory sparkStorageFactory = new SparkStorageFactory(client, settings);
    IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
        () -> sparkStorageFactory.getStorageEngine(properties));
    Assertions.assertEquals("EMR auth keys are missing.",
        exception.getMessage());
  }

  @Test
  @SneakyThrows
  void testMissingAuthSecretKey() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("spark.connector", "emr");
    properties.put("emr.cluster", EMR_CLUSTER_ID);
    properties.put("emr.auth.type", "awssigv4");
    properties.put("emr.auth.access_key", "test");
    SparkStorageFactory sparkStorageFactory = new SparkStorageFactory(client, settings);
    IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
        () -> sparkStorageFactory.getStorageEngine(properties));
    Assertions.assertEquals("EMR auth keys are missing.",
        exception.getMessage());
  }

  @Test
  void testCreateDataSourceSuccess() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("spark.connector", "emr");
    properties.put("emr.cluster", EMR_CLUSTER_ID);
    properties.put("emr.auth.type", "awssigv4");
    properties.put("emr.auth.access_key", "access_key");
    properties.put("emr.auth.secret_key", "secret_key");
    properties.put("emr.auth.region", "region");
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

  @Test
  void testSetSparkJars() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put("spark.connector", "emr");
    properties.put("spark.sql.application", "s3://spark/spark-sql-job.jar");
    properties.put("emr.cluster", EMR_CLUSTER_ID);
    properties.put("emr.auth.type", "awssigv4");
    properties.put("emr.auth.access_key", "access_key");
    properties.put("emr.auth.secret_key", "secret_key");
    properties.put("emr.auth.region", "region");
    properties.put("spark.datasource.flint.integration", "s3://spark/flint-spark-integration.jar");

    DataSourceMetadata metadata = new DataSourceMetadata();
    metadata.setName("spark");
    metadata.setConnector(DataSourceType.SPARK);
    metadata.setProperties(properties);

    DataSource dataSource = new SparkStorageFactory(client, settings).createDataSource(metadata);
    Assertions.assertTrue(dataSource.getStorageEngine() instanceof SparkStorageEngine);
  }

}
