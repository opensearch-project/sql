/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.storage;

import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import lombok.SneakyThrows;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.storage.StorageEngine;

@ExtendWith(MockitoExtension.class)
public class PrometheusStorageFactoryTest {

  @Mock private Settings settings;

  @Test
  void testGetConnectorType() {
    PrometheusStorageFactory prometheusStorageFactory = new PrometheusStorageFactory(settings);
    Assertions.assertEquals(
        DataSourceType.PROMETHEUS, prometheusStorageFactory.getDataSourceType());
  }

  @Test
  @SneakyThrows
  void testGetStorageEngineWithBasicAuth() {
    when(settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST))
        .thenReturn(Collections.emptyList());
    PrometheusStorageFactory prometheusStorageFactory = new PrometheusStorageFactory(settings);
    HashMap<String, String> properties = new HashMap<>();
    properties.put("prometheus.uri", "http://localhost:9090");
    properties.put("prometheus.auth.type", "basicauth");
    properties.put("prometheus.auth.username", "admin");
    properties.put("prometheus.auth.password", "admin");
    StorageEngine storageEngine = prometheusStorageFactory.getStorageEngine(properties);
    Assertions.assertTrue(storageEngine instanceof PrometheusStorageEngine);
  }

  @Test
  @SneakyThrows
  void testGetStorageEngineWithAWSSigV4Auth() {
    when(settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST))
        .thenReturn(Collections.emptyList());
    PrometheusStorageFactory prometheusStorageFactory = new PrometheusStorageFactory(settings);
    HashMap<String, String> properties = new HashMap<>();
    properties.put("prometheus.uri", "http://localhost:9090");
    properties.put("prometheus.auth.type", "awssigv4");
    properties.put("prometheus.auth.region", "us-east-1");
    properties.put("prometheus.auth.secret_key", "accessKey");
    properties.put("prometheus.auth.access_key", "secretKey");
    StorageEngine storageEngine = prometheusStorageFactory.getStorageEngine(properties);
    Assertions.assertTrue(storageEngine instanceof PrometheusStorageEngine);
  }

  @Test
  @SneakyThrows
  void testGetStorageEngineWithMissingURI() {
    PrometheusStorageFactory prometheusStorageFactory = new PrometheusStorageFactory(settings);
    HashMap<String, String> properties = new HashMap<>();
    properties.put("prometheus.auth.type", "awssigv4");
    properties.put("prometheus.auth.region", "us-east-1");
    properties.put("prometheus.auth.secret_key", "accessKey");
    properties.put("prometheus.auth.access_key", "secretKey");
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> prometheusStorageFactory.getStorageEngine(properties));
    Assertions.assertEquals(
        "Missing [prometheus.uri] fields " + "in the Prometheus connector properties.",
        exception.getMessage());
  }

  @Test
  @SneakyThrows
  void testGetStorageEngineWithMissingRegionInAWS() {
    PrometheusStorageFactory prometheusStorageFactory = new PrometheusStorageFactory(settings);
    HashMap<String, String> properties = new HashMap<>();
    properties.put("prometheus.uri", "http://dummyprometheus:9090");
    properties.put("prometheus.auth.type", "awssigv4");
    properties.put("prometheus.auth.secret_key", "accessKey");
    properties.put("prometheus.auth.access_key", "secretKey");
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> prometheusStorageFactory.getStorageEngine(properties));
    Assertions.assertEquals(
        "Missing [prometheus.auth.region] fields in the " + "Prometheus connector properties.",
        exception.getMessage());
  }

  @Test
  @SneakyThrows
  void testGetStorageEngineWithLongConfigProperties() {
    PrometheusStorageFactory prometheusStorageFactory = new PrometheusStorageFactory(settings);
    HashMap<String, String> properties = new HashMap<>();
    properties.put("prometheus.uri", RandomStringUtils.random(1001));
    properties.put("prometheus.auth.type", "awssigv4");
    properties.put("prometheus.auth.secret_key", "accessKey");
    properties.put("prometheus.auth.access_key", "secretKey");
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> prometheusStorageFactory.getStorageEngine(properties));
    Assertions.assertEquals(
        "Missing [prometheus.auth.region] fields in the "
            + "Prometheus connector properties."
            + "Fields [prometheus.uri] exceeds more than 1000 characters.",
        exception.getMessage());
  }

  @Test
  @SneakyThrows
  void testGetStorageEngineWithWrongAuthType() {
    when(settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST))
        .thenReturn(Collections.emptyList());
    PrometheusStorageFactory prometheusStorageFactory = new PrometheusStorageFactory(settings);
    HashMap<String, String> properties = new HashMap<>();
    properties.put("prometheus.uri", "https://test.com");
    properties.put("prometheus.auth.type", "random");
    properties.put("prometheus.auth.region", "us-east-1");
    properties.put("prometheus.auth.secret_key", "accessKey");
    properties.put("prometheus.auth.access_key", "secretKey");
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> prometheusStorageFactory.getStorageEngine(properties));
    Assertions.assertEquals(
        "AUTH Type : random is not supported with Prometheus Connector", exception.getMessage());
  }

  @Test
  @SneakyThrows
  void testGetStorageEngineWithNONEAuthType() {
    when(settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST))
        .thenReturn(Collections.emptyList());
    PrometheusStorageFactory prometheusStorageFactory = new PrometheusStorageFactory(settings);
    HashMap<String, String> properties = new HashMap<>();
    properties.put("prometheus.uri", "https://test.com");
    StorageEngine storageEngine = prometheusStorageFactory.getStorageEngine(properties);
    Assertions.assertTrue(storageEngine instanceof PrometheusStorageEngine);
  }

  @Test
  @SneakyThrows
  void testGetStorageEngineWithInvalidURISyntax() {
    PrometheusStorageFactory prometheusStorageFactory = new PrometheusStorageFactory(settings);
    HashMap<String, String> properties = new HashMap<>();
    properties.put("prometheus.uri", "http://dummyprometheus.com:9090? param");
    properties.put("prometheus.auth.type", "basicauth");
    properties.put("prometheus.auth.username", "admin");
    properties.put("prometheus.auth.password", "admin");
    RuntimeException exception =
        Assertions.assertThrows(
            RuntimeException.class, () -> prometheusStorageFactory.getStorageEngine(properties));
    Assertions.assertTrue(
        exception.getMessage().contains("Invalid URI in prometheus properties: "));
  }

  @Test
  void createDataSourceSuccess() {
    when(settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST))
        .thenReturn(Collections.emptyList());
    HashMap<String, String> properties = new HashMap<>();
    properties.put("prometheus.uri", "http://localhost:9090");
    properties.put("prometheus.auth.type", "basicauth");
    properties.put("prometheus.auth.username", "admin");
    properties.put("prometheus.auth.password", "admin");

    DataSourceMetadata metadata = new DataSourceMetadata();
    metadata.setName("prometheus");
    metadata.setConnector(DataSourceType.PROMETHEUS);
    metadata.setProperties(properties);

    DataSource dataSource = new PrometheusStorageFactory(settings).createDataSource(metadata);
    Assertions.assertTrue(dataSource.getStorageEngine() instanceof PrometheusStorageEngine);
  }

  @Test
  void createDataSourceSuccessWithLocalhost() {
    when(settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST))
        .thenReturn(Collections.emptyList());
    HashMap<String, String> properties = new HashMap<>();
    properties.put("prometheus.uri", "http://localhost:9090");
    properties.put("prometheus.auth.type", "basicauth");
    properties.put("prometheus.auth.username", "admin");
    properties.put("prometheus.auth.password", "admin");

    DataSourceMetadata metadata = new DataSourceMetadata();
    metadata.setName("prometheus");
    metadata.setConnector(DataSourceType.PROMETHEUS);
    metadata.setProperties(properties);

    DataSource dataSource = new PrometheusStorageFactory(settings).createDataSource(metadata);
    Assertions.assertTrue(dataSource.getStorageEngine() instanceof PrometheusStorageEngine);
  }

  @Test
  void createDataSourceWithHostnameNotMatchingWithAllowHostsConfig() {
    when(settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST))
        .thenReturn(Collections.singletonList("127.0.0.0/8"));
    HashMap<String, String> properties = new HashMap<>();
    properties.put("prometheus.uri", "http://localhost:9090");
    properties.put("prometheus.auth.type", "basicauth");
    properties.put("prometheus.auth.username", "admin");
    properties.put("prometheus.auth.password", "admin");

    DataSourceMetadata metadata = new DataSourceMetadata();
    metadata.setName("prometheus");
    metadata.setConnector(DataSourceType.PROMETHEUS);
    metadata.setProperties(properties);

    PrometheusStorageFactory prometheusStorageFactory = new PrometheusStorageFactory(settings);
    RuntimeException exception =
        Assertions.assertThrows(
            RuntimeException.class, () -> prometheusStorageFactory.createDataSource(metadata));
    Assertions.assertEquals(
        "Disallowed hostname in the uri. "
            + "Validate with plugins.query.datasources.uri.hosts.denylist config",
        exception.getMessage());
  }
}
