/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.client;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.client.exceptions.DataSourceClientException;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.utils.PrometheusClientUtils;

@RunWith(MockitoJUnitRunner.class)
public class DataSourceClientFactoryTest {

  @Mock private DataSourceService dataSourceService;

  @Mock private Settings settings;

  private DataSourceClientFactory dataSourceClientFactory;

  @Before
  public void setUp() {
    when(settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST))
        .thenReturn(ImmutableList.of("http://localhost:9200"));
    dataSourceClientFactory = new DataSourceClientFactory(dataSourceService, settings);
  }

  @Test
  public void testCreatePrometheusClientSuccessful() {
    // Setup
    String dataSourceName = "prometheusDataSource";
    Map<String, String> properties = new HashMap<>();
    properties.put(DataSourceClientFactory.URI, "http://prometheus:9090");

    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setName(dataSourceName)
            .setConnector(DataSourceType.PROMETHEUS)
            .setProperties(properties)
            .build();

    when(dataSourceService.dataSourceExists(dataSourceName)).thenReturn(true);
    when(dataSourceService.getDataSourceMetadata(dataSourceName)).thenReturn(metadata);

    // Test
    PrometheusClient client = dataSourceClientFactory.createClient(dataSourceName);

    // Verify
    assertNotNull("Client should not be null", client);
    verify(dataSourceService).dataSourceExists(dataSourceName);
    verify(dataSourceService).getDataSourceMetadata(dataSourceName);
  }

  @Test
  public void testCreatePrometheusClientWithBasicAuth() {
    // Setup
    String dataSourceName = "prometheusWithAuth";
    Map<String, String> properties = new HashMap<>();
    properties.put(DataSourceClientFactory.URI, "http://prometheus:9090");
    properties.put(PrometheusClientUtils.AUTH_TYPE, "basicauth");
    properties.put(PrometheusClientUtils.USERNAME, "user");
    properties.put(PrometheusClientUtils.PASSWORD, "pass");

    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setName(dataSourceName)
            .setConnector(DataSourceType.PROMETHEUS)
            .setProperties(properties)
            .build();

    when(dataSourceService.dataSourceExists(dataSourceName)).thenReturn(true);
    when(dataSourceService.getDataSourceMetadata(dataSourceName)).thenReturn(metadata);

    // Test
    PrometheusClient client = dataSourceClientFactory.createClient(dataSourceName);

    // Verify
    assertNotNull("Client should not be null", client);
  }

  @Test
  public void testCreatePrometheusClientWithAwsAuth() {
    // Setup
    String dataSourceName = "prometheusWithAwsAuth";
    Map<String, String> properties = new HashMap<>();
    properties.put(DataSourceClientFactory.URI, "http://prometheus:9090");
    properties.put(PrometheusClientUtils.AUTH_TYPE, "awssigv4");
    properties.put(PrometheusClientUtils.ACCESS_KEY, "access-key");
    properties.put(PrometheusClientUtils.SECRET_KEY, "secret-key");
    properties.put(PrometheusClientUtils.REGION, "us-west-1");

    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setName(dataSourceName)
            .setConnector(DataSourceType.PROMETHEUS)
            .setProperties(properties)
            .build();

    when(dataSourceService.dataSourceExists(dataSourceName)).thenReturn(true);
    when(dataSourceService.getDataSourceMetadata(dataSourceName)).thenReturn(metadata);

    // Test
    PrometheusClient client = dataSourceClientFactory.createClient(dataSourceName);

    // Verify
    assertNotNull("Client should not be null", client);
  }

  @Test(expected = DataSourceClientException.class)
  public void testCreateClientForNonexistentDataSource() {
    // Setup
    String dataSourceName = "nonExistent";
    when(dataSourceService.dataSourceExists(dataSourceName)).thenReturn(false);

    // Test - should throw exception
    dataSourceClientFactory.createClient(dataSourceName);
  }

  @Test(expected = DataSourceClientException.class)
  public void testCreateClientForUnsupportedDataSourceType() {
    // Setup
    String dataSourceName = "unsupportedType";
    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setName(dataSourceName)
            .setConnector(DataSourceType.OPENSEARCH) // Unsupported type in current implementation
            .setProperties(new HashMap<>())
            .build();

    when(dataSourceService.dataSourceExists(dataSourceName)).thenReturn(true);
    when(dataSourceService.getDataSourceMetadata(dataSourceName)).thenReturn(metadata);

    // Test - should throw exception
    dataSourceClientFactory.createClient(dataSourceName);
  }

  @Test(expected = DataSourceClientException.class)
  public void testCreatePrometheusClientWithMissingUri() {
    // Setup
    String dataSourceName = "missingUri";
    Map<String, String> properties = new HashMap<>();
    // Missing URI property

    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setName(dataSourceName)
            .setConnector(DataSourceType.PROMETHEUS)
            .setProperties(properties)
            .build();

    when(dataSourceService.dataSourceExists(dataSourceName)).thenReturn(true);
    when(dataSourceService.getDataSourceMetadata(dataSourceName)).thenReturn(metadata);

    // Test - should throw exception
    dataSourceClientFactory.createClient(dataSourceName);
  }

  @Test(expected = DataSourceClientException.class)
  public void testCreatePrometheusClientWithInvalidUri() {
    // Setup
    String dataSourceName = "invalidUri";
    Map<String, String> properties = new HashMap<>();
    // Using malformed URI that will definitely be rejected
    properties.put(DataSourceClientFactory.URI, "ht tp://invalid:9090");

    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setName(dataSourceName)
            .setConnector(DataSourceType.PROMETHEUS)
            .setProperties(properties)
            .build();

    when(dataSourceService.dataSourceExists(dataSourceName)).thenReturn(true);
    when(dataSourceService.getDataSourceMetadata(dataSourceName)).thenReturn(metadata);

    // Test - should throw exception
    dataSourceClientFactory.createClient(dataSourceName);
  }

  @Test
  public void testCreatePrometheusClientWithUnsupportedAuthType() {
    // Setup
    String dataSourceName = "unsupportedAuth";
    Map<String, String> properties = new HashMap<>();
    properties.put(DataSourceClientFactory.URI, "http://prometheus:9090");
    properties.put(PrometheusClientUtils.AUTH_TYPE, "unsupported");

    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setName(dataSourceName)
            .setConnector(DataSourceType.PROMETHEUS)
            .setProperties(properties)
            .build();

    when(dataSourceService.dataSourceExists(dataSourceName)).thenReturn(true);
    when(dataSourceService.getDataSourceMetadata(dataSourceName)).thenReturn(metadata);

    // Test - should throw DataSourceClientException caused by IllegalArgumentException
    try {
      dataSourceClientFactory.createClient(dataSourceName);
      fail("Expected DataSourceClientException to be thrown");
    } catch (DataSourceClientException e) {
      assertTrue(
          "Cause should be IllegalArgumentException",
          e.getCause() instanceof IllegalArgumentException);
      assertTrue(e.getCause().getMessage().contains("AUTH Type : unsupported is not supported"));
    }
  }

  @Test
  public void testSuppressedWarningOnGenericTypeUsage() {
    // This test verifies the @SuppressWarnings("unchecked") annotation is properly used
    // by checking that the generic method works correctly with different return types

    // Setup for Prometheus client
    String prometheusDs = "prometheusSource";
    Map<String, String> properties = new HashMap<>();
    properties.put(DataSourceClientFactory.URI, "http://prometheus:9090");

    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setName(prometheusDs)
            .setConnector(DataSourceType.PROMETHEUS)
            .setProperties(properties)
            .build();

    when(dataSourceService.dataSourceExists(prometheusDs)).thenReturn(true);
    when(dataSourceService.getDataSourceMetadata(prometheusDs)).thenReturn(metadata);

    // Test that generic type inference works for explicit type parameter
    PrometheusClient prometheusClient = dataSourceClientFactory.createClient(prometheusDs);
    assertNotNull(prometheusClient);

    // Test with Object return type
    Object genericClient = dataSourceClientFactory.createClient(prometheusDs);
    assertTrue(genericClient instanceof PrometheusClient);
  }
}
