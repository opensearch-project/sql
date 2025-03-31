/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
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
    properties.put(PrometheusClientUtils.PROMETHEUS_URI, "http://prometheus:9090");

    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setName(dataSourceName)
            .setConnector(DataSourceType.PROMETHEUS)
            .setProperties(properties)
            .build();

    when(dataSourceService.dataSourceExists(dataSourceName)).thenReturn(true);
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(dataSourceName, null))
        .thenReturn(metadata);

    // Test
    PrometheusClient client = dataSourceClientFactory.createClient(dataSourceName);

    // Verify
    assertNotNull("Client should not be null", client);
    verify(dataSourceService).dataSourceExists(dataSourceName);
    verify(dataSourceService).verifyDataSourceAccessAndGetRawMetadata(dataSourceName, null);
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
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(dataSourceName, null))
        .thenReturn(metadata);

    // Test - should throw exception
    dataSourceClientFactory.createClient(dataSourceName);
  }

  @Test(expected = DataSourceClientException.class)
  public void testCreateClientWrapsNonDataSourceClientException() {
    // Setup
    String dataSourceName = "exceptionSource";
    RuntimeException genericException = new RuntimeException("Generic error");

    when(dataSourceService.dataSourceExists(dataSourceName)).thenReturn(true);
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(dataSourceName, null))
        .thenThrow(genericException);

    // Test - should wrap the generic exception in a DataSourceClientException
    dataSourceClientFactory.createClient(dataSourceName);
  }

  @Test
  public void testSuppressedWarningOnGenericTypeUsage() {
    // This test verifies the @SuppressWarnings("unchecked") annotation is properly used
    // by checking that the generic method works correctly with different return types

    // Setup for Prometheus client
    String prometheusDs = "prometheusSource";
    Map<String, String> properties = new HashMap<>();
    properties.put(PrometheusClientUtils.PROMETHEUS_URI, "http://prometheus:9090");

    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setName(prometheusDs)
            .setConnector(DataSourceType.PROMETHEUS)
            .setProperties(properties)
            .build();

    when(dataSourceService.dataSourceExists(prometheusDs)).thenReturn(true);
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(prometheusDs, null))
        .thenReturn(metadata);

    // Test that generic type inference works for explicit type parameter
    PrometheusClient prometheusClient = dataSourceClientFactory.createClient(prometheusDs);
    assertNotNull(prometheusClient);

    // Test with Object return type
    Object genericClient = dataSourceClientFactory.createClient(prometheusDs);
    assertTrue(genericClient instanceof PrometheusClient);
  }

  @Test
  public void testGetDataSourceTypeSuccessful() {
    // Setup
    String dataSourceName = "prometheusDataSource";
    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setName(dataSourceName)
            .setConnector(DataSourceType.PROMETHEUS)
            .build();

    when(dataSourceService.dataSourceExists(dataSourceName)).thenReturn(true);
    when(dataSourceService.getDataSourceMetadata(dataSourceName)).thenReturn(metadata);

    // Test
    DataSourceType dataSourceType = dataSourceClientFactory.getDataSourceType(dataSourceName);

    // Verify
    assertEquals(DataSourceType.PROMETHEUS, dataSourceType);
    verify(dataSourceService).dataSourceExists(dataSourceName);
    verify(dataSourceService).getDataSourceMetadata(dataSourceName);
  }

  @Test(expected = DataSourceClientException.class)
  public void testGetDataSourceTypeForNonexistentDataSource() {
    // Setup
    String dataSourceName = "nonExistent";
    when(dataSourceService.dataSourceExists(dataSourceName)).thenReturn(false);

    // Test - should throw exception
    dataSourceClientFactory.getDataSourceType(dataSourceName);
  }
}
