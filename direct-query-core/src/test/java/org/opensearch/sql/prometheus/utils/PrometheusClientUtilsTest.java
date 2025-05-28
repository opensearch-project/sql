/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
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
import org.opensearch.sql.datasource.client.exceptions.DataSourceClientException;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.prometheus.client.PrometheusClient;

@RunWith(MockitoJUnitRunner.class)
public class PrometheusClientUtilsTest {

  @Mock private Settings settings;

  @Before
  public void setUp() {
    when(settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST))
        .thenReturn(ImmutableList.of("http://localhost:9200"));
  }

  @Test
  public void testHasAlertmanagerConfigWithAlertmanagerUri() {
    // Setup
    Map<String, String> properties = new HashMap<>();
    properties.put(PrometheusClientUtils.ALERTMANAGER_URI, "http://alertmanager:9093");

    // Test
    boolean result = PrometheusClientUtils.hasAlertmanagerConfig(properties);

    // Verify
    assertTrue("Should return true when Alertmanager URI is present", result);
  }

  @Test
  public void testHasAlertmanagerConfigWithoutAlertmanagerUri() {
    // Setup
    Map<String, String> properties = new HashMap<>();
    properties.put(PrometheusClientUtils.PROMETHEUS_URI, "http://prometheus:9090");
    // No Alertmanager URI

    // Test
    boolean result = PrometheusClientUtils.hasAlertmanagerConfig(properties);

    // Verify
    assertFalse("Should return false when Alertmanager URI is not present", result);
  }

  @Test
  public void testHasAlertmanagerConfigWithEmptyProperties() {
    // Setup
    Map<String, String> properties = new HashMap<>();

    // Test
    boolean result = PrometheusClientUtils.hasAlertmanagerConfig(properties);

    // Verify
    assertFalse("Should return false when properties are empty", result);
  }

  @Test
  public void testCreateAlertmanagerPropertiesWithBasicAuth() {
    // Setup
    Map<String, String> properties = new HashMap<>();
    properties.put(PrometheusClientUtils.ALERTMANAGER_URI, "http://alertmanager:9093");
    properties.put(PrometheusClientUtils.ALERTMANAGER_AUTH_TYPE, "basicauth");
    properties.put(PrometheusClientUtils.ALERTMANAGER_USERNAME, "admin");
    properties.put(PrometheusClientUtils.ALERTMANAGER_PASSWORD, "password");

    // Test
    Map<String, String> result = PrometheusClientUtils.createAlertmanagerProperties(properties);

    // Verify
    assertNotNull("Result should not be null", result);
    assertEquals("basicauth", result.get(PrometheusClientUtils.AUTH_TYPE));
    assertEquals("admin", result.get(PrometheusClientUtils.USERNAME));
    assertEquals("password", result.get(PrometheusClientUtils.PASSWORD));
  }

  @Test
  public void testCreateAlertmanagerPropertiesWithAwsAuth() {
    // Setup
    Map<String, String> properties = new HashMap<>();
    properties.put(PrometheusClientUtils.ALERTMANAGER_URI, "http://alertmanager:9093");
    properties.put(PrometheusClientUtils.ALERTMANAGER_AUTH_TYPE, "awssigv4auth");
    properties.put(PrometheusClientUtils.ALERTMANAGER_ACCESS_KEY, "access-key");
    properties.put(PrometheusClientUtils.ALERTMANAGER_SECRET_KEY, "secret-key");
    properties.put(PrometheusClientUtils.ALERTMANAGER_REGION, "us-west-1");

    // Test
    Map<String, String> result = PrometheusClientUtils.createAlertmanagerProperties(properties);

    // Verify
    assertNotNull("Result should not be null", result);
    assertEquals("awssigv4auth", result.get(PrometheusClientUtils.AUTH_TYPE));
    assertEquals("access-key", result.get(PrometheusClientUtils.ACCESS_KEY));
    assertEquals("secret-key", result.get(PrometheusClientUtils.SECRET_KEY));
    assertEquals("us-west-1", result.get(PrometheusClientUtils.REGION));
  }

  @Test
  public void testCreateAlertmanagerPropertiesWithNoAuth() {
    // Setup
    Map<String, String> properties = new HashMap<>();
    properties.put(PrometheusClientUtils.ALERTMANAGER_URI, "http://alertmanager:9093");
    // No auth properties

    // Test
    Map<String, String> result = PrometheusClientUtils.createAlertmanagerProperties(properties);

    // Verify
    assertTrue("Result should be empty when no auth is provided", result.isEmpty());
  }

  @Test
  public void testCreateAlertmanagerPropertiesWithNullAuthType() {
    // Setup
    Map<String, String> properties = new HashMap<>();
    properties.put(PrometheusClientUtils.ALERTMANAGER_URI, "http://alertmanager:9093");
    properties.put(PrometheusClientUtils.ALERTMANAGER_AUTH_TYPE, null);

    // Test
    Map<String, String> result = PrometheusClientUtils.createAlertmanagerProperties(properties);

    // Verify
    assertNotNull("Result should not be null", result);
    assertEquals(null, result.get(PrometheusClientUtils.AUTH_TYPE));
  }

  @Test
  public void testCreateAlertmanagerPropertiesWithUnsupportedAuthType() {
    // Setup
    Map<String, String> properties = new HashMap<>();
    properties.put(PrometheusClientUtils.ALERTMANAGER_URI, "http://alertmanager:9093");
    properties.put(PrometheusClientUtils.ALERTMANAGER_AUTH_TYPE, "unsupportedauth");

    // Test
    Map<String, String> result = PrometheusClientUtils.createAlertmanagerProperties(properties);

    // Verify
    assertNotNull("Result should not be null", result);
    assertEquals("unsupportedauth", result.get(PrometheusClientUtils.AUTH_TYPE));
    // Should not contain any auth credentials since auth type is not recognized
    assertFalse(result.containsKey(PrometheusClientUtils.USERNAME));
    assertFalse(result.containsKey(PrometheusClientUtils.PASSWORD));
    assertFalse(result.containsKey(PrometheusClientUtils.ACCESS_KEY));
    assertFalse(result.containsKey(PrometheusClientUtils.SECRET_KEY));
    assertFalse(result.containsKey(PrometheusClientUtils.REGION));
  }

  @Test
  public void testCreatePrometheusClientWithAlertmanagerConfig() {
    // Setup
    Map<String, String> properties = new HashMap<>();
    properties.put(PrometheusClientUtils.PROMETHEUS_URI, "http://prometheus:9090");
    properties.put(PrometheusClientUtils.ALERTMANAGER_URI, "http://alertmanager:9093");
    properties.put(PrometheusClientUtils.ALERTMANAGER_AUTH_TYPE, "basicauth");
    properties.put(PrometheusClientUtils.ALERTMANAGER_USERNAME, "admin");
    properties.put(PrometheusClientUtils.ALERTMANAGER_PASSWORD, "password");

    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.getProperties()).thenReturn(properties);
    // Removed unnecessary stubbing for metadata.getConnector()

    // Test
    PrometheusClient client = PrometheusClientUtils.createPrometheusClient(metadata, settings);

    // Verify
    assertNotNull("Client should not be null", client);
  }

  @Test
  public void testCreatePrometheusClientWithoutAlertmanagerConfig() {
    // Setup
    Map<String, String> properties = new HashMap<>();
    properties.put(PrometheusClientUtils.PROMETHEUS_URI, "http://prometheus:9090");
    // No Alertmanager URI

    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.getProperties()).thenReturn(properties);
    // Removed unnecessary stubbing for metadata.getConnector()

    // Test
    PrometheusClient client = PrometheusClientUtils.createPrometheusClient(metadata, settings);

    // Verify
    assertNotNull("Client should not be null", client);
  }

  @Test
  public void testCreatePrometheusClientWithBasicAuth() {
    // Setup
    Map<String, String> properties = new HashMap<>();
    properties.put(PrometheusClientUtils.PROMETHEUS_URI, "http://prometheus:9090");
    properties.put(PrometheusClientUtils.AUTH_TYPE, "basicauth");
    properties.put(PrometheusClientUtils.USERNAME, "user");
    properties.put(PrometheusClientUtils.PASSWORD, "pass");

    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.getProperties()).thenReturn(properties);

    // Test
    PrometheusClient client = PrometheusClientUtils.createPrometheusClient(metadata, settings);

    // Verify
    assertNotNull("Client should not be null", client);
  }

  @Test
  public void testCreatePrometheusClientWithAwsAuth() {
    // Setup
    Map<String, String> properties = new HashMap<>();
    properties.put(PrometheusClientUtils.PROMETHEUS_URI, "http://prometheus:9090");
    properties.put(PrometheusClientUtils.AUTH_TYPE, "awssigv4");
    properties.put(PrometheusClientUtils.ACCESS_KEY, "access-key");
    properties.put(PrometheusClientUtils.SECRET_KEY, "secret-key");
    properties.put(PrometheusClientUtils.REGION, "us-west-1");

    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.getProperties()).thenReturn(properties);

    // Test
    PrometheusClient client = PrometheusClientUtils.createPrometheusClient(metadata, settings);

    // Verify
    assertNotNull("Client should not be null", client);
  }

  @Test
  public void testCreatePrometheusClientWithAlertmanagerAndAwsAuth() {
    // Setup
    Map<String, String> properties = new HashMap<>();
    properties.put(PrometheusClientUtils.PROMETHEUS_URI, "http://prometheus:9090");
    properties.put(PrometheusClientUtils.AUTH_TYPE, "awssigv4");
    properties.put(PrometheusClientUtils.ACCESS_KEY, "access-key");
    properties.put(PrometheusClientUtils.SECRET_KEY, "secret-key");
    properties.put(PrometheusClientUtils.REGION, "us-west-1");
    properties.put(PrometheusClientUtils.ALERTMANAGER_URI, "http://alertmanager:9093");

    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.getProperties()).thenReturn(properties);

    // Test
    PrometheusClient client = PrometheusClientUtils.createPrometheusClient(metadata, settings);

    // Verify
    assertNotNull("Client should not be null", client);
  }

  @Test(expected = DataSourceClientException.class)
  public void testCreatePrometheusClientWithMissingUri() {
    // Setup
    Map<String, String> properties = new HashMap<>();
    // Missing URI property

    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.getProperties()).thenReturn(properties);

    // Test - should throw exception
    PrometheusClientUtils.createPrometheusClient(metadata, settings);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreatePrometheusClientWithInvalidUri() {
    // Setup
    Map<String, String> properties = new HashMap<>();
    // Using malformed URI that will definitely be rejected
    properties.put(PrometheusClientUtils.PROMETHEUS_URI, "ht tp://invalid:9090");

    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.getProperties()).thenReturn(properties);

    // Test - should throw exception
    PrometheusClientUtils.createPrometheusClient(metadata, settings);
  }

  @Test
  public void testCreatePrometheusClientWithUnsupportedAuthType() {
    // Setup
    Map<String, String> properties = new HashMap<>();
    properties.put(PrometheusClientUtils.PROMETHEUS_URI, "http://prometheus:9090");
    properties.put(PrometheusClientUtils.AUTH_TYPE, "unsupported");

    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.getProperties()).thenReturn(properties);

    // Test - should throw IllegalArgumentException
    try {
      PrometheusClientUtils.createPrometheusClient(metadata, settings);
      fail("Expected IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("AUTH Type : unsupported is not supported"));
    }
  }
}
