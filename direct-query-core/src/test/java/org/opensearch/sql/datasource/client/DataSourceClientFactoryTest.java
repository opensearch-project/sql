/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Field;
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

/*
 * @opensearch.experimental
 */
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

  /**
   * Building a client per request gave every query its own empty OAuth2 token cache, so every query
   * minted a fresh token and tripped IdP rate limits. This pins the reuse.
   */
  @Test
  public void testClientIsReusedAcrossCallsForTheSameMetadata() {
    String dataSourceName = "prometheusCached";
    when(dataSourceService.dataSourceExists(dataSourceName)).thenReturn(true);
    // Two distinct but value-equal instances, which is what the storage layer hands back on
    // each read - it holds no cache of its own. The client cache therefore has to key on
    // equality rather than identity, or it would never hit.
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(dataSourceName, null))
        .thenReturn(oauth2PrometheusMetadata(dataSourceName, "secret-1"))
        .thenReturn(oauth2PrometheusMetadata(dataSourceName, "secret-1"));

    PrometheusClient first = dataSourceClientFactory.createClient(dataSourceName);
    PrometheusClient second = dataSourceClientFactory.createClient(dataSourceName);

    assertSame("second call should reuse the cached client", first, second);
  }

  /**
   * The metadata is the cache key, so rotating the secret must not serve the old client - it still
   * holds a bearer token minted with the previous credentials.
   */
  @Test
  public void testRotatedClientSecretProducesANewClient() {
    String dataSourceName = "prometheusRotated";
    when(dataSourceService.dataSourceExists(dataSourceName)).thenReturn(true);
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(dataSourceName, null))
        .thenReturn(oauth2PrometheusMetadata(dataSourceName, "secret-1"))
        .thenReturn(oauth2PrometheusMetadata(dataSourceName, "secret-2"));

    PrometheusClient before = dataSourceClientFactory.createClient(dataSourceName);
    PrometheusClient after = dataSourceClientFactory.createClient(dataSourceName);

    assertNotSame("a rotated secret must not be served from cache", before, after);
  }

  /**
   * An incomplete OAuth2 block makes the interceptor constructor throw IllegalArgumentException.
   * Guava reports unchecked loader failures as UncheckedExecutionException rather than
   * ExecutionException, so unless both are unwrapped the operator sees a bare cache-internal
   * wrapper instead of the reason and the data source name.
   */
  @Test
  public void testIncompleteOAuth2ConfigReportsTheUnderlyingReason() {
    String dataSourceName = "prometheusIncompleteOAuth2";
    Map<String, String> properties = new HashMap<>();
    properties.put(PrometheusClientUtils.PROMETHEUS_URI, "http://prometheus:9090");
    properties.put("prometheus.auth.type", "oauth2");
    properties.put("prometheus.oauth2.clientId", "test-client-id");
    // clientSecret and tokenUrl are missing.
    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setName(dataSourceName)
            .setConnector(DataSourceType.PROMETHEUS)
            .setProperties(properties)
            .build();

    when(dataSourceService.dataSourceExists(dataSourceName)).thenReturn(true);
    when(dataSourceService.verifyDataSourceAccessAndGetRawMetadata(dataSourceName, null))
        .thenReturn(metadata);

    try {
      dataSourceClientFactory.createClient(dataSourceName);
      fail("expected the incomplete OAuth2 configuration to be rejected");
    } catch (DataSourceClientException e) {
      assertTrue(
          "expected the data source name in the message, got: " + e.getMessage(),
          e.getMessage().contains(dataSourceName));
      assertNotNull("the underlying reason must be preserved", e.getCause());
      assertTrue(
          "expected the configuration error as the cause, got: " + e.getCause(),
          e.getCause().getMessage().contains("OAuth2 configuration incomplete"));
    }
  }

  /**
   * Each cached client owns an OkHttp dispatcher thread pool and connection pool, so an evicted or
   * replaced client has to be closed - otherwise it is unreachable while its threads and sockets
   * stay alive until OkHttp's own idle timers fire.
   */
  @Test
  public void testCachedClientsAreClosedOnFactoryClose() throws Exception {
    DataSourceClient first = mock(DataSourceClient.class);
    DataSourceClient second = mock(DataSourceClient.class);
    seedCache(oauth2PrometheusMetadata("dsOne", "secret-1"), first);
    seedCache(oauth2PrometheusMetadata("dsTwo", "secret-2"), second);

    dataSourceClientFactory.close();

    verify(first).close();
    verify(second).close();
  }

  /**
   * Guava invokes the removal listener inside whichever cache operation triggered the eviction, so
   * a client that fails to release its resources must not surface there as a failed query.
   */
  @Test
  public void testAFailingCloseDoesNotPropagate() throws Exception {
    DataSourceClient breaking = mock(DataSourceClient.class);
    doThrow(new IllegalStateException("dispatcher already gone")).when(breaking).close();
    DataSourceClient healthy = mock(DataSourceClient.class);
    seedCache(oauth2PrometheusMetadata("dsBreaking", "secret-1"), breaking);
    seedCache(oauth2PrometheusMetadata("dsHealthy", "secret-2"), healthy);

    dataSourceClientFactory.close();

    verify(breaking).close();
    // The other client is still released despite the failure above.
    verify(healthy).close();
  }

  /**
   * The cache is private and only ever populated through createClient, which builds real clients.
   */
  @SuppressWarnings("unchecked")
  private void seedCache(DataSourceMetadata metadata, DataSourceClient client) throws Exception {
    Field field = DataSourceClientFactory.class.getDeclaredField("clientCache");
    field.setAccessible(true);
    ((Cache<DataSourceMetadata, DataSourceClient>) field.get(dataSourceClientFactory))
        .put(metadata, client);
  }

  private static DataSourceMetadata oauth2PrometheusMetadata(String name, String clientSecret) {
    Map<String, String> properties = new HashMap<>();
    properties.put(PrometheusClientUtils.PROMETHEUS_URI, "http://prometheus:9090");
    properties.put("prometheus.auth.type", "oauth2");
    properties.put("prometheus.oauth2.clientId", "test-client-id");
    properties.put("prometheus.oauth2.clientSecret", clientSecret);
    properties.put("prometheus.oauth2.tokenUrl", "https://auth.example.com/token");
    return new DataSourceMetadata.Builder()
        .setName(name)
        .setConnector(DataSourceType.PROMETHEUS)
        .setProperties(properties)
        .build();
  }
}
