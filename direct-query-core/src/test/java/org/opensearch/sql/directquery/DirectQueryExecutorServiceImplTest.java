/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.datasource.client.DataSourceClientFactory;
import org.opensearch.sql.datasource.client.exceptions.DataSourceClientException;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasource.query.QueryHandler;
import org.opensearch.sql.datasource.query.QueryHandlerRegistry;
import org.opensearch.sql.directquery.rest.model.DirectQueryResourceType;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryResponse;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesRequest;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesResponse;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.model.PrometheusOptions;
import org.opensearch.sql.prometheus.model.PrometheusQueryType;
import org.opensearch.sql.spark.rest.model.LangType;

@ExtendWith(MockitoExtension.class)
public class DirectQueryExecutorServiceImplTest {

  @Mock private DataSourceClientFactory dataSourceClientFactory;

  @Mock private QueryHandlerRegistry queryHandlerRegistry;

  @Mock private QueryHandler<PrometheusClient> queryHandler;

  @Mock private PrometheusClient prometheusClient;

  private DirectQueryExecutorServiceImpl executorService;

  @BeforeEach
  public void setUp() {
    executorService =
        new DirectQueryExecutorServiceImpl(dataSourceClientFactory, queryHandlerRegistry);
  }

  @Test
  public void testExecuteDirectQuerySuccessful() throws IOException {
    // Setup
    String dataSource = "prometheusDataSource";
    String query = "up";
    String sessionId = "test-session-id";

    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources(dataSource);
    request.setQuery(query);
    request.setLanguage(LangType.PROMQL);
    request.setSessionId(sessionId);

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(PrometheusQueryType.INSTANT);
    options.setTime("1609459200");
    request.setPrometheusOptions(options);

    when(dataSourceClientFactory.createClient(dataSource)).thenReturn(prometheusClient);
    when(dataSourceClientFactory.getDataSourceType(dataSource))
        .thenReturn(DataSourceType.PROMETHEUS);
    when(queryHandlerRegistry.getQueryHandler(prometheusClient))
        .thenReturn(Optional.of(queryHandler));

    String expectedResult = "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\"}}";
    when(queryHandler.executeQuery(eq(prometheusClient), eq(request))).thenReturn(expectedResult);

    // Test
    ExecuteDirectQueryResponse response = executorService.executeDirectQuery(request);

    // Verify
    assertNotNull(response);
    assertNotNull(response.getQueryId());
    assertEquals(expectedResult, response.getResult());
    assertEquals(sessionId, response.getSessionId());
  }

  @Test
  public void testExecuteDirectQueryWithUnregisteredHandler() {
    // Setup
    String dataSource = "unsupportedDataSource";
    String query = "up";

    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources(dataSource);
    request.setQuery(query);

    when(dataSourceClientFactory.createClient(dataSource)).thenReturn(prometheusClient);
    when(dataSourceClientFactory.getDataSourceType(dataSource))
        .thenReturn(DataSourceType.PROMETHEUS);
    when(queryHandlerRegistry.getQueryHandler(prometheusClient)).thenReturn(Optional.empty());

    // Test
    ExecuteDirectQueryResponse response = executorService.executeDirectQuery(request);

    // Verify
    assertNotNull(response);
    assertNotNull(response.getQueryId());
    JSONObject result = new JSONObject(response.getResult());
    assertTrue(result.has("error"));
    assertEquals("Unsupported data source type", result.getString("error"));
  }

  @Test
  public void testExecuteDirectQueryWithClientError() throws IOException {
    // Setup
    String dataSource = "errorDataSource";
    String query = "up";

    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources(dataSource);
    request.setQuery(query);

    when(dataSourceClientFactory.getDataSourceType(dataSource))
        .thenReturn(DataSourceType.PROMETHEUS);
    when(dataSourceClientFactory.createClient(dataSource))
        .thenThrow(new DataSourceClientException("Failed to create client"));

    // Test
    ExecuteDirectQueryResponse response = executorService.executeDirectQuery(request);

    // Verify
    assertNotNull(response);
    assertNotNull(response.getQueryId());
    JSONObject result = new JSONObject(response.getResult());
    assertTrue(result.has("error"));
    assertEquals("Failed to create client", result.getString("error"));
  }

  @Test
  public void testExecuteDirectQueryWithExecutionError() throws IOException {
    // Setup
    String dataSource = "prometheusDataSource";
    String query = "up";

    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources(dataSource);
    request.setQuery(query);

    when(dataSourceClientFactory.createClient(dataSource)).thenReturn(prometheusClient);
    when(dataSourceClientFactory.getDataSourceType(dataSource))
        .thenReturn(DataSourceType.PROMETHEUS);
    when(queryHandlerRegistry.getQueryHandler(prometheusClient))
        .thenReturn(Optional.of(queryHandler));
    when(queryHandler.executeQuery(eq(prometheusClient), eq(request)))
        .thenThrow(new IOException("Query execution failed"));

    // Test
    ExecuteDirectQueryResponse response = executorService.executeDirectQuery(request);

    // Verify
    assertNotNull(response);
    assertNotNull(response.getQueryId());
    JSONObject result = new JSONObject(response.getResult());
    assertTrue(result.has("error"));
    assertTrue(result.getString("error").contains("Error executing query"));
  }

  @Test
  public void testGetDirectQueryResourcesSuccessful() throws IOException {
    // Setup
    String dataSource = "prometheusDataSource";

    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    request.setDataSource(dataSource);
    request.setResourceType(DirectQueryResourceType.LABELS);

    when(dataSourceClientFactory.createClient(dataSource)).thenReturn(prometheusClient);
    when(queryHandlerRegistry.getQueryHandler(prometheusClient))
        .thenReturn(Optional.of(queryHandler));

    Map<String, Object> resourcesMap = new HashMap<>();
    resourcesMap.put("labels", new String[] {"job", "instance"});

    // Use the factory method from GetDirectQueryResourcesResponse
    @SuppressWarnings("unchecked")
    GetDirectQueryResourcesResponse<Map<String, Object>> expectedResponse =
        GetDirectQueryResourcesResponse.withMap(resourcesMap);

    // Use a raw type for the mock setup to avoid generic type issues
    when(queryHandler.getResources(eq(prometheusClient), eq(request)))
        .thenReturn((GetDirectQueryResourcesResponse) expectedResponse);

    // Test
    GetDirectQueryResourcesResponse<?> response = executorService.getDirectQueryResources(request);

    // Verify
    assertNotNull(response);
    assertEquals(
        expectedResponse.getData(), ((GetDirectQueryResourcesResponse<?>) response).getData());
  }

  @Test
  public void testGetDirectQueryResourcesWithUnregisteredHandler() {
    // Setup
    String dataSource = "unsupportedDataSource";

    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    request.setDataSource(dataSource);
    request.setResourceType(DirectQueryResourceType.LABELS);

    when(dataSourceClientFactory.createClient(dataSource)).thenReturn(prometheusClient);
    when(queryHandlerRegistry.getQueryHandler(prometheusClient)).thenReturn(Optional.empty());

    // Test - should throw exception
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> executorService.getDirectQueryResources(request));

    // Verify exception message
    assertEquals("Unsupported data source type: " + dataSource, exception.getMessage());
  }

  @Test
  public void testGetDirectQueryResourcesWithIOError() throws IOException {
    // Setup
    String dataSource = "prometheusDataSource";

    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    request.setDataSource(dataSource);
    request.setResourceType(DirectQueryResourceType.LABELS);

    when(dataSourceClientFactory.createClient(dataSource)).thenReturn(prometheusClient);
    when(queryHandlerRegistry.getQueryHandler(prometheusClient))
        .thenReturn(Optional.of(queryHandler));
    when(queryHandler.getResources(eq(prometheusClient), eq(request)))
        .thenThrow(new IOException("Failed to get resources"));

    // Test
    DataSourceClientException exception =
        assertThrows(
            DataSourceClientException.class,
            () -> executorService.getDirectQueryResources(request));

    // Verify
    assertNotNull(exception);
    assertEquals(
        "Error retrieving resources for data source type: " + dataSource, exception.getMessage());
    assertTrue(exception.getCause() instanceof IOException);
    assertEquals("Failed to get resources", exception.getCause().getMessage());
  }

  @Test
  public void testExecuteDirectQueryWithNullClient() {
    // Setup
    String dataSource = "nullClientDataSource";
    String query = "up";

    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setDataSources(dataSource);
    request.setQuery(query);

    when(dataSourceClientFactory.createClient(dataSource)).thenReturn(null);
    when(dataSourceClientFactory.getDataSourceType(dataSource))
        .thenReturn(DataSourceType.PROMETHEUS);

    // Test
    ExecuteDirectQueryResponse response = executorService.executeDirectQuery(request);

    // Verify
    assertNotNull(response);
    assertNotNull(response.getQueryId());
    JSONObject result = new JSONObject(response.getResult());
    assertTrue(result.has("error"));
    assertEquals("Unsupported data source type", result.getString("error"));
  }
}
