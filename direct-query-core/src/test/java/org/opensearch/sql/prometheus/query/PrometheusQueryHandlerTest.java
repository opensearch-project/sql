/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.directquery.rest.model.DirectQueryResourceType;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesRequest;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesResponse;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.exception.PrometheusClientException;
import org.opensearch.sql.prometheus.model.MetricMetadata;
import org.opensearch.sql.prometheus.model.PrometheusOptions;
import org.opensearch.sql.prometheus.model.PrometheusQueryType;

@RunWith(MockitoJUnitRunner.class)
public class PrometheusQueryHandlerTest {

  private PrometheusQueryHandler handler;

  @Mock private PrometheusClient prometheusClient;

  @Before
  public void setUp() {
    handler = new PrometheusQueryHandler();
  }

  @Test
  public void testGetSupportedDataSourceType() {
    assertEquals(DataSourceType.PROMETHEUS, handler.getSupportedDataSourceType());
  }

  @Test
  public void testCanHandle() {
    assertTrue(handler.canHandle(prometheusClient));
    assertFalse(handler.canHandle(null));
  }

  @Test
  public void testGetClientClass() {
    assertEquals(PrometheusClient.class, handler.getClientClass());
  }

  @Test
  public void testExecuteQueryWithRangeQuery() throws IOException {
    // Setup
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setQuery("up");
    request.setMaxResults(100);
    request.setTimeout(30);

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(PrometheusQueryType.RANGE);
    options.setStart("1609459200"); // 2021-01-01
    options.setEnd("1609545600"); // 2021-01-02
    options.setStep("15s");
    request.setPrometheusOptions(options);

    JSONObject responseJson =
        new JSONObject("{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\"}}");
    when(prometheusClient.queryRange(
            eq("up"), eq(1609459200L), eq(1609545600L), eq("15s"), eq(100), eq(30)))
        .thenReturn(responseJson);

    // Test
    String result = handler.executeQuery(prometheusClient, request);

    // Verify
    assertNotNull(result);
    JSONObject resultJson = new JSONObject(result);
    assertEquals("success", resultJson.getString("status"));
    assertEquals("matrix", resultJson.getJSONObject("data").getString("resultType"));
  }

  @Test
  public void testExecuteQueryWithInstantQuery() throws IOException {
    // Setup
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setQuery("up");
    request.setMaxResults(100);
    request.setTimeout(30);

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(PrometheusQueryType.INSTANT);
    options.setTime("1609459200"); // 2021-01-01
    request.setPrometheusOptions(options);

    JSONObject responseJson =
        new JSONObject("{\"status\":\"success\",\"data\":{\"resultType\":\"vector\"}}");
    when(prometheusClient.query(eq("up"), eq(1609459200L), eq(100), eq(30)))
        .thenReturn(responseJson);

    // Test
    String result = handler.executeQuery(prometheusClient, request);

    // Verify
    assertNotNull(result);
    JSONObject resultJson = new JSONObject(result);
    assertEquals("success", resultJson.getString("status"));
    assertEquals("vector", resultJson.getJSONObject("data").getString("resultType"));
  }

  @Test
  public void testExecuteQueryWithMissingStartEndTimes() throws IOException {
    // Setup
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setQuery("up");

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(PrometheusQueryType.RANGE);
    // Missing start and end times
    options.setStep("15s");
    request.setPrometheusOptions(options);

    // Test
    String result = handler.executeQuery(prometheusClient, request);

    // Verify
    assertNotNull(result);
    JSONObject resultJson = new JSONObject(result);
    assertTrue(resultJson.has("error"));
    assertEquals(
        "Start and end times are required for Prometheus queries", resultJson.getString("error"));
  }

  @Test
  public void testExecuteQueryWithMissingEndTime() throws IOException {
    // Setup
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setQuery("up");

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(PrometheusQueryType.RANGE);
    options.setStep("15s");
    options.setStart("now");
    request.setPrometheusOptions(options);

    // Test
    String result = handler.executeQuery(prometheusClient, request);

    // Verify
    assertNotNull(result);
    JSONObject resultJson = new JSONObject(result);
    assertTrue(resultJson.has("error"));
    assertEquals(
        "Start and end times are required for Prometheus queries", resultJson.getString("error"));
  }

  @Test
  public void testExecuteQueryWithMissingTimeForInstant() throws IOException {
    // Setup
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setQuery("up");

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(PrometheusQueryType.INSTANT);
    // Missing time
    request.setPrometheusOptions(options);

    // Test
    String result = handler.executeQuery(prometheusClient, request);

    // Verify
    assertNotNull(result);
    JSONObject resultJson = new JSONObject(result);
    assertTrue(resultJson.has("error"));
    assertEquals("Time is required for instant Prometheus queries", resultJson.getString("error"));
  }

  @Test
  public void testExecuteQueryWithInvalidTimeFormat() throws IOException {
    // Setup
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setQuery("up");

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(PrometheusQueryType.INSTANT);
    options.setTime("invalid-time");
    request.setPrometheusOptions(options);

    // Test
    String result = handler.executeQuery(prometheusClient, request);

    // Verify
    assertNotNull(result);
    JSONObject resultJson = new JSONObject(result);
    assertTrue(resultJson.has("error"));
    assertTrue(resultJson.getString("error").contains("Invalid time format"));
  }

  @Test
  public void testExecuteQueryWithNullQueryType() throws IOException {
    // Setup
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setQuery("up");

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(null); // Invalid query type
    request.setPrometheusOptions(options);

    // Test
    String result = handler.executeQuery(prometheusClient, request);

    // Verify
    assertNotNull(result);
    JSONObject resultJson = new JSONObject(result);
    assertTrue(resultJson.has("error"));
    assertEquals("Query type is required for Prometheus queries", resultJson.getString("error"));
  }

  @Test
  public void testGetResourcesLabels() throws IOException {
    // Setup
    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    request.setResourceType(DirectQueryResourceType.LABELS);
    Map<String, String> queryParams = new HashMap<>();
    request.setQueryParams(queryParams);

    List<String> labels = Arrays.asList("job", "instance", "env");
    when(prometheusClient.getLabels(queryParams)).thenReturn(labels);

    // Test
    GetDirectQueryResourcesResponse<?> response = handler.getResources(prometheusClient, request);

    // Verify
    assertNotNull(response);
    assertEquals(labels, response.getData());
  }

  @Test
  public void testGetResourcesLabel() throws IOException {
    // Setup
    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    request.setResourceType(DirectQueryResourceType.LABEL);
    request.setResourceName("job");
    Map<String, String> queryParams = new HashMap<>();
    request.setQueryParams(queryParams);

    List<String> labelValues = Arrays.asList("prometheus", "node-exporter", "cadvisor");
    when(prometheusClient.getLabel("job", queryParams)).thenReturn(labelValues);

    // Test
    GetDirectQueryResourcesResponse<?> response = handler.getResources(prometheusClient, request);

    // Verify
    assertNotNull(response);
    assertEquals(labelValues, response.getData());
  }

  @Test
  public void testGetResourcesMetadata() throws IOException {
    // Setup
    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    request.setResourceType(DirectQueryResourceType.METADATA);
    Map<String, String> queryParams = new HashMap<>();
    request.setQueryParams(queryParams);

    Map<String, List<MetricMetadata>> metadata = new HashMap<>();
    metadata.put(
        "up", Arrays.asList(new MetricMetadata("up", "gauge", "Whether the target is up")));
    when(prometheusClient.getAllMetrics(queryParams)).thenReturn(metadata);

    // Test
    GetDirectQueryResourcesResponse<?> response = handler.getResources(prometheusClient, request);

    // Verify
    assertNotNull(response);
    assertEquals(metadata, response.getData());
  }

  @Test
  public void testGetResourcesSeries() throws IOException {
    // Setup
    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    request.setResourceType(DirectQueryResourceType.SERIES);
    Map<String, String> queryParams = new HashMap<>();
    request.setQueryParams(queryParams);

    List<Map<String, String>> series =
        Arrays.asList(
            Map.of("__name__", "up", "job", "prometheus"),
            Map.of("__name__", "up", "job", "node-exporter"));
    when(prometheusClient.getSeries(queryParams)).thenReturn(series);

    // Test
    GetDirectQueryResourcesResponse<?> response = handler.getResources(prometheusClient, request);

    // Verify
    assertNotNull(response);
    assertEquals(series, response.getData());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetResourcesInvalidType() {
    // Setup
    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    request.setResourceTypeFromString("INVALID_TYPE");

    // Test - should throw exception
    handler.getResources(prometheusClient, request);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetResourcesUnknownType() {
    // Setup
    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    request.setResourceType(DirectQueryResourceType.UNKNOWN);

    // Test - should throw exception
    handler.getResources(prometheusClient, request);
  }

  @Test(expected = PrometheusClientException.class)
  public void testGetResourcesIOException() throws IOException {
    // Setup
    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    request.setResourceType(DirectQueryResourceType.LABELS);
    Map<String, String> queryParams = new HashMap<>();
    request.setQueryParams(queryParams);

    when(prometheusClient.getLabels(queryParams)).thenThrow(new IOException("Connection failed"));

    // Test - should throw exception
    handler.getResources(prometheusClient, request);
  }

  @Test
  public void testExecuteQueryWithPrometheusClientException() throws IOException {
    // Setup
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setQuery("up");

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(PrometheusQueryType.INSTANT);
    options.setTime("1609459200"); // 2021-01-01
    request.setPrometheusOptions(options);

    String errorMessage = "Prometheus server error";
    when(prometheusClient.query(eq("up"), eq(1609459200L), eq(null), eq(null)))
        .thenThrow(
            new org.opensearch.sql.prometheus.exception.PrometheusClientException(errorMessage));

    // Test
    String result = handler.executeQuery(prometheusClient, request);

    // Verify
    assertNotNull(result);
    JSONObject resultJson = new JSONObject(result);
    assertTrue(resultJson.has("error"));
    assertEquals(errorMessage, resultJson.getString("error"));
  }

  @Test
  public void testExecuteQueryWithIOException() throws IOException {
    // Setup
    ExecuteDirectQueryRequest request = new ExecuteDirectQueryRequest();
    request.setQuery("up");

    PrometheusOptions options = new PrometheusOptions();
    options.setQueryType(PrometheusQueryType.INSTANT);
    options.setTime("1609459200"); // 2021-01-01
    request.setPrometheusOptions(options);

    String errorMessage = "Network connection error";
    when(prometheusClient.query(eq("up"), eq(1609459200L), eq(null), eq(null)))
        .thenThrow(new IOException(errorMessage));

    // Test
    String result = handler.executeQuery(prometheusClient, request);

    // Verify
    assertNotNull(result);
    JSONObject resultJson = new JSONObject(result);
    assertTrue(resultJson.has("error"));
    assertEquals(errorMessage, resultJson.getString("error"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetResourcesWithNullResourceType() {
    // Setup
    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    request.setResourceType(null); // Null resource type

    // Test - should throw exception
    handler.getResources(prometheusClient, request);
  }

  @Test
  public void testGetResourcesAlerts() throws IOException {
    // Setup
    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    request.setResourceType(DirectQueryResourceType.ALERTS);

    JSONObject alertsJson = new JSONObject();
    alertsJson.put("status", "success");
    alertsJson.put(
        "data",
        new JSONObject()
            .put(
                "alerts",
                new JSONObject()
                    .put(
                        "alerts",
                        Arrays.asList(
                            new JSONObject()
                                .put("name", "HighCPULoad")
                                .put("state", "firing")
                                .put("activeAt", "2023-01-01T00:00:00Z"),
                            new JSONObject()
                                .put("name", "InstanceDown")
                                .put("state", "pending")
                                .put("activeAt", "2023-01-01T00:05:00Z")))));

    when(prometheusClient.getAlerts()).thenReturn(alertsJson);

    // Test
    GetDirectQueryResourcesResponse<?> response = handler.getResources(prometheusClient, request);

    // Verify
    assertNotNull(response);
    Map<?, ?> data = (Map<?, ?>) response.getData();
    assertEquals("success", data.get("status"));
    assertTrue(data.containsKey("data"));
  }

  @Test
  public void testGetResourcesRules() throws IOException {
    // Setup
    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    request.setResourceType(DirectQueryResourceType.RULES);
    Map<String, String> queryParams = new HashMap<>();
    request.setQueryParams(queryParams);

    JSONObject rulesJson = new JSONObject();
    rulesJson.put("status", "success");
    rulesJson.put(
        "data",
        new JSONObject()
            .put(
                "groups",
                Arrays.asList(
                    new JSONObject()
                        .put("name", "example")
                        .put(
                            "rules",
                            Arrays.asList(
                                new JSONObject()
                                    .put("name", "HighErrorRate")
                                    .put(
                                        "query",
                                        "rate(http_requests_total{status=~\"5..\"}[5m]) > 0.5")
                                    .put("type", "alerting"),
                                new JSONObject()
                                    .put("name", "RequestRate")
                                    .put("query", "rate(http_requests_total[5m])")
                                    .put("type", "recording"))))));

    when(prometheusClient.getRules(queryParams)).thenReturn(rulesJson);

    // Test
    GetDirectQueryResourcesResponse<?> response = handler.getResources(prometheusClient, request);

    // Verify
    assertNotNull(response);
    Map<?, ?> data = (Map<?, ?>) response.getData();
    assertEquals("success", data.get("status"));
    assertTrue(data.containsKey("data"));
  }

  @Test
  public void testGetResourcesAlertmanagerAlerts() throws IOException {
    // Setup
    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    request.setResourceType(DirectQueryResourceType.ALERTMANAGER_ALERTS);
    Map<String, String> queryParams = new HashMap<>();
    request.setQueryParams(queryParams);

    JSONObject alert1 =
        new JSONObject()
            .put("status", "firing")
            .put(
                "labels",
                new JSONObject().put("alertname", "HighCPULoad").put("severity", "critical"))
            .put(
                "annotations",
                new JSONObject()
                    .put("summary", "High CPU load on instance")
                    .put("description", "CPU load is above 90%"));

    JSONObject alert2 =
        new JSONObject()
            .put("status", "resolved")
            .put(
                "labels",
                new JSONObject().put("alertname", "InstanceDown").put("severity", "critical"))
            .put(
                "annotations",
                new JSONObject()
                    .put("summary", "Instance is down")
                    .put("description", "Instance has been down for more than 5 minutes"));

    JSONArray alertsArray = new JSONArray();
    alertsArray.put(alert1);
    alertsArray.put(alert2);

    when(prometheusClient.getAlertmanagerAlerts(queryParams)).thenReturn(alertsArray);

    // Test
    GetDirectQueryResourcesResponse<?> response = handler.getResources(prometheusClient, request);

    // Verify
    assertNotNull(response);
    List<?> data = (List<?>) response.getData();
    assertEquals(2, data.size());
  }

  @Test
  public void testGetResourcesAlertmanagerAlertGroups() throws IOException {
    // Setup
    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    request.setResourceType(DirectQueryResourceType.ALERTMANAGER_ALERT_GROUPS);
    Map<String, String> queryParams = new HashMap<>();
    request.setQueryParams(queryParams);

    JSONObject group1 =
        new JSONObject()
            .put("labels", new JSONObject().put("severity", "critical"))
            .put(
                "alerts",
                new JSONArray()
                    .put(
                        new JSONObject()
                            .put("status", "firing")
                            .put(
                                "labels",
                                new JSONObject()
                                    .put("alertname", "HighCPULoad")
                                    .put("severity", "critical"))));

    JSONObject group2 =
        new JSONObject()
            .put("labels", new JSONObject().put("severity", "warning"))
            .put(
                "alerts",
                new JSONArray()
                    .put(
                        new JSONObject()
                            .put("status", "firing")
                            .put(
                                "labels",
                                new JSONObject()
                                    .put("alertname", "HighMemoryUsage")
                                    .put("severity", "warning"))));

    JSONArray groupsArray = new JSONArray();
    groupsArray.put(group1);
    groupsArray.put(group2);

    when(prometheusClient.getAlertmanagerAlertGroups(queryParams)).thenReturn(groupsArray);

    // Test
    GetDirectQueryResourcesResponse<?> response = handler.getResources(prometheusClient, request);

    // Verify
    assertNotNull(response);
    List<?> data = (List<?>) response.getData();
    assertEquals(2, data.size());
  }

  @Test
  public void testGetResourcesAlertmanagerReceivers() throws IOException {
    // Setup
    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    request.setResourceType(DirectQueryResourceType.ALERTMANAGER_RECEIVERS);

    JSONArray receiversArray = new JSONArray();
    receiversArray.put(new JSONObject().put("name", "email-notifications"));
    receiversArray.put(new JSONObject().put("name", "slack-alerts"));
    receiversArray.put(new JSONObject().put("name", "pagerduty"));

    when(prometheusClient.getAlertmanagerReceivers()).thenReturn(receiversArray);

    // Test
    GetDirectQueryResourcesResponse<?> response = handler.getResources(prometheusClient, request);

    // Verify
    assertNotNull(response);
    List<?> data = (List<?>) response.getData();
    assertEquals(3, data.size());
  }

  @Test
  public void testGetResourcesAlertmanagerSilences() throws IOException {
    // Setup
    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    request.setResourceType(DirectQueryResourceType.ALERTMANAGER_SILENCES);

    JSONArray silencesArray = new JSONArray();
    silencesArray.put(
        new JSONObject()
            .put("id", "silence-1")
            .put("status", "active")
            .put("createdBy", "admin")
            .put("comment", "Maintenance window"));
    silencesArray.put(
        new JSONObject()
            .put("id", "silence-2")
            .put("status", "expired")
            .put("createdBy", "admin")
            .put("comment", "Weekend maintenance"));

    when(prometheusClient.getAlertmanagerSilences()).thenReturn(silencesArray);

    // Test
    GetDirectQueryResourcesResponse<?> response = handler.getResources(prometheusClient, request);

    // Verify
    assertNotNull(response);
    List<?> data = (List<?>) response.getData();
    assertEquals(2, data.size());
  }
}
