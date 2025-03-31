/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.prometheus.exception.PrometheusClientException;

public class PrometheusClientImplTest {

  private MockWebServer mockWebServer;
  private PrometheusClientImpl client;

  @BeforeEach
  public void setUp() throws IOException {
    mockWebServer = new MockWebServer();
    mockWebServer.start();
    OkHttpClient httpClient = new OkHttpClient.Builder().build();
    client =
        new PrometheusClientImpl(
            httpClient,
            URI.create(String.format("http://%s:%s", "localhost", mockWebServer.getPort())));
  }

  @AfterEach
  public void tearDown() throws IOException {
    mockWebServer.shutdown();
  }

  @Test
  public void testQueryRange() throws IOException {
    // Setup
    String successResponse =
        "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[{\"metric\":{\"__name__\":\"up\",\"job\":\"prometheus\",\"instance\":\"localhost:9090\"},\"values\":[[1435781430.781,\"1\"],[1435781445.781,\"1\"],[1435781460.781,\"1\"]]}]}}";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    JSONObject result = client.queryRange("up", 1435781430L, 1435781460L, "15s", 100, 30);

    // Verify
    assertNotNull(result);
    // assertEquals("success", result.getString("status"));
    // JSONObject data = result.getJSONObject("data");
    assertEquals("matrix", result.getString("resultType"));
    JSONArray resultArray = result.getJSONArray("result");
    assertEquals(1, resultArray.length());
    JSONObject metric = resultArray.getJSONObject(0).getJSONObject("metric");
    assertEquals("up", metric.getString("__name__"));
  }

  @Test
  public void testQueryRangeSimpleOverload() throws IOException {
    // Setup
    String successResponse =
        "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[{\"metric\":{\"__name__\":\"up\",\"job\":\"prometheus\",\"instance\":\"localhost:9090\"},\"values\":[[1435781430.781,\"1\"],[1435781445.781,\"1\"],[1435781460.781,\"1\"]]}]}}";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    JSONObject result = client.queryRange("up", 1435781430L, 1435781460L, "15s");

    // Verify
    assertNotNull(result);
    // assertEquals("success", result.getString("status"));
    // JSONObject data = result.getJSONObject("data");
    assertEquals("matrix", result.getString("resultType"));
    JSONArray resultArray = result.getJSONArray("result");
    assertEquals(1, resultArray.length());
    JSONObject metric = resultArray.getJSONObject(0).getJSONObject("metric");
    assertEquals("up", metric.getString("__name__"));
  }

  @Test
  public void testQueryRangeWith2xxStatusAndError() {
    // Setup
    String errorResponse = "{\"status\":\"error\",\"error\":\"Error\"}";
    mockWebServer.enqueue(new MockResponse().setBody(errorResponse).setResponseCode(200));

    // Test & Verify
    PrometheusClientException exception =
        assertThrows(
            PrometheusClientException.class,
            () -> client.queryRange("up", 1435781430L, 1435781460L, "15s"));
    assertEquals("Error", exception.getMessage());
  }

  @Test
  public void testQueryRangeWithNonJsonResponse() {
    // Setup
    String nonJsonResponse = "Not a JSON response";
    mockWebServer.enqueue(new MockResponse().setBody(nonJsonResponse).setResponseCode(200));

    // Test & Verify
    PrometheusClientException exception =
        assertThrows(
            PrometheusClientException.class,
            () -> client.queryRange("up", 1435781430L, 1435781460L, "15s"));
    assertTrue(
        exception
            .getMessage()
            .contains(
                "Prometheus returned unexpected body, please verify your prometheus server"
                    + " setup."));
  }

  @Test
  public void testQueryRangeWithNon2xxError() {
    // Setup
    mockWebServer.enqueue(new MockResponse().setResponseCode(400).setBody("Mock Error"));

    // Test & Verify
    PrometheusClientException exception =
        assertThrows(
            PrometheusClientException.class,
            () -> client.queryRange("up", 1435781430L, 1435781460L, "15s"));
    assertTrue(
        exception
            .getMessage()
            .contains(
                "Request to Prometheus is Unsuccessful with code: 400. Error details: Mock Error"));
  }

  /** response.body() is @Nullable, to test the null path we need to create a spy client. */
  @Test
  public void testQueryRangeWithNon2xxErrorNullBody() {
    Request dummyRequest = new Request.Builder().url(mockWebServer.url("/")).build();
    Response nullBodyResponse =
        new Response.Builder()
            .request(dummyRequest)
            .protocol(Protocol.HTTP_1_1)
            .code(400)
            .message("Bad Request")
            .body(null)
            .build();
    OkHttpClient spyClient = spy(new OkHttpClient());
    Call mockCall = mock(Call.class);
    try {
      when(mockCall.execute()).thenReturn(nullBodyResponse);
    } catch (IOException e) {
      fail("Unexpected IOException");
    }
    doAnswer(invocation -> mockCall).when(spyClient).newCall(any(Request.class));

    PrometheusClientImpl nullBodyClient =
        new PrometheusClientImpl(
            spyClient,
            URI.create(String.format("http://%s:%s", "localhost", mockWebServer.getPort())));

    PrometheusClientException exception =
        assertThrows(
            PrometheusClientException.class,
            () -> nullBodyClient.queryRange("up", 1435781430L, 1435781460L, "15s"));
    assertTrue(
        exception
            .getMessage()
            .contains(
                "Request to Prometheus is Unsuccessful with code: 400. Error details: No response"
                    + " body"));
  }

  @Test
  public void testQuery() throws IOException {
    // Setup
    String successResponse =
        "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[{\"metric\":{\"__name__\":\"up\",\"job\":\"prometheus\",\"instance\":\"localhost:9090\"},\"value\":[1435781460.781,\"1\"]}]}}";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    JSONObject result = client.query("up", 1435781460L, 100, 30);

    // Verify
    assertNotNull(result);
    assertEquals("success", result.getString("status"));
    JSONObject data = result.getJSONObject("data");
    assertEquals("vector", data.getString("resultType"));
    JSONArray resultArray = data.getJSONArray("result");
    assertEquals(1, resultArray.length());
    JSONObject metric = resultArray.getJSONObject(0).getJSONObject("metric");
    assertEquals("up", metric.getString("__name__"));
  }

  @Test
  public void testQueryWithNullParams() throws IOException {
    // Setup
    String successResponse =
        "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[{\"metric\":{\"__name__\":\"up\",\"job\":\"prometheus\",\"instance\":\"localhost:9090\"},\"value\":[1435781460.781,\"1\"]}]}}";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    JSONObject result = client.query("up", null, null, null);

    // Verify
    assertNotNull(result);
    assertEquals("success", result.getString("status"));
    JSONObject data = result.getJSONObject("data");
    assertEquals("vector", data.getString("resultType"));
    JSONArray resultArray = data.getJSONArray("result");
    assertEquals(1, resultArray.length());
    JSONObject metric = resultArray.getJSONObject(0).getJSONObject("metric");
    assertEquals("up", metric.getString("__name__"));
  }

  @Test
  public void testGetLabels() throws IOException {
    // Setup
    String successResponse = "{\"status\":\"success\",\"data\":[\"job\",\"instance\",\"version\"]}";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    List<String> labels = client.getLabels(new HashMap<>());

    // Verify
    assertNotNull(labels);
    assertEquals(3, labels.size());
    assertTrue(labels.contains("job"));
    assertTrue(labels.contains("instance"));
    assertTrue(labels.contains("version"));
  }

  @Test
  public void testGetLabelsByMetricName() throws IOException {
    // Setup
    String successResponse =
        "{\"status\":\"success\",\"data\":[\"job\",\"instance\",\"__name__\"]}";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    List<String> labels = client.getLabels("http_requests_total");

    // Verify
    assertNotNull(labels);
    assertEquals(2, labels.size());
    assertTrue(labels.contains("job"));
    assertTrue(labels.contains("instance"));
    // __name__ should be filtered out by the implementation
    assertFalse(labels.contains("__name__"));
  }

  @Test
  public void testGetLabel() throws IOException {
    // Setup
    String successResponse = "{\"status\":\"success\",\"data\":[\"prometheus\",\"node-exporter\"]}";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    List<String> labelValues = client.getLabel("job", new HashMap<>());

    // Verify
    assertNotNull(labelValues);
    assertEquals(2, labelValues.size());
    assertTrue(labelValues.contains("prometheus"));
    assertTrue(labelValues.contains("node-exporter"));
  }

  @Test
  public void testQueryExemplars() throws IOException {
    // Setup
    String successResponse =
        "{\"status\":\"success\",\"data\":[{\"seriesLabels\":{\"__name__\":\"http_request_duration_seconds_bucket\",\"handler\":\"/api/v1/query_range\",\"le\":\"1\"},\"exemplars\":[{\"labels\":{\"traceID\":\"19a801c37fb022d6\"},\"value\":0.207396059,\"timestamp\":1659284721.762}]}]}";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    JSONArray result =
        client.queryExemplars("http_request_duration_seconds_bucket", 1659284721L, 1659284722L);

    // Verify
    assertNotNull(result);
    assertEquals(1, result.length());
    JSONObject exemplarData = result.getJSONObject(0);
    assertTrue(exemplarData.has("seriesLabels"));
    assertTrue(exemplarData.has("exemplars"));
  }

  @Test
  public void testGetAllMetricsWithParams() throws IOException {
    // Setup
    String successResponse =
        "{\"status\":\"success\",\"data\":{\"go_gc_duration_seconds\":[{\"type\":\"histogram\",\"help\":\"A"
            + " summary of the pause duration of garbage collection cycles.\",\"unit\":\"\"}]}}";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    HashMap<String, String> params = new HashMap<>();
    params.put("limit", "100");
    var result = client.getAllMetrics(params);

    // Verify
    assertNotNull(result);
    assertEquals(1, result.size());
    assertTrue(result.containsKey("go_gc_duration_seconds"));
    assertEquals(1, result.get("go_gc_duration_seconds").size());
    assertEquals("histogram", result.get("go_gc_duration_seconds").getFirst().getType());
    assertEquals(
        "A summary of the pause duration of garbage collection cycles.",
        result.get("go_gc_duration_seconds").getFirst().getHelp());
  }

  @Test
  public void testGetAllMetrics() throws IOException {
    // Setup
    String successResponse =
        "{\"status\":\"success\",\"data\":{\"http_requests_total\":[{\"type\":\"counter\",\"help\":\"Total"
            + " number of HTTP requests\",\"unit\":\"requests\"}]}}";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    var result = client.getAllMetrics();

    // Verify
    assertNotNull(result);
    assertEquals(1, result.size());
    assertTrue(result.containsKey("http_requests_total"));
    assertEquals(1, result.get("http_requests_total").size());
    assertEquals("counter", result.get("http_requests_total").getFirst().getType());
    assertEquals(
        "Total number of HTTP requests", result.get("http_requests_total").getFirst().getHelp());
    assertEquals("requests", result.get("http_requests_total").getFirst().getUnit());
  }

  @Test
  public void testGetSeries() throws IOException {
    // Setup
    String successResponse =
        "{\"status\":\"success\",\"data\":[{\"__name__\":\"up\",\"job\":\"prometheus\",\"instance\":\"localhost:9090\"},{\"__name__\":\"up\",\"job\":\"node\",\"instance\":\"localhost:9100\"}]}";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    HashMap<String, String> params = new HashMap<>();
    params.put("match[]", "up");
    var result = client.getSeries(params);

    // Verify
    assertNotNull(result);
    assertEquals(2, result.size());

    // First series
    assertEquals("up", result.getFirst().get("__name__"));
    assertEquals("prometheus", result.getFirst().get("job"));
    assertEquals("localhost:9090", result.getFirst().get("instance"));

    // Second series
    assertEquals("up", result.get(1).get("__name__"));
    assertEquals("node", result.get(1).get("job"));
    assertEquals("localhost:9100", result.get(1).get("instance"));
  }

  @Test
  public void testGetAlerts() throws IOException {
    // Setup
    String successResponse =
        "{\"status\":\"success\",\"data\":{\"alerts\":[{\"labels\":{\"alertname\":\"HighErrorRate\",\"severity\":\"critical\"},\"annotations\":{\"summary\":\"High"
            + " request error"
            + " rate\"},\"state\":\"firing\",\"activeAt\":\"2023-01-01T00:00:00Z\",\"value\":\"0.15\"}]}}";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    JSONObject result = client.getAlerts();

    // Verify
    assertNotNull(result);
    assertTrue(result.has("alerts"));
    JSONArray alerts = result.getJSONArray("alerts");
    assertEquals(1, alerts.length());
    JSONObject alert = alerts.getJSONObject(0);
    assertEquals("HighErrorRate", alert.getJSONObject("labels").getString("alertname"));
    assertEquals("critical", alert.getJSONObject("labels").getString("severity"));
    assertEquals("firing", alert.getString("state"));
  }

  @Test
  public void testGetRules() throws IOException {
    // Setup
    String successResponse =
        "{\"status\":\"success\",\"data\":{\"groups\":[{\"name\":\"example\",\"file\":\"rules.yml\",\"rules\":[{\"name\":\"HighErrorRate\",\"query\":\"rate(http_requests_total{status=~\\\"5..\\\"}[5m])"
            + " / rate(http_requests_total[5m]) >"
            + " 0.1\",\"type\":\"alerting\",\"health\":\"ok\",\"state\":\"inactive\"}]}]}}";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    HashMap<String, String> params = new HashMap<>();
    params.put("type", "alert");
    JSONObject result = client.getRules(params);

    // Verify
    assertNotNull(result);
    assertTrue(result.has("groups"));
    JSONArray groups = result.getJSONArray("groups");
    assertEquals(1, groups.length());
    JSONObject group = groups.getJSONObject(0);
    assertEquals("example", group.getString("name"));
    JSONArray rules = group.getJSONArray("rules");
    assertEquals(1, rules.length());
    JSONObject rule = rules.getJSONObject(0);
    assertEquals("HighErrorRate", rule.getString("name"));
    assertEquals("alerting", rule.getString("type"));
  }

  @Test
  public void testGetAlertmanagerAlerts() throws IOException {
    // Setup
    String successResponse =
        "[{\"labels\":{\"alertname\":\"HighErrorRate\",\"severity\":\"critical\"},\"annotations\":{\"summary\":\"High"
            + " request error"
            + " rate\"},\"state\":\"active\",\"activeAt\":\"2023-01-01T00:00:00Z\",\"value\":\"0.15\"}]";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    HashMap<String, String> params = new HashMap<>();
    params.put("active", "true");
    JSONArray result = client.getAlertmanagerAlerts(params);

    // Verify
    assertNotNull(result);
    assertEquals(1, result.length());
    JSONObject alert = result.getJSONObject(0);
    assertEquals("HighErrorRate", alert.getJSONObject("labels").getString("alertname"));
    assertEquals("critical", alert.getJSONObject("labels").getString("severity"));
    assertEquals("active", alert.getString("state"));
  }

  @Test
  public void testGetAlertmanagerAlertGroups() throws IOException {
    // Setup
    String successResponse =
        "[{\"labels\":{\"severity\":\"critical\"},\"alerts\":[{\"labels\":{\"alertname\":\"HighErrorRate\",\"severity\":\"critical\"},\"annotations\":{\"summary\":\"High"
            + " request error rate\"},\"state\":\"active\"}]}]";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    HashMap<String, String> params = new HashMap<>();
    params.put("active", "true");
    JSONArray result = client.getAlertmanagerAlertGroups(params);

    // Verify
    assertNotNull(result);
    assertEquals(1, result.length());
    JSONObject group = result.getJSONObject(0);
    assertEquals("critical", group.getJSONObject("labels").getString("severity"));
    JSONArray alerts = group.getJSONArray("alerts");
    assertEquals(1, alerts.length());
    JSONObject alert = alerts.getJSONObject(0);
    assertEquals("HighErrorRate", alert.getJSONObject("labels").getString("alertname"));
  }

  @Test
  public void testGetAlertmanagerReceivers() throws IOException {
    // Setup
    String successResponse =
        "[{\"name\":\"email\",\"email_configs\":[{\"to\":\"admin@example.com\"}]},{\"name\":\"slack\",\"slack_configs\":[{\"channel\":\"#alerts\"}]}]";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    JSONArray result = client.getAlertmanagerReceivers();

    // Verify
    assertNotNull(result);
    assertEquals(2, result.length());
    assertEquals("email", result.getJSONObject(0).getString("name"));
    assertEquals("slack", result.getJSONObject(1).getString("name"));
  }

  @Test
  public void testGetAlertmanagerSilences() throws IOException {
    // Setup
    String successResponse =
        "[{\"id\":\"silence-123\",\"status\":{\"state\":\"active\"},\"createdBy\":\"admin\",\"comment\":\"Maintenance"
            + " window\",\"startsAt\":\"2023-01-01T00:00:00Z\",\"endsAt\":\"2023-01-02T00:00:00Z\",\"matchers\":[{\"name\":\"severity\",\"value\":\"critical\",\"isRegex\":false}]}]";
    mockWebServer.enqueue(new MockResponse().setBody(successResponse));

    // Test
    JSONArray result = client.getAlertmanagerSilences();

    // Verify
    assertNotNull(result);
    assertEquals(1, result.length());
    JSONObject silence = result.getJSONObject(0);
    assertEquals("silence-123", silence.getString("id"));
    assertEquals("active", silence.getJSONObject("status").getString("state"));
    assertEquals("admin", silence.getString("createdBy"));
    JSONArray matchers = silence.getJSONArray("matchers");
    assertEquals(1, matchers.length());
    assertEquals("severity", matchers.getJSONObject(0).getString("name"));
    assertEquals("critical", matchers.getJSONObject(0).getString("value"));
  }

  @Test
  public void testReadResponseWithRequestId() throws Exception {
    // Setup
    String successResponse = "{\"status\":\"success\",\"data\":[\"job\",\"instance\"]}";
    MockResponse mockResponse =
        new MockResponse().setBody(successResponse).addHeader("X-Request-ID", "test-request-id");
    mockWebServer.enqueue(mockResponse);

    // Test - the request ID will be logged but we can verify the method completes successfully
    List<String> labels = client.getLabels(new HashMap<>());

    // Verify the method executed successfully
    assertNotNull(labels);
    assertEquals(2, labels.size());
  }

  @Test
  public void testAlertmanagerResponseError() {
    // Setup
    mockWebServer.enqueue(new MockResponse().setResponseCode(500).setBody("Internal Server Error"));

    // Test & Verify
    PrometheusClientException exception =
        assertThrows(
            PrometheusClientException.class, () -> client.getAlertmanagerAlerts(new HashMap<>()));
    assertTrue(exception.getMessage().contains("Alertmanager request failed with code: 500"));
  }

  @Test
  public void testAlertmanagerResponseErrorWithNullBody() throws IOException {
    // Setup - Create a mock response with null body
    Request dummyRequest = new Request.Builder().url(mockWebServer.url("/")).build();
    Response nullBodyResponse =
        new Response.Builder()
            .request(dummyRequest)
            .protocol(Protocol.HTTP_1_1)
            .code(500)
            .message("Internal Server Error")
            .body(null)
            .build();

    // Create spy client that returns our custom response
    OkHttpClient spyClient = spy(new OkHttpClient());
    Call mockCall = mock(Call.class);
    when(mockCall.execute()).thenReturn(nullBodyResponse);
    doAnswer(invocation -> mockCall).when(spyClient).newCall(any(Request.class));

    // Create client with our spy
    PrometheusClientImpl nullBodyClient =
        new PrometheusClientImpl(
            new OkHttpClient(),
            URI.create(String.format("http://%s:%s", "localhost", mockWebServer.getPort())),
            spyClient,
            URI.create(
                String.format("http://%s:%s/alertmanager", "localhost", mockWebServer.getPort())));

    // Test & Verify
    PrometheusClientException exception =
        assertThrows(
            PrometheusClientException.class,
            () -> nullBodyClient.getAlertmanagerAlerts(new HashMap<>()));
    assertTrue(exception.getMessage().contains("Alertmanager request failed with code: 500"));
    assertTrue(exception.getMessage().contains("No response body"));
  }
}
