/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.prometheus.constants.TestConstants.ENDTIME;
import static org.opensearch.sql.prometheus.constants.TestConstants.METRIC_NAME;
import static org.opensearch.sql.prometheus.constants.TestConstants.QUERY;
import static org.opensearch.sql.prometheus.constants.TestConstants.STARTTIME;
import static org.opensearch.sql.prometheus.constants.TestConstants.STEP;
import static org.opensearch.sql.prometheus.utils.TestUtils.getJson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.prometheus.exceptions.PrometheusClientException;
import org.opensearch.sql.prometheus.request.system.model.MetricMetadata;

@ExtendWith(MockitoExtension.class)
public class PrometheusClientImplTest {

  private MockWebServer mockWebServer;
  private PrometheusClient prometheusClient;

  @BeforeEach
  void setUp() throws IOException {
    this.mockWebServer = new MockWebServer();
    this.mockWebServer.start();
    this.prometheusClient =
        new PrometheusClientImpl(new OkHttpClient(), mockWebServer.url("").uri().normalize());
  }

  @Test
  @SneakyThrows
  void testQueryRange() {
    MockResponse mockResponse =
        new MockResponse()
            .addHeader("Content-Type", "application/json; charset=utf-8")
            .setBody(getJson("query_range_response.json"));
    mockWebServer.enqueue(mockResponse);
    JSONObject jsonObject = prometheusClient.queryRange(QUERY, STARTTIME, ENDTIME, STEP);
    assertTrue(new JSONObject(getJson("query_range_result.json")).similar(jsonObject));
    RecordedRequest recordedRequest = mockWebServer.takeRequest();
    verifyQueryRangeCall(recordedRequest);
  }

  @Test
  @SneakyThrows
  void testQueryRangeWith2xxStatusAndError() {
    MockResponse mockResponse =
        new MockResponse()
            .addHeader("Content-Type", "application/json; charset=utf-8")
            .setBody(getJson("error_response.json"));
    mockWebServer.enqueue(mockResponse);
    PrometheusClientException prometheusClientException =
        assertThrows(
            PrometheusClientException.class,
            () -> prometheusClient.queryRange(QUERY, STARTTIME, ENDTIME, STEP));
    assertEquals("Error", prometheusClientException.getMessage());
    RecordedRequest recordedRequest = mockWebServer.takeRequest();
    verifyQueryRangeCall(recordedRequest);
  }

  @Test
  @SneakyThrows
  void testQueryRangeWithNonJsonResponse() {
    MockResponse mockResponse =
        new MockResponse()
            .addHeader("Content-Type", "application/json; charset=utf-8")
            .setBody(getJson("non_json_response.json"));
    mockWebServer.enqueue(mockResponse);
    PrometheusClientException prometheusClientException =
        assertThrows(
            PrometheusClientException.class,
            () -> prometheusClient.queryRange(QUERY, STARTTIME, ENDTIME, STEP));
    assertEquals(
        "Prometheus returned unexpected body, " + "please verify your prometheus server setup.",
        prometheusClientException.getMessage());
    RecordedRequest recordedRequest = mockWebServer.takeRequest();
    verifyQueryRangeCall(recordedRequest);
  }

  @Test
  @SneakyThrows
  void testQueryRangeWithNon2xxError() {
    MockResponse mockResponse =
        new MockResponse()
            .addHeader("Content-Type", "application/json; charset=utf-8")
            .setResponseCode(400);
    mockWebServer.enqueue(mockResponse);
    PrometheusClientException prometheusClientException =
        assertThrows(
            PrometheusClientException.class,
            () -> prometheusClient.queryRange(QUERY, STARTTIME, ENDTIME, STEP));
    assertEquals(
        "Request to Prometheus is Unsuccessful with code : 400",
        prometheusClientException.getMessage());
    RecordedRequest recordedRequest = mockWebServer.takeRequest();
    verifyQueryRangeCall(recordedRequest);
  }

  @Test
  @SneakyThrows
  void testGetLabel() {
    MockResponse mockResponse =
        new MockResponse()
            .addHeader("Content-Type", "application/json; charset=utf-8")
            .setBody(getJson("get_labels_response.json"));
    mockWebServer.enqueue(mockResponse);
    List<String> response = prometheusClient.getLabels(METRIC_NAME);
    assertEquals(
        new ArrayList<String>() {
          {
            add("call");
            add("code");
          }
        },
        response);
    RecordedRequest recordedRequest = mockWebServer.takeRequest();
    verifyGetLabelsCall(recordedRequest);
  }

  @Test
  @SneakyThrows
  void testGetAllMetrics() {
    MockResponse mockResponse =
        new MockResponse()
            .addHeader("Content-Type", "application/json; charset=utf-8")
            .setBody(getJson("all_metrics_response.json"));
    mockWebServer.enqueue(mockResponse);
    Map<String, List<MetricMetadata>> response = prometheusClient.getAllMetrics();
    Map<String, List<MetricMetadata>> expected = new HashMap<>();
    expected.put(
        "go_gc_duration_seconds",
        Collections.singletonList(
            new MetricMetadata(
                "summary", "A summary of the pause duration of garbage collection cycles.", "")));
    expected.put(
        "go_goroutines",
        Collections.singletonList(
            new MetricMetadata("gauge", "Number of goroutines that currently exist.", "")));
    assertEquals(expected, response);
    RecordedRequest recordedRequest = mockWebServer.takeRequest();
    verifyGetAllMetricsCall(recordedRequest);
  }

  @Test
  @SneakyThrows
  void testQueryExemplars() {
    MockResponse mockResponse =
        new MockResponse()
            .addHeader("Content-Type", "application/json; charset=utf-8")
            .setBody(getJson("query_exemplars_response.json"));
    mockWebServer.enqueue(mockResponse);
    JSONArray jsonArray = prometheusClient.queryExemplars(QUERY, STARTTIME, ENDTIME);
    assertTrue(new JSONArray(getJson("query_exemplars_result.json")).similar(jsonArray));
    RecordedRequest recordedRequest = mockWebServer.takeRequest();
    verifyQueryExemplarsCall(recordedRequest);
  }

  @AfterEach
  void tearDown() throws IOException {
    mockWebServer.shutdown();
  }

  private void verifyQueryRangeCall(RecordedRequest recordedRequest) {
    HttpUrl httpUrl = recordedRequest.getRequestUrl();
    assertEquals("GET", recordedRequest.getMethod());
    assertNotNull(httpUrl);
    assertEquals("/api/v1/query_range", httpUrl.encodedPath());
    assertEquals(QUERY, httpUrl.queryParameter("query"));
    assertEquals(STARTTIME.toString(), httpUrl.queryParameter("start"));
    assertEquals(ENDTIME.toString(), httpUrl.queryParameter("end"));
    assertEquals(STEP, httpUrl.queryParameter("step"));
  }

  private void verifyGetLabelsCall(RecordedRequest recordedRequest) {
    HttpUrl httpUrl = recordedRequest.getRequestUrl();
    assertEquals("GET", recordedRequest.getMethod());
    assertNotNull(httpUrl);
    assertEquals("/api/v1/labels", httpUrl.encodedPath());
    assertEquals(METRIC_NAME, httpUrl.queryParameter("match[]"));
  }

  private void verifyGetAllMetricsCall(RecordedRequest recordedRequest) {
    HttpUrl httpUrl = recordedRequest.getRequestUrl();
    assertEquals("GET", recordedRequest.getMethod());
    assertNotNull(httpUrl);
    assertEquals("/api/v1/metadata", httpUrl.encodedPath());
  }

  private void verifyQueryExemplarsCall(RecordedRequest recordedRequest) {
    HttpUrl httpUrl = recordedRequest.getRequestUrl();
    assertEquals("GET", recordedRequest.getMethod());
    assertNotNull(httpUrl);
    assertEquals("/api/v1/query_exemplars", httpUrl.encodedPath());
    assertEquals(QUERY, httpUrl.queryParameter("query"));
    assertEquals(STARTTIME.toString(), httpUrl.queryParameter("start"));
    assertEquals(ENDTIME.toString(), httpUrl.queryParameter("end"));
  }
}
