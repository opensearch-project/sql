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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.http.HttpStatus;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PrometheusClientImplTest {

  private MockWebServer mockWebServer;
  private PrometheusClient prometheusClient;


  @BeforeEach
  void setUp() throws IOException {
    this.mockWebServer = new MockWebServer();
    this.mockWebServer.start();
    this.prometheusClient =
        new PrometheusClientImpl(new OkHttpClient(), mockWebServer.url("/").uri());
  }


  @Test
  @SneakyThrows
  void testQueryRange() {
    MockResponse mockResponse = new MockResponse()
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
    MockResponse mockResponse = new MockResponse()
        .addHeader("Content-Type", "application/json; charset=utf-8")
        .setBody(getJson("error_response.json"));
    mockWebServer.enqueue(mockResponse);
    RuntimeException runtimeException
        = assertThrows(RuntimeException.class,
          () -> prometheusClient.queryRange(QUERY, STARTTIME, ENDTIME, STEP));
    assertEquals("Error", runtimeException.getMessage());
    RecordedRequest recordedRequest = mockWebServer.takeRequest();
    verifyQueryRangeCall(recordedRequest);
  }

  @Test
  @SneakyThrows
  void testQueryRangeWithNon2xxError() {
    MockResponse mockResponse = new MockResponse()
        .addHeader("Content-Type", "application/json; charset=utf-8")
        .setResponseCode(HttpStatus.SC_BAD_REQUEST);
    mockWebServer.enqueue(mockResponse);
    RuntimeException runtimeException
        = assertThrows(RuntimeException.class,
          () -> prometheusClient.queryRange(QUERY, STARTTIME, ENDTIME, STEP));
    assertTrue(
        runtimeException.getMessage().contains("Request to Prometheus is Unsuccessful with :"));
    RecordedRequest recordedRequest = mockWebServer.takeRequest();
    verifyQueryRangeCall(recordedRequest);
  }

  @Test
  @SneakyThrows
  void testGetLabel() {
    MockResponse mockResponse = new MockResponse()
        .addHeader("Content-Type", "application/json; charset=utf-8")
        .setBody(getJson("get_labels_response.json"));
    mockWebServer.enqueue(mockResponse);
    List<String> response = prometheusClient.getLabels(METRIC_NAME);
    assertEquals(new ArrayList<String>() {{
        add("__name__");
        add("call");
        add("code");
      }
      }, response);
    RecordedRequest recordedRequest = mockWebServer.takeRequest();
    verifyGetLabelsCall(recordedRequest);
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

}
