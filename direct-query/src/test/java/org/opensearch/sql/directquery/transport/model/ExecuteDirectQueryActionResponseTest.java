/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.sql.directquery.transport.model.datasource.DataSourceResult;
import org.opensearch.sql.directquery.transport.model.datasource.PrometheusResult;

/*
 * @opensearch.experimental
 */
public class ExecuteDirectQueryActionResponseTest {

  @Test
  public void testConstructorWithMapResults() {
    String queryId = "query-123";
    String sessionId = "session-456";
    Map<String, DataSourceResult> results = new HashMap<>();
    PrometheusResult prometheusResult = new PrometheusResult();
    prometheusResult.setResultType("prometheus");
    results.put("prom-ds", prometheusResult);

    ExecuteDirectQueryActionResponse response =
        new ExecuteDirectQueryActionResponse(queryId, results, sessionId);

    assertEquals(queryId, response.getQueryId());
    assertEquals(sessionId, response.getSessionId());
    assertEquals(1, response.getResults().size());
    assertTrue(response.getResults().containsKey("prom-ds"));
  }

  @Test
  public void testConstructorWithNullValues() {
    ExecuteDirectQueryActionResponse response =
        new ExecuteDirectQueryActionResponse(null, new HashMap<>(), null);

    assertEquals(null, response.getQueryId());
    assertEquals(null, response.getSessionId());
    assertNotNull(response.getResults());
    assertEquals(0, response.getResults().size());
  }

  @Test
  public void testConstructorWithRawResultPrometheus() throws IOException {
    String queryId = "query-789";
    String sessionId = "session-012";
    String rawResult = "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[]}}";
    String dataSourceName = "prometheus-1";
    String dataSourceType = "prometheus";

    ExecuteDirectQueryActionResponse response =
        new ExecuteDirectQueryActionResponse(queryId, rawResult, sessionId, dataSourceName, dataSourceType);

    assertEquals(queryId, response.getQueryId());
    assertEquals(sessionId, response.getSessionId());
    assertEquals(1, response.getResults().size());
    assertTrue(response.getResults().containsKey(dataSourceName));
    assertInstanceOf(PrometheusResult.class, response.getResults().get(dataSourceName));
  }

  @Test
  public void testConstructorWithRawResultAlreadyHasType() throws IOException {
    String queryId = "query-456";
    String sessionId = "session-789";
    String rawResult = "{\"type\":\"prometheus\",\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[]}}";
    String dataSourceName = "prom-ds";
    String dataSourceType = "prometheus";

    ExecuteDirectQueryActionResponse response =
        new ExecuteDirectQueryActionResponse(queryId, rawResult, sessionId, dataSourceName, dataSourceType);

    assertEquals(queryId, response.getQueryId());
    assertEquals(sessionId, response.getSessionId());
    assertEquals(1, response.getResults().size());
    assertInstanceOf(PrometheusResult.class, response.getResults().get(dataSourceName));
  }

  @Test
  public void testConstructorWithUnsupportedDataSourceType() {
    String queryId = "query-error";
    String sessionId = "session-error";
    String rawResult = "{\"data\": \"some data\"}";
    String dataSourceName = "unknown-ds";
    String dataSourceType = "unsupported";

    assertThrows(IOException.class, () ->
        new ExecuteDirectQueryActionResponse(queryId, rawResult, sessionId, dataSourceName, dataSourceType));
  }

  @Test
  public void testConstructorWithInvalidJson() {
    String queryId = "query-invalid";
    String sessionId = "session-invalid";
    String rawResult = "invalid json string {";
    String dataSourceName = "prom-ds";
    String dataSourceType = "prometheus";

    assertThrows(IOException.class, () ->
        new ExecuteDirectQueryActionResponse(queryId, rawResult, sessionId, dataSourceName, dataSourceType));
  }

  @Test
  public void testStreamSerializationWithPrometheusResult() throws IOException {
    String queryId = "query-stream";
    String sessionId = "session-stream";
    Map<String, DataSourceResult> results = new HashMap<>();
    PrometheusResult prometheusResult = new PrometheusResult();
    prometheusResult.setResultType("prometheus");
    results.put("prom-ds-1", prometheusResult);

    ExecuteDirectQueryActionResponse response =
        new ExecuteDirectQueryActionResponse(queryId, results, sessionId);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StreamOutput streamOutput = new OutputStreamStreamOutput(outputStream);
    response.writeTo(streamOutput);
    streamOutput.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    ExecuteDirectQueryActionResponse deserializedResponse =
        new ExecuteDirectQueryActionResponse(streamInput);
    streamInput.close();

    assertEquals(queryId, deserializedResponse.getQueryId());
    assertEquals(sessionId, deserializedResponse.getSessionId());
    assertEquals(1, deserializedResponse.getResults().size());
    assertTrue(deserializedResponse.getResults().containsKey("prom-ds-1"));
    assertInstanceOf(PrometheusResult.class, deserializedResponse.getResults().get("prom-ds-1"));
  }

  @Test
  public void testStreamSerializationWithNullSessionId() throws IOException {
    String queryId = "query-no-session";
    Map<String, DataSourceResult> results = new HashMap<>();
    PrometheusResult prometheusResult = new PrometheusResult();
    prometheusResult.setResultType("prometheus");
    results.put("prom-ds", prometheusResult);

    ExecuteDirectQueryActionResponse response =
        new ExecuteDirectQueryActionResponse(queryId, results, null);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StreamOutput streamOutput = new OutputStreamStreamOutput(outputStream);
    response.writeTo(streamOutput);
    streamOutput.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    ExecuteDirectQueryActionResponse deserializedResponse =
        new ExecuteDirectQueryActionResponse(streamInput);
    streamInput.close();

    assertEquals(queryId, deserializedResponse.getQueryId());
    assertEquals(null, deserializedResponse.getSessionId());
    assertEquals(1, deserializedResponse.getResults().size());
  }

  @Test
  public void testStreamSerializationWithMultipleResults() throws IOException {
    String queryId = "query-multi";
    String sessionId = "session-multi";
    Map<String, DataSourceResult> results = new HashMap<>();

    PrometheusResult result1 = new PrometheusResult();
    result1.setResultType("prometheus");
    results.put("prom-ds-1", result1);

    PrometheusResult result2 = new PrometheusResult();
    result2.setResultType("prometheus");
    results.put("prom-ds-2", result2);

    ExecuteDirectQueryActionResponse response =
        new ExecuteDirectQueryActionResponse(queryId, results, sessionId);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StreamOutput streamOutput = new OutputStreamStreamOutput(outputStream);
    response.writeTo(streamOutput);
    streamOutput.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    ExecuteDirectQueryActionResponse deserializedResponse =
        new ExecuteDirectQueryActionResponse(streamInput);
    streamInput.close();

    assertEquals(queryId, deserializedResponse.getQueryId());
    assertEquals(sessionId, deserializedResponse.getSessionId());
    assertEquals(2, deserializedResponse.getResults().size());
    assertTrue(deserializedResponse.getResults().containsKey("prom-ds-1"));
    assertTrue(deserializedResponse.getResults().containsKey("prom-ds-2"));
  }

  @Test
  public void testStreamSerializationWithEmptyResults() throws IOException {
    String queryId = "query-empty";
    String sessionId = "session-empty";
    Map<String, DataSourceResult> results = new HashMap<>();

    ExecuteDirectQueryActionResponse response =
        new ExecuteDirectQueryActionResponse(queryId, results, sessionId);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    StreamOutput streamOutput = new OutputStreamStreamOutput(outputStream);
    response.writeTo(streamOutput);
    streamOutput.close();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    StreamInput streamInput = new InputStreamStreamInput(inputStream);
    ExecuteDirectQueryActionResponse deserializedResponse =
        new ExecuteDirectQueryActionResponse(streamInput);
    streamInput.close();

    assertEquals(queryId, deserializedResponse.getQueryId());
    assertEquals(sessionId, deserializedResponse.getSessionId());
    assertEquals(0, deserializedResponse.getResults().size());
  }

  @Test
  public void testGetterMethods() {
    String queryId = "getter-test-query";
    String sessionId = "getter-test-session";
    Map<String, DataSourceResult> results = new HashMap<>();
    PrometheusResult prometheusResult = new PrometheusResult();
    results.put("test-ds", prometheusResult);

    ExecuteDirectQueryActionResponse response =
        new ExecuteDirectQueryActionResponse(queryId, results, sessionId);

    assertEquals(queryId, response.getQueryId());
    assertEquals(sessionId, response.getSessionId());
    assertNotNull(response.getResults());
    assertEquals(results, response.getResults());
  }

  @Test
  public void testConstructorWithComplexPrometheusResult() throws IOException {
    String queryId = "complex-query";
    String sessionId = "complex-session";
    String complexResult = "{\"resultType\":\"matrix\",\"result\":[{\"metric\":{\"__name__\":\"up\",\"instance\":\"localhost:9090\"},\"values\":[[1609459200,\"1\"],[1609459260,\"1\"]]}]}";
    String dataSourceName = "complex-prom";
    String dataSourceType = "prometheus";

    ExecuteDirectQueryActionResponse response =
        new ExecuteDirectQueryActionResponse(queryId, complexResult, sessionId, dataSourceName, dataSourceType);

    assertEquals(queryId, response.getQueryId());
    assertEquals(sessionId, response.getSessionId());
    assertEquals(1, response.getResults().size());
    PrometheusResult result = (PrometheusResult) response.getResults().get(dataSourceName);
    assertNotNull(result);
    assertEquals("matrix", result.getResultType());
  }

  @Test
  public void testConstructorWithPrometheusError() throws IOException {
    String queryId = "error-query";
    String sessionId = "error-session";
    String errorResult = "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"invalid query\"}";
    String dataSourceName = "error-prom";
    String dataSourceType = "prometheus";

    ExecuteDirectQueryActionResponse response =
        new ExecuteDirectQueryActionResponse(queryId, errorResult, sessionId, dataSourceName, dataSourceType);

    assertEquals(queryId, response.getQueryId());
    assertEquals(sessionId, response.getSessionId());
    assertEquals(1, response.getResults().size());
    PrometheusResult result = (PrometheusResult) response.getResults().get(dataSourceName);
    assertNotNull(result);
  }
}