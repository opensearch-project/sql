/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.rest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.MockSettings;
import org.mockito.Mockito;
import org.opensearch.OpenSearchException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.client.exceptions.DataSourceClientException;
import org.opensearch.sql.directquery.transport.model.ExecuteDirectQueryActionResponse;
import org.opensearch.sql.directquery.transport.model.datasource.DataSourceResult;
import org.opensearch.sql.directquery.transport.model.datasource.PrometheusResult;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

public class RestDirectQueryManagementActionTest {

  private OpenSearchSettings settings;
  private RestRequest request;
  private RestChannel channel;
  private NodeClient nodeClient;
  private ThreadPool threadPool;
  private RestDirectQueryManagementAction unit;

  @BeforeEach
  public void setup() {
    // allow mocking final methods
    MockSettings mockSettings = withSettings().mockMaker("mock-maker-inline");
    settings = mock(OpenSearchSettings.class);
    request = mock(RestRequest.class, mockSettings);
    channel = mock(RestChannel.class);
    nodeClient = mock(NodeClient.class, mockSettings);
    threadPool = mock(ThreadPool.class);

    when(nodeClient.threadPool()).thenReturn(threadPool);
    when(nodeClient.threadPool()).thenReturn(threadPool);

    unit = new RestDirectQueryManagementAction(settings);
  }

  @Test
  @SneakyThrows
  public void testWhenDataSourcesAreDisabled() {
    setDataSourcesEnabled(false);
    unit.handleRequest(request, channel, nodeClient);
    Mockito.verifyNoInteractions(nodeClient);
    ArgumentCaptor<RestResponse> response = ArgumentCaptor.forClass(RestResponse.class);
    verify(channel, Mockito.times(1)).sendResponse(response.capture());
    verify(channel, Mockito.times(1)).sendResponse(response.capture());
    Assertions.assertEquals(400, response.getValue().status().getStatus());
    JsonObject actualResponseJson =
        new Gson().fromJson(response.getValue().content().utf8ToString(), JsonObject.class);
    JsonObject expectedResponseJson = new JsonObject();
    expectedResponseJson.addProperty("status", 400);
    expectedResponseJson.add("error", new JsonObject());
    expectedResponseJson.getAsJsonObject("error").addProperty("type", "IllegalAccessException");
    expectedResponseJson.getAsJsonObject("error").addProperty("reason", "Invalid Request");
    expectedResponseJson
        .getAsJsonObject("error")
        .addProperty("details", "plugins.query.datasources.enabled setting is false");
    Assertions.assertEquals(expectedResponseJson, actualResponseJson);
  }

  @Test
  @SneakyThrows
  public void testWhenDataSourcesAreEnabled() {
    setDataSourcesEnabled(true);
    when(request.method()).thenReturn(RestRequest.Method.POST);
    when(request.param("dataSources")).thenReturn("testDataSource");
    when(request.method()).thenReturn(RestRequest.Method.POST);
    when(request.param("dataSources")).thenReturn("testDataSource");
    String requestContent =
        "{\"query\":\"up\",\"language\":\"promql\",\"options\":{\"queryType\":\"instant\",\"time\":\"1609459200\"}}";
    when(request.contentParser())
        .thenReturn(
            new org.opensearch.common.xcontent.json.JsonXContentParser(
                org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                new com.fasterxml.jackson.core.JsonFactory().createParser(requestContent)));
    when(request.contentParser())
        .thenReturn(
            new org.opensearch.common.xcontent.json.JsonXContentParser(
                org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                new com.fasterxml.jackson.core.JsonFactory().createParser(requestContent)));

    unit.handleRequest(request, channel, nodeClient);
    verify(threadPool, Mockito.times(1))
        .schedule(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
    Mockito.verifyNoInteractions(channel);
  }

  @Test
  @SneakyThrows
  public void testUnsupportedMethod() {
    setDataSourcesEnabled(true);
    when(request.method()).thenReturn(RestRequest.Method.GET);
    when(request.consumedParams()).thenReturn(new java.util.ArrayList<>());
    when(request.method()).thenReturn(RestRequest.Method.GET);
    when(request.consumedParams()).thenReturn(new java.util.ArrayList<>());
    unit.handleRequest(request, channel, nodeClient);

    ArgumentCaptor<RestResponse> response = ArgumentCaptor.forClass(RestResponse.class);
    verify(channel, Mockito.times(1)).sendResponse(response.capture());
    verify(channel, Mockito.times(1)).sendResponse(response.capture());
    Assertions.assertEquals(405, response.getValue().status().getStatus());
  }

  @Test
  public void testGetName() {
    Assertions.assertEquals("direct_query_actions", unit.getName());
  }

  @Test
  public void testRoutes() {
    List<RestDirectQueryManagementAction.Route> routes = unit.routes();
    Assertions.assertNotNull(routes);
    Assertions.assertEquals(1, routes.size());

    RestDirectQueryManagementAction.Route route = routes.get(0);
    Assertions.assertEquals(RestRequest.Method.POST, route.getMethod());
    Assertions.assertEquals("/_plugins/_directquery/_query/{dataSources}", route.getPath());
  }

  @Test
  @SneakyThrows
  public void testIllegalArgumentExceptionWithNullContent() {
    setDataSourcesEnabled(true);
    Mockito.when(request.method()).thenReturn(RestRequest.Method.POST);
    Mockito.when(request.param("dataSources")).thenReturn("testDataSource");

    unit.handleRequest(request, channel, nodeClient);
    ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel, Mockito.times(1)).sendResponse(responseCaptor.capture());

    RestResponse capturedResponse = responseCaptor.getValue();
    Assertions.assertEquals(400, capturedResponse.status().getStatus());

    JsonObject actualResponseJson =
        new Gson().fromJson(capturedResponse.content().utf8ToString(), JsonObject.class);
    Assertions.assertEquals(400, actualResponseJson.get("status").getAsInt());
    Assertions.assertTrue(actualResponseJson.has("error"));
    Assertions.assertEquals(
        "IllegalArgumentException",
        actualResponseJson.getAsJsonObject("error").get("type").getAsString());
    Assertions.assertEquals(
        "Invalid Request", actualResponseJson.getAsJsonObject("error").get("reason").getAsString());
  }

  @Test
  @SneakyThrows
  public void testSuccessfulResponse() {
    PrometheusResult prometheusResult = new PrometheusResult();

    Map<String, DataSourceResult> resultsMap = new HashMap<>();
    resultsMap.put("testDataSource", prometheusResult);

    ExecuteDirectQueryActionResponse response =
        new ExecuteDirectQueryActionResponse("test-query-id", resultsMap, "test-session-id");

    ActionListener listener =
        makeRequest(
            "{\"query\":\"up\",\"language\":\"promql\",\"options\":{\"queryType\":\"instant\",\"time\":\"1609459200\"},\"datasource\":\"mock-datasource\",\"sessionId\":\"mock-session\"}");
    listener.onResponse(response);

    ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
    verify(channel).sendResponse(responseCaptor.capture());

    RestResponse capturedResponse = responseCaptor.getValue();
    Assertions.assertEquals(200, capturedResponse.status().getStatus());
    Assertions.assertEquals("application/json; charset=UTF-8", capturedResponse.contentType());

    String responseContent = capturedResponse.content().utf8ToString();
    assertThat(responseContent, containsString("queryId"));
    assertThat(responseContent, containsString("test-query-id"));
    assertThat(responseContent, containsString("sessionId"));
    assertThat(responseContent, containsString("test-session-id"));
  }

  @Test
  @SneakyThrows
  public void testFormatDirectQueryResponseError() {
    // this fails and triggers format error because mocked class can't be serialized
    PrometheusResult mockPrometheusResult = Mockito.mock(PrometheusResult.class);

    Map<String, DataSourceResult> resultsMap = new HashMap<>();
    resultsMap.put("testDataSource", mockPrometheusResult);

    ExecuteDirectQueryActionResponse response =
        Mockito.mock(ExecuteDirectQueryActionResponse.class);
    Mockito.when(response.getResults()).thenReturn(resultsMap);

    String expectedJson = "{\"testDataSource\":{}}"; // Simple representation

    ObjectMapper mockMapper = Mockito.mock(ObjectMapper.class);
    Mockito.when(mockMapper.writeValueAsString(resultsMap)).thenReturn(expectedJson);

    ActionListener listener =
        makeRequest(
            "{\"query\":\"up\",\"language\":\"promql\",\"options\":{\"queryType\":\"instant\",\"time\":\"1609459200\"},\"datasource\":\"mock-datasource\",\"sessionId\":\"mock-session\"}");
    listener.onResponse(response);

    ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
    verify(channel).sendResponse(responseCaptor.capture());

    RestResponse capturedResponse = responseCaptor.getValue();
    Assertions.assertEquals(200, capturedResponse.status().getStatus());
    Assertions.assertEquals("application/json; charset=UTF-8", capturedResponse.contentType());
    assertThat(capturedResponse.content().utf8ToString(), containsString("\"error\""));
  }

  @Test
  @SneakyThrows
  public void testBadRequestResponse() {
    IllegalAccessException clientError = new IllegalAccessException("Invalid access");

    ActionListener listener = makeRequest();
    listener.onFailure(clientError);

    ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
    verify(channel).sendResponse(responseCaptor.capture());
    verify(channel).sendResponse(responseCaptor.capture());

    RestResponse capturedResponse = responseCaptor.getValue();
    Assertions.assertEquals(400, capturedResponse.status().getStatus());

    JsonObject actualResponseJson =
        new Gson().fromJson(capturedResponse.content().utf8ToString(), JsonObject.class);
    Assertions.assertEquals(400, actualResponseJson.get("status").getAsInt());
    Assertions.assertTrue(actualResponseJson.has("error"));
    Assertions.assertEquals(
        "IllegalAccessException",
        actualResponseJson.getAsJsonObject("error").get("type").getAsString());
    Assertions.assertEquals(
        "Invalid Request", actualResponseJson.getAsJsonObject("error").get("reason").getAsString());
    Assertions.assertEquals(
        "Invalid Request", actualResponseJson.getAsJsonObject("error").get("reason").getAsString());
  }

  @Test
  @SneakyThrows
  public void testInternalServerErrorResponse() {
    RuntimeException serverError = new RuntimeException("Internal server error");

    ActionListener listener = makeRequest();
    listener.onFailure(serverError);

    ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
    verify(channel).sendResponse(responseCaptor.capture());
    verify(channel).sendResponse(responseCaptor.capture());

    RestResponse capturedResponse = responseCaptor.getValue();
    Assertions.assertEquals(500, capturedResponse.status().getStatus());

    JsonObject actualResponseJson =
        new Gson().fromJson(capturedResponse.content().utf8ToString(), JsonObject.class);
    Assertions.assertEquals(500, actualResponseJson.get("status").getAsInt());
    Assertions.assertTrue(actualResponseJson.has("error"));
    Assertions.assertEquals(
        "RuntimeException", actualResponseJson.getAsJsonObject("error").get("type").getAsString());
    Assertions.assertEquals(
        "There was internal problem at backend",
        actualResponseJson.getAsJsonObject("error").get("reason").getAsString());
  }

  @Test
  @SneakyThrows
  public void testOpenSearchException() {
    OpenSearchException opensearchError = new OpenSearchException("OpenSearch specific error");

    ActionListener listener = makeRequest();
    listener.onFailure(opensearchError);

    ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
    verify(channel).sendResponse(responseCaptor.capture());

    RestResponse capturedResponse = responseCaptor.getValue();
    Assertions.assertEquals(500, capturedResponse.status().getStatus());

    JsonObject actualResponseJson =
        new Gson().fromJson(capturedResponse.content().utf8ToString(), JsonObject.class);
    Assertions.assertEquals(500, actualResponseJson.get("status").getAsInt());
    Assertions.assertTrue(actualResponseJson.has("error"));
    Assertions.assertEquals(
        "OpenSearchException",
        actualResponseJson.getAsJsonObject("error").get("type").getAsString());
    Assertions.assertEquals(
        "OpenSearch specific error",
        actualResponseJson.getAsJsonObject("error").get("details").getAsString());
  }

  @Test
  @SneakyThrows
  public void testIllegalStateException() {
    IllegalStateException illegalStateException = new IllegalStateException("Illegal state");

    ActionListener listener = makeRequest();
    listener.onFailure(illegalStateException);

    ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel).sendResponse(responseCaptor.capture());

    RestResponse capturedResponse = responseCaptor.getValue();
    Assertions.assertEquals(400, capturedResponse.status().getStatus());

    JsonObject actualResponseJson =
        new Gson().fromJson(capturedResponse.content().utf8ToString(), JsonObject.class);
    Assertions.assertEquals(400, actualResponseJson.get("status").getAsInt());
    Assertions.assertTrue(actualResponseJson.has("error"));
    Assertions.assertEquals(
        "IllegalStateException",
        actualResponseJson.getAsJsonObject("error").get("type").getAsString());
    Assertions.assertEquals(
        "Illegal state", actualResponseJson.getAsJsonObject("error").get("details").getAsString());
  }

  @Test
  @SneakyThrows
  public void testDataSourceClientException() {
    DataSourceClientException dataSourceClientException =
        new DataSourceClientException("Data source client error");

    ActionListener listener = makeRequest();
    listener.onFailure(dataSourceClientException);

    ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel).sendResponse(responseCaptor.capture());

    RestResponse capturedResponse = responseCaptor.getValue();
    Assertions.assertEquals(400, capturedResponse.status().getStatus());

    JsonObject actualResponseJson =
        new Gson().fromJson(capturedResponse.content().utf8ToString(), JsonObject.class);
    Assertions.assertEquals(400, actualResponseJson.get("status").getAsInt());
    Assertions.assertTrue(actualResponseJson.has("error"));
    Assertions.assertEquals(
        "DataSourceClientException",
        actualResponseJson.getAsJsonObject("error").get("type").getAsString());
    Assertions.assertEquals(
        "Data source client error",
        actualResponseJson.getAsJsonObject("error").get("details").getAsString());
  }

  private void setDataSourcesEnabled(boolean value) {
    Mockito.when(settings.getSettingValue(Settings.Key.DATASOURCES_ENABLED)).thenReturn(value);
  }

  @SneakyThrows
  private ActionListener makeRequest() {
    String requestContent =
        "{\"query\":\"up\",\"language\":\"promql\",\"options\":{\"queryType\":\"instant\",\"time\":\"1609459200\"}}";
    return makeRequest(requestContent);
  }

  @SneakyThrows
  private ActionListener makeRequest(String requestContent) {
    setDataSourcesEnabled(true);
    Mockito.when(request.method()).thenReturn(RestRequest.Method.POST);
    Mockito.when(request.param("dataSources")).thenReturn("testDataSource");
    Mockito.when(request.contentParser())
        .thenReturn(
            new org.opensearch.common.xcontent.json.JsonXContentParser(
                org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                new com.fasterxml.jackson.core.JsonFactory().createParser(requestContent)));
    Mockito.when(request.consumedParams()).thenReturn(java.util.Collections.emptyList());
    Mockito.when(request.params()).thenReturn(java.util.Collections.emptyMap());

    ArgumentCaptor<ActionListener> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

    Mockito.doAnswer(
            invocation -> {
              Runnable runnable = invocation.getArgument(0);
              runnable.run();
              return null;
            })
        .when(threadPool)
        .schedule(Mockito.any(Runnable.class), Mockito.any(), Mockito.any());

    Mockito.doAnswer(
            invocation -> {
              ActionListener listener = invocation.getArgument(2);
              return null;
            })
        .when(nodeClient)
        .execute(Mockito.any(), Mockito.any(), listenerCaptor.capture());

    unit.handleRequest(request, channel, nodeClient);
    return listenerCaptor.getValue();
  }
}
