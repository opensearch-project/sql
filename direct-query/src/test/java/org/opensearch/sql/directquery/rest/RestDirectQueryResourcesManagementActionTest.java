/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.rest;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.List;
import java.util.Locale;
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
import org.opensearch.sql.directquery.rest.model.DirectQueryResourceType;
import org.opensearch.sql.directquery.transport.model.GetDirectQueryResourcesActionRequest;
import org.opensearch.sql.directquery.transport.model.GetDirectQueryResourcesActionResponse;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

public class RestDirectQueryResourcesManagementActionTest {

  private OpenSearchSettings settings;
  private RestRequest request;
  private RestChannel channel;
  private NodeClient nodeClient;
  private ThreadPool threadPool;
  private RestDirectQueryResourcesManagementAction unit;

  @BeforeEach
  public void setup() {
    // allow mocking final methods
    MockSettings mockSettings = withSettings().mockMaker("mock-maker-inline");
    settings = mock(OpenSearchSettings.class);
    request = mock(RestRequest.class, mockSettings);
    channel = mock(RestChannel.class);
    nodeClient = mock(NodeClient.class, mockSettings);
    threadPool = mock(ThreadPool.class);

    Mockito.when(nodeClient.threadPool()).thenReturn(threadPool);

    unit = new RestDirectQueryResourcesManagementAction(settings);
  }

  @Test
  @SneakyThrows
  public void testWhenDataSourcesAreDisabled() {
    setDataSourcesEnabled(false);
    unit.handleRequest(request, channel, nodeClient);
    Mockito.verifyNoInteractions(nodeClient);
    ArgumentCaptor<RestResponse> response = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel, Mockito.times(1)).sendResponse(response.capture());
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
    Mockito.when(request.method()).thenReturn(RestRequest.Method.GET);
    Mockito.when(request.path())
        .thenReturn("/_plugins/_directquery/_resources/testDataSource/api/v1/labels");
    Map<String, String> requestParams =
        Map.of(
            "dataSource", "testDataSource",
            "resourceType", "labels");
    Mockito.when(request.param(Mockito.anyString()))
        .thenAnswer(i -> requestParams.get(i.getArgument(0)));
    Mockito.when(request.consumedParams()).thenReturn(List.of("dataSource", "resourceType"));
    Mockito.when(request.params()).thenReturn(ImmutableMap.copyOf(requestParams));

    unit.handleRequest(request, channel, nodeClient);
    Mockito.verify(threadPool, Mockito.times(1))
        .schedule(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
    Mockito.verifyNoInteractions(channel);
  }

  @Test
  @SneakyThrows
  public void testUnsupportedMethod() {
    setDataSourcesEnabled(true);
    Mockito.when(request.method()).thenReturn(RestRequest.Method.PUT);
    Mockito.when(request.consumedParams()).thenReturn(new java.util.ArrayList<>());
    unit.handleRequest(request, channel, nodeClient);

    ArgumentCaptor<RestResponse> response = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel, Mockito.times(1)).sendResponse(response.capture());
    Assertions.assertEquals(405, response.getValue().status().getStatus());
  }

  @Test
  public void testGetName() {
    Assertions.assertEquals("direct_query_resources_actions", unit.getName());
  }

  @Test
  public void testRoutes() {
    List<RestDirectQueryResourcesManagementAction.Route> routes = unit.routes();
    Assertions.assertNotNull(routes);
    Assertions.assertEquals(4, routes.size());

    boolean foundResourceTypeRoute = false;
    boolean foundResourceValuesRoute = false;
    boolean foundAlertmanagerResourceRoute = false;
    boolean foundAlertmanagerAlertGroupsRoute = false;

    for (RestDirectQueryResourcesManagementAction.Route route : routes) {
      if (RestRequest.Method.GET.equals(route.getMethod())
          && route
              .getPath()
              .equals(
                  String.format(
                      Locale.ROOT,
                      "%s/api/v1/{resourceType}",
                      RestDirectQueryResourcesManagementAction.BASE_DIRECT_QUERY_RESOURCES_URL))) {
        foundResourceTypeRoute = true;
      }
      if (RestRequest.Method.GET.equals(route.getMethod())
          && route
              .getPath()
              .equals(
                  String.format(
                      Locale.ROOT,
                      "%s/api/v1/{resourceType}/{resourceName}/values",
                      RestDirectQueryResourcesManagementAction.BASE_DIRECT_QUERY_RESOURCES_URL))) {
        foundResourceValuesRoute = true;
      }
      if (RestRequest.Method.GET.equals(route.getMethod())
          && route
              .getPath()
              .equals(
                  String.format(
                      Locale.ROOT,
                      "%s/alertmanager/api/v2/{resourceType}",
                      RestDirectQueryResourcesManagementAction.BASE_DIRECT_QUERY_RESOURCES_URL))) {
        foundAlertmanagerResourceRoute = true;
      }
      if (RestRequest.Method.GET.equals(route.getMethod())
          && route
              .getPath()
              .equals(
                  String.format(
                      Locale.ROOT,
                      "%s/alertmanager/api/v2/alerts/groups",
                      RestDirectQueryResourcesManagementAction.BASE_DIRECT_QUERY_RESOURCES_URL))) {
        foundAlertmanagerAlertGroupsRoute = true;
      }
    }

    Assertions.assertTrue(foundResourceTypeRoute, "Resource type route not found");
    Assertions.assertTrue(foundResourceValuesRoute, "Resource values route not found");
    Assertions.assertTrue(
        foundAlertmanagerResourceRoute, "Alertmanager resource type route not found");
    Assertions.assertTrue(
        foundAlertmanagerAlertGroupsRoute, "Alertmanager alert groups route not found");
  }

  @Test
  @SneakyThrows
  public void testSuccessfulResponse() {
    setDataSourcesEnabled(true);
    String successResponse = "{\"result\":\"success\"}";
    GetDirectQueryResourcesActionResponse response =
        new GetDirectQueryResourcesActionResponse(successResponse);

    Mockito.when(request.method()).thenReturn(RestRequest.Method.GET);
    Map<String, String> requestParams =
        Map.of(
            "dataSource", "testDataSource",
            "resourceType", "labels",
            "resourceName", "testResourceName",
            "mockParamKey1", "mockParamVal1",
            "mockParamKey2", "mockParamVal2");
    Mockito.when(request.param(Mockito.anyString()))
        .thenAnswer(i -> requestParams.get(i.getArgument(0)));
    Mockito.when(request.consumedParams())
        .thenReturn(List.of("dataSource", "resourceType", "resourceName"));
    Mockito.when(request.params()).thenReturn(ImmutableMap.copyOf(requestParams));
    Mockito.when(request.path())
        .thenReturn(
            "/_plugins/_directquery/_resources/testDataSource/api/v1/labels/testResourceName/values");

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
              Runnable runnable = invocation.getArgument(0);
              runnable.run();
              return null;
            })
        .when(threadPool)
        .schedule(Mockito.any(Runnable.class), Mockito.any(), Mockito.any());

    Mockito.doAnswer(
            invocation -> {
              GetDirectQueryResourcesActionRequest request = invocation.getArgument(1);
              Assertions.assertEquals(
                  "testDataSource", request.getDirectQueryRequest().getDataSource());
              Assertions.assertEquals(
                  DirectQueryResourceType.LABELS,
                  request.getDirectQueryRequest().getResourceType());
              Assertions.assertEquals(
                  "testResourceName", request.getDirectQueryRequest().getResourceName());
              Assertions.assertEquals(
                  Map.of(
                      "mockParamKey1", "mockParamVal1",
                      "mockParamKey2", "mockParamVal2"),
                  request.getDirectQueryRequest().getQueryParams());
              return null;
            })
        .when(nodeClient)
        .execute(Mockito.any(), Mockito.any(), listenerCaptor.capture());

    unit.handleRequest(request, channel, nodeClient);

    ActionListener listener = listenerCaptor.getValue();
    listener.onResponse(response);

    ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel).sendResponse(responseCaptor.capture());

    RestResponse capturedResponse = responseCaptor.getValue();
    Assertions.assertEquals(200, capturedResponse.status().getStatus());
    Assertions.assertEquals("application/json; charset=UTF-8", capturedResponse.contentType());
    Assertions.assertEquals(successResponse, capturedResponse.content().utf8ToString());
  }

  @Test
  @SneakyThrows
  public void testBadRequestResponse() {
    IllegalArgumentException clientError =
        new IllegalArgumentException("Invalid request parameter");

    ActionListener listener = makeRequest();
    listener.onFailure(clientError);

    ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel).sendResponse(responseCaptor.capture());

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
  public void testInternalServerErrorResponse() {
    RuntimeException serverError = new RuntimeException("Internal server error");

    ActionListener listener = makeRequest();
    listener.onFailure(serverError);

    ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel).sendResponse(responseCaptor.capture());

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
    Mockito.verify(channel).sendResponse(responseCaptor.capture());

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
  public void testIllegalAccessException() {
    IllegalAccessException illegalAccessException = new IllegalAccessException("Illegal access");

    ActionListener listener = makeRequest();
    listener.onFailure(illegalAccessException);

    ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel).sendResponse(responseCaptor.capture());

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
        "Illegal access", actualResponseJson.getAsJsonObject("error").get("details").getAsString());
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
    setDataSourcesEnabled(true);
    Mockito.when(request.method()).thenReturn(RestRequest.Method.GET);
    Map<String, String> requestParams =
        Map.of(
            "dataSource", "testDataSource",
            "resourceType", "labels");
    Mockito.when(request.param(Mockito.anyString()))
        .thenAnswer(i -> requestParams.get(i.getArgument(0)));
    Mockito.when(request.consumedParams()).thenReturn(List.of("dataSource", "resourceType"));
    Mockito.when(request.params()).thenReturn(ImmutableMap.copyOf(requestParams));
    Mockito.when(request.path())
        .thenReturn("/_plugins/_directquery/_resources/testDataSource/api/v1/labels");

    ArgumentCaptor<ActionListener> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

    Mockito.doAnswer(
            invocation -> {
              Runnable runnable = invocation.getArgument(0);
              runnable.run();
              return null;
            })
        .when(threadPool)
        .schedule(Mockito.any(Runnable.class), Mockito.any(), Mockito.any());

    Mockito.doAnswer(invocation -> null)
        .when(nodeClient)
        .execute(Mockito.any(), Mockito.any(), listenerCaptor.capture());

    unit.handleRequest(request, channel, nodeClient);

    return listenerCaptor.getValue();
  }
}
