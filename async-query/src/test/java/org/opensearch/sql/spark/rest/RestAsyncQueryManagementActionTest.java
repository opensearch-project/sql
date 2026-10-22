/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.rest;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.spark.transport.TransportCancelAsyncQueryRequestAction;
import org.opensearch.sql.spark.transport.model.CancelAsyncQueryActionRequest;
import org.opensearch.sql.spark.transport.model.CancelAsyncQueryActionResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

public class RestAsyncQueryManagementActionTest {

  private OpenSearchSettings settings;
  private RestRequest request;
  private RestChannel channel;
  private NodeClient nodeClient;
  private ThreadPool threadPool;
  private RestAsyncQueryManagementAction unit;

  @BeforeEach
  public void setup() {
    settings = Mockito.mock(OpenSearchSettings.class);
    request = Mockito.mock(RestRequest.class);
    channel = Mockito.mock(RestChannel.class);
    nodeClient = Mockito.mock(NodeClient.class);
    threadPool = Mockito.mock(ThreadPool.class);

    Mockito.when(nodeClient.threadPool()).thenReturn(threadPool);

    unit = new RestAsyncQueryManagementAction(settings);
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
    unit.handleRequest(request, channel, nodeClient);
    Mockito.verify(threadPool, Mockito.times(1))
        .schedule(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
    Mockito.verifyNoInteractions(channel);
  }

  @Test
  public void testGetName() {
    Assertions.assertEquals("async_query_actions", unit.getName());
  }

  @Test
  public void isPplRequest_trueForGetWithQueryJobId() {
    String pplId = org.opensearch.sql.job.QueryJobId.create("node-a").encode();
    RestRequest req = Mockito.mock(RestRequest.class);
    Mockito.when(req.method()).thenReturn(RestRequest.Method.GET);
    Mockito.when(req.param("queryId")).thenReturn(pplId);
    Assertions.assertTrue(RestAsyncQueryManagementAction.isPplRequest(req));
  }

  @Test
  public void isPplRequest_trueForDeleteWithQueryJobId() {
    String pplId = org.opensearch.sql.job.QueryJobId.create("node-a").encode();
    RestRequest req = Mockito.mock(RestRequest.class);
    Mockito.when(req.method()).thenReturn(RestRequest.Method.DELETE);
    Mockito.when(req.param("queryId")).thenReturn(pplId);
    Assertions.assertTrue(RestAsyncQueryManagementAction.isPplRequest(req));
  }

  @Test
  public void isPplRequest_falseForPost() {
    RestRequest req = Mockito.mock(RestRequest.class);
    Mockito.when(req.method()).thenReturn(RestRequest.Method.POST);
    Assertions.assertFalse(RestAsyncQueryManagementAction.isPplRequest(req));
  }

  @Test
  public void isPplRequest_falseForSparkQueryId() {
    RestRequest req = Mockito.mock(RestRequest.class);
    Mockito.when(req.method()).thenReturn(RestRequest.Method.GET);
    Mockito.when(req.param("queryId")).thenReturn("00abc1234efghij5");
    Assertions.assertFalse(RestAsyncQueryManagementAction.isPplRequest(req));
  }

  @Test
  public void isPplRequest_falseForMissingQueryId() {
    RestRequest req = Mockito.mock(RestRequest.class);
    Mockito.when(req.method()).thenReturn(RestRequest.Method.GET);
    Mockito.when(req.param("queryId")).thenReturn(null);
    Assertions.assertFalse(RestAsyncQueryManagementAction.isPplRequest(req));
  }

  @Test
  @SneakyThrows
  public void pplGet_passesWhenDataSourcesDisabled() {
    // The gate skip for PPL ids (P2-6): with datasources disabled, a GET whose queryId parses
    // as QueryJobId must NOT be rejected — it reaches the transport action instead.
    setDataSourcesEnabled(false);
    Mockito.when(request.method()).thenReturn(RestRequest.Method.GET);
    Mockito.when(request.param("queryId"))
        .thenReturn(org.opensearch.sql.job.QueryJobId.create("node-a").encode());
    unit.handleRequest(request, channel, nodeClient);
    // Request dispatched to the thread pool (same path as datasources-enabled); no 400 sent to
    // the channel.
    Mockito.verify(threadPool, Mockito.times(1))
        .schedule(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
    Mockito.verifyNoInteractions(channel);
  }

  @Test
  @SneakyThrows
  public void pplDelete_respondsOkWithStatusAcknowledgement() {
    setDataSourcesEnabled(false);
    String acknowledgement = "{\"status\":\"CANCELLED\"}";

    RestResponse response =
        deleteAndRespond(
            org.opensearch.sql.job.QueryJobId.create("node-a").encode(), acknowledgement);

    Assertions.assertEquals(RestStatus.OK, response.status());
    Assertions.assertEquals(acknowledgement, response.content().utf8ToString());
  }

  @Test
  @SneakyThrows
  public void sparkDelete_keepsNoContentStatus() {
    setDataSourcesEnabled(true);

    RestResponse response =
        deleteAndRespond("00abc1234efghij5", "Deleted async query with id: 00abc1234efghij5");

    Assertions.assertEquals(RestStatus.NO_CONTENT, response.status());
  }

  @SuppressWarnings("unchecked")
  private RestResponse deleteAndRespond(String queryId, String transportResult) throws Exception {
    Mockito.when(request.method()).thenReturn(RestRequest.Method.DELETE);
    Mockito.when(request.param("queryId")).thenReturn(queryId);
    unit.handleRequest(request, channel, nodeClient);
    ArgumentCaptor<Runnable> scheduled = ArgumentCaptor.forClass(Runnable.class);
    Mockito.verify(threadPool)
        .schedule(scheduled.capture(), ArgumentMatchers.any(), ArgumentMatchers.any());
    scheduled.getValue().run();
    ArgumentCaptor<ActionListener<CancelAsyncQueryActionResponse>> listener =
        ArgumentCaptor.forClass(ActionListener.class);
    Mockito.verify(nodeClient)
        .execute(
            ArgumentMatchers.eq(TransportCancelAsyncQueryRequestAction.ACTION_TYPE),
            ArgumentMatchers.argThat(
                (CancelAsyncQueryActionRequest r) -> queryId.equals(r.getQueryId())),
            listener.capture());
    listener.getValue().onResponse(new CancelAsyncQueryActionResponse(transportResult));
    ArgumentCaptor<RestResponse> response = ArgumentCaptor.forClass(RestResponse.class);
    Mockito.verify(channel).sendResponse(response.capture());
    return response.getValue();
  }

  private void setDataSourcesEnabled(boolean value) {
    Mockito.when(settings.getSettingValue(Settings.Key.DATASOURCES_ENABLED)).thenReturn(value);
  }
}
