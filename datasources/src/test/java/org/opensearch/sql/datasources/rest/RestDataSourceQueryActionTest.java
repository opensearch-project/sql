package org.opensearch.sql.datasources.rest;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.threadpool.ThreadPool;

public class RestDataSourceQueryActionTest {

  private OpenSearchSettings settings;
  private RestRequest request;
  private RestChannel channel;
  private NodeClient nodeClient;
  private ThreadPool threadPool;
  private RestDataSourceQueryAction unit;

  @BeforeEach
  public void setup() {
    settings = Mockito.mock(OpenSearchSettings.class);
    request = Mockito.mock(RestRequest.class);
    channel = Mockito.mock(RestChannel.class);
    nodeClient = Mockito.mock(NodeClient.class);
    threadPool = Mockito.mock(ThreadPool.class);

    Mockito.when(nodeClient.threadPool()).thenReturn(threadPool);

    unit = new RestDataSourceQueryAction(settings);
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
    expectedResponseJson.getAsJsonObject("error").addProperty("type", "OpenSearchStatusException");
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
    Assertions.assertEquals("datasource_actions", unit.getName());
  }

  private void setDataSourcesEnabled(boolean value) {
    Mockito.when(settings.getSettingValue(Settings.Key.DATASOURCES_ENABLED)).thenReturn(value);
  }
}
