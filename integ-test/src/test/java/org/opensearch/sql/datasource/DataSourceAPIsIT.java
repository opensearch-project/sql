/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class DataSourceAPIsIT extends PPLIntegTestCase {

  @SneakyThrows
  @Test
  public void createDataSourceAPITest() {
    //create datasource
    DataSourceMetadata createDSM =
        new DataSourceMetadata("create_prometheus", DataSourceType.PROMETHEUS,
            ImmutableList.of(), ImmutableMap.of("prometheus.uri", "https://localhost:9090"));
    Request createRequest = getCreateDataSourceRequest(createDSM);
    Response response = client().performRequest(createRequest);
    Assert.assertEquals(201, response.getStatusLine().getStatusCode());
    String createResponseString = getResponseBody(response);
    Assert.assertEquals("Created DataSource with name create_prometheus", createResponseString);
    //Datasource is not immediately created. so introducing a sleep of 2s.
    Thread.sleep(2000);

    //get datasource to validate the creation.
    Request getRequest = getFetchDataSourceRequest("create_prometheus");
    Response getResponse = client().performRequest(getRequest);
    Assert.assertEquals(200, getResponse.getStatusLine().getStatusCode());
    String getResponseString = getResponseBody(getResponse);
    DataSourceMetadata dataSourceMetadata =
        new Gson().fromJson(getResponseString, DataSourceMetadata.class);
    Assert.assertEquals("https://localhost:9090",
        dataSourceMetadata.getProperties().get("prometheus.uri"));
  }


  @SneakyThrows
  @Test
  public void updateDataSourceAPITest() {
    //create datasource
    DataSourceMetadata createDSM =
        new DataSourceMetadata("update_prometheus", DataSourceType.PROMETHEUS,
            ImmutableList.of(), ImmutableMap.of("prometheus.uri", "https://localhost:9090"));
    Request createRequest = getCreateDataSourceRequest(createDSM);
    client().performRequest(createRequest);
    //Datasource is not immediately created. so introducing a sleep of 2s.
    Thread.sleep(2000);

    //update datasource
    DataSourceMetadata updateDSM =
        new DataSourceMetadata("update_prometheus", DataSourceType.PROMETHEUS,
            ImmutableList.of(), ImmutableMap.of("prometheus.uri", "https://randomtest:9090"));
    Request updateRequest = getUpdateDataSourceRequest(updateDSM);
    Response updateResponse = client().performRequest(updateRequest);
    Assert.assertEquals(200, updateResponse.getStatusLine().getStatusCode());
    String updateResponseString = getResponseBody(updateResponse);
    Assert.assertEquals("Updated DataSource with name update_prometheus", updateResponseString);

    //Datasource is not immediately updated. so introducing a sleep of 2s.
    Thread.sleep(2000);

    //get datasource to validate the modification.
    //get datasource
    Request getRequest = getFetchDataSourceRequest("update_prometheus");
    Response getResponse = client().performRequest(getRequest);
    Assert.assertEquals(200, getResponse.getStatusLine().getStatusCode());
    String getResponseString = getResponseBody(getResponse);
    DataSourceMetadata dataSourceMetadata =
        new Gson().fromJson(getResponseString, DataSourceMetadata.class);
    Assert.assertEquals("https://randomtest:9090",
        dataSourceMetadata.getProperties().get("prometheus.uri"));
  }


  @SneakyThrows
  @Test
  public void deleteDataSourceTest() {

    //create datasource for deletion
    DataSourceMetadata createDSM =
        new DataSourceMetadata("delete_prometheus", DataSourceType.PROMETHEUS,
            ImmutableList.of(), ImmutableMap.of("prometheus.uri", "https://localhost:9090"));
    Request createRequest = getCreateDataSourceRequest(createDSM);
    client().performRequest(createRequest);
    //Datasource is not immediately created. so introducing a sleep of 2s.
    Thread.sleep(2000);

    //delete datasource
    Request deleteRequest = getDeleteDataSourceRequest("delete_prometheus");
    Response deleteResponse = client().performRequest(deleteRequest);
    Assert.assertEquals(204, deleteResponse.getStatusLine().getStatusCode());

    //Datasource is not immediately deleted. so introducing a sleep of 2s.
    Thread.sleep(2000);

    //get datasources to verify the deletion
    final Request prometheusGetRequest = getFetchDataSourceRequest("delete_prometheus");
    ResponseException prometheusGetResponseException
        = Assert.assertThrows(ResponseException.class, () -> client().performRequest(prometheusGetRequest));
    Assert.assertEquals( 400, prometheusGetResponseException.getResponse().getStatusLine().getStatusCode());
    String prometheusGetResponseString = getResponseBody(prometheusGetResponseException.getResponse());
    JsonObject errorMessage = new Gson().fromJson(prometheusGetResponseString, JsonObject.class);
    Assert.assertEquals("DataSource with name: delete_prometheus doesn't exist.",
        errorMessage.get("error").getAsJsonObject().get("details").getAsString());

  }

  @SneakyThrows
  @Test
  public void getAllDataSourceTest() {
//create datasource for deletion
    DataSourceMetadata createDSM =
        new DataSourceMetadata("get_all_prometheus", DataSourceType.PROMETHEUS,
            ImmutableList.of(), ImmutableMap.of("prometheus.uri", "https://localhost:9090"));
    Request createRequest = getCreateDataSourceRequest(createDSM);
    client().performRequest(createRequest);
    //Datasource is not immediately created. so introducing a sleep of 2s.
    Thread.sleep(2000);

    Request getRequest = getFetchDataSourceRequest(null);
    Response getResponse = client().performRequest(getRequest);
    Assert.assertEquals(200, getResponse.getStatusLine().getStatusCode());
    String getResponseString = getResponseBody(getResponse);
    Type listType = new TypeToken<ArrayList<DataSourceMetadata>>() {}.getType();
    List<DataSourceMetadata> dataSourceMetadataList =
        new Gson().fromJson(getResponseString, listType);
    Assert.assertTrue(
        dataSourceMetadataList.stream().anyMatch(ds -> ds.getName().equals("get_all_prometheus")));
  }


    private Request getCreateDataSourceRequest(DataSourceMetadata dataSourceMetadata) {
    Request request = new Request("POST", "/_plugins/_query/_datasources");
    request.setJsonEntity(new Gson().toJson(dataSourceMetadata));
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    return request;
  }

  private Request getUpdateDataSourceRequest(DataSourceMetadata dataSourceMetadata) {
    Request request = new Request("PUT", "/_plugins/_query/_datasources");
    request.setJsonEntity(new Gson().toJson(dataSourceMetadata));
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    return request;
  }

  private Request getFetchDataSourceRequest(String name) {
    Request request = new Request("GET", "/_plugins/_query/_datasources" + "/" + name);
    if (StringUtils.isEmpty(name)) {
      request = new Request("GET", "/_plugins/_query/_datasources");
    }
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    return request;
  }


  private Request getDeleteDataSourceRequest(String name) {
    Request request = new Request("DELETE", "/_plugins/_query/_datasources" + "/" + name);
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    return request;
  }

}
