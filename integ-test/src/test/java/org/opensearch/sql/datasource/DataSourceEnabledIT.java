/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;

import java.io.IOException;
import lombok.SneakyThrows;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class DataSourceEnabledIT extends PPLIntegTestCase {

  @After
  public void cleanUp() throws IOException {
    wipeAllClusterSettings();
  }

  @Test
  public void testAsyncQueryAPIFailureIfSettingIsDisabled() {
    setDataSourcesEnabled("transient", false);
    assertDataSourceCount(0);
    assertSelectFromDataSourceReturnsDoesNotExist();
    assertAsyncQueryApiDisabled();
  }

  @Test
  public void testDataSourceCreationWithDefaultSettings() {
    createOpenSearchDataSource();
    createIndex();
    assertDataSourceCount(1);
    assertSelectFromDataSourceReturnsSuccess();
    assertSelectFromDummyIndexInValidDataSourceDataSourceReturnsDoesNotExist();
    deleteSelfDataSourceCreated();
    deleteIndex();
  }

  @Test
  public void testAfterPreviousEnable() {
    setDataSourcesEnabled("transient", true);
    createOpenSearchDataSource();
    createIndex();
    assertDataSourceCount(1);
    assertSelectFromDataSourceReturnsSuccess();
    assertSelectFromDummyIndexInValidDataSourceDataSourceReturnsDoesNotExist();
    setDataSourcesEnabled("transient", false);
    assertDataSourceCount(0);
    assertSelectFromDataSourceReturnsDoesNotExist();
    assertAsyncQueryApiDisabled();
    setDataSourcesEnabled("transient", true);
    deleteSelfDataSourceCreated();
    deleteIndex();
  }

  @SneakyThrows
  private void assertSelectFromDataSourceReturnsDoesNotExist() {
    Request request = new Request("POST", "/_plugins/_sql");
    request.setJsonEntity(new JSONObject().put("query", "select * from self.myindex").toString());
    Response response = performRequest(request);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
    String result = getResponseBody(response);
    Assert.assertTrue(result.contains("IndexNotFoundException[no such index [self.myindex]]"));
  }

  @SneakyThrows
  private void assertSelectFromDummyIndexInValidDataSourceDataSourceReturnsDoesNotExist() {
    Request request = new Request("POST", "/_plugins/_sql");
    request.setJsonEntity(new JSONObject().put("query", "select * from self.dummy").toString());
    Response response = performRequest(request);
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
    String result = getResponseBody(response);
    // subtle difference in error messaging shows that it resolved self to a data source
    Assert.assertTrue(result.contains("IndexNotFoundException[no such index [dummy]]"));
  }

  @SneakyThrows
  private void assertSelectFromDataSourceReturnsSuccess() {
    Request request = new Request("POST", "/_plugins/_sql");
    request.setJsonEntity(new JSONObject().put("query", "select * from self.myindex").toString());
    Response response = performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    JSONObject result = new JSONObject(getResponseBody(response));
    Assert.assertTrue(result.has("datarows"));
    Assert.assertTrue(result.has("schema"));
    Assert.assertTrue(result.has("total"));
    Assert.assertTrue(result.has("size"));
    Assert.assertEquals(200, result.getNumber("status"));
  }

  private void createIndex() {
    Request request = new Request("PUT", "/myindex");
    Response response = performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  private void deleteIndex() {
    Request request = new Request("DELETE", "/myindex");
    Response response = performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  private void createOpenSearchDataSource() {
    Request request = new Request("POST", "/_plugins/_query/_datasources");
    request.setJsonEntity(
        new JSONObject().put("connector", "OPENSEARCH").put("name", "self").toString());
    Response response = performRequest(request);
    Assert.assertEquals(201, response.getStatusLine().getStatusCode());
  }

  @SneakyThrows
  private void assertAsyncQueryApiDisabled() {

    Request request = new Request("POST", "/_plugins/_async_query");

    request.setJsonEntity(
        new JSONObject()
            .put("query", "select * from self.myindex")
            .put("datasource", "self")
            .put("lang", "sql")
            .toString());

    Response response = performRequest(request);
    Assert.assertEquals(400, response.getStatusLine().getStatusCode());

    String expectBodyToContain = "plugins.query.datasources.enabled setting is false";
    Assert.assertTrue(getResponseBody(response).contains(expectBodyToContain));
  }

  @SneakyThrows
  private void assertDataSourceCount(int expected) {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(new JSONObject().put("query", "show datasources").toString());
    Response response = performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    JSONObject jsonBody = new JSONObject(getResponseBody(response));
    Assert.assertEquals(expected, jsonBody.getNumber("size"));
    Assert.assertEquals(expected, jsonBody.getNumber("total"));
    Assert.assertEquals(expected, jsonBody.getJSONArray("datarows").length());
  }

  @SneakyThrows
  private Response performRequest(Request request) {
    try {
      return client().performRequest(request);
    } catch (ResponseException e) {
      return e.getResponse();
    }
  }

  @SneakyThrows
  private void deleteSelfDataSourceCreated() {
    Request deleteRequest = getDeleteDataSourceRequest("self");
    Response deleteResponse = client().performRequest(deleteRequest);
    Assert.assertEquals(204, deleteResponse.getStatusLine().getStatusCode());
  }
}
