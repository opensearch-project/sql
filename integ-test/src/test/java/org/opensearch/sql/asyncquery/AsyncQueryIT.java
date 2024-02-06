/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.asyncquery;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.sql.legacy.TestUtils.getResponseBody;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.opensearch.sql.util.TestUtils;

public class AsyncQueryIT extends PPLIntegTestCase {

  public static final String ASYNC_QUERY_ACTION_URL = "/_plugins/_async_query";

  @Test
  public void asyncQueryEnabledSettingsTest() throws IOException {
    String setting = "plugins.query.executionengine.async_query.enabled";
    // disable
    updateClusterSettings(new ClusterSetting(PERSISTENT, setting, "false"));

    String query = "select 1";
    Response response = null;
    try {
      executeAsyncQueryToString (query);
    } catch (ResponseException ex) {
      response = ex.getResponse();
    }

    JSONObject result = new JSONObject(TestUtils.getResponseBody(response));
    assertThat(result.getInt("status"), equalTo(400));
    JSONObject error = result.getJSONObject("error");
    assertThat(error.getString("reason"), equalTo("Invalid Request"));
    assertThat(
        error.getString("details"),
        equalTo("plugins.query.executionengine.async_query.enabled setting is false"));
    assertThat(error.getString("type"), equalTo("IllegalAccessException"));

    // reset the setting
    updateClusterSettings(new ClusterSetting(PERSISTENT, setting, null));
  }

  protected String executeAsyncQueryToString(String query) throws IOException {
    Response response = client().performRequest(buildAsyncRequest(query, ASYNC_QUERY_ACTION_URL));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return getResponseBody(response, true);
  }

  protected Request buildAsyncRequest(String query, String endpoint) {
    Request request = new Request("POST", endpoint);
    request.setJsonEntity(String.format(Locale.ROOT, "{\n" + "  \"query\": \"%s\"\n" + "}", query));
    request.setJsonEntity(String.format(Locale.ROOT, "{\n" +
        "  \"datasource\": \"mys3\",\n" +
        "  \"lang\": \"sql\",\n" +
        "  \"query\": \"%s\"\n" +
        "}", query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    return request;
  }
}
