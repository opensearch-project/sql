/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.plugin.rest.RestQuerySettingsAction.SETTINGS_API_ENDPOINT;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.utils.StringUtils;

/** For backward compatibility, check if legacy API endpoints are accessible. */
public class LegacyAPICompatibilityIT extends SQLIntegTestCase {

  public static final String LEGACY_QUERY_API_ENDPOINT = "/_opendistro/_sql";
  public static final String LEGACY_CURSOR_CLOSE_ENDPOINT = LEGACY_QUERY_API_ENDPOINT + "/close";
  public static final String LEGACY_EXPLAIN_API_ENDPOINT = LEGACY_QUERY_API_ENDPOINT + "/_explain";
  public static final String LEGACY_SQL_SETTINGS_API_ENDPOINT =
      LEGACY_QUERY_API_ENDPOINT + "/settings";
  public static final String LEGACY_STATS_API_ENDPOINT = LEGACY_QUERY_API_ENDPOINT + "/stats";

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void query() {
    String requestBody = makeRequest("SELECT 1");
    Request request = new Request("POST", LEGACY_QUERY_API_ENDPOINT);
    request.setJsonEntity(requestBody);
    assertBadRequest(() -> client().performRequest(request));
  }

  @Test
  public void explain() {
    String requestBody = makeRequest("SELECT 1");
    Request request = new Request("POST", LEGACY_EXPLAIN_API_ENDPOINT);
    request.setJsonEntity(requestBody);
    assertBadRequest(() -> client().performRequest(request));
  }

  @Test
  public void closeCursor() throws IOException {
    String sql =
        StringUtils.format("SELECT firstname FROM %s WHERE balance > 100", TEST_INDEX_ACCOUNT);
    JSONObject result = new JSONObject(executeFetchQuery(sql, 50, "jdbc"));

    Request request = new Request("POST", LEGACY_CURSOR_CLOSE_ENDPOINT);
    request.setJsonEntity(makeCursorRequest(result.getString("cursor")));
    request.setOptions(buildJsonOption());
    assertBadRequest(() -> client().performRequest(request));
  }

  @Test
  public void stats() {
    Request request = new Request("GET", LEGACY_STATS_API_ENDPOINT);
    assertBadRequest(() -> client().performRequest(request));
  }

  @Test
  public void legacySettingNewEndpoint() {
    String requestBody =
        "{" + "  \"persistent\": {" + "    \"opendistro.query.size_limit\": \"100\"" + "  }" + "}";
    assertBadRequest(() -> updateSetting(SETTINGS_API_ENDPOINT, requestBody));
  }

  @Test
  public void newSettingsLegacyEndpoint() throws IOException {
    String requestBody =
        "{" + "  \"persistent\": {" + "    \"plugins.sql.slowlog\": \"10\"" + "  }" + "}";
    assertBadRequest(() -> updateSetting(LEGACY_SQL_SETTINGS_API_ENDPOINT, requestBody));
  }

  @Test
  public void newSettingNewEndpoint() throws IOException {
    String requestBody =
        "{"
            + "  \"persistent\": {"
            + "    \"plugins.query.metrics.rolling_interval\": \"80\""
            + "  }"
            + "}";
    Response response = updateSetting(SETTINGS_API_ENDPOINT, requestBody);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  private Response updateSetting(String endpoint, String requestBody) throws IOException {
    Request request = new Request("PUT", endpoint);
    request.setJsonEntity(requestBody);
    request.setOptions(buildJsonOption());
    return client().performRequest(request);
  }

  private RequestOptions.Builder buildJsonOption() {
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    return restOptionsBuilder;
  }
}
