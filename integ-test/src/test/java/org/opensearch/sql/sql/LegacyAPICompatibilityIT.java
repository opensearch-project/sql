/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.LEGACY_CURSOR_CLOSE_ENDPOINT;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.LEGACY_EXPLAIN_API_ENDPOINT;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.LEGACY_QUERY_API_ENDPOINT;
import static org.opensearch.sql.legacy.plugin.RestSqlStatsAction.LEGACY_STATS_API_ENDPOINT;
import static org.opensearch.sql.plugin.rest.RestQuerySettingsAction.LEGACY_SQL_SETTINGS_API_ENDPOINT;
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

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void query() throws IOException {
    String requestBody = makeRequest("SELECT 1");
    Request request = new Request("POST", LEGACY_QUERY_API_ENDPOINT);
    request.setJsonEntity(requestBody);

    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void explain() throws IOException {
    String requestBody = makeRequest("SELECT 1");
    Request request = new Request("POST", LEGACY_EXPLAIN_API_ENDPOINT);
    request.setJsonEntity(requestBody);

    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void closeCursor() throws IOException {
    String sql =
        StringUtils.format("SELECT firstname FROM %s WHERE balance > 100", TEST_INDEX_ACCOUNT);
    JSONObject result = new JSONObject(executeFetchQuery(sql, 50, "jdbc"));

    Request request = new Request("POST", LEGACY_CURSOR_CLOSE_ENDPOINT);
    request.setJsonEntity(makeCursorRequest(result.getString("cursor")));
    request.setOptions(buildJsonOption());
    JSONObject response = new JSONObject(executeRequest(request));
    assertThat(response.getBoolean("succeeded"), equalTo(true));
  }

  @Test
  public void stats() throws IOException {
    Request request = new Request("GET", LEGACY_STATS_API_ENDPOINT);
    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void legacySettingNewEndpoint() throws IOException {
    String requestBody =
        "{" + "  \"persistent\": {" + "    \"opendistro.query.size_limit\": \"100\"" + "  }" + "}";
    Response response = updateSetting(SETTINGS_API_ENDPOINT, requestBody);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void newSettingsLegacyEndpoint() throws IOException {
    String requestBody =
        "{" + "  \"persistent\": {" + "    \"plugins.sql.slowlog\": \"10\"" + "  }" + "}";
    Response response = updateSetting(LEGACY_SQL_SETTINGS_API_ENDPOINT, requestBody);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
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
