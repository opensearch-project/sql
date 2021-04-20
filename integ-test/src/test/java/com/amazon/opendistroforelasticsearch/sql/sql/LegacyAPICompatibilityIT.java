package com.amazon.opendistroforelasticsearch.sql.sql;

import static com.amazon.opendistroforelasticsearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static com.amazon.opendistroforelasticsearch.sql.legacy.plugin.RestSqlAction.LEGACY_EXPLAIN_API_ENDPOINT;
import static com.amazon.opendistroforelasticsearch.sql.legacy.plugin.RestSqlAction.LEGACY_QUERY_API_ENDPOINT;
import static com.amazon.opendistroforelasticsearch.sql.legacy.plugin.RestSqlSettingsAction.LEGACY_SETTINGS_API_ENDPOINT;
import static com.amazon.opendistroforelasticsearch.sql.legacy.plugin.RestSqlStatsAction.LEGACY_STATS_API_ENDPOINT;
import static org.hamcrest.Matchers.equalTo;

import com.amazon.opendistroforelasticsearch.sql.legacy.SQLIntegTestCase;
import com.amazon.opendistroforelasticsearch.sql.legacy.utils.StringUtils;
import java.io.IOException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;

/**
 * For backward compatibility, check if legacy API endpoints are accessible.
 */
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
    updateClusterSettings(
        new ClusterSetting("transient", "opendistro.sql.cursor.enabled", "true"));

    try {
      String selectQuery = StringUtils.format(
          "SELECT firstname, state FROM %s WHERE balance > 100 and age < 40", TEST_INDEX_ACCOUNT);
      JSONObject result = new JSONObject(executeFetchQuery(selectQuery, 50, "jdbc"));
      JSONObject closeResp = executeCursorCloseQuery(result.getString("cursor"));
      assertThat(closeResp.getBoolean("succeeded"), equalTo(true));
    } finally {
      updateClusterSettings(
          new ClusterSetting("transient", "opendistro.sql.cursor.enabled", "false"));
    }
  }

  @Test
  public void stats() throws IOException {
    Request request = new Request("GET", LEGACY_STATS_API_ENDPOINT);
    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void updateSettings() throws IOException {
    String requestBody = "{" +
        "  \"persistent\": {" +
        "    \"opendistro.sql.metrics.rollinginterval\": \"80\"" +
        "  }" +
        "}";
    Request request = new Request("PUT", LEGACY_SETTINGS_API_ENDPOINT);
    request.setJsonEntity(requestBody);

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

}
