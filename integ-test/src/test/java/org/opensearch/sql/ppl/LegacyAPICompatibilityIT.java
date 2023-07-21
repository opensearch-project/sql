/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl;

import static org.opensearch.sql.plugin.rest.RestPPLQueryAction.LEGACY_EXPLAIN_API_ENDPOINT;
import static org.opensearch.sql.plugin.rest.RestPPLQueryAction.LEGACY_QUERY_API_ENDPOINT;
import static org.opensearch.sql.plugin.rest.RestPPLStatsAction.PPL_LEGACY_STATS_API_ENDPOINT;
import static org.opensearch.sql.plugin.rest.RestQuerySettingsAction.SETTINGS_API_ENDPOINT;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;

/**
 * For backward compatibility, check if legacy API endpoints are accessible.
 */
public class LegacyAPICompatibilityIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void query() throws IOException {
    String query = "source=opensearch-sql_test_index_account | where age > 30";
    Request request = buildRequest(query, LEGACY_QUERY_API_ENDPOINT);
    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void explain() throws IOException {
    String query = "source=opensearch-sql_test_index_account | where age > 30";
    Request request = buildRequest(query, LEGACY_EXPLAIN_API_ENDPOINT);
    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void stats() throws IOException {
    Request request = new Request("GET", PPL_LEGACY_STATS_API_ENDPOINT);
    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void legacySettingNewEndpoint() throws IOException {
    String requestBody = "{"
        + "  \"persistent\": {"
        + "    \"opendistro.ppl.query.memory_limit\": \"80%\""
        + "  }"
        + "}";
    Response response = updateSetting(SETTINGS_API_ENDPOINT, requestBody);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void newSettingNewEndpoint() throws IOException {
    String requestBody = "{"
        + "  \"persistent\": {"
        + "    \"plugins.query.size_limit\": \"100\""
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
