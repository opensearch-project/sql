/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.ppl;

import static org.opensearch.sql.plugin.rest.RestQuerySettingsAction.SETTINGS_API_ENDPOINT;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;

/** For backward compatibility, check if legacy API endpoints are accessible. */
public class LegacyAPICompatibilityIT extends PPLIntegTestCase {
  public static final String LEGACY_PPL_API_ENDPOINT = "/_opendistro/_ppl";
  public static final String LEGACY_PPL_EXPLAIN_API_ENDPOINT = "/_opendistro/_ppl/_explain";
  public static final String LEGACY_PPL_SETTINGS_API_ENDPOINT = "/_opendistro/_ppl/settings";
  public static final String LEGACY_PPL_STATS_API_ENDPOINT = "/_opendistro/_ppl/stats";

  @Override
  public void init() throws IOException {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void query() {
    String query = "source=opensearch-sql_test_index_account | where age > 30";
    Request request = buildRequest(query, LEGACY_PPL_API_ENDPOINT);
    assertBadRequest(() -> client().performRequest(request));
  }

  @Test
  public void explain() {
    String query = "source=opensearch-sql_test_index_account | where age > 30";
    Request request = buildRequest(query, LEGACY_PPL_EXPLAIN_API_ENDPOINT);
    assertBadRequest(() -> client().performRequest(request));
  }

  @Test
  public void stats() {
    Request request = new Request("GET", LEGACY_PPL_STATS_API_ENDPOINT);
    assertBadRequest(() -> client().performRequest(request));
  }

  @Test
  public void legacyPPLSettingNewEndpoint() {
    String requestBody =
        "{"
            + "  \"persistent\": {"
            + "    \"opendistro.ppl.query.memory_limit\": \"80%\""
            + "  }"
            + "}";
    assertBadRequest(() -> updateSetting(SETTINGS_API_ENDPOINT, requestBody));
  }

  @Test
  public void newPPLSettingsLegacyEndpoint() {
    String requestBody =
        "{"
            + "  \"persistent\": {"
            + "    \"plugins.ppl.query.memory_limit\": \"90%\""
            + "  }"
            + "}";
    assertBadRequest(() -> updateSetting(LEGACY_PPL_SETTINGS_API_ENDPOINT, requestBody));
  }

  @Test
  public void newPPLSettingNewEndpoint() throws IOException {
    String requestBody =
        "{" + "  \"persistent\": {" + "    \"plugins.query.size_limit\": \"100\"" + "  }" + "}";
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
