/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.Locale;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.util.TestUtils;

public class PPLPluginIT extends PPLIntegTestCase {
  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  private static final String PERSISTENT = "persistent";

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
  }

  @Test
  public void testQueryEndpointShouldOK() throws IOException {
    Request request = new Request("PUT", "/a/_doc/1?refresh=true");
    request.setJsonEntity("{\"name\": \"hello\"}");
    client().performRequest(request);

    JSONObject response = executeQuery("search source=a");
    verifySchema(response, schema("name", null, "string"));
    verifyDataRows(response, rows("hello"));
  }

  @Test
  public void testQueryEndpointShouldFail() throws IOException {
    exceptionRule.expect(ResponseException.class);
    exceptionRule.expect(hasProperty("response", statusCode(400)));

    client().performRequest(makePPLRequest("search invalid"));
  }

  @Test
  public void testQueryEndpointShouldFailWithNonExistIndex() throws IOException {
    exceptionRule.expect(ResponseException.class);
    exceptionRule.expect(hasProperty("response", statusCode(404)));

    client().performRequest(makePPLRequest("search source=non_exist_index"));
  }

  @Test
  public void sqlEnableSettingsTest() throws IOException {
    String query =
        String.format("search source=%s firstname='Hattie' | fields firstname", TEST_INDEX_BANK);
    // enable by default
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("Hattie"));

    // disable
    updateClusterSettings(new ClusterSetting(PERSISTENT, "plugins.ppl.enabled", "false"));
    Response response = null;
    try {
      result = executeQuery(query);
    } catch (ResponseException ex) {
      response = ex.getResponse();
    }

    result = new JSONObject(TestUtils.getResponseBody(response));
    assertThat(result.getInt("status"), equalTo(400));
    JSONObject error = result.getJSONObject("error");
    assertThat(error.getString("reason"), equalTo("Invalid Query"));
    assertThat(
        error.getString("details"),
        equalTo(
            "Either plugins.ppl.enabled or rest.action.multi.allow_explicit_index setting is "
                + "false"));
    assertThat(error.getString("type"), equalTo("IllegalAccessException"));

    // reset the setting
    updateClusterSettings(new ClusterSetting(PERSISTENT, "plugins.ppl.enabled", null));
  }

  protected Request makePPLRequest(String query) {
    Request post = new Request("POST", "/_plugins/_ppl");
    post.setJsonEntity(String.format(Locale.ROOT, "{\n" + "  \"query\": \"%s\"\n" + "}", query));
    return post;
  }

  private TypeSafeMatcher<Response> statusCode(int statusCode) {
    return new TypeSafeMatcher<Response>() {
      @Override
      public void describeTo(Description description) {
        description.appendText(String.format(Locale.ROOT, "statusCode=%d", statusCode));
      }

      @Override
      protected boolean matchesSafely(Response resp) {
        return resp.getStatusLine().getStatusCode() == statusCode;
      }
    };
  }
}
