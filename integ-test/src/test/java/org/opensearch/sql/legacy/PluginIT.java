/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.legacy;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.plugin.RestSqlSettingsAction.SETTINGS_API_ENDPOINT;
import static org.opensearch.sql.legacy.plugin.SqlSettings.SQL_DELETE_ENABLED;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.utils.StringUtils;

public class PluginIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    wipeAllClusterSettings();
  }

  @Test
  public void sqlEnableSettingsTest() throws IOException {
    loadIndex(Index.ACCOUNT);
    updateClusterSettings(new ClusterSetting(PERSISTENT, "opensearch.sql.enabled", "true"));
    String query = String
        .format(Locale.ROOT, "SELECT firstname FROM %s WHERE account_number=1", TEST_INDEX_ACCOUNT);
    JSONObject queryResult = executeQuery(query);
    assertThat(getHits(queryResult).length(), equalTo(1));

    updateClusterSettings(new ClusterSetting(PERSISTENT, "opensearch.sql.enabled", "false"));
    Response response = null;
    try {
      queryResult = executeQuery(query);
    } catch (ResponseException ex) {
      response = ex.getResponse();
    }

    queryResult = new JSONObject(TestUtils.getResponseBody(response));
    assertThat(queryResult.getInt("status"), equalTo(400));
    JSONObject error = queryResult.getJSONObject("error");
    assertThat(error.getString("reason"), equalTo("Invalid SQL query"));
    assertThat(error.getString("details"), equalTo(
        "Either opensearch.sql.enabled or rest.action.multi.allow_explicit_index setting is false"));
    assertThat(error.getString("type"), equalTo("SQLFeatureDisabledException"));
    wipeAllClusterSettings();
  }

  @Test
  public void sqlDeleteSettingsTest() throws IOException {
    updateClusterSettings(new ClusterSetting(PERSISTENT, SQL_DELETE_ENABLED, "false"));

    String deleteQuery = StringUtils.format("DELETE FROM %s", TestsConstants.TEST_INDEX_ACCOUNT);
    final ResponseException exception =
        expectThrows(ResponseException.class, () -> executeQuery(deleteQuery));
    JSONObject actual = new JSONObject(TestUtils.getResponseBody(exception.getResponse()));
    JSONObject expected =
        new JSONObject(
            "{\n"
                + "  \"error\": {\n"
                + "    \"reason\": \"Invalid SQL query\",\n"
                + "    \"details\": \"DELETE clause is disabled by default and will be deprecated. Using "
                + "the opensearch.sql.delete.enabled setting to enable it\",\n"
                + "    \"type\": \"SQLFeatureDisabledException\"\n"
                + "  },\n"
                + "  \"status\": 400\n"
                + "}");
    assertTrue(actual.toString(), actual.similar(expected));

    wipeAllClusterSettings();
  }

  @Test
  public void sqlTransientOnlySettingTest() throws IOException {
    // (1) compact form
    String settings = "{" +
        "  \"transient\": {" +
        "    \"opensearch.sql.metrics.rollinginterval\": \"80\"" +
        "  }" +
        "}";
    JSONObject actual = updateViaSQLSettingsAPI(settings);
    JSONObject expected = new JSONObject("{" +
        "  \"acknowledged\" : true," +
        "  \"persistent\" : { }," +
        "  \"transient\" : {" +
        "    \"opensearch\" : {" +
        "      \"sql\" : {" +
        "        \"metrics\" : {" +
        "          \"rollinginterval\" : \"80\"" +
        "        }" +
        "      }" +
        "    }" +
        "  }" +
        "}");
    assertTrue(actual.similar(expected));

    // (2) partial expanded form
    settings = "{" +
        "  \"transient\": {" +
        "    \"opensearch\" : {" +
        "      \"sql\" : {" +
        "        \"metrics.rollinginterval\": \"75\"" +
        "      }" +
        "    }" +
        "  }" +
        "}";
    actual = updateViaSQLSettingsAPI(settings);
    expected = new JSONObject("{" +
        "  \"acknowledged\" : true," +
        "  \"persistent\" : { }," +
        "  \"transient\" : {" +
        "    \"opensearch\" : {" +
        "      \"sql\" : {" +
        "        \"metrics\" : {" +
        "          \"rollinginterval\" : \"75\"" +
        "        }" +
        "      }" +
        "    }" +
        "  }" +
        "}");
    assertTrue(actual.similar(expected));


    // (3) full expanded form
    settings = "{" +
        "  \"transient\": {" +
        "    \"opensearch\" : {" +
        "      \"sql\" : {" +
        "        \"metrics\": {" +
        "          \"rollinginterval\": \"65\"" +
        "        }" +
        "      }" +
        "    }" +
        "  }" +
        "}";
    actual = updateViaSQLSettingsAPI(settings);
    expected = new JSONObject("{" +
        "  \"acknowledged\" : true," +
        "  \"persistent\" : { }," +
        "  \"transient\" : {" +
        "    \"opensearch\" : {" +
        "      \"sql\" : {" +
        "        \"metrics\" : {" +
        "          \"rollinginterval\" : \"65\"" +
        "        }" +
        "      }" +
        "    }" +
        "  }" +
        "}");
    assertTrue(actual.similar(expected));
  }

  @Test
  public void sqlPersistentOnlySettingTest() throws IOException {
    // (1) compact form
    String settings = "{" +
        "  \"persistent\": {" +
        "    \"opensearch.sql.metrics.rollinginterval\": \"80\"" +
        "  }" +
        "}";
    JSONObject actual = updateViaSQLSettingsAPI(settings);
    JSONObject expected = new JSONObject("{" +
        "  \"acknowledged\" : true," +
        "  \"transient\" : { }," +
        "  \"persistent\" : {" +
        "    \"opensearch\" : {" +
        "      \"sql\" : {" +
        "        \"metrics\" : {" +
        "          \"rollinginterval\" : \"80\"" +
        "        }" +
        "      }" +
        "    }" +
        "  }" +
        "}");
    assertTrue(actual.similar(expected));

    // (2) partial expanded form
    settings = "{" +
        "  \"persistent\": {" +
        "    \"opensearch\" : {" +
        "      \"sql\" : {" +
        "        \"metrics.rollinginterval\": \"75\"" +
        "      }" +
        "    }" +
        "  }" +
        "}";
    actual = updateViaSQLSettingsAPI(settings);
    expected = new JSONObject("{" +
        "  \"acknowledged\" : true," +
        "  \"transient\" : { }," +
        "  \"persistent\" : {" +
        "    \"opensearch\" : {" +
        "      \"sql\" : {" +
        "        \"metrics\" : {" +
        "          \"rollinginterval\" : \"75\"" +
        "        }" +
        "      }" +
        "    }" +
        "  }" +
        "}");
    assertTrue(actual.similar(expected));


    // (3) full expanded form
    settings = "{" +
        "  \"persistent\": {" +
        "    \"opensearch\" : {" +
        "      \"sql\" : {" +
        "        \"metrics\": {" +
        "          \"rollinginterval\": \"65\"" +
        "        }" +
        "      }" +
        "    }" +
        "  }" +
        "}";
    actual = updateViaSQLSettingsAPI(settings);
    expected = new JSONObject("{" +
        "  \"acknowledged\" : true," +
        "  \"transient\" : { }," +
        "  \"persistent\" : {" +
        "    \"opensearch\" : {" +
        "      \"sql\" : {" +
        "        \"metrics\" : {" +
        "          \"rollinginterval\" : \"65\"" +
        "        }" +
        "      }" +
        "    }" +
        "  }" +
        "}");
    assertTrue(actual.similar(expected));
  }

  /**
   * Both transient and persistent settings are applied for same settings.
   * This is similar to _cluster/settings behavior
   */
  @Test
  public void sqlCombinedSettingTest() throws IOException {
    String settings = "{" +
        "  \"transient\": {" +
        "    \"opensearch.sql.metrics.rollingwindow\": \"3700\"" +
        "  }," +
        "  \"persistent\": {" +
        "    \"opensearch.sql.query.slowlog\" : \"2\"" +
        "  }" +
        "}";
    JSONObject actual = updateViaSQLSettingsAPI(settings);
    JSONObject expected = new JSONObject("{" +
        "  \"acknowledged\" : true," +
        "  \"persistent\" : {" +
        "    \"opensearch\" : {" +
        "      \"sql\" : {" +
        "        \"query\" : {" +
        "          \"slowlog\" : \"2\"" +
        "        }" +
        "      }" +
        "    }" +
        "  }," +
        "  \"transient\" : {" +
        "    \"opensearch\" : {" +
        "      \"sql\" : {" +
        "        \"metrics\" : {" +
        "          \"rollingwindow\" : \"3700\"" +
        "        }" +
        "      }" +
        "    }" +
        "  }" +
        "}");
    assertTrue(actual.similar(expected));
  }

  /**
   * Ignore all non opensearch.sql settings.
   * Only settings starting with opensearch.sql. are affected
   */
  @Test
  public void ignoreNonSQLSettingsTest() throws IOException {
    String settings = "{" +
        "  \"transient\": {" +
        "    \"opensearch.sql.metrics.rollingwindow\": \"3700\"," +
        "    \"opensearch.alerting.metrics.rollingwindow\": \"3700\"," +
        "    \"search.max_buckets\": \"10000\"," +
        "    \"search.max_keep_alive\": \"24h\"" +
        "  }," +
        "  \"persistent\": {" +
        "    \"opensearch.sql.query.slowlog\": \"2\"," +
        "    \"opensearch.alerting.metrics.rollingwindow\": \"3700\"," +
        "    \"thread_pool.analyze.queue_size\": \"16\"" +
        "  }" +
        "}";
    JSONObject actual = updateViaSQLSettingsAPI(settings);
    JSONObject expected = new JSONObject("{" +
        "  \"acknowledged\" : true," +
        "  \"persistent\" : {" +
        "    \"opensearch\" : {" +
        "      \"sql\" : {" +
        "        \"query\" : {" +
        "          \"slowlog\" : \"2\"" +
        "        }" +
        "      }" +
        "    }" +
        "  }," +
        "  \"transient\" : {" +
        "    \"opensearch\" : {" +
        "      \"sql\" : {" +
        "        \"metrics\" : {" +
        "          \"rollingwindow\" : \"3700\"" +
        "        }" +
        "      }" +
        "    }" +
        "  }" +
        "}");
    assertTrue(actual.similar(expected));
  }

  @Test
  public void ignoreNonTransientNonPersistentSettingsTest() throws IOException {
    String settings = "{" +
        "  \"transient\": {" +
        "    \"opensearch.sql.metrics.rollingwindow\": \"3700\"" +
        "  }," +
        "  \"persistent\": {" +
        "    \"opensearch.sql.query.slowlog\": \"2\"" +
        "  }," +
        "  \"hello\": {" +
        "    \"world\" : {" +
        "      \"name\" : \"John Doe\"" +
        "    }" +
        "  }" +
        "}";
    JSONObject actual = updateViaSQLSettingsAPI(settings);
    JSONObject expected = new JSONObject("{" +
        "  \"acknowledged\" : true," +
        "  \"persistent\" : {" +
        "    \"opensearch\" : {" +
        "      \"sql\" : {" +
        "        \"query\" : {" +
        "          \"slowlog\" : \"2\"" +
        "        }" +
        "      }" +
        "    }" +
        "  }," +
        "  \"transient\" : {" +
        "    \"opensearch\" : {" +
        "      \"sql\" : {" +
        "        \"metrics\" : {" +
        "          \"rollingwindow\" : \"3700\"" +
        "        }" +
        "      }" +
        "    }" +
        "  }" +
        "}");
    assertTrue(actual.similar(expected));
  }

  @Test
  public void sqlCombinedMixedSettingTest() throws IOException {
    String settings = "{" +
        "  \"transient\": {" +
        "    \"opensearch.sql.metrics.rollingwindow\": \"3700\"" +
        "  }," +
        "  \"persistent\": {" +
        "    \"opensearch\": {" +
        "      \"sql\": {" +
        "        \"query\": {" +
        "          \"slowlog\": \"1\"" +
        "        }" +
        "      }" +
        "    }" +
        "  }," +
        "  \"hello\": {" +
        "    \"world\": {" +
        "      \"city\": \"Seattle\"" +
        "    }" +
        "  }" +
        "}";
    JSONObject actual = updateViaSQLSettingsAPI(settings);
    JSONObject expected = new JSONObject("{" +
        "  \"acknowledged\" : true," +
        "  \"persistent\" : {" +
        "    \"opensearch\" : {" +
        "      \"sql\" : {" +
        "        \"query\" : {" +
        "          \"slowlog\" : \"1\"" +
        "        }" +
        "      }" +
        "    }" +
        "  }," +
        "  \"transient\" : {" +
        "    \"opensearch\" : {" +
        "      \"sql\" : {" +
        "        \"metrics\" : {" +
        "          \"rollingwindow\" : \"3700\"" +
        "        }" +
        "      }" +
        "    }" +
        "  }" +
        "}");
    assertTrue(actual.similar(expected));
  }

  @Test
  public void nonRegisteredSQLSettingsThrowException() throws IOException {
    String settings = "{" +
        "  \"transient\": {" +
        "    \"opensearch.sql.query.state.city\": \"Seattle\"" +
        "  }" +
        "}";

    JSONObject actual;
    Response response = null;
    try {
      actual = updateViaSQLSettingsAPI(settings);
    } catch (ResponseException ex) {
      response = ex.getResponse();
    }

    actual = new JSONObject(TestUtils.getResponseBody(response));
    assertThat(actual.getInt("status"), equalTo(400));
    assertThat(actual.query("/error/type"), equalTo("illegal_argument_exception"));
    assertThat(
        actual.query("/error/reason"),
        equalTo("transient setting [opensearch.sql.query.state.city], not recognized")
    );
  }

  protected static JSONObject updateViaSQLSettingsAPI(String body) throws IOException {
    Request request = new Request("PUT", SETTINGS_API_ENDPOINT);
    request.setJsonEntity(body);
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }
}
