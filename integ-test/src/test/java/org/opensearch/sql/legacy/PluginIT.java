/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.plugin.rest.RestQuerySettingsAction;

public class PluginIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    wipeAllClusterSettings();
  }

  @Test
  public void sqlEnableSettingsTest() throws IOException {
    loadIndex(Index.ACCOUNT);
    updateClusterSettings(new ClusterSetting(PERSISTENT, "plugins.sql.enabled", "true"));
    String query =
        String.format(
            Locale.ROOT, "SELECT firstname FROM %s WHERE account_number=1", TEST_INDEX_ACCOUNT);
    JSONObject queryResult = executeQuery(query);
    assertThat(getHits(queryResult).length(), equalTo(1));

    updateClusterSettings(new ClusterSetting(PERSISTENT, "plugins.sql.enabled", "false"));
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
    assertThat(
        error.getString("details"),
        equalTo(
            "Either plugins.sql.enabled or rest.action.multi.allow_explicit_index setting is"
                + " false"));
    assertThat(error.getString("type"), equalTo("SQLFeatureDisabledException"));
    wipeAllClusterSettings();
  }

  @Test
  public void sqlTransientOnlySettingTest() throws IOException {
    // (1) compact form
    String settings =
        "{"
            + "  \"transient\": {"
            + "    \"plugins.query.metrics.rolling_interval\": \"80\""
            + "  }"
            + "}";
    JSONObject actual = updateViaSQLSettingsAPI(settings);
    JSONObject expected =
        new JSONObject(
            "{"
                + "  \"acknowledged\" : true,"
                + "  \"persistent\" : { },"
                + "  \"transient\" : {"
                + "    \"plugins\" : {"
                + "      \"query\" : {"
                + "        \"metrics\" : {"
                + "          \"rolling_interval\" : \"80\""
                + "        }"
                + "      }"
                + "    }"
                + "  }"
                + "}");
    assertTrue(actual.similar(expected));

    // (2) partial expanded form
    settings =
        "{"
            + "  \"transient\": {"
            + "    \"plugins\" : {"
            + "      \"query\" : {"
            + "        \"metrics.rolling_interval\": \"75\""
            + "      }"
            + "    }"
            + "  }"
            + "}";
    actual = updateViaSQLSettingsAPI(settings);
    expected =
        new JSONObject(
            "{"
                + "  \"acknowledged\" : true,"
                + "  \"persistent\" : { },"
                + "  \"transient\" : {"
                + "    \"plugins\" : {"
                + "      \"query\" : {"
                + "        \"metrics\" : {"
                + "          \"rolling_interval\" : \"75\""
                + "        }"
                + "      }"
                + "    }"
                + "  }"
                + "}");
    assertTrue(actual.similar(expected));

    // (3) full expanded form
    settings =
        "{"
            + "  \"transient\": {"
            + "    \"plugins\" : {"
            + "      \"query\" : {"
            + "        \"metrics\": {"
            + "          \"rolling_interval\": \"65\""
            + "        }"
            + "      }"
            + "    }"
            + "  }"
            + "}";
    actual = updateViaSQLSettingsAPI(settings);
    expected =
        new JSONObject(
            "{"
                + "  \"acknowledged\" : true,"
                + "  \"persistent\" : { },"
                + "  \"transient\" : {"
                + "    \"plugins\" : {"
                + "      \"query\" : {"
                + "        \"metrics\" : {"
                + "          \"rolling_interval\" : \"65\""
                + "        }"
                + "      }"
                + "    }"
                + "  }"
                + "}");
    assertTrue(actual.similar(expected));
  }

  @Test
  public void sqlPersistentOnlySettingTest() throws IOException {
    // (1) compact form
    String settings =
        "{"
            + "  \"persistent\": {"
            + "    \"plugins.query.metrics.rolling_interval\": \"80\""
            + "  }"
            + "}";
    JSONObject actual = updateViaSQLSettingsAPI(settings);
    JSONObject expected =
        new JSONObject(
            "{"
                + "  \"acknowledged\" : true,"
                + "  \"transient\" : { },"
                + "  \"persistent\" : {"
                + "    \"plugins\" : {"
                + "      \"query\" : {"
                + "        \"metrics\" : {"
                + "          \"rolling_interval\" : \"80\""
                + "        }"
                + "      }"
                + "    }"
                + "  }"
                + "}");
    assertTrue(actual.similar(expected));

    // (2) partial expanded form
    settings =
        "{"
            + "  \"persistent\": {"
            + "    \"plugins\" : {"
            + "      \"query\" : {"
            + "        \"metrics.rolling_interval\": \"75\""
            + "      }"
            + "    }"
            + "  }"
            + "}";
    actual = updateViaSQLSettingsAPI(settings);
    expected =
        new JSONObject(
            "{"
                + "  \"acknowledged\" : true,"
                + "  \"transient\" : { },"
                + "  \"persistent\" : {"
                + "    \"plugins\" : {"
                + "      \"query\" : {"
                + "        \"metrics\" : {"
                + "          \"rolling_interval\" : \"75\""
                + "        }"
                + "      }"
                + "    }"
                + "  }"
                + "}");
    assertTrue(actual.similar(expected));

    // (3) full expanded form
    settings =
        "{"
            + "  \"persistent\": {"
            + "    \"plugins\" : {"
            + "      \"query\" : {"
            + "        \"metrics\": {"
            + "          \"rolling_interval\": \"65\""
            + "        }"
            + "      }"
            + "    }"
            + "  }"
            + "}";
    actual = updateViaSQLSettingsAPI(settings);
    expected =
        new JSONObject(
            "{"
                + "  \"acknowledged\" : true,"
                + "  \"transient\" : { },"
                + "  \"persistent\" : {"
                + "    \"plugins\" : {"
                + "      \"query\" : {"
                + "        \"metrics\" : {"
                + "          \"rolling_interval\" : \"65\""
                + "        }"
                + "      }"
                + "    }"
                + "  }"
                + "}");
    assertTrue(actual.similar(expected));
  }

  /**
   * Both transient and persistent settings are applied for same settings. This is similar to
   * _cluster/settings behavior
   */
  @Test
  public void sqlCombinedSettingTest() throws IOException {
    String settings =
        "{"
            + "  \"transient\": {"
            + "    \"plugins.query.metrics.rolling_window\": \"3700\""
            + "  },"
            + "  \"persistent\": {"
            + "    \"plugins.sql.slowlog\" : \"2\""
            + "  }"
            + "}";
    JSONObject actual = updateViaSQLSettingsAPI(settings);
    JSONObject expected =
        new JSONObject(
            "{"
                + "  \"acknowledged\" : true,"
                + "  \"persistent\" : {"
                + "    \"plugins\" : {"
                + "      \"sql\" : {"
                + "        \"slowlog\" : \"2\""
                + "      }"
                + "    }"
                + "  },"
                + "  \"transient\" : {"
                + "    \"plugins\" : {"
                + "      \"query\" : {"
                + "        \"metrics\" : {"
                + "          \"rolling_window\" : \"3700\""
                + "        }"
                + "      }"
                + "    }"
                + "  }"
                + "}");
    assertTrue(actual.similar(expected));
  }

  /** Ignore all non plugins.sql settings. Only settings starting with plugins.sql. are affected */
  @Test
  public void ignoreNonSQLSettingsTest() throws IOException {
    String settings =
        "{"
            + "  \"transient\": {"
            + "    \"plugins.query.metrics.rolling_window\": \"3700\","
            + "    \"plugins.alerting.metrics.rolling_window\": \"3700\","
            + "    \"search.max_buckets\": \"10000\","
            + "    \"search.max_keep_alive\": \"24h\""
            + "  },"
            + "  \"persistent\": {"
            + "    \"plugins.sql.slowlog\": \"2\","
            + "    \"plugins.alerting.metrics.rolling_window\": \"3700\","
            + "    \"thread_pool.analyze.queue_size\": \"16\""
            + "  }"
            + "}";
    JSONObject actual = updateViaSQLSettingsAPI(settings);
    JSONObject expected =
        new JSONObject(
            "{"
                + "  \"acknowledged\" : true,"
                + "  \"persistent\" : {"
                + "    \"plugins\" : {"
                + "      \"sql\" : {"
                + "        \"slowlog\" : \"2\""
                + "      }"
                + "    }"
                + "  },"
                + "  \"transient\" : {"
                + "    \"plugins\" : {"
                + "      \"query\" : {"
                + "        \"metrics\" : {"
                + "          \"rolling_window\" : \"3700\""
                + "        }"
                + "      }"
                + "    }"
                + "  }"
                + "}");
    assertTrue(actual.similar(expected));
  }

  @Test
  public void ignoreNonTransientNonPersistentSettingsTest() throws IOException {
    String settings =
        "{"
            + "  \"transient\": {"
            + "    \"plugins.query.metrics.rolling_window\": \"3700\""
            + "  },"
            + "  \"persistent\": {"
            + "    \"plugins.sql.slowlog\": \"2\""
            + "  },"
            + "  \"hello\": {"
            + "    \"world\" : {"
            + "      \"name\" : \"John Doe\""
            + "    }"
            + "  }"
            + "}";
    JSONObject actual = updateViaSQLSettingsAPI(settings);
    JSONObject expected =
        new JSONObject(
            "{"
                + "  \"acknowledged\" : true,"
                + "  \"persistent\" : {"
                + "    \"plugins\" : {"
                + "      \"sql\" : {"
                + "        \"slowlog\" : \"2\""
                + "      }"
                + "    }"
                + "  },"
                + "  \"transient\" : {"
                + "    \"plugins\" : {"
                + "      \"query\" : {"
                + "        \"metrics\" : {"
                + "          \"rolling_window\" : \"3700\""
                + "        }"
                + "      }"
                + "    }"
                + "  }"
                + "}");
    assertTrue(actual.similar(expected));
  }

  @Test
  public void sqlCombinedMixedSettingTest() throws IOException {
    String settings =
        "{"
            + "  \"transient\": {"
            + "    \"plugins.query.metrics.rolling_window\": \"3700\""
            + "  },"
            + "  \"persistent\": {"
            + "    \"plugins\": {"
            + "      \"sql\": {"
            + "        \"slowlog\": \"1\""
            + "      }"
            + "    }"
            + "  },"
            + "  \"hello\": {"
            + "    \"world\": {"
            + "      \"city\": \"Seattle\""
            + "    }"
            + "  }"
            + "}";
    JSONObject actual = updateViaSQLSettingsAPI(settings);
    JSONObject expected =
        new JSONObject(
            "{"
                + "  \"acknowledged\" : true,"
                + "  \"persistent\" : {"
                + "    \"plugins\" : {"
                + "      \"sql\" : {"
                + "        \"slowlog\" : \"1\""
                + "      }"
                + "    }"
                + "  },"
                + "  \"transient\" : {"
                + "    \"plugins\" : {"
                + "      \"query\" : {"
                + "        \"metrics\" : {"
                + "          \"rolling_window\" : \"3700\""
                + "        }"
                + "      }"
                + "    }"
                + "  }"
                + "}");
    assertTrue(actual.similar(expected));
  }

  @Test
  public void nonRegisteredSQLSettingsThrowException() throws IOException {
    String settings =
        "{"
            + "  \"transient\": {"
            + "    \"plugins.sql.query.state.city\": \"Seattle\""
            + "  }"
            + "}";

    JSONObject actual;
    Response response = null;
    try {
      actual = updateViaSQLSettingsAPI(settings);
    } catch (ResponseException ex) {
      response = ex.getResponse();
    }

    actual = new JSONObject(TestUtils.getResponseBody(response));
    assertThat(actual.getInt("status"), equalTo(400));
    assertThat(actual.query("/error/type"), equalTo("settings_exception"));
    assertThat(
        actual.query("/error/reason"),
        equalTo("transient setting [plugins.sql.query.state.city], not recognized"));
  }

  protected static JSONObject updateViaSQLSettingsAPI(String body) throws IOException {
    Request request = new Request("PUT", RestQuerySettingsAction.SETTINGS_API_ENDPOINT);
    request.setJsonEntity(body);
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }
}
