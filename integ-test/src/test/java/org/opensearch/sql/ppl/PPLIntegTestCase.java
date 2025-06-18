/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.plugin.rest.RestPPLQueryAction.EXPLAIN_API_ENDPOINT;
import static org.opensearch.sql.plugin.rest.RestPPLQueryAction.QUERY_API_ENDPOINT;

import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/** OpenSearch Rest integration test base for PPL testing. */
public abstract class PPLIntegTestCase extends SQLIntegTestCase {
  private static final Logger LOG = LogManager.getLogger();

  @Override
  protected void init() throws Exception {
    super.init();
    updatePushdownSettings();
  }

  protected JSONObject executeQuery(String query) throws IOException {
    return jsonify(executeQueryToString(query));
  }

  protected String executeQueryToString(String query) throws IOException {
    Response response = client().performRequest(buildRequest(query, QUERY_API_ENDPOINT));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return getResponseBody(response, true);
  }

  protected String explainQueryToString(String query) throws IOException {
    Response response = client().performRequest(buildRequest(query, EXPLAIN_API_ENDPOINT));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String responseBody = getResponseBody(response, true);
    return responseBody.replace("\\r\\n", "\\n");
  }

  protected String executeCsvQuery(String query, boolean sanitize) throws IOException {
    Request request =
        buildRequest(
            query,
            QUERY_API_ENDPOINT + String.format(Locale.ROOT, "?format=csv&sanitize=%b", sanitize));
    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return getResponseBody(response, true);
  }

  protected String executeCsvQuery(String query) throws IOException {
    return executeCsvQuery(query, true);
  }

  protected void failWithMessage(String query, String message) {
    try {
      client().performRequest(buildRequest(query, QUERY_API_ENDPOINT));
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains(message));
    }
  }

  protected Request buildRequest(String query, String endpoint) {
    Request request = new Request("POST", endpoint);
    request.setJsonEntity(String.format(Locale.ROOT, "{\n" + "  \"query\": \"%s\"\n" + "}", query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    return request;
  }

  protected static JSONObject updateClusterSettings(ClusterSetting setting) throws IOException {
    Request request = new Request("PUT", "/_cluster/settings");
    String persistentSetting =
        String.format(
            Locale.ROOT, "{\"%s\": {\"%s\": %s}}", setting.type, setting.name, setting.value);
    request.setJsonEntity(persistentSetting);
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    return new JSONObject(executeRequest(request));
  }

  protected static class ClusterSetting {
    private final String type;
    private final String name;
    private final String value;

    public ClusterSetting(String type, String name, String value) {
      this.type = type;
      this.name = name;
      this.value = (value == null) ? "null" : ("\"" + value + "\"");
    }

    SQLIntegTestCase.ClusterSetting nullify() {
      return new SQLIntegTestCase.ClusterSetting(type, name, null);
    }

    @Override
    public String toString() {
      return "ClusterSetting{"
          + "type='"
          + type
          + '\''
          + ", path='"
          + name
          + '\''
          + ", value='"
          + value
          + '\''
          + '}';
    }
  }

  protected JSONObject jsonify(String text) {
    try {
      return new JSONObject(text);
    } catch (JSONException e) {
      throw new IllegalStateException(String.format("Failed to transform %s to JSON format", text));
    }
  }

  protected static boolean isCalciteEnabled() throws IOException {
    return Boolean.parseBoolean(
        getClusterSetting(Settings.Key.CALCITE_ENGINE_ENABLED.getKeyValue(), "persistent"));
  }

  public static void enableCalcite() throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "persistent", Settings.Key.CALCITE_ENGINE_ENABLED.getKeyValue(), "true"));
  }

  public static void disableCalcite() throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "persistent", Settings.Key.CALCITE_ENGINE_ENABLED.getKeyValue(), "false"));
  }

  public static void allowCalciteFallback() throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "persistent", Settings.Key.CALCITE_FALLBACK_ALLOWED.getKeyValue(), "true"));
    LOG.info("{} enabled", Settings.Key.CALCITE_FALLBACK_ALLOWED.name());
  }

  public static void disallowCalciteFallback() throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "persistent", Settings.Key.CALCITE_FALLBACK_ALLOWED.getKeyValue(), "false"));
    LOG.info("{} disabled", Settings.Key.CALCITE_FALLBACK_ALLOWED.name());
  }

  protected static boolean isFallbackEnabled() throws IOException {
    return Boolean.parseBoolean(
        getClusterSetting(Settings.Key.CALCITE_FALLBACK_ALLOWED.getKeyValue(), "persistent"));
  }

  public static void withFallbackEnabled(Runnable f, String msg) throws IOException {
    LOG.info("Need fallback to v2 due to {}", msg);
    boolean isFallbackEnabled = isFallbackEnabled();
    if (isFallbackEnabled) f.run();
    else {
      try {
        updateClusterSettings(
            new SQLIntegTestCase.ClusterSetting(
                "persistent", Settings.Key.CALCITE_FALLBACK_ALLOWED.getKeyValue(), "true"));
        LOG.info(
            "Set {} to enabled and run the test", Settings.Key.CALCITE_FALLBACK_ALLOWED.name());
        f.run();
      } finally {
        updateClusterSettings(
            new SQLIntegTestCase.ClusterSetting(
                "persistent", Settings.Key.CALCITE_FALLBACK_ALLOWED.getKeyValue(), "false"));
        LOG.info("Reset {} back to disabled", Settings.Key.CALCITE_FALLBACK_ALLOWED.name());
      }
    }
  }

  public static void withSettings(Key setting, String value, Runnable f) throws IOException {
    String originalValue = getClusterSetting(setting.getKeyValue(), "transient");
    if (originalValue.equals(value)) f.run();
    else {
      try {
        updateClusterSettings(
            new SQLIntegTestCase.ClusterSetting("transient", setting.getKeyValue(), value));
        LOG.info("Set {} to {} and run the test", setting.name(), value);
        f.run();
      } finally {
        updateClusterSettings(
            new SQLIntegTestCase.ClusterSetting("transient", setting.getKeyValue(), originalValue));
        LOG.info("Reset {} back to {}", setting.name(), originalValue);
      }
    }
  }

  protected boolean isStandaloneTest() {
    return false; // Override this method in subclasses if needed
  }

  /**
   * assertThrows by replacing the expected throwable with {@link ResponseException} if the test is
   * not a standalone test.
   *
   * <p>In remote tests, the expected exception is always {@link ResponseException}, while in
   * standalone tests, the underlying exception can be retrieved.
   *
   * @param expectedThrowable the expected throwable type if the test is standalone
   * @param runnable the runnable that is expected to throw the exception
   * @return the thrown exception
   */
  public Throwable assertThrowsWithReplace(
      Class<? extends Throwable> expectedThrowable, org.junit.function.ThrowingRunnable runnable) {
    Class<? extends Throwable> expectedWithReplace;
    if (isStandaloneTest()) {
      expectedWithReplace = expectedThrowable;
    } else {
      expectedWithReplace = ResponseException.class;
    }
    return assertThrows(expectedWithReplace, runnable);
  }

  public static class GlobalPushdownConfig {
    /** Whether the global pushdown is enabled or not. Enable by default. */
    public static boolean enabled = true;
  }

  public boolean isPushdownEnabled() throws IOException {
    return Boolean.parseBoolean(
        getClusterSetting(Settings.Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(), "transient"));
  }

  public void updatePushdownSettings() throws IOException {
    String pushdownEnabled = String.valueOf(GlobalPushdownConfig.enabled);
    assert !pushdownEnabled.isBlank() : "Pushdown enabled setting cannot be empty";
    if (isPushdownEnabled() != GlobalPushdownConfig.enabled) {
      LOG.info(
          "Updating {} to {}",
          Settings.Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(),
          GlobalPushdownConfig.enabled);
      updateClusterSettings(
          new SQLIntegTestCase.ClusterSetting(
              "transient",
              Settings.Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(),
              String.valueOf(GlobalPushdownConfig.enabled)));
    }
  }

  // Utility methods

  /**
   * Load a file from the resources directory and return its content as a String.
   *
   * @param filename the name of the file to load
   * @return the content of the file as a String
   */
  protected static String loadFromFile(String filename) {
    try {
      URI uri = Resources.getResource(filename).toURI();
      return new String(Files.readAllBytes(Paths.get(uri)));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
