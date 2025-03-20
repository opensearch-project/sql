/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.bwc;

import static org.opensearch.sql.legacy.TestUtils.createIndexByRestClient;
import static org.opensearch.sql.legacy.TestUtils.isIndexExist;
import static org.opensearch.sql.legacy.TestUtils.loadDataByRestClient;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.sql.LegacyAPICompatibilityIT.LEGACY_QUERY_API_ENDPOINT;
import static org.opensearch.sql.sql.LegacyAPICompatibilityIT.LEGACY_SQL_SETTINGS_API_ENDPOINT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.TestUtils.getResponseBody;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.json.JSONObject;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;
import org.opensearch.test.rest.OpenSearchRestTestCase;

public class SQLBackwardsCompatibilityIT extends SQLIntegTestCase {

  private static final ClusterType CLUSTER_TYPE =
      ClusterType.parse(System.getProperty("tests.rest.bwcsuite"));
  private static final String CLUSTER_NAME = System.getProperty("tests.clustername");

  @Override
  protected final boolean preserveIndicesUponCompletion() {
    return true;
  }

  @Override
  protected final boolean preserveReposUponCompletion() {
    return true;
  }

  @Override
  protected boolean preserveTemplatesUponCompletion() {
    return true;
  }

  @Override
  protected final Settings restClientSettings() {
    return Settings.builder()
        .put(super.restClientSettings())
        // increase the timeout here to 90 seconds to handle long waits for a green
        // cluster health. the waits for green need to be longer than a minute to
        // account for delayed shards
        .put(OpenSearchRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")
        .build();
  }

  private enum ClusterType {
    OLD,
    MIXED,
    UPGRADED;

    public static ClusterType parse(String value) {
      switch (value) {
        case "old_cluster":
          return OLD;
        case "mixed_cluster":
          return MIXED;
        case "upgraded_cluster":
          return UPGRADED;
        default:
          throw new AssertionError("unknown cluster type: " + value);
      }
    }
  }

  @SuppressWarnings("unchecked")
  public void testBackwardsCompatibility() throws Exception {
    String uri = getUri();
    Map<String, Map<String, Object>> responseMap =
        (Map<String, Map<String, Object>>) getAsMap(uri).get("nodes");
    for (Map<String, Object> response : responseMap.values()) {
      List<Map<String, Object>> plugins = (List<Map<String, Object>>) response.get("plugins");
      Set<Object> pluginNames =
          plugins.stream().map(map -> map.get("name")).collect(Collectors.toSet());
      String version = (String) response.get("version");

      boolean isBackwardsIncompatibleVersion = version.startsWith("2.");

      switch (CLUSTER_TYPE) {
        case OLD:
          Assert.assertTrue(pluginNames.contains("opensearch-sql"));
          if (isBackwardsIncompatibleVersion) {
            updateLegacySQLSettings();
          }
          loadIndex(Index.ACCOUNT);
          verifySQLQueries(
              isBackwardsIncompatibleVersion ? LEGACY_QUERY_API_ENDPOINT : QUERY_API_ENDPOINT);
          break;
        case MIXED:
          Assert.assertTrue(pluginNames.contains("opensearch-sql"));
          if (isBackwardsIncompatibleVersion) {
            verifySQLSettings();
          } else {
            // For upgraded nodes, we don't need to verify legacy settings
          }
          verifySQLQueries(
              isBackwardsIncompatibleVersion ? LEGACY_QUERY_API_ENDPOINT : QUERY_API_ENDPOINT);
          break;
        case UPGRADED:
          Assert.assertTrue(pluginNames.contains("opensearch-sql"));
          // For fully upgraded clusters, we don't need to verify legacy settings
          verifySQLQueries(QUERY_API_ENDPOINT);
          break;
      }
      break;
    }
  }

  private String getUri() {
    switch (CLUSTER_TYPE) {
      case OLD:
        return "_nodes/" + CLUSTER_NAME + "-0/plugins";
      case MIXED:
        String round = System.getProperty("tests.rest.bwcsuite_round");
        if (round.equals("second")) {
          return "_nodes/" + CLUSTER_NAME + "-1/plugins";
        } else if (round.equals("third")) {
          return "_nodes/" + CLUSTER_NAME + "-2/plugins";
        } else {
          return "_nodes/" + CLUSTER_NAME + "-0/plugins";
        }
      case UPGRADED:
        return "_nodes/plugins";
      default:
        throw new AssertionError("unknown cluster type: " + CLUSTER_TYPE);
    }
  }

  private void updateLegacySQLSettings() throws IOException {
    Request request = new Request("PUT", LEGACY_SQL_SETTINGS_API_ENDPOINT);
    request.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{\n" + "  \"persistent\" : {\n    \"%s\" : \"%s\"\n  }\n}",
            "opendistro.sql.cursor.keep_alive",
            "7m"));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    JSONObject jsonObject = new JSONObject(getResponseBody(response));
    Assert.assertTrue((boolean) jsonObject.get("acknowledged"));
  }

  private void verifySQLSettings() throws IOException {
    Request request = new Request("GET", "_cluster/settings?flat_settings");

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    JSONObject jsonObject = new JSONObject(getResponseBody(response));
    Assert.assertEquals(
        "{\"transient\":{},\"persistent\":{\"opendistro.sql.cursor.keep_alive\":\"7m\"}}",
        jsonObject.toString());
  }

  private void verifySQLQueries(String endpoint) throws IOException {
    JSONObject filterResponse =
        executeSQLQuery(
            endpoint,
            "SELECT COUNT(*) FILTER(WHERE age > 35) FROM " + TestsConstants.TEST_INDEX_ACCOUNT);
    verifySchema(filterResponse, schema("COUNT(*) FILTER(WHERE age > 35)", null, "integer"));
    verifyDataRows(filterResponse, rows(238));

    JSONObject aggResponse =
        executeSQLQuery(
            endpoint, "SELECT COUNT(DISTINCT age) FROM " + TestsConstants.TEST_INDEX_ACCOUNT);
    verifySchema(aggResponse, schema("COUNT(DISTINCT age)", null, "integer"));
    verifyDataRows(aggResponse, rows(21));

    JSONObject groupByResponse =
        executeSQLQuery(
            endpoint,
            "select a.gender from "
                + TestsConstants.TEST_INDEX_ACCOUNT
                + " a group by a.gender having count(*) > 0");
    verifySchema(groupByResponse, schema("gender", null, "text"));
    Assert.assertEquals("[[\"F\"],[\"M\"]]", groupByResponse.getJSONArray("datarows").toString());
  }

  private JSONObject executeSQLQuery(String endpoint, String query) throws IOException {
    Request request = new Request("POST", endpoint);
    request.setJsonEntity(String.format(Locale.ROOT, "{  \"query\" : \"%s\"}", query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }

  @Override
  public boolean shouldResetQuerySizeLimit() {
    return false;
  }

  @Override
  protected synchronized void loadIndex(Index index) throws IOException {
    String indexName = index.getName();
    String mapping = index.getMapping();
    // current directory becomes 'integ-test/build/testrun/sqlBwcCluster#<task>' during bwc
    String dataSet = "../../../" + index.getDataSet();

    if (!isIndexExist(client(), indexName)) {
      createIndexByRestClient(client(), indexName, mapping);
      loadDataByRestClient(client(), indexName, dataSet);
    }
  }
}
