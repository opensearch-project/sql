/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_GEOIP;
import static org.opensearch.sql.plugin.rest.RestPPLQueryAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import lombok.SneakyThrows;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/** Cross Cluster Search tests to be executed with security plugin. */
public class CrossClusterSearchIT extends PPLIntegTestCase {

  static {
    // find a remote cluster
    String[] clusterNames = System.getProperty("cluster.names").split(",");
    var remote = "remoteCluster";
    for (var cluster : clusterNames) {
      if (cluster.startsWith("remote")) {
        remote = cluster;
        break;
      }
    }
    REMOTE_CLUSTER = remote;
  }

  public static final String REMOTE_CLUSTER;

  private static final String TEST_INDEX_BANK_REMOTE = REMOTE_CLUSTER + ":" + TEST_INDEX_BANK;
  private static final String TEST_INDEX_DOG_REMOTE = REMOTE_CLUSTER + ":" + TEST_INDEX_DOG;
  private static final String TEST_INDEX_GEOIP_REMOTE = REMOTE_CLUSTER + ":" + TEST_INDEX_GEOIP;
  private static final String TEST_INDEX_DOG_MATCH_ALL_REMOTE =
      MATCH_ALL_REMOTE_CLUSTER + ":" + TEST_INDEX_DOG;
  private static final String TEST_INDEX_ACCOUNT_REMOTE = REMOTE_CLUSTER + ":" + TEST_INDEX_ACCOUNT;

  private static boolean initialized = false;

  @SneakyThrows
  @BeforeEach
  public void initialize() {
    if (!initialized) {
      setUpIndices();
      initialized = true;
    }
  }

  @Override
  protected void init() throws Exception {
    configureMultiClusters(REMOTE_CLUSTER);
    loadIndex(Index.BANK);
    loadIndex(Index.BANK, remoteClient());
    loadIndex(Index.DOG);
    loadIndex(Index.DOG, remoteClient());
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.GEOIP);
    loadIndex(Index.GEOIP, remoteClient());
  }


  @Test
  public void testGeoPluginInstallation() throws IOException {

    Request request = new Request("GET", "/_cat/plugins?v");
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertTrue(getResponseBody(response, true).contains("opensearch-geospatial"));
  }


  @SneakyThrows
  @Test
  public void testGeoIpEnrichment() {
    // Disable the denyList
    updateClusterSetting(Map.of("plugins.geospatial.ip2geo.datasource.endpoint.denylist", Collections.emptyList()));
    Map<String, Object> datasourceProperties = Map.of(
            "endpoint",
            "https://raw.githubusercontent.com/opensearch-project/geospatial/main/src/test/resources/ip2geo/server/city/manifest.json"
    );

    // Create a new dataSource
    createDatasource("dummycityindex", datasourceProperties);
    // Create Wait till setup is completed
    waitForDatasourceToBeAvailable("dummycityindex", Duration.ofSeconds(10));

    // Make sure test-data loaded correctly.
    JSONObject result = executeQuery(String.format("search source=%s", TEST_INDEX_GEOIP_REMOTE));
    verifyColumn(result, columnName("name"), columnName("ip"));
    verifyDataRows(result,
            rows("Test user - USA", "10.1.1.1"),
            rows("Test user - Canada", "127.1.1.1"));

    JSONObject resultGeoIp = executeQuery(
            String.format("search source=%s | eval enrichmentResult = geoip(\\\"%s\\\",%s)",
                    TEST_INDEX_GEOIP_REMOTE, "dummycityindex", "ip"));

    verifyColumn(resultGeoIp, columnName("name"), columnName("ip"), columnName("enrichmentResult"));
    verifyDataRows(resultGeoIp,
            rows("Test user - USA", "10.1.1.1", Map.of("country", "USA", "city", "Seattle")),
            rows("Test user - Canada", "127.1.1.1", Map.of("country", "Canada", "city", "Vancouver")));

//    Expected: iterable with items [[Test user - USA, 10.1.1.1, x], [Test user - Canada, 127.1.1.1, x]] in any order
//         but: not matched: <["Test user - USA","10.1.1.1",{"country":"USA","city":"Seattle"}]>
  }


  @Test
  public void testCrossClusterSearchAllFields() throws IOException {
    JSONObject result = executeQuery(String.format("search source=%s", TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("holdersName"), columnName("age"));
  }

  @Test
  public void testMatchAllCrossClusterSearchAllFields() throws IOException {
    JSONObject result =
        executeQuery(String.format("search source=%s", TEST_INDEX_DOG_MATCH_ALL_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("holdersName"), columnName("age"));
  }

  @Test
  public void testCrossClusterSearchWithoutLocalFieldMappingShouldFail() throws IOException {
    var exception =
        assertThrows(
            ResponseException.class,
            () -> executeQuery(String.format("search source=%s", TEST_INDEX_ACCOUNT_REMOTE)));
    assertTrue(
        exception.getMessage().contains("IndexNotFoundException")
            && exception.getMessage().contains("404 Not Found"));
  }

  @Test
  public void testCrossClusterSearchCommandWithLogicalExpression() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s firstname='Hattie' | fields firstname", TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows("Hattie"));
  }

  @Test
  public void testCrossClusterSearchMultiClusters() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s,%s firstname='Hattie' | fields firstname",
                TEST_INDEX_BANK_REMOTE, TEST_INDEX_BANK));
    verifyDataRows(result, rows("Hattie"), rows("Hattie"));
  }

  @Test
  public void testCrossClusterDescribeAllFields() throws IOException {
    JSONObject result = executeQuery(String.format("describe %s", TEST_INDEX_DOG_REMOTE));
    verifyColumn(
        result,
        columnName("TABLE_CAT"),
        columnName("TABLE_SCHEM"),
        columnName("TABLE_NAME"),
        columnName("COLUMN_NAME"),
        columnName("DATA_TYPE"),
        columnName("TYPE_NAME"),
        columnName("COLUMN_SIZE"),
        columnName("BUFFER_LENGTH"),
        columnName("DECIMAL_DIGITS"),
        columnName("NUM_PREC_RADIX"),
        columnName("NULLABLE"),
        columnName("REMARKS"),
        columnName("COLUMN_DEF"),
        columnName("SQL_DATA_TYPE"),
        columnName("SQL_DATETIME_SUB"),
        columnName("CHAR_OCTET_LENGTH"),
        columnName("ORDINAL_POSITION"),
        columnName("IS_NULLABLE"),
        columnName("SCOPE_CATALOG"),
        columnName("SCOPE_SCHEMA"),
        columnName("SCOPE_TABLE"),
        columnName("SOURCE_DATA_TYPE"),
        columnName("IS_AUTOINCREMENT"),
        columnName("IS_GENERATEDCOLUMN"));
  }

  @Test
  public void testMatchAllCrossClusterDescribeAllFields() throws IOException {
    JSONObject result = executeQuery(String.format("describe %s", TEST_INDEX_DOG_MATCH_ALL_REMOTE));
    verifyColumn(
        result,
        columnName("TABLE_CAT"),
        columnName("TABLE_SCHEM"),
        columnName("TABLE_NAME"),
        columnName("COLUMN_NAME"),
        columnName("DATA_TYPE"),
        columnName("TYPE_NAME"),
        columnName("COLUMN_SIZE"),
        columnName("BUFFER_LENGTH"),
        columnName("DECIMAL_DIGITS"),
        columnName("NUM_PREC_RADIX"),
        columnName("NULLABLE"),
        columnName("REMARKS"),
        columnName("COLUMN_DEF"),
        columnName("SQL_DATA_TYPE"),
        columnName("SQL_DATETIME_SUB"),
        columnName("CHAR_OCTET_LENGTH"),
        columnName("ORDINAL_POSITION"),
        columnName("IS_NULLABLE"),
        columnName("SCOPE_CATALOG"),
        columnName("SCOPE_SCHEMA"),
        columnName("SCOPE_TABLE"),
        columnName("SOURCE_DATA_TYPE"),
        columnName("IS_AUTOINCREMENT"),
        columnName("IS_GENERATEDCOLUMN"));
  }


  protected Response updateClusterSetting(final Map<String, Object> properties) throws IOException {
    XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
    builder.startObject("transient");
    for (Map.Entry<String, Object> config : properties.entrySet()) {
      builder.field(config.getKey(), config.getValue());
    }
    builder.endObject();
    builder.endObject();

    Request request = new Request("PUT", "/_cluster/settings");
    request.setJsonEntity(builder.toString());
    return client().performRequest(request);
  }

  protected Response createDatasource(final String name, Map<String, Object> properties) throws IOException {
    XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
    for (Map.Entry<String, Object> config : properties.entrySet()) {
      builder.field(config.getKey(), config.getValue());
    }
    builder.endObject();

    Request request = new Request("PUT", "/_plugins/geospatial/ip2geo/datasource/" + name);
    request.setJsonEntity(builder.toString());
    return client().performRequest(request);
  }

  protected void waitForDatasourceToBeAvailable(final String name, final Duration timeout) throws Exception {
    Instant start = Instant.now();
    while ("AVAILABLE".equals(getDatasourceState(name)) == false) {
      if (Duration.between(start, Instant.now()).compareTo(timeout) > 0) {
        throw new RuntimeException(
                String.format(
                        Locale.ROOT,
                        "Datasource state didn't change to %s after %d seconds",
                        "AVAILABLE",
                        timeout.toSeconds()
                )
        );
      }
      Thread.sleep(1000);
    }
  }

  private String getDatasourceState(final String name) throws Exception {
    List<Map<String, Object>> datasources = (List<Map<String, Object>>) getDatasource(name).get("datasources");
    return (String) datasources.get(0).get("state");
  }

  protected Map<String, Object> getDatasource(final String name) throws Exception {
    Request request = new Request("GET", "_plugins/geospatial/ip2geo/datasource/" + name);
    Response response = client().performRequest(request);
    return createParser(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity())).map();
  }



}
