/*
 *
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.geo;

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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_GEOIP;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

/** Cross Cluster Search tests to be executed with security plugin. */
public class PplIpEnrichmentIT extends PPLIntegTestCase {

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
    loadIndex(Index.GEOIP);
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
    JSONObject result = executeQuery(String.format("search source=%s", TEST_INDEX_GEOIP));
    verifyColumn(result, columnName("name"), columnName("ip"));
    verifyDataRows(result,
            rows("Test user - USA", "10.1.1.1"),
            rows("Test user - Canada", "127.1.1.1"));

    JSONObject resultGeoIp = executeQuery(
            String.format("search source=%s | eval enrichmentResult = geoip(\\\"%s\\\",%s)",
                    TEST_INDEX_GEOIP, "dummycityindex", "ip"));

    verifyColumn(resultGeoIp, columnName("name"), columnName("ip"), columnName("enrichmentResult"));
    verifyDataRows(resultGeoIp,
            rows("Test user - USA", "10.1.1.1", Map.of("country", "USA", "city", "Seattle")),
            rows("Test user - Canada", "127.1.1.1", Map.of("country", "Canada", "city", "Vancouver")));

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
