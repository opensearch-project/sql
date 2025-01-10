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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.sql.ppl.PPLIntegTestCase;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_GEOIP;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

/**
 * IP enrichment PPL request with OpenSearch Geo-sptial plugin
 */
public class PplIpEnrichmentIT extends PPLIntegTestCase {

  private static boolean initialized = false;

  private static Map<String, Object> MANIFEST_LOCATION = Map.of(
          "endpoint",
          "https://raw.githubusercontent.com/opensearch-project/geospatial/main/src/test/resources/ip2geo/server/city/manifest.json"
  );

  private static String DATASOURCE_NAME = "dummycityindex";

  private static String PLUGIN_NAME = "opensearch-geospatial";

  private static String GEO_SPATIAL_DATASOURCE_PATH = "/_plugins/geospatial/ip2geo/datasource/";

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
    // Create a new dataSource
    createDatasource(DATASOURCE_NAME, MANIFEST_LOCATION);
    waitForDatasourceToBeAvailable(DATASOURCE_NAME, Duration.ofSeconds(10));
  }


  @Test
  public void testGeoPluginInstallation() throws IOException {

    Request request = new Request("GET", "/_cat/plugins?v");
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertTrue(getResponseBody(response, true).contains(PLUGIN_NAME));
  }

  @SneakyThrows
  @Test
  public void testGeoIpEnrichment() {
    JSONObject resultGeoIp = executeQuery(
            String.format("search source=%s | eval enrichmentResult = geoip(\\\"%s\\\",%s)",
                    TEST_INDEX_GEOIP, "dummycityindex", "ip"));

    verifyColumn(resultGeoIp, columnName("name"), columnName("ip"), columnName("enrichmentResult"));
    verifyDataRows(resultGeoIp,
            rows("Test user - USA", "10.1.1.1", Map.of("country", "USA", "city", "Seattle")),
            rows("Test user - Canada", "127.1.1.1", Map.of("country", "Canada", "city", "Vancouver")));

  }

  /**
   * Helper method to send a PUT request to create a dummy dataSource with provided endpoint for integration test.
   * @param name Name of the dataSource
   * @param properties Request payload in Json format
   * @return Response for the create dataSource request.
   * @throws IOException In case of network failure
   */
  private Response createDatasource(final String name, Map<String, Object> properties) throws IOException {
    XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
    for (Map.Entry<String, Object> config : properties.entrySet()) {
      builder.field(config.getKey(), config.getValue());
    }
    builder.endObject();
    Request request = new Request("PUT", GEO_SPATIAL_DATASOURCE_PATH + name);
    request.setJsonEntity(builder.toString());
    return client().performRequest(request);
  }

  /**
   * Helper method check the status of dataSource creation within the specific timeframe.
   * @param name The name of the dataSource to assert
   * @param timeout The timeout value in seconds
   * @throws Exception Exception
   */
  private void waitForDatasourceToBeAvailable(final String name, final Duration timeout) throws Exception {
    Instant start = Instant.now();
    while (!"AVAILABLE".equals(getDatasourceState(name))) {
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

  /**
   * Helper method to fetch the DataSource creation status via REST client.
   * @param name dataSource name
   * @return Status in String
   * @throws Exception IO.
   */
  private String getDatasourceState(final String name) throws Exception {
    Request request = new Request("GET", GEO_SPATIAL_DATASOURCE_PATH + name);
    Response response = client().performRequest(request);
    var responseInMap = createParser(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity())).map();
    var datasources = (List<Map<String, Object>>) responseInMap.get("datasources");
    return (String) datasources.get(0).get("state");
  }
}
