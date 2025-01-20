/*
 *
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;

import java.io.IOException;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;

/** IP enrichment PPL request with OpenSearch Geo-sptial plugin */
public class GeoIpFunctionsIT extends PPLIntegTestCase {

  private static boolean initialized = false;

  private static String PLUGIN_NAME = "opensearch-geospatial";

  @SneakyThrows
  @BeforeEach
  public void initialize() {
    if (!initialized) {
      setUpIndices();
      initialized = true;
    }
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
}
