/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.plugin.rest.RestPPLQueryAction.QUERY_API_ENDPOINT;

import java.io.IOException;
import java.util.Locale;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

public class VisualizationFormatIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
  }

  @Test
  void format() throws IOException {
    String result =
        executeVizQuery(
            String.format(Locale.ROOT, "source=%s | fields firstname, age", TEST_INDEX_BANK), true);
    assertEquals(
        "{\n"
            + "  \"data\": {\n"
            + "    \"firstname\": [\n"
            + "      \"Amber JOHnny\",\n"
            + "      \"Hattie\",\n"
            + "      \"Nanette\",\n"
            + "      \"Dale\",\n"
            + "      \"Elinor\",\n"
            + "      \"Virginia\",\n"
            + "      \"Dillard\"\n"
            + "    ],\n"
            + "    \"age\": [\n"
            + "      32,\n"
            + "      36,\n"
            + "      28,\n"
            + "      33,\n"
            + "      36,\n"
            + "      39,\n"
            + "      34\n"
            + "    ]\n"
            + "  },\n"
            + "  \"metadata\": {\n"
            + "    \"fields\": [\n"
            + "      {\n"
            + "        \"name\": \"firstname\",\n"
            + "        \"type\": \"keyword\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"name\": \"age\",\n"
            + "        \"type\": \"integer\"\n"
            + "      }\n"
            + "    ]\n"
            + "  },\n"
            + "  \"size\": 7,\n"
            + "  \"status\": 200\n"
            + "}",
        result);
  }

  private String executeVizQuery(String query, boolean pretty) throws IOException {
    Request request =
        buildRequest(
            query,
            QUERY_API_ENDPOINT + String.format(Locale.ROOT, "?format=csv&pretty=%b", pretty));
    Response response = client().performRequest(request);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    return getResponseBody(response, true);
  }
}
