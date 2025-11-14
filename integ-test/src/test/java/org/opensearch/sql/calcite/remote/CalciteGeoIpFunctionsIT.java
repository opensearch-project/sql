/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WEBLOGS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.Map;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.GeoIpFunctionsIT;

public class CalciteGeoIpFunctionsIT extends GeoIpFunctionsIT {
  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.WEBLOG);
    enableCalcite();

    // Only limited IPs are loaded into geospatial data sources. Therefore, we insert IPs that match
    // those known ones for test purpose
    Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
    bulkRequest.setJsonEntity(
        String.format(
            "{\"index\":{\"_index\":\"%s\",\"_id\":6}}\n"
                + "{\"host\":\"10.0.0.1\",\"method\":\"POST\"}\n"
                + "{\"index\":{\"_index\":\"%s\",\"_id\":7}}\n"
                + "{\"host\":\"fd12:2345:6789:1:a1b2:c3d4:e5f6:789a\",\"method\":\"POST\"}\n",
            TEST_INDEX_WEBLOGS, TEST_INDEX_WEBLOGS));
    client().performRequest(bulkRequest);
  }

  // In v2 it supports only string as IP inputs
  @Test
  public void testGeoIpEnrichmentWithIpFieldAsInput() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where method='POST' | eval ip_to_country = geoip('%s', host,"
                    + " 'country') | fields host, ip_to_country",
                TEST_INDEX_WEBLOGS, DATASOURCE_NAME));
    verifySchema(result, schema("host", "ip"), schema("ip_to_country", "struct"));
    verifyDataRows(
        result,
        rows("10.0.0.1", Map.of("country", "USA")),
        rows("fd12:2345:6789:1:a1b2:c3d4:e5f6:789a", Map.of("country", "India")));
  }

  @Test
  public void testGeoIpInAggregation() throws IOException {
    JSONObject result1 =
        executeQuery(
            String.format(
                "source=%s | where method='POST' | eval info = geoip('%s', host) | eval"
                    + " date=DATE('2020-12-10') | stats count() by info.city, method, span(date,"
                    + " 1month) as month",
                TEST_INDEX_WEBLOGS, DATASOURCE_NAME));
    verifySchema(
        result1,
        schema("count()", "bigint"),
        schema("month", "date"),
        schema("info.city", "string"),
        schema("method", "string"));
    verifyDataRows(
        result1,
        rows(1, "2020-12-01", "Seattle", "POST"),
        rows(1, "2020-12-01", "Bengaluru", "POST"));

    // This case is pushed down into DSL with scripts
    JSONObject result2 =
        executeQuery(
            String.format(
                "source=%s | where method='POST' | eval info = geoip('%s', host) | stats count() by"
                    + " info.city",
                TEST_INDEX_WEBLOGS, DATASOURCE_NAME));
    verifySchema(result2, schema("count()", "bigint"), schema("info.city", "string"));
    verifyDataRows(result2, rows(1, "Seattle"), rows(1, "Bengaluru"));
  }

  @Test
  public void testGeoIpEnrichmentAccessingSubField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where method='POST' | eval info = geoip('%s', host) | fields host,"
                    + " info, info.country",
                TEST_INDEX_WEBLOGS, DATASOURCE_NAME));
    verifySchema(
        result, schema("host", "ip"), schema("info", "struct"), schema("info.country", "string"));
    verifyDataRows(
        result,
        rows("10.0.0.1", Map.of("country", "USA", "city", "Seattle"), "USA"),
        rows(
            "fd12:2345:6789:1:a1b2:c3d4:e5f6:789a",
            Map.of("country", "India", "city", "Bengaluru"),
            "India"));
  }
}
