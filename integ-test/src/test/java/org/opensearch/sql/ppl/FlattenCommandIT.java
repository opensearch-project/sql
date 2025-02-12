/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_EXPAND_FLATTEN;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.Map;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.utils.StringUtils;

public class FlattenCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.EXPAND_FLATTEN);
  }

  @Test
  public void testBasic() throws IOException {
    String query =
        StringUtils.format(
            "source=%s | flatten location | fields city, country, province, coordinates, state",
            TEST_INDEX_EXPAND_FLATTEN);
    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("city", "string"),
        schema("country", "string"),
        schema("province", "string"),
        schema("coordinates", "struct"),
        schema("state", "string"));
    verifyDataRows(
        result,
        rows(
            "Seattle",
            "United States",
            null,
            Map.of("latitude", 47.6061, "longitude", -122.3328),
            "Washington"),
        rows(
            "Vancouver",
            "Canada",
            "British Columbia",
            Map.of("latitude", 49.2827, "longitude", -123.1207),
            null),
        rows(
            "San Antonio",
            "United States",
            null,
            Map.of("latitude", 29.4252, "longitude", -98.4946),
            "Texas"),
        rows("Null City", null, null, null, null),
        rows("Missing City", null, null, null, null));
  }

  @Test
  public void testMultiple() throws IOException {
    String query =
        StringUtils.format(
            "source=%s | flatten location | flatten coordinates | fields city, location, latitude,"
                + " longitude",
            TEST_INDEX_EXPAND_FLATTEN);
    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("city", "string"),
        schema("location", "struct"),
        schema("latitude", "double"),
        schema("longitude", "double"));
    verifyDataRows(
        result,
        rows(
            "Seattle",
            Map.ofEntries(
                Map.entry("state", "Washington"),
                Map.entry("country", "United States"),
                Map.entry("coordinates", Map.of("latitude", 47.6061, "longitude", -122.3328))),
            47.6061,
            -122.3328),
        rows(
            "Vancouver",
            Map.ofEntries(
                Map.entry("country", "Canada"),
                Map.entry("province", "British Columbia"),
                Map.entry("coordinates", Map.of("latitude", 49.2827, "longitude", -123.1207))),
            49.2827,
            -123.1207),
        rows(
            "San Antonio",
            Map.ofEntries(
                Map.entry("state", "Texas"),
                Map.entry("country", "United States"),
                Map.entry("coordinates", Map.of("latitude", 29.4252, "longitude", -98.4946))),
            29.4252,
            -98.4946),
        rows("Null City", null, null, null),
        rows("Missing City", null, null, null));
  }

  @Test
  public void testNested() throws IOException {
    String query =
        StringUtils.format(
            "source=%s | flatten location.coordinates | fields city, location",
            TEST_INDEX_EXPAND_FLATTEN);
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("city", "string"), schema("location", "struct"));
    verifyDataRows(
        result,
        rows(
            "Seattle",
            Map.ofEntries(
                Map.entry("state", "Washington"),
                Map.entry("country", "United States"),
                Map.entry("coordinates", Map.of("latitude", 47.6061, "longitude", -122.3328)),
                Map.entry("latitude", 47.6061),
                Map.entry("longitude", -122.3328))),
        rows(
            "Vancouver",
            Map.ofEntries(
                Map.entry("country", "Canada"),
                Map.entry("province", "British Columbia"),
                Map.entry("coordinates", Map.of("latitude", 49.2827, "longitude", -123.1207)),
                Map.entry("latitude", 49.2827),
                Map.entry("longitude", -123.1207))),
        rows(
            "San Antonio",
            Map.ofEntries(
                Map.entry("state", "Texas"),
                Map.entry("country", "United States"),
                Map.entry("coordinates", Map.of("latitude", 29.4252, "longitude", -98.4946)),
                Map.entry("latitude", 29.4252),
                Map.entry("longitude", -98.4946))),
        rows("Null City", null),
        rows("Missing City", null));
  }

  @Test
  public void testWithFlatten() throws IOException {
    String query =
        StringUtils.format(
            "source=%s | where city = 'San Antonio' | flatten teams | expand title | fields name,"
                + " title",
            TEST_INDEX_EXPAND_FLATTEN);
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("name", "string"), schema("title", "integer"));
    verifyDataRows(
        result,
        rows("San Antonio Spurs", 1999),
        rows("San Antonio Spurs", 2003),
        rows("San Antonio Spurs", 2005),
        rows("San Antonio Spurs", 2007),
        rows("San Antonio Spurs", 2014));
  }
}
