/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_FLATTEN;
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
    loadIndex(Index.FLATTEN);
  }

  @Test
  public void testBasic() throws IOException {
    String query =
        StringUtils.format(
            "source=%s | flatten location | fields name, country, province, coordinates, state",
                TEST_INDEX_FLATTEN);
    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("name", "string"),
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
            Map.ofEntries(Map.entry("latitude", 47.6061), Map.entry("longitude", -122.3328)),
            "Washington"),
        rows(
            "Vancouver",
            "Canada",
            "British Columbia",
            Map.ofEntries(Map.entry("latitude", 49.2827), Map.entry("longitude", -123.1207)),
            null),
        rows("Null Location", null, null, null, null),
        rows("Null Coordinates", "Australia", null, null, "Victoria"));
  }

  @Test
  public void testMultiple() throws IOException {
    String query =
        StringUtils.format(
            "source=%s | flatten location | flatten coordinates | fields name, country, province,"
                + " state, latitude, longitude",
                TEST_INDEX_FLATTEN);
    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("name", "string"),
        schema("country", "string"),
        schema("province", "string"),
        schema("state", "string"),
        schema("latitude", "float"),
        schema("longitude", "float"));
    verifyDataRows(
        result,
        rows("Seattle", "United States", null, "Washington", 47.6061, -122.3328),
        rows("Vancouver", "Canada", "British Columbia", null, 49.2827, -123.1207),
        rows("Null Location", null, null, null, null, null),
        rows("Null Coordinates", "Australia", null, "Victoria", null, null));
  }

  @Test
  public void testNested() throws IOException {
    String query =
        StringUtils.format(
            "source=%s | flatten location.coordinates | fields name, location", TEST_INDEX_FLATTEN);
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("name", "string"), schema("location", "struct"));
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
        rows("Null Location", null),
        rows("Null Coordinates", Map.of("state", "Victoria", "country", "Australia")));
  }
}
