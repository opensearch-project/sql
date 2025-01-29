/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CITIES;
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
    loadIndex(Index.CITIES);
  }

  @Test
  public void testFlattenStruct() throws IOException {
    String query =
        StringUtils.format(
            "source=%s | flatten location | fields state, province, country, coordinates",
            TEST_INDEX_CITIES);
    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("state", "string"),
        schema("province", "string"),
        schema("country", "string"),
        schema("coordinates", "struct"));
    verifyDataRows(
        result,
        rows(
            "Washington",
            null,
            "United States",
            Map.of("latitude", 47.6061, "longitude", -122.3328)),
        rows(
            null,
            "British Columbia",
            "Canada",
            Map.of("latitude", 49.2827, "longitude", -123.1207)));
  }

  @Test
  public void testFlattenStructMultiple() throws IOException {
    String query =
        StringUtils.format(
            "source=%s | flatten location | flatten coordinates | fields state, province, country,"
                + " latitude, longitude",
            TEST_INDEX_CITIES);
    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("state", "string"),
        schema("province", "string"),
        schema("country", "string"),
        schema("latitude", "float"),
        schema("longitude", "float"));
    verifyDataRows(
        result,
        rows("Washington", null, "United States", 47.6061, -122.3328),
        rows(null, "British Columbia", "Canada", 49.2827, -123.1207));
  }
}
