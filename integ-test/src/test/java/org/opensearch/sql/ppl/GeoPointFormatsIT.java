/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_GEOPOINT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.sql.GeopointFormatsIT;

public class GeoPointFormatsIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.GEOPOINTS);
  }

  @Test
  public void testReadingGeopoints() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("search source=%s | head 5 | fields point ", TEST_INDEX_GEOPOINT));
    verifySchema(result, schema("point", null, "geo_point"));
    verifyDataRows(
        result,
        rows(Map.of("lon", 74, "lat", 40.71)),
        rows(Map.of("lon", 74, "lat", 40.71)),
        rows(Map.of("lon", 74, "lat", 40.71)),
        rows(Map.of("lon", 74, "lat", 40.71)),
        rows(Map.of("lon", 74, "lat", 40.71)));
  }

  @Test
  public void testReadingGeoHash() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | where _id = '6' | fields point ", TEST_INDEX_GEOPOINT));
    verifySchema(result, schema("point", null, "geo_point"));
    Pair<Double, Double> point = GeopointFormatsIT.getGeoValue(result);
    assertEquals(40.71, point.getLeft(), GeopointFormatsIT.TOLERANCE);
    assertEquals(74, point.getRight(), GeopointFormatsIT.TOLERANCE);
  }
}
