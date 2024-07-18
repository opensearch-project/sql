/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class GeopointFormatsIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    loadIndex(Index.GEOPOINTS);
  }

  @Test
  public void testReadingGeopoints() throws IOException {
    String query = String.format("SELECT point FROM %s LIMIT 5", Index.GEOPOINTS.getName());
    JSONObject result = executeJdbcRequest(query);
    verifySchema(result, schema("point", null, "geo_point"));
    verifyDataRows(
        result,
        rows(Map.of("lon", 74, "lat", 40.71)),
        rows(Map.of("lon", 74, "lat", 40.71)),
        rows(Map.of("lon", 74, "lat", 40.71)),
        rows(Map.of("lon", 74, "lat", 40.71)),
        rows(Map.of("lon", 74, "lat", 40.71)));
  }

  private static final double TOLERANCE = 1E-5;

  public void testReadingGeoHash() throws IOException {
    String query = String.format("SELECT point FROM %s WHERE _id='6'", Index.GEOPOINTS.getName());
    JSONObject result = executeJdbcRequest(query);
    verifySchema(result, schema("point", null, "geo_point"));
    Pair<Double, Double> point = getGeoValue(result);
    assertEquals(40.71, point.getLeft(), TOLERANCE);
    assertEquals(74, point.getRight(), TOLERANCE);
  }

  private Pair<Double, Double> getGeoValue(JSONObject result) {
    JSONObject geoRaw =
        (JSONObject) ((JSONArray) ((JSONArray) result.get("datarows")).get(0)).get(0);
    double lat = geoRaw.getDouble("lat");
    double lon = geoRaw.getDouble("lon");
    return Pair.of(lat, lon);
  }
}
