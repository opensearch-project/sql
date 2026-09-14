/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.util.Capability.GEOPOINT_TYPE;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.util.RequiresCapability;

public class GeopointFormatsIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    loadIndex(Index.GEOPOINTS);
  }

  @Test
  @RequiresCapability(GEOPOINT_TYPE)
  public void testReadingGeopoints() throws IOException {
    String query = String.format("SELECT point FROM %s LIMIT 5", Index.GEOPOINTS.getName());
    JSONObject result = executeJdbcRequest(query);
    verifySchema(result, schema("point", null, "geo_point"));
    // geo_point values are stored with Lucene's lossy 32-bit lat/lon encoding, so the decoded
    // coordinates differ from the source (40.71, 74) by a sub-meter epsilon. Assert each returned
    // point within tolerance instead of exact equality. The five source documents all describe the
    // same point in different formats, so any five rows selected by LIMIT satisfy this regardless
    // of shard scan order.
    JSONArray dataRows = result.getJSONArray("datarows");
    assertEquals(5, dataRows.length());
    for (int i = 0; i < dataRows.length(); i++) {
      JSONObject point = ((JSONArray) dataRows.get(i)).getJSONObject(0);
      assertEquals(40.71, point.getDouble("lat"), TOLERANCE);
      assertEquals(74, point.getDouble("lon"), TOLERANCE);
    }
  }

  public static final double TOLERANCE = 1E-5;

  @RequiresCapability(GEOPOINT_TYPE)
  public void testReadingGeoHash() throws IOException {
    String query = String.format("SELECT point FROM %s WHERE _id='6'", Index.GEOPOINTS.getName());
    JSONObject result = executeJdbcRequest(query);
    verifySchema(result, schema("point", null, "geo_point"));
    Pair<Double, Double> point = getGeoValue(result);
    assertEquals(40.71, point.getLeft(), TOLERANCE);
    assertEquals(74, point.getRight(), TOLERANCE);
  }

  public static Pair<Double, Double> getGeoValue(JSONObject result) {
    JSONObject geoRaw =
        (JSONObject) ((JSONArray) ((JSONArray) result.get("datarows")).get(0)).get(0);
    double lat = geoRaw.getDouble("lat");
    double lon = geoRaw.getDouble("lon");
    return Pair.of(lat, lon);
  }
}
